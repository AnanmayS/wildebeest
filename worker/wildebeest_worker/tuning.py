"""Device selection and inference tuning shared by both model stages.

DEVICE=auto|cpu|mps|cuda picks where the models run (default cpu, which is what the Linux
containers always get). `auto` prefers cuda, then mps, then cpu. An explicit device that is not
available is an error rather than a silent fallback, so a "GPU" worker can never quietly run on CPU.

CPU tuning, measured in the linux/arm64 worker image on an Apple M2 (2 torch threads, 12 Serengeti
images, median per image; docs/DECISIONS.md has the full table):

    MegaDetector v5a @ 640   fp32 0.81 s -> channels_last 0.66 s  (identical detections)
    SpeciesNet classifier    fp32 0.37 s -> channels_last 0.31 s  (identical labels)

Tried and rejected: Conv+BN fusion (no change), oneDNN bf16 fast-math (slower, 1.1-1.3 s),
ONNX Runtime (slower, 0.97 s), 4 threads per worker (0.61 s, barely better than 2).

On MPS (native macOS, docs/decisions/d-native.md) channels_last is measured per model and only
kept where it helps; CHANNELS_LAST_BACKENDS records the result.
"""

import logging
import os
import sys

import torch

log = logging.getLogger("wildebeest.worker")

DEVICES = ("auto", "cpu", "mps", "cuda")

# Backends where NHWC weights were measured to be faster, per stage (docs/decisions/d-native.md).
# "cpu" is Linux (the containers, oneDNN/ACL kernels); "cpu-darwin" is native macOS CPU, where the
# SpeciesNet classifier gets ~9x *slower* with channels_last (2.08 s vs 0.23 s) while MegaDetector
# still gains (0.30 s vs 0.42 s). On MPS both models are slower with it (detector 0.145 s vs
# 0.121 s, classifier 0.32 s vs 0.23 s). CUDA is unmeasured, so it stays off there.
CHANNELS_LAST_BACKENDS = {
    "detect": {"cpu", "cpu-darwin"},
    "classify": {"cpu"},
}


def _mps_available() -> bool:
    return bool(getattr(torch.backends, "mps", None)) and torch.backends.mps.is_available()


def resolve_device(requested: str | None = None) -> str:
    """Turn DEVICE (auto|cpu|mps|cuda) into a concrete, available torch device name."""
    requested = (requested if requested is not None else os.environ.get("DEVICE", "cpu")).strip().lower()
    if requested not in DEVICES:
        raise ValueError(f"DEVICE must be one of {', '.join(DEVICES)}; got {requested!r}")
    if requested == "auto":
        if torch.cuda.is_available():
            return "cuda"
        if _mps_available():
            return "mps"
        return "cpu"
    if requested == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("DEVICE=cuda but torch.cuda.is_available() is False")
    if requested == "mps" and not _mps_available():
        raise RuntimeError("DEVICE=mps but torch.backends.mps.is_available() is False "
                           "(needs macOS on Apple silicon and a torch build with MPS; containers have none)")
    return requested


def announce_device(device: str) -> None:
    """Publish the resolved device for the register call (runtime.py reads WORKER_DEVICE).

    Only fills it in when unset, so an explicit WORKER_DEVICE from the launcher wins.
    """
    os.environ.setdefault("WORKER_DEVICE", device)
    if os.environ["WORKER_DEVICE"] != device:
        log.warning("WORKER_DEVICE=%s but the model runs on %s", os.environ["WORKER_DEVICE"], device)


def use_channels_last(stage: str, device: str) -> bool:
    """CHANNELS_LAST=1/0 forces it (for measurements); otherwise the measured table decides."""
    forced = os.environ.get("CHANNELS_LAST", "").strip()
    if forced in ("0", "1"):
        return forced == "1"
    backend = "cpu-darwin" if device == "cpu" and sys.platform == "darwin" else device
    return backend in CHANNELS_LAST_BACKENDS.get(stage, set())


def channels_last(model: torch.nn.Module) -> torch.nn.Module:
    """Store conv weights as NHWC, the layout the ARM (oneDNN/ACL) conv kernels run natively.

    Only 4-D parameters are converted; YOLOv5's Detect head holds other tensors that can't be.
    PyTorch propagates the layout to activations, so inputs need no change.
    """
    for p in model.parameters():
        if p.dim() == 4:
            p.data = p.data.contiguous(memory_format=torch.channels_last)
    return model


def tune(model: torch.nn.Module, stage: str, device: str) -> torch.nn.Module:
    """eval() plus whatever layout was measured to help this stage on this device."""
    model = model.eval()
    if use_channels_last(stage, device):
        model = channels_last(model)
    return model


def synchronize(device: str) -> None:
    """Wait for queued GPU work, so wall-clock timings are honest (no-op on CPU)."""
    if device == "mps":
        torch.mps.synchronize()
    elif device == "cuda":
        torch.cuda.synchronize()
