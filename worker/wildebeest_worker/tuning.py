"""CPU inference tuning shared by both model stages.

Measured in the linux/arm64 worker image on an Apple M2 (2 torch threads, 12 Serengeti images,
median per image; docs/DECISIONS.md has the full table):

    MegaDetector v5a @ 640   fp32 0.81 s -> channels_last 0.66 s  (identical detections)
    SpeciesNet classifier    fp32 0.37 s -> channels_last 0.31 s  (identical labels)

Tried and rejected: Conv+BN fusion (no change), oneDNN bf16 fast-math (slower, 1.1-1.3 s),
ONNX Runtime (slower, 0.97 s), 4 threads per worker (0.61 s, barely better than 2).
"""

import torch


def channels_last(model: torch.nn.Module) -> torch.nn.Module:
    """Store conv weights as NHWC, the layout the ARM (oneDNN/ACL) conv kernels run natively.

    Only 4-D parameters are converted; YOLOv5's Detect head holds other tensors that can't be.
    PyTorch propagates the layout to activations, so inputs need no change.
    """
    for p in model.parameters():
        if p.dim() == 4:
            p.data = p.data.contiguous(memory_format=torch.channels_last)
    return model
