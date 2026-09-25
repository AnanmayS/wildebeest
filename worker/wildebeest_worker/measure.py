"""Measure one worker type's memory and latency without a coordinator.

    python -m wildebeest_worker.measure --stage detect --images /data/sample --n 10

Loads the model the same way the worker does, runs it on N images and prints
model load time, RSS after load, peak RSS and per-image latency.
"""

import argparse
import json
import os
import resource
import statistics
import time
import warnings
from pathlib import Path

from .labels import final_category
from .runtime import rss_mb
from .storage import open_rgb


def peak_rss_mb() -> float:
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)  # Linux: KB


def main() -> None:
    warnings.filterwarnings("ignore")
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=["detect", "classify"], required=True)
    parser.add_argument("--images", default="/data/sample")
    parser.add_argument("--n", type=int, default=10)
    parser.add_argument("--detections", default="/tmp/detections.json",
                        help="detect writes it, classify reads it (so the classifier process "
                             "never loads MegaDetector and its RSS is honest)")
    args = parser.parse_args()

    import torch

    torch.set_num_threads(int(os.environ.get("TORCH_NUM_THREADS", "2")))
    from .__main__ import SPECIESNET_MODEL

    rss_before = rss_mb()
    t0 = time.perf_counter()
    if args.stage == "detect":
        from .detector import SpeciesNetDetectorModel

        model = SpeciesNetDetectorModel(SPECIESNET_MODEL)
        files = sorted(Path(args.images).glob("*.jpg"))[: args.n]
    else:
        from .classifier import SpeciesNetClassifierModel

        model = SpeciesNetClassifierModel(SPECIESNET_MODEL, geofence=True)
        saved = json.loads(Path(args.detections).read_text())
        files = [Path(args.images) / name for name, dets in saved.items()
                 if final_category(dets, 0.2) == "animal"][: args.n]
    load_s = time.perf_counter() - t0
    rss_after_load = rss_mb()

    latencies, detections = [], {}
    for f in files:
        img = open_rgb(f.read_bytes())
        t = time.perf_counter()
        if args.stage == "detect":
            detections[f.name] = model.detect(img)
        else:
            model.classify(img, saved[f.name], "TZA")
        latencies.append((time.perf_counter() - t) * 1000)
    if args.stage == "detect":
        Path(args.detections).write_text(json.dumps(detections))

    print(f"stage={args.stage} torch_threads={torch.get_num_threads()} images={len(latencies)}")
    print(f"rss_before_load_mb={rss_before} load_s={load_s:.1f} rss_after_load_mb={rss_after_load}")
    print(f"rss_after_inference_mb={rss_mb()} peak_rss_mb={peak_rss_mb()}")
    print(f"latency_ms first={latencies[0]:.0f} median={statistics.median(latencies[1:] or latencies):.0f} "
          f"max={max(latencies):.0f}")


if __name__ == "__main__":
    main()
