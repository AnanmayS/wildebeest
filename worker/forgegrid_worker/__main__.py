"""Entrypoint: `python -m forgegrid_worker`, dispatching on WORKER_STAGE (detect | classify)."""

import logging
import os
import sys
import time
import warnings

from .runtime import Worker, rss_mb

# Pinned model; the version the coordinator stores comes from the env vars below.
SPECIESNET_MODEL = os.environ.get("SPECIESNET_MODEL", "kaggle:google/speciesnet/pyTorch/v4.0.3a/1")


def build_handler(stage: str, backend: str, storage):
    """Load the model once and return handle(lease) -> result."""
    fake_delay = int(os.environ.get("FAKE_MODEL_DELAY_MS", "300"))

    if stage == "detect":
        from .detector import make_detect_handler

        version = os.environ.get("DETECTOR_MODEL_VERSION", "speciesnet-md_v5a.0.1")
        if backend == "fake":
            from .fake import FakeDetector

            model = FakeDetector(fake_delay)
        else:
            from .detector import SpeciesNetDetectorModel

            model = SpeciesNetDetectorModel(SPECIESNET_MODEL)
        return make_detect_handler(model, storage, version)

    if stage == "classify":
        from .classifier import make_classify_handler

        version = os.environ.get("CLASSIFIER_MODEL_VERSION", "speciesnet-v4.0.3a")
        if backend == "fake":
            from .fake import FakeClassifier

            model = FakeClassifier(fake_delay)
        else:
            from .classifier import SpeciesNetClassifierModel

            model = SpeciesNetClassifierModel(SPECIESNET_MODEL, geofence=True)
        return make_classify_handler(model, storage, version)

    raise SystemExit(f"WORKER_STAGE must be 'detect' or 'classify', got {stage!r}")


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    warnings.filterwarnings("ignore")
    log = logging.getLogger("forgegrid.worker")

    import redis

    from .storage import Storage

    stage = os.environ.get("WORKER_STAGE", "")
    backend = os.environ.get("MODEL_BACKEND", "speciesnet")
    if backend != "fake":
        import torch

        # Several workers share the host's cores; a full-width thread pool per worker
        # just oversubscribes them. See docs/DECISIONS.md.
        torch.set_num_threads(int(os.environ.get("TORCH_NUM_THREADS", "2")))

    started = time.perf_counter()
    handler = build_handler(stage, backend, Storage())
    log.info("%s worker ready: backend=%s, model load %.1fs, rss %.0f MB",
             stage, backend, time.perf_counter() - started, rss_mb())

    worker = Worker(
        stage=stage,
        handler=handler,
        redis_client=redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379")),
        coordinator_url=os.environ.get("COORDINATOR_URL", "http://coordinator:3000"),
    )
    worker.run()


if __name__ == "__main__":
    main()
