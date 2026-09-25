"""Many fake workers in one process, for the orchestration-ceiling benchmark (bench/).

`python -m wildebeest_worker.swarm` runs SWARM_WORKERS independent worker loops as threads.
Each loop is an ordinary `runtime.Worker`: it registers under its own ID
(`{stage}-{hostname}-{i}`), heartbeats, claims through Redis + claim-confirm, and completes
over HTTP, exactly like a real worker. Only the handler is different: it sleeps for
SWARM_TASK_MS and returns a canned result, so the coordinator, Redis and Postgres are the only
things being measured.

Why threads in one process: a worker container costs ~80 MB, so 64 of them would not fit next to
everything else on an 8 GB Docker VM. A worker loop is almost entirely network wait, so a few
loops per process share the GIL without becoming the bottleneck. The harness still spreads
large N over several swarm containers (8 loops each by default) to keep client CPU out of the
measurement.

Environment (all optional):
  WORKER_STAGE        detect | classify                      (default detect)
  SWARM_WORKERS       number of worker loops                  (default 8)
  SWARM_TASK_MS       fake task time in ms                    (default 0)
  SWARM_DETECTIONS    empty | fake. `empty` finalises every image after stage 1, so each
                      task is one image and no classify tasks are created (default empty)
  SWARM_FETCH         none | bytes. `bytes` GETs the object from MinIO (no decode) unless
                      the key is `synthetic/...` (default none)
  SWARM_NAME          prefix for the worker hostnames         (default: container hostname)
  COORDINATOR_URL, REDIS_URL, S3_*, DETECTOR_MODEL_VERSION, CLASSIFIER_MODEL_VERSION as usual.

SIGTERM/SIGINT stops every loop: each finishes its in-flight task, deregisters and exits.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import signal
import socket
import sys
import threading
import time

from .fake import fake_classification, fake_detections
from .runtime import Worker

log = logging.getLogger("wildebeest.swarm")


class HandlerStats:
    """Actual time spent in the fake handler. time.sleep(5 ms) really takes ~6.4 ms in a Docker
    Desktop container, so the harness subtracts this, not the nominal task time, to get overhead."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.count = 0
        self.total_ms = 0.0

    def add(self, ms: float) -> None:
        with self.lock:
            self.count += 1
            self.total_ms += ms

    def line(self) -> str:
        with self.lock:
            mean = self.total_ms / self.count if self.count else None
            return "SWARM_STATS " + json.dumps({"tasks": self.count, "handlerMeanMs": mean})


STATS = HandlerStats()


def make_handler(stage: str, task_ms: float, detections: str, fetch: str):
    """handle(lease) -> result, like detector/classifier handlers, minus the model."""
    task_s = max(0.0, task_ms) / 1000
    det_version = os.environ.get("DETECTOR_MODEL_VERSION", "fake-detector-v1")
    cls_version = os.environ.get("CLASSIFIER_MODEL_VERSION", "fake-classifier-v1")
    storage = None
    if fetch == "bytes":
        from .storage import Storage

        storage = Storage()

    def handle(lease: dict) -> dict:
        started = time.perf_counter()
        key = lease.get("imageKey") or ""
        if storage is not None and key and not key.startswith("synthetic/"):
            storage.s3.get_object(Bucket=storage.bucket, Key=key)["Body"].read()
        if task_s:
            time.sleep(task_s)
        sha = lease.get("sha256") or ""
        STATS.add((time.perf_counter() - started) * 1000)
        if stage == "detect":
            dets = [] if detections == "empty" else fake_detections(sha)
            return {"modelVersion": det_version, "detections": dets}
        result = fake_classification(sha)
        result["modelVersion"] = cls_version
        result["cropKey"] = None
        return result

    return handle


def _quiet_signal_in_threads() -> None:
    """Worker.run() installs SIGTERM/SIGINT handlers, which Python only allows on the main
    thread. The swarm owns signals itself, so calls from worker threads become no-ops."""
    original = signal.signal

    def patched(signum, handler):
        if threading.current_thread() is threading.main_thread():
            return original(signum, handler)
        return signal.getsignal(signum)

    signal.signal = patched  # type: ignore[assignment]


def build_worker(stage: str, handler, redis_client, coordinator_url: str, hostname: str) -> Worker:
    kwargs = dict(stage=stage, handler=handler, redis_client=redis_client, coordinator_url=coordinator_url)
    params = inspect.signature(Worker.__init__).parameters
    if "hostname" in params:
        kwargs["hostname"] = hostname
    w = Worker(**kwargs)
    if "hostname" not in params:  # older/newer runtime without the kwarg: set it before register()
        w.hostname = hostname
    return w


def stop_worker(w: Worker) -> None:
    stop = getattr(w, "stop", None)
    if callable(stop):
        stop()
    else:
        w.stopping.set()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "WARNING"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger("wildebeest.swarm").setLevel(logging.INFO)
    import redis

    stage = os.environ.get("WORKER_STAGE", "detect")
    n = int(os.environ.get("SWARM_WORKERS", "8"))
    task_ms = float(os.environ.get("SWARM_TASK_MS", "0"))
    detections = os.environ.get("SWARM_DETECTIONS", "empty")
    fetch = os.environ.get("SWARM_FETCH", "none")
    name = os.environ.get("SWARM_NAME") or socket.gethostname()
    coordinator_url = os.environ.get("COORDINATOR_URL", "http://coordinator:3000")

    _quiet_signal_in_threads()
    handler = make_handler(stage, task_ms, detections, fetch)
    # One connection pool shared by all loops (redis-py pools are thread-safe; each blocking
    # BLMOVE checks out its own connection).
    redis_client = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379"))
    workers = [build_worker(stage, handler, redis_client, coordinator_url, f"{name}-{i:02d}") for i in range(n)]

    def on_signal(signum, _frame):
        log.info("received %s: stopping %d worker loops", signal.Signals(signum).name, n)
        for w in workers:
            stop_worker(w)

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    threads = [threading.Thread(target=w.run, name=f"worker-{i}", daemon=True) for i, w in enumerate(workers)]
    for t in threads:
        t.start()
    log.info("swarm up: %d %s loops, task %.1f ms, detections=%s, fetch=%s", n, stage, task_ms, detections, fetch)
    while any(t.is_alive() for t in threads):
        for t in threads:
            t.join(timeout=0.5)
    print(STATS.line(), flush=True)
    log.info("swarm exited")


if __name__ == "__main__":
    main()
