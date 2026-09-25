"""Claim/complete flow against fakeredis and a scripted fake coordinator."""

import fakeredis
import PIL.Image
import pytest
import requests

from wildebeest_worker.classifier import crop_key_for, make_classify_handler
from wildebeest_worker.detector import make_detect_handler
from wildebeest_worker.fake import FakeClassifier, FakeDetector, fake_detections
from wildebeest_worker.runtime import Worker


class Resp:
    def __init__(self, status: int, body: dict | None = None) -> None:
        self.status_code = status
        self.ok = status < 400
        self._body = body or {}

    def json(self) -> dict:
        return self._body

    def raise_for_status(self) -> None:
        if not self.ok:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeCoordinator:
    """Stands in for requests.Session: records every POST and answers like the coordinator."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.epochs: dict[str, int] = {}
        self.complete_status = 200
        self.heartbeat_status = 200
        self.register_failures = 0
        self.registrations = 0

    def paths(self, suffix: str) -> list[dict]:
        return [body for path, body in self.calls if path.endswith(suffix)]

    def post(self, url: str, json: dict, timeout: float) -> Resp:
        path = url.removeprefix("http://coord")
        self.calls.append((path, json))
        if path == "/workers/register":
            if self.register_failures:
                self.register_failures -= 1
                raise requests.ConnectionError("coordinator not up yet")
            self.registrations += 1
            return Resp(200, {"workerId": f"detect-w{self.registrations}",
                              "config": {"heartbeatMs": 2000, "claimBatchSize": 1}})
        if path == "/tasks/claim-confirm":
            leases = []
            for task_id in json["taskIds"]:
                self.epochs[task_id] = self.epochs.get(task_id, 0) + 1
                leases.append({"taskId": task_id, "leaseEpoch": self.epochs[task_id], "stage": "detect",
                               "imageKey": f"images/{task_id}.jpg", "sha256": f"sha-{task_id}",
                               "countryCode": "TZA", "detections": None})
            return Resp(200, {"leases": leases})
        if path.endswith("/complete"):
            return Resp(self.complete_status, {"error": "STALE_LEASE"} if self.complete_status == 409 else {"ok": True})
        if path.endswith("/heartbeat"):
            return Resp(self.heartbeat_status, {"error": "WORKER_DEAD"} if self.heartbeat_status == 410 else {"ok": True})
        return Resp(200, {"ok": True})  # fail, deregister


def make_worker(handler=None, batch=1):
    coord = FakeCoordinator()
    r = fakeredis.FakeRedis()
    worker = Worker(
        stage="detect",
        handler=handler or (lambda lease: {"modelVersion": "fake-detector-v1", "detections": []}),
        redis_client=r,
        coordinator_url="http://coord",
        http=coord,
        hostname="abc123",
        startup_timeout_s=5,
        runtime="container",
    )
    worker.register()
    worker.batch_size = batch
    return worker, coord, r


def test_register_sends_stage_and_container_id():
    worker, coord, _ = make_worker()
    assert worker.worker_id == "detect-w1"
    assert coord.paths("/workers/register")[0] == {"stage": "detect", "hostname": "abc123", "containerId": "abc123",
                                                   "runtime": "container", "device": "cpu"}


def test_register_retries_until_coordinator_is_up(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    coord = FakeCoordinator()
    coord.register_failures = 3
    worker = Worker("detect", lambda l: {}, fakeredis.FakeRedis(), "http://coord", http=coord, hostname="h")
    worker.register()
    assert worker.worker_id == "detect-w1"
    assert len(coord.paths("/workers/register")) == 4


def test_claim_moves_ids_to_processing_list_then_completes():
    worker, coord, r = make_worker()
    r.rpush("queue:detect", "t1", "t2")

    assert worker.run_once() == 1

    # BLMOVE took exactly one ID into this worker's processing list (the coordinator LREMs it).
    assert r.lrange("queue:detect", 0, -1) == [b"t2"]
    assert r.lrange("processing:detect-w1", 0, -1) == [b"t1"]
    assert coord.paths("/tasks/claim-confirm") == [{"workerId": "detect-w1", "taskIds": ["t1"]}]
    [complete] = coord.paths("/tasks/t1/complete")
    assert complete["workerId"] == "detect-w1"
    assert complete["leaseEpoch"] == 1
    assert complete["result"]["modelVersion"] == "fake-detector-v1"
    assert isinstance(complete["result"]["latencyMs"], int)
    assert worker.tasks_done == 1
    assert worker.held_task_ids == []


def test_empty_queue_claims_nothing():
    worker, coord, _ = make_worker()
    worker.redis.blmove = lambda *a: None  # skip the real 1 s block
    assert worker.run_once() == 0
    assert coord.paths("/tasks/claim-confirm") == []


def test_batch_claim_takes_up_to_batch_size():
    worker, coord, r = make_worker(batch=3)
    r.rpush("queue:detect", "a", "b", "c", "d")
    assert worker.run_once() == 3
    assert coord.paths("/tasks/claim-confirm")[0]["taskIds"] == ["a", "b", "c"]
    assert r.lrange("queue:detect", 0, -1) == [b"d"]


def test_stale_lease_on_complete_is_discarded_not_raised():
    worker, coord, r = make_worker()
    coord.complete_status = 409
    r.rpush("queue:detect", "t1")
    worker.run_once()
    assert len(coord.paths("/tasks/t1/complete")) == 1
    assert worker.tasks_done == 0


def test_handler_exception_reports_fail_with_lease_epoch():
    def boom(lease):
        raise OSError("image not found")

    worker, coord, r = make_worker(handler=boom)
    r.rpush("queue:detect", "t1")
    worker.run_once()
    assert coord.paths("/tasks/t1/complete") == []
    [fail] = coord.paths("/tasks/t1/fail")
    assert fail["leaseEpoch"] == 1 and "image not found" in fail["error"]


def test_heartbeat_reports_held_tasks_and_metrics():
    seen = {}

    def handler(lease):
        worker.heartbeat_once()  # heartbeat while the task is in flight
        seen.update(coord.paths("/heartbeat")[-1])
        return {"modelVersion": "v", "detections": []}

    worker, coord, r = make_worker(handler=handler)
    r.rpush("queue:detect", "t1")
    worker.run_once()
    assert seen["taskIds"] == ["t1"]
    assert seen["metrics"]["currentImageKey"] == "images/t1.jpg"
    # claimBatch (P2): the coordinator sizes queue:detect from its workers' claim windows.
    assert set(seen["metrics"]) == {"tasksDone", "avgLatencyMs", "rssMb", "currentImageKey", "claimBatch"}


def test_worker_dead_still_reports_in_flight_result_once_then_reregisters(monkeypatch):
    def handler(lease):
        coord.heartbeat_status = 410
        coord.complete_status = 409  # the task was reassigned meanwhile
        worker.heartbeat_once()  # coordinator declares us dead mid-task
        return {"modelVersion": "v", "detections": []}

    worker, coord, r = make_worker(handler=handler)
    r.rpush("queue:detect", "t1")
    worker.run_once()
    assert worker.dead.is_set()
    # Reported once so the coordinator can fence it visibly (409 STALE_LEASE), never counted as done.
    assert len(coord.paths("/tasks/t1/complete")) == 1
    assert worker.tasks_done == 0

    # The main loop re-registers under a new ID before claiming again.
    coord.heartbeat_status = 200
    monkeypatch.setattr(worker, "run_once", lambda: worker.stopping.set())
    worker.loop()
    assert worker.worker_id == "detect-w2"
    assert not worker.dead.is_set()


def test_sigterm_finishes_current_task_skips_rest_and_deregisters(monkeypatch):
    monkeypatch.setattr("signal.signal", lambda *a: None)

    def handler(lease):
        worker._on_signal(15, None)  # SIGTERM arrives while the first task runs
        return {"modelVersion": "v", "detections": []}

    worker, coord, r = make_worker(handler=handler, batch=2)
    r.rpush("queue:detect", "t1", "t2")
    worker.batch_size = 2
    worker.run()  # registers again (w2), claims 2, finishes t1, stops

    assert len(coord.paths("/tasks/t1/complete")) == 1  # in-flight task reported
    assert coord.paths("/tasks/t2/complete") == []  # unstarted lease left to the coordinator
    assert coord.calls[-1][0] == "/workers/detect-w2/deregister"


# ------------------------------------------------------------------ stage handlers


class MemoryStorage:
    def __init__(self) -> None:
        self.puts: dict[str, bytes] = {}

    def get_image(self, key: str) -> PIL.Image.Image:
        return PIL.Image.new("RGB", (640, 480), "green")

    def put_jpeg(self, key: str, data: bytes) -> None:
        self.puts[key] = data


def lease_for(sha: str, detections=None) -> dict:
    return {"taskId": "t", "leaseEpoch": 1, "imageKey": f"images/{sha}.jpg", "sha256": sha,
            "countryCode": "TZA", "detections": detections}


def animal_sha() -> str:
    return next(f"{i:064x}" for i in range(1000) if any(d["label"] == "animal" and d["conf"] >= 0.2
                                                          for d in fake_detections(f"{i:064x}")))


def test_detect_handler_returns_detect_result():
    sha = animal_sha()
    handle = make_detect_handler(FakeDetector(0), MemoryStorage(), "fake-detector-v1")
    result = handle(lease_for(sha))
    assert result == {"modelVersion": "fake-detector-v1", "detections": fake_detections(sha)}


def test_classify_handler_uploads_crop_under_sanitised_key():
    sha = animal_sha()
    storage = MemoryStorage()
    handle = make_classify_handler(FakeClassifier(0), storage, "speciesnet/v4.0.3a")
    result = handle(lease_for(sha, detections=fake_detections(sha)))

    assert result["cropKey"] == crop_key_for(sha, "speciesnet/v4.0.3a") == f"crops/{sha}_speciesnet_v4.0.3a.jpg"
    assert result["cropKey"] in storage.puts
    crop = PIL.Image.open(__import__("io").BytesIO(storage.puts[result["cropKey"]]))
    assert max(crop.size) <= 256
    assert set(result) >= {"modelVersion", "label", "commonName", "confidence", "cropKey", "raw"}


def test_classify_handler_without_animal_fails():
    handle = make_classify_handler(FakeClassifier(0), MemoryStorage(), "v")
    with pytest.raises(ValueError):
        handle(lease_for("ab" * 32, detections=[]))
