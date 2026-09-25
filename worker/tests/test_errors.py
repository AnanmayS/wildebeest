"""Error classification, /release + circuit breaker, the report retry budget, and task timings."""

import io
import threading

import fakeredis
import PIL.Image
import pytest
import requests
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError

from wildebeest_worker.fake import FakeClassifier, FakeDetector, fake_classification, fake_detections, synthetic_aware
from wildebeest_worker.runtime import (
    FATAL,
    INFRA,
    TASK,
    CircuitBreaker,
    InfraError,
    NonRetryableError,
    Worker,
    classify_error,
    full_jitter,
)
from wildebeest_worker.storage import (
    CorruptImage,
    IoTimer,
    MissingObject,
    Storage,
    StorageUnavailable,
    open_rgb,
    translate_s3_error,
)

from test_runtime import FakeCoordinator, Resp  # rootdir-relative: tests/ is on sys.path


def client_error(code: str, status: int) -> ClientError:
    return ClientError({"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}, "GetObject")


# ------------------------------------------------------------------ classification


@pytest.mark.parametrize(
    "exc, kind",
    [
        (NonRetryableError("bad input"), FATAL),
        (CorruptImage("cannot decode"), FATAL),
        (MissingObject("gone"), FATAL),
        (PIL.UnidentifiedImageError("cannot identify image file"), FATAL),
        (InfraError("down"), INFRA),
        (StorageUnavailable("down"), INFRA),
        (requests.ConnectionError("refused"), INFRA),
        (requests.Timeout("slow"), INFRA),
        (ConnectionRefusedError("refused"), INFRA),
        (TimeoutError("slow"), INFRA),
        (EndpointConnectionError(endpoint_url="http://minio:9000"), INFRA),
        (ReadTimeoutError(endpoint_url="http://minio:9000"), INFRA),
        (client_error("InternalError", 500), INFRA),
        (client_error("AccessDenied", 403), TASK),
        (ValueError("classify task without an animal detection"), TASK),
        (OSError("image not found"), TASK),
        (RuntimeError("detector failed"), TASK),
    ],
)
def test_classify_error(exc, kind):
    assert classify_error(exc) == kind


@pytest.mark.parametrize(
    "exc, expected",
    [
        (EndpointConnectionError(endpoint_url="http://minio:9000"), StorageUnavailable),
        (ReadTimeoutError(endpoint_url="http://minio:9000"), StorageUnavailable),
        (client_error("SlowDown", 503), StorageUnavailable),
        (client_error("InternalError", 500), StorageUnavailable),
        (client_error("NoSuchKey", 404), MissingObject),
    ],
)
def test_s3_errors_are_translated(exc, expected):
    assert isinstance(translate_s3_error(exc, "images/x.jpg"), expected)


def test_s3_client_errors_that_are_our_fault_pass_through():
    e = client_error("AccessDenied", 403)
    assert translate_s3_error(e, "k") is e


def test_undecodable_bytes_are_a_corrupt_image():
    with pytest.raises(CorruptImage):
        open_rgb(b"this is not a jpeg")


def test_full_jitter_stays_within_the_capped_exponential():
    assert full_jitter(0, 0.5, 30, rng=lambda: 1.0) == 0.5
    assert full_jitter(3, 0.5, 30, rng=lambda: 1.0) == 4.0
    assert full_jitter(10, 0.5, 30, rng=lambda: 1.0) == 30
    assert full_jitter(10, 0.5, 30, rng=lambda: 0.0) == 0


# ------------------------------------------------------------------ storage timing


class StubS3:
    def __init__(self, get_error: Exception | None = None, head_error: Exception | None = None) -> None:
        self.get_error, self.head_error = get_error, head_error
        buf = io.BytesIO()
        PIL.Image.new("RGB", (32, 24), "green").save(buf, format="JPEG")
        self.jpeg = buf.getvalue()
        self.puts: list[str] = []

    def get_object(self, Bucket, Key):
        if self.get_error:
            raise self.get_error
        return {"Body": io.BytesIO(self.jpeg)}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.puts.append(Key)

    def head_bucket(self, Bucket):
        if self.head_error:
            raise self.head_error


def stub_storage(**kwargs) -> Storage:
    storage = Storage(IoTimer())
    storage.s3 = StubS3(**kwargs)
    return storage


def test_storage_times_reads_and_writes_into_the_io_timer():
    storage = stub_storage()
    img = storage.get_image("images/a.jpg")
    storage.put_jpeg("crops/a.jpg", b"x")
    assert img.size == (32, 24)
    assert storage.timer.fetch_ms > 0 and storage.timer.upload_ms > 0
    storage.timer.reset()
    assert storage.timer.fetch_ms == 0 == storage.timer.upload_ms


def test_storage_raises_typed_errors_and_ping_probes_the_bucket():
    with pytest.raises(StorageUnavailable):
        stub_storage(get_error=EndpointConnectionError(endpoint_url="http://minio:9000")).get_image("k")
    with pytest.raises(MissingObject):
        stub_storage(get_error=client_error("NoSuchKey", 404)).get_image("k")
    stub_storage().ping()
    with pytest.raises(StorageUnavailable):
        stub_storage(head_error=EndpointConnectionError(endpoint_url="http://minio:9000")).ping()


# ------------------------------------------------------------------ worker behaviour


def make_worker(handler, batch=1, probe=None, io_timer=None):
    coord = FakeCoordinator()
    r = fakeredis.FakeRedis()
    worker = Worker("detect", handler, r, "http://coord", http=coord, hostname="abc123", startup_timeout_s=5,
                    runtime="container", io_timer=io_timer, probe=probe)
    worker.register()
    worker.batch_size = batch
    return worker, coord, r


def test_infra_error_releases_the_task_and_opens_the_breaker():
    def handler(lease):
        raise StorageUnavailable("S3 unreachable")

    worker, coord, r = make_worker(handler)
    r.rpush("queue:detect", "t1", "t2")
    worker.run_once()

    [release] = coord.paths("/tasks/t1/release")
    assert release["leaseEpoch"] == 1 and "S3 unreachable" in release["reason"]
    assert coord.paths("/tasks/t1/fail") == []
    assert worker.breaker.is_open


def test_open_breaker_claims_nothing_until_the_probe_succeeds(monkeypatch):
    probes = []

    def probe():
        probes.append(1)
        if len(probes) < 3:
            raise StorageUnavailable("still down")

    worker, coord, r = make_worker(lambda lease: {"modelVersion": "v", "detections": []}, probe=probe)
    worker.breaker.rng = lambda: 0.0  # no real waiting
    r.rpush("queue:detect", "t1")
    worker.breaker.trip("S3 unreachable")

    rounds = []
    real_run_once = worker.run_once

    def run_once():
        rounds.append(worker.breaker.is_open)
        real_run_once()
        worker.stopping.set()

    monkeypatch.setattr(worker, "run_once", run_once)
    worker.loop()

    assert len(probes) == 3  # two failed probes with backoff, then success
    assert rounds == [False]  # the claim loop only ran once the circuit had closed
    assert r.lrange("queue:detect", 0, -1) == []  # ...and then it claimed normally
    assert len(coord.paths("/tasks/t1/complete")) == 1
    assert worker.breaker.failed_probes == 0


def test_heartbeats_continue_while_the_breaker_is_open():
    worker, coord, _ = make_worker(lambda lease: {})
    worker.breaker.trip("S3 unreachable")
    worker.heartbeat_once()
    assert len(coord.paths("/heartbeat")) == 1


def test_breaker_backoff_grows_with_failed_probes():
    waits = []

    class Stop:
        def wait(self, s):
            waits.append(s)

        def is_set(self):
            return False

    def probe():
        raise StorageUnavailable("down")

    b = CircuitBreaker(probe, base_s=0.5, cap_s=4, rng=lambda: 1.0)
    b.trip("down")
    for _ in range(5):
        assert b.wait_and_probe(Stop()) is False
    assert waits == [0.5, 1.0, 2.0, 4.0, 4.0]
    assert b.is_open and b.trips == 1


def test_breaker_tripped_mid_batch_releases_the_unstarted_leases():
    def handler(lease):
        raise StorageUnavailable("S3 unreachable")

    worker, coord, r = make_worker(handler, batch=3)
    r.rpush("queue:detect", "a", "b", "c")
    assert worker.run_once() == 3
    assert [p for p, _ in coord.calls if p.startswith("/tasks/") and not p.endswith("claim-confirm")] == [
        "/tasks/a/release", "/tasks/b/release", "/tasks/c/release"]
    assert "circuit open" in coord.paths("/tasks/b/release")[0]["reason"]
    assert worker.held_task_ids == []


def test_undecodable_image_fails_non_retryably():
    def handler(lease):
        return open_rgb(b"garbage")

    worker, coord, r = make_worker(handler)
    r.rpush("queue:detect", "t1")
    worker.run_once()
    [fail] = coord.paths("/tasks/t1/fail")
    assert fail["nonRetryable"] is True and "CorruptImage" in fail["error"]
    assert not worker.breaker.is_open


def test_ordinary_task_error_fails_retryably():
    def handler(lease):
        raise ValueError("model exploded")

    worker, coord, r = make_worker(handler)
    r.rpush("queue:detect", "t1")
    worker.run_once()
    [fail] = coord.paths("/tasks/t1/fail")
    assert "nonRetryable" not in fail


def test_complete_carries_claim_fetch_infer_upload_timings():
    timer = IoTimer()

    def handler(lease):
        timer.fetch_ms += 40.0  # as Storage would record while downloading
        timer.upload_ms += 5.0
        return {"modelVersion": "v", "detections": []}

    worker, coord, r = make_worker(handler, io_timer=timer)
    r.rpush("queue:detect", "t1")
    worker.run_once()
    [complete] = coord.paths("/tasks/t1/complete")
    t = complete["timings"]
    assert set(t) == {"claimMs", "fetchMs", "inferMs", "uploadMs"}
    assert t["fetchMs"] == 40.0 and t["uploadMs"] == 5.0
    assert t["claimMs"] >= 0 and t["inferMs"] >= 0


def test_io_timer_is_reset_per_task():
    timer = IoTimer()
    timer.fetch_ms = 999.0  # left over from a previous task

    worker, coord, r = make_worker(lambda lease: {"modelVersion": "v", "detections": []}, io_timer=timer)
    r.rpush("queue:detect", "t1")
    worker.run_once()
    assert coord.paths("/tasks/t1/complete")[0]["timings"]["fetchMs"] == 0.0


class FlakyCoordinator(FakeCoordinator):
    """Refuses the first `failures` completes, like a coordinator that is restarting."""

    def __init__(self, failures: int, mode: str = "connect") -> None:
        super().__init__()
        self.failures, self.mode = failures, mode

    def post(self, url, json, timeout):
        if url.endswith("/complete") and self.failures > 0:
            self.failures -= 1
            self.calls.append((url.removeprefix("http://coord"), json))
            if self.mode == "connect":
                raise requests.ConnectionError("connection refused")
            return Resp(503, {"error": "restarting"})
        return super().post(url, json, timeout)


def flaky_worker(failures: int, mode: str = "connect"):
    coord = FlakyCoordinator(failures, mode)
    r = fakeredis.FakeRedis()
    worker = Worker("detect", lambda l: {"modelVersion": "v", "detections": []}, r, "http://coord", http=coord,
                    hostname="h", runtime="container")
    worker.register()
    return worker, coord, r


def test_completes_are_retried_for_up_to_a_lease_length(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    worker, coord, r = flaky_worker(failures=12)  # the old budget was 5 tries
    r.rpush("queue:detect", "t1")
    worker.run_once()
    assert len(coord.paths("/tasks/t1/complete")) == 13
    assert worker.tasks_done == 1


def test_503_from_a_restarting_coordinator_is_retried(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    worker, coord, r = flaky_worker(failures=3, mode="503")
    r.rpush("queue:detect", "t1")
    worker.run_once()
    assert worker.tasks_done == 1


def test_report_gives_up_when_the_budget_is_spent(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    worker, coord, r = flaky_worker(failures=100)
    worker.lease_s = 0  # no budget left at all
    r.rpush("queue:detect", "t1")
    with pytest.raises(requests.ConnectionError):
        worker.run_once()
    assert worker.held_task_ids == []  # the lease is simply left to expire


def test_report_retries_stop_once_the_worker_is_declared_dead(monkeypatch):
    worker, coord, r = flaky_worker(failures=100)
    monkeypatch.setattr("time.sleep", lambda s: worker.dead.set())  # 410 arrives during the backoff
    r.rpush("queue:detect", "t1")
    with pytest.raises(requests.ConnectionError):
        worker.run_once()
    assert len(coord.paths("/tasks/t1/complete")) == 1


def test_lease_length_comes_from_register_config():
    coord = FakeCoordinator()
    real_post = coord.post

    def post(url, json, timeout):
        resp = real_post(url, json, timeout)
        if url.endswith("/workers/register"):
            resp._body["config"]["leaseMs"] = 30000
        return resp

    coord.post = post
    worker = Worker("detect", lambda l: {}, fakeredis.FakeRedis(), "http://coord", http=coord, hostname="h",
                    runtime="container")
    worker.register()
    assert worker.lease_s == 30


def test_native_worker_registers_without_a_container():
    coord = FakeCoordinator()
    worker = Worker("detect", lambda l: {}, fakeredis.FakeRedis(), "http://coord", http=coord,
                    hostname="macbook", runtime="native", device="mps")
    worker.register()
    assert coord.paths("/workers/register")[0] == {
        "stage": "detect", "hostname": "macbook", "containerId": "native-macbook", "runtime": "native", "device": "mps"}


# ------------------------------------------------------------------ synthetic fake tasks


class ExplodingStorage:
    def get_image(self, key):
        raise AssertionError(f"synthetic task downloaded {key}")

    def put_jpeg(self, key, data):
        raise AssertionError(f"synthetic task uploaded {key}")


def test_synthetic_detect_skips_the_download():
    sha = "ab" * 32
    inner = lambda lease: ExplodingStorage().get_image(lease["imageKey"])  # noqa: E731
    handle = synthetic_aware(inner, "detect", FakeDetector(0), "fake-detector-v1")
    result = handle({"imageKey": f"synthetic/{sha}", "sha256": sha})
    assert result == {"modelVersion": "fake-detector-v1", "detections": fake_detections(sha)}


def test_synthetic_classify_uploads_no_crop_even_without_detections():
    sha = "cd" * 32
    handle = synthetic_aware(lambda lease: ExplodingStorage().put_jpeg("crops/x", b""), "classify",
                             FakeClassifier(0), "fake-classifier-v1")
    result = handle({"imageKey": f"synthetic/{sha}", "sha256": sha, "detections": []})
    assert result == {"modelVersion": "fake-classifier-v1", **fake_classification(sha), "cropKey": None}


def test_real_keys_still_go_through_the_normal_handler():
    seen = []
    handle = synthetic_aware(lambda lease: seen.append(lease["imageKey"]) or {"ok": True}, "detect",
                             FakeDetector(0), "v")
    assert handle({"imageKey": "images/x.jpg", "sha256": "x"}) == {"ok": True}
    assert seen == ["images/x.jpg"]
