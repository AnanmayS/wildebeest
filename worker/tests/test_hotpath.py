"""P2 worker hot path: claim sizing, batched completes, complete-and-claim-next, the Postgres claim
mode, prefetch (and that it can't break fencing), and the native-worker settings."""

import io
import threading
import time

import fakeredis
import PIL.Image
import pytest
import requests

from wildebeest_worker.runtime import ClaimSizer, Worker
from wildebeest_worker.storage import IoTimer, MissingObject, Storage

from test_errors import StubS3, client_error
from test_runtime import FakeCoordinator, Resp


class HotCoordinator(FakeCoordinator):
    """FakeCoordinator plus the P2 endpoints. Tasks in `pending` are handed out by complete+next
    and by POST /tasks/claim (postgres mode); `stale` task IDs are answered as fenced off."""

    def __init__(self, claim_mode="hybrid", max_batch=1, floor=1) -> None:
        super().__init__()
        self.claim_mode, self.max_batch, self.floor = claim_mode, max_batch, floor
        self.pending: list[str] = []
        self.stale: set[str] = set()

    def _lease(self, task_id: str) -> dict:
        self.epochs[task_id] = self.epochs.get(task_id, 0) + 1
        return {"taskId": task_id, "leaseEpoch": self.epochs[task_id], "stage": "detect",
                "imageKey": f"images/{task_id}.jpg", "sha256": f"sha-{task_id}", "countryCode": "TZA",
                "detections": None}

    def _next(self, k: int) -> list[dict]:
        taken, self.pending = self.pending[:k], self.pending[k:]
        return [self._lease(t) for t in taken]

    def post(self, url, json, timeout):
        path = url.removeprefix("http://coord")
        if path == "/workers/register":
            resp = super().post(url, json, timeout)
            resp._body["config"].update(claimMode=self.claim_mode, maxClaimBatch=self.max_batch,
                                        claimBatchSize=self.floor)
            return resp
        if path == "/tasks/claim":
            self.calls.append((path, json))
            return Resp(200, {"leases": self._next(json["max"])})
        if path == "/tasks/complete-batch":
            self.calls.append((path, json))
            results = [{"taskId": i["taskId"], "status": "stale" if i["taskId"] in self.stale else "ok"}
                       for i in json["items"]]
            return Resp(200, {"results": results, "leases": self._next(json.get("next", 0))})
        if path.endswith("/complete"):
            self.calls.append((path, json))
            task_id = path.split("/")[2]
            if task_id in self.stale:
                return Resp(409, {"error": "STALE_LEASE"})
            body = {"ok": True}
            if json.get("next"):
                body["leases"] = self._next(json["next"])
            return Resp(200, body)
        return super().post(url, json, timeout)


def hot_worker(handler=None, prefetch=None, **coord_kwargs):
    coord = HotCoordinator(**coord_kwargs)
    r = fakeredis.FakeRedis()
    worker = Worker("detect", handler or (lambda lease: {"modelVersion": "v", "detections": []}), r,
                    "http://coord", http=coord, hostname="abc123", runtime="container", prefetch=prefetch)
    worker.register()
    return worker, coord, r


def completes(coord):
    return [(p, b) for p, b in coord.calls if p.endswith("/complete") or p == "/tasks/complete-batch"]


# ------------------------------------------------------------------ claim sizing


def test_claim_size_follows_rtt_over_service_time_within_bounds():
    s = ClaimSizer(alpha=1.0)
    assert s.size(1, 16) == 1  # nothing measured yet: the floor
    s.observe_rtt(4.0)
    s.observe_service(700.0)  # a real model
    assert s.size(1, 16) == 1
    s.observe_service(1.0)  # 4 ms round trips, 1 ms tasks
    assert s.size(1, 16) == 4
    s.observe_service(0.0)  # 0 ms tasks: as big as allowed
    assert s.size(1, 16) == 16
    assert s.size(3, 3) == 3  # floor == cap pins it


def test_hybrid_claim_moves_a_whole_batch_in_one_transaction():
    worker, coord, r = hot_worker(max_batch=4, floor=4)
    r.rpush("queue:detect", "a", "b", "c", "d", "e")
    assert worker.claim_ids() == ["a", "b", "c", "d"]
    assert r.lrange("processing:detect-w1", 0, -1) == [b"a", b"b", b"c", b"d"]
    assert r.lrange("queue:detect", 0, -1) == [b"e"]


# ------------------------------------------------------------------ batched completes


def test_results_of_a_batch_go_back_in_one_request_and_a_stale_row_is_just_dropped():
    worker, coord, r = hot_worker(max_batch=3, floor=3)
    r.rpush("queue:detect", "a", "b", "c")
    coord.stale.add("b")  # b was reassigned while we worked on it
    assert worker.run_once() == 3

    [(path, body)] = completes(coord)
    assert path == "/tasks/complete-batch"
    assert [i["taskId"] for i in body["items"]] == ["a", "b", "c"]
    assert all(set(i) == {"taskId", "leaseEpoch", "result", "timings"} for i in body["items"])
    assert body["next"] == 3  # refill the window in the same request
    assert worker.tasks_done == 2
    assert worker.held_task_ids == []


def test_complete_batch_env_flushes_every_n(monkeypatch):
    monkeypatch.setenv("COMPLETE_BATCH", "1")
    worker, coord, r = hot_worker(max_batch=3, floor=3)
    r.rpush("queue:detect", "a", "b", "c")
    worker.run_once()
    assert [p for p, _ in completes(coord)] == ["/tasks/a/complete", "/tasks/b/complete", "/tasks/c/complete"]


def test_a_zombie_posts_its_buffered_results_once_and_asks_for_nothing():
    def handler(lease):
        if lease["taskId"] == "b":
            coord.heartbeat_status = 410
            worker.heartbeat_once()  # declared dead while b runs
        return {"modelVersion": "v", "detections": []}

    worker, coord, r = hot_worker(handler, max_batch=3, floor=3)
    r.rpush("queue:detect", "a", "b", "c")
    coord.stale.update({"a", "b"})
    worker.run_once()
    [(path, body)] = completes(coord)
    assert [i["taskId"] for i in body["items"]] == ["a", "b"]  # c never started
    assert "next" not in body
    assert worker.tasks_done == 0 and worker.held_task_ids == []


# ------------------------------------------------------------------ complete-and-claim-next


def test_leases_returned_with_a_complete_are_processed_without_another_claim():
    worker, coord, r = hot_worker()
    r.rpush("queue:detect", "t1")
    coord.pending = ["t2", "t3"]
    assert worker.run_once() == 3
    assert coord.paths("/tasks/claim-confirm") == [{"workerId": "detect-w1", "taskIds": ["t1"]}]
    assert [p for p, _ in completes(coord)] == ["/tasks/t1/complete", "/tasks/t2/complete", "/tasks/t3/complete"]
    assert all(b.get("next") == 1 for _, b in completes(coord))
    assert worker.tasks_done == 3


# ------------------------------------------------------------------ postgres claim mode


def test_postgres_mode_long_polls_the_coordinator_and_never_touches_redis():
    worker, coord, r = hot_worker(claim_mode="postgres", max_batch=4, floor=2)
    assert worker.claim_mode == "postgres"
    coord.pending = ["p1", "p2", "p3"]
    r.rpush("queue:detect", "should-not-be-touched")
    assert worker.run_once() == 3
    [claim] = coord.paths("/tasks/claim")
    assert claim == {"workerId": "detect-w1", "stage": "detect", "max": 2, "waitMs": 1000}
    assert r.lrange("queue:detect", 0, -1) == [b"should-not-be-touched"]
    assert worker.tasks_done == 3


def test_postgres_mode_empty_claim_processes_nothing():
    worker, coord, _ = hot_worker(claim_mode="postgres")
    assert worker.run_once() == 0
    assert completes(coord) == []


# ------------------------------------------------------------------ prefetch


def test_prefetch_starts_the_next_download_while_heartbeats_report_every_held_lease():
    prefetched, seen = [], {}

    def handler(lease):
        if lease["taskId"] == "t1":
            assert prefetched == ["images/t2.jpg"]  # t2's download began before t1 ran
            worker.heartbeat_once()
            seen.update(coord.paths("/heartbeat")[-1])
        return {"modelVersion": "v", "detections": []}

    worker, coord, r = hot_worker(handler, prefetch=lambda lease: prefetched.append(lease["imageKey"]))
    assert worker.window() == 2  # one running + one prefetched
    r.rpush("queue:detect", "t1", "t2", "t3")
    worker.run_once()
    assert seen["taskIds"] == ["t1", "t2"]  # running + waiting: both leases are renewed
    assert seen["metrics"]["claimBatch"] == 2


def test_prefetch_cannot_break_fencing():
    """The prefetched task was reassigned meanwhile: its result is fenced off like any other."""
    worker, coord, r = hot_worker(prefetch=lambda lease: None)
    r.rpush("queue:detect", "t1", "t2")
    coord.stale.add("t2")
    worker.run_once()
    statuses = {p: b for p, b in completes(coord)}
    assert set(statuses) == {"/tasks/t1/complete", "/tasks/t2/complete"}
    assert worker.tasks_done == 1
    assert worker.held_task_ids == []


def test_a_failing_prefetch_never_costs_the_task():
    def boom(lease):
        raise RuntimeError("prefetch exploded")

    worker, coord, r = hot_worker(prefetch=boom)
    r.rpush("queue:detect", "t1", "t2")
    worker.run_once()
    assert worker.tasks_done == 2


def jpeg() -> bytes:
    buf = io.BytesIO()
    PIL.Image.new("RGB", (16, 12), "green").save(buf, format="JPEG")
    return buf.getvalue()


class SlowS3(StubS3):
    def __init__(self, delay_s=0.0, **kw) -> None:
        super().__init__(**kw)
        self.delay_s, self.gets = delay_s, []

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        time.sleep(self.delay_s)
        return super().get_object(Bucket, Key)


def test_storage_prefetch_is_used_by_get_image_and_only_the_wait_counts_as_fetch():
    storage = Storage(IoTimer())
    storage.s3 = SlowS3(delay_s=0.2)
    storage.prefetch("images/a.jpg")
    time.sleep(0.3)  # the "current task" runs while a.jpg downloads
    img = storage.get_image("images/a.jpg")
    assert img.size == (32, 24)
    assert storage.s3.gets == ["images/a.jpg"]  # downloaded once
    assert storage.timer.fetch_ms < 100  # it was already there


def test_storage_prefetch_errors_surface_as_typed_errors_on_use():
    storage = Storage(IoTimer())
    storage.s3 = SlowS3(get_error=client_error("NoSuchKey", 404))
    storage.prefetch("images/gone.jpg")
    with pytest.raises(MissingObject):
        storage.get_image("images/gone.jpg")


def test_storage_keeps_at_most_two_prefetched_images():
    storage = Storage(IoTimer())
    storage.s3 = SlowS3()
    for k in ("a", "b", "c"):
        storage.prefetch(f"images/{k}.jpg")
    assert list(storage._prefetched) == ["images/b.jpg", "images/c.jpg"]


# ------------------------------------------------------------------ native-worker settings


def test_container_id_and_hostname_overrides_and_late_device(monkeypatch):
    monkeypatch.setenv("WORKER_CONTAINER_ID", "native-aro")
    monkeypatch.setenv("WORKER_HOSTNAME", "aro-2")
    coord = FakeCoordinator()
    worker = Worker("detect", lambda l: {}, fakeredis.FakeRedis(), "http://coord", http=coord, runtime="native")
    monkeypatch.setenv("WORKER_DEVICE", "mps")  # set by the model while it loads (DEVICE=auto)
    worker.register()
    assert coord.paths("/workers/register")[0] == {
        "stage": "detect", "hostname": "aro-2", "containerId": "native-aro", "runtime": "native", "device": "mps"}


def test_heartbeat_retries_once_straight_away_on_a_dropped_connection(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)

    class Flaky(FakeCoordinator):
        drops = 1

        def post(self, url, json, timeout):
            if url.endswith("/heartbeat") and self.drops:
                self.drops -= 1
                raise requests.ConnectionError("connection reset by peer")
            return super().post(url, json, timeout)

    coord = Flaky()
    worker = Worker("detect", lambda l: {}, fakeredis.FakeRedis(), "http://coord", http=coord, hostname="h",
                    runtime="container")
    worker.register()
    worker.heartbeat_once()
    assert len(coord.paths("/heartbeat")) == 1 and not worker.dead.is_set()


def test_stop_ends_run_cleanly():
    worker, coord, r = hot_worker()
    worker.redis.blmove = lambda *a: None  # empty queue, no real 1 s block
    t = threading.Thread(target=worker.loop)
    t.start()
    worker.stop()
    t.join(timeout=3)
    assert not t.is_alive()


def test_a_heartbeat_carries_the_claim_mode_so_running_workers_follow_a_switch():
    worker, coord, _ = hot_worker()
    real = coord.post

    def post(url, json, timeout):
        resp = real(url, json, timeout)
        if url.endswith("/heartbeat"):
            resp._body["claimMode"] = "postgres"
        return resp

    coord.post = post
    worker.heartbeat_once()
    assert worker.claim_mode == "postgres"
