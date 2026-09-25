"""P3 speculation on the worker side: taking offered copies, heartbeat `cancel`, and the
ALREADY_DONE outcome of a lost race."""

from test_hotpath import HotCoordinator, completes, hot_worker
from test_runtime import Resp


class SpecCoordinator(HotCoordinator):
    """HotCoordinator plus scripted heartbeat cancels and ALREADY_DONE answers."""

    def __init__(self, **kw) -> None:
        super().__init__(**kw)
        self.cancel: list[str] = []
        self.already_done: set[str] = set()

    def post(self, url, json, timeout):
        path = url.removeprefix("http://coord")
        if path.endswith("/heartbeat"):
            self.calls.append((path, json))
            return Resp(200, {"ok": True, "claimMode": self.claim_mode, "cancel": self.cancel})
        if path == "/tasks/complete-batch":
            self.calls.append((path, json))
            results = [{"taskId": i["taskId"], "status": "already_done" if i["taskId"] in self.already_done else "ok"}
                       for i in json["items"]]
            return Resp(200, {"results": results, "leases": self._next(json.get("next", 0))})
        if path.endswith("/complete") and path.split("/")[2] in self.already_done:
            self.calls.append((path, json))
            return Resp(409, {"error": "ALREADY_DONE"})
        return super().post(url, json, timeout)


def spec_worker(handler=None, **kw):
    import fakeredis

    from wildebeest_worker.runtime import Worker

    coord = SpecCoordinator(**kw)
    r = fakeredis.FakeRedis()
    worker = Worker("detect", handler or (lambda lease: {"modelVersion": "v", "detections": []}), r,
                    "http://coord", http=coord, hostname="abc123", runtime="container")
    worker.register()
    return worker, coord, r


def test_hybrid_claim_takes_an_offered_copy_before_the_shared_queue():
    worker, coord, r = hot_worker(max_batch=2, floor=2)
    r.rpush("spec:detect-w1", "copy")
    r.rpush("queue:detect", "a", "b", "c")
    assert worker.claim_ids() == ["copy", "a", "b"]
    assert r.lrange("processing:detect-w1", 0, -1) == [b"copy", b"a", b"b"]
    assert r.llen("spec:detect-w1") == 0


def test_an_offered_copy_is_found_while_the_shared_queue_is_empty():
    worker, coord, r = hot_worker()
    r.rpush("spec:detect-w1", "copy")
    assert worker.claim_ids() == ["copy"]


def test_heartbeat_cancel_drops_waiting_leases_without_running_them():
    ran = []

    def handler(lease):
        ran.append(lease["taskId"])
        if lease["taskId"] == "a":
            coord.cancel = ["b"]  # while a runs, another attempt finishes b
            worker.heartbeat_once()
        return {"modelVersion": "v", "detections": []}

    worker, coord, r = spec_worker(handler, max_batch=3, floor=3)
    r.rpush("queue:detect", "a", "b", "c")
    worker.run_once()
    assert ran == ["a", "c"]
    reported = [i["taskId"] for _, body in completes(coord) for i in body.get("items", [body])]
    assert "b" not in reported
    assert worker.tasks_cancelled == 1


def test_heartbeat_cancel_of_the_running_task_drops_its_result():
    def handler(lease):
        coord.cancel = [lease["taskId"]]
        worker.heartbeat_once()
        # While cancelled it is no longer reported as held (its lease isn't ours to renew).
        assert lease["taskId"] not in worker.held_task_ids
        return {"modelVersion": "v", "detections": []}

    worker, coord, r = spec_worker(handler)
    r.rpush("queue:detect", "a")
    worker.run_once()
    assert completes(coord) == []
    assert worker.tasks_done == 0 and worker.tasks_cancelled == 1
    assert worker.cancelled == set()


def test_a_cancel_for_a_finished_unreported_result_is_left_to_the_report():
    worker, coord, r = spec_worker(max_batch=2, floor=2)
    r.rpush("queue:detect", "a", "b")
    worker.acquire()
    lease, claim_ms = worker.backlog.popleft()
    worker.execute(lease, claim_ms)  # a is finished, waiting in the report buffer
    worker.cancel(["a"])
    assert [f.item["taskId"] for f in worker.finished] == ["a"]
    assert worker.tasks_cancelled == 0


def test_already_done_on_a_single_complete_is_discarded_quietly():
    worker, coord, r = spec_worker()
    r.rpush("queue:detect", "a", "b")
    coord.already_done.add("a")
    worker.run_once()  # a: 409 ALREADY_DONE carries no leases, so the worker claims b itself
    worker.run_once()
    assert [p for p, _ in completes(coord)] == ["/tasks/a/complete", "/tasks/b/complete"]
    assert worker.tasks_done == 1  # b only
    assert worker.held_task_ids == []


def test_already_done_in_a_batch_is_discarded_and_the_rest_counts():
    worker, coord, r = spec_worker(max_batch=3, floor=3)
    r.rpush("queue:detect", "a", "b", "c")
    coord.already_done.add("b")
    worker.run_once()
    [(path, body)] = completes(coord)
    assert path == "/tasks/complete-batch"
    assert worker.tasks_done == 2


def test_error_codes_are_read_from_the_body():
    from wildebeest_worker.runtime import _error_code

    assert _error_code(Resp(409, {"error": "ALREADY_DONE"})) == "ALREADY_DONE"
    assert _error_code(Resp(409, {})) == "HTTP 409"


def test_register_forgets_pending_cancels():
    worker, coord, r = spec_worker()
    worker.cancelled.add("x")
    worker.register()
    assert worker.cancelled == set()


def test_postgres_mode_copies_come_from_the_claim_like_any_lease():
    worker, coord, r = spec_worker(claim_mode="postgres")
    coord.pending = ["copy"]
    assert worker.run_once() == 1
    assert [p for p, _ in completes(coord)] == ["/tasks/copy/complete"]
    assert r.keys("*") == []  # postgres mode never touches Redis, offers included
