"""Task IDs a claim moved into our processing list without our knowing (docs/decisions/f-fixups.md).

A connection reset while BLMOVE or the MULTI of LMOVEs is in flight: Redis has moved the IDs, the
reply is lost. redis-py then either raises or, by default, silently retries the command, which moves
*more* IDs. Either way the first ones sit in processing:{workerId}, their tasks PENDING, and before
this fix nothing ever confirmed them. The claim MULTI now reads the processing list first."""

import fakeredis
import redis
import requests

from test_runtime import FakeCoordinator
from wildebeest_worker.runtime import Worker


class LostReply:
    """Wraps a fakeredis client: the next `losses` claim commands run on the server but their
    reply is lost. mode "raise": the caller sees a ConnectionError; mode "retry": the client
    re-sends the command and returns the second reply, as redis-py's default Retry does."""

    def __init__(self, r, losses: int = 1, mode: str = "raise", blmove: bool = False) -> None:
        self.r, self.losses, self.mode, self.on_blmove = r, losses, mode, blmove

    def __getattr__(self, name):
        return getattr(self.r, name)

    def _lose(self, run):
        if self.losses <= 0:
            return run()
        self.losses -= 1
        run()  # applied on the server...
        if self.mode == "retry":
            return run()  # ...and sent again by the client's retry, which moves more IDs
        raise redis.ConnectionError("Connection reset by peer")

    def blmove(self, *args):
        if self.on_blmove:
            return self._lose(lambda: self.r.blmove(*args))
        return self.r.blmove(*args)

    def pipeline(self, transaction=True):
        outer = self
        pipe = self.r.pipeline(transaction=transaction)

        class Pipe:
            def __getattr__(self, name):
                attr = getattr(pipe, name)
                if not callable(attr) or name == "execute":
                    return attr

                def queue(*a, **kw):
                    attr(*a, **kw)
                    return self

                return queue

            def execute(self):
                if outer.on_blmove:
                    return pipe.execute()
                commands = list(pipe.command_stack)

                def run():
                    p = outer.r.pipeline(transaction=True)
                    p.command_stack = list(commands)
                    return p.execute()

                return outer._lose(run)

        return Pipe()


def worker_with(r, coord=None):
    coord = coord or FakeCoordinator()
    coord.redis = r.r if isinstance(r, LostReply) else r
    worker = Worker("detect", lambda lease: {"modelVersion": "v", "detections": []}, r, "http://coord",
                    http=coord, hostname="abc123", runtime="container")
    worker.register()
    return worker, coord


def completed(coord) -> list[str]:
    return [p.split("/")[2] for p, _ in coord.calls if p.endswith("/complete")] + [
        i["taskId"] for p, b in coord.calls if p == "/tasks/complete-batch" for i in b["items"]]


def test_a_claim_whose_reply_was_lost_is_confirmed_on_the_next_claim():
    base = fakeredis.FakeRedis()
    base.rpush("queue:detect", "t1", "t2", "t3")
    r = LostReply(base, mode="raise")
    worker, coord = worker_with(r)
    worker.max_batch = worker.batch_size = 2  # claim two at a time

    try:
        worker.run_once()
        raise AssertionError("expected the reset to surface")
    except redis.ConnectionError:
        pass
    # Moved on the server, unknown to the worker: the orphan the fault matrix found.
    assert base.lrange("processing:detect-w1", 0, -1) == [b"t1", b"t2"]
    assert coord.paths("/tasks/claim-confirm") == []

    worker.run_once()  # the same MULTI read t1, t2 back and moved t3: all three confirmed together
    assert coord.paths("/tasks/claim-confirm")[0]["taskIds"] == ["t1", "t2", "t3"]
    assert base.lrange("processing:detect-w1", 0, -1) == []
    assert sorted(completed(coord)) == ["t1", "t2", "t3"]
    assert worker.orphans_recovered == 2


def test_a_silently_retried_claim_confirms_what_the_lost_attempt_moved():
    base = fakeredis.FakeRedis()
    base.rpush("queue:detect", "t1", "t2", "t3")
    r = LostReply(base, mode="retry")
    worker, coord = worker_with(r)

    worker.run_once()  # k = 1: the lost attempt moved t1, the retry read it back and moved t2
    assert coord.paths("/tasks/claim-confirm")[0]["taskIds"] == ["t1", "t2"]
    assert base.lrange("processing:detect-w1", 0, -1) == []
    assert sorted(completed(coord)) == ["t1", "t2"]


def test_a_blocking_move_whose_reply_was_lost_is_confirmed_on_the_next_claim():
    base = fakeredis.FakeRedis()
    r = LostReply(base, mode="retry", blmove=True)
    worker, coord = worker_with(r)
    real_blmove = r.blmove

    def blmove(*args):  # the queue was empty for the MULTI; work arrives while we block
        base.rpush("queue:detect", "t1", "t2")
        r.blmove = real_blmove
        return real_blmove(*args)

    r.blmove = blmove
    # BLMOVE's reply for t1 is lost and redis-py re-sends it, getting t2.
    worker.run_once()
    assert coord.paths("/tasks/claim-confirm")[0]["taskIds"] == ["t2"]
    assert base.lrange("processing:detect-w1", 0, -1) == [b"t1"]

    worker.run_once()  # the next claim's MULTI reads t1 back
    assert coord.paths("/tasks/claim-confirm")[1]["taskIds"] == ["t1"]
    assert base.lrange("processing:detect-w1", 0, -1) == []
    assert sorted(completed(coord)) == ["t1", "t2"]


class ConfirmDown(FakeCoordinator):
    """claim-confirm unreachable for the first `failures` requests."""

    def __init__(self, failures: int) -> None:
        super().__init__()
        self.failures = failures

    def post(self, url, json, timeout):
        if url.endswith("/tasks/claim-confirm") and self.failures:
            self.failures -= 1
            self.calls.append(("/tasks/claim-confirm(failed)", json))
            raise requests.ConnectionError("coordinator unreachable")
        return super().post(url, json, timeout)


def test_ids_whose_claim_confirm_never_got_through_are_confirmed_by_the_next_claim(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    base = fakeredis.FakeRedis()
    base.rpush("queue:detect", "t1", "t2")
    worker, coord = worker_with(base, ConfirmDown(failures=5))  # all 5 tries of the first confirm fail

    try:
        worker.run_once()
        raise AssertionError("expected the confirm to fail")
    except requests.ConnectionError:
        pass
    assert base.lrange("processing:detect-w1", 0, -1) == [b"t1"]

    worker.run_once()  # t1 read back, t2 newly moved
    assert coord.paths("/tasks/claim-confirm")[0]["taskIds"] == ["t1", "t2"]
    assert sorted(completed(coord)) == ["t1", "t2"]


def test_no_leftovers_means_an_ordinary_claim():
    base = fakeredis.FakeRedis()
    base.rpush("queue:detect", "t1")
    worker, coord = worker_with(base)
    worker.run_once()
    assert coord.paths("/tasks/claim-confirm") == [{"workerId": "detect-w1", "taskIds": ["t1"]}]
    assert worker.orphans_recovered == 0
