"""Unit tests for the invariant checker, on hand-written histories (no Docker needed).

    .venv/bin/python -m pytest tests/invariants -q

Each test builds a small history that is correct, then breaks exactly one thing and asserts the
checker names the right invariant. `test_live_*` runs the checker against a live stack's
Postgres when WB_DATABASE_URL is set, and the fault matrix when WB_FAULTS_TARGET is set.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from checker import History, LiveSampler, check_epochs, check_history, check_result_rows, check_single_success, check_terminal  # noqa: E402

JOB, IMG, TASK, SHA = "job-1", "img-1", "task-1", "sha-1"


def ev(i, typ, task=TASK, epoch=None, worker="detect-a"):
    return {"id": i, "task_id": task, "worker_id": worker, "type": typ, "at": None,
            "detail": {"leaseEpoch": epoch} if epoch is not None else {}}


def good() -> History:
    """One image, detected empty after one lost lease: claim(1) → reassigned → claim(2) → success(2)."""
    return History(
        jobs=[{"id": JOB, "status": "done"}],
        images=[{"id": IMG, "job_id": JOB, "sha256": SHA, "final_category": "empty", "species_common_name": None}],
        tasks=[{"id": TASK, "image_id": IMG, "stage": "detect", "state": "SUCCEEDED", "lease_epoch": 2, "attempts": 1}],
        events=[ev(1, "claimed", epoch=1), ev(2, "reassigned"), ev(3, "claimed", epoch=2, worker="detect-b"),
                ev(4, "succeeded", epoch=2, worker="detect-b"), ev(5, "stale_rejected", epoch=1)],
        detections=[{"sha256": SHA, "model_version": "fake-detector-v1", "detections": [], "n": 1}],
    )


def names(violations):
    return sorted({v["invariant"] if isinstance(v, dict) else v.invariant for v in violations})


def test_good_history_passes():
    res = check_history(good())
    assert res["violations"] == []
    assert res["stats"]["staleRejected"] == 1
    assert res["stats"]["reexecutions"] == 1


def test_double_success():
    h = good()
    h.events.append(ev(6, "succeeded", epoch=2))
    assert names(check_single_success(h)) == ["I1 single_success"]


def test_claimed_after_success():
    h = good()
    h.events.append(ev(6, "claimed", epoch=3))
    h.tasks[0]["lease_epoch"] = 3
    assert "I1 single_success" in names(check_single_success(h))


def test_stale_epoch_accepted():
    h = good()
    h.events[3] = ev(4, "succeeded", epoch=1)  # the paused worker's late result got through
    v, _ = check_epochs(h)
    assert names(v) == ["I3 fenced_completion"]


def test_epoch_not_increasing():
    h = good()
    h.events[2] = ev(3, "claimed", epoch=1, worker="detect-b")
    h.events[3] = ev(4, "succeeded", epoch=1, worker="detect-b")
    h.tasks[0]["lease_epoch"] = 1
    v, _ = check_epochs(h)
    assert "I4 epochs_increase" in names(v)


def spec(i, epoch, worker="detect-c"):
    """A speculative copy started next to the current lease (P3): `speculated` with detail.epoch."""
    return {"id": i, "task_id": TASK, "worker_id": worker, "type": "speculated", "at": None, "detail": {"epoch": epoch}}


def speculated_history(winner: int) -> History:
    """claim(1) on a straggler → copy at epoch 2 → the winner's epoch succeeds; the loser gets ALREADY_DONE."""
    h = good()
    h.events = [ev(1, "claimed", epoch=1), spec(2, 2), ev(3, "succeeded", epoch=winner)]
    h.tasks[0]["lease_epoch"] = winner
    return h


@pytest.mark.parametrize("winner", [1, 2])
def test_either_speculative_attempt_may_win(winner):
    v, unverifiable = check_epochs(speculated_history(winner))
    assert v == [] and unverifiable == 0


def test_attempt_before_the_copy_is_still_fenced():
    h = good()  # claim(1) lost → claim(2) → copy(3); epoch 1 is stale even though a copy exists
    h.events = [ev(1, "claimed", epoch=1), ev(2, "claimed", epoch=2), spec(3, 3), ev(4, "succeeded", epoch=1)]
    h.tasks[0]["lease_epoch"] = 1
    assert "I3 fenced_completion" in names(check_epochs(h)[0])


def test_task_row_must_hold_the_winners_epoch():
    h = speculated_history(2)
    h.tasks[0]["lease_epoch"] = 1
    assert "I4 epochs_increase" in names(check_epochs(h)[0])


def test_missing_epoch_is_unverifiable_not_violation():
    h = good()
    h.events[3] = {**h.events[3], "detail": {}}
    v, unverifiable = check_epochs(h)
    assert v == [] and unverifiable == 1


def test_category_disagrees_with_stored_result():
    h = good()
    h.detections[0]["detections"] = [{"label": "human", "conf": 0.9, "bbox": [0, 0, 1, 1]}]
    assert names(check_result_rows(h)) == ["I2 result_rows"]


def test_duplicate_result_rows():
    h = good()
    h.detections[0]["n"] = 2
    assert names(check_result_rows(h)) == ["I2 result_rows"]


def test_animal_needs_classification():
    h = good()
    h.detections[0]["detections"] = [{"label": "animal", "conf": 0.9, "bbox": [0, 0, 1, 1]}]
    h.images[0]["final_category"] = "animal"
    h.images[0]["species_common_name"] = "lion"
    assert names(check_result_rows(h)) == ["I2 result_rows"]
    h.classifications = [{"sha256": SHA, "model_version": "fake-classifier-v1", "common_name": "lion", "n": 1}]
    assert check_result_rows(h) == []


def test_blank_override_is_consistent():
    h = good()
    h.detections[0]["detections"] = [{"label": "animal", "conf": 0.9, "bbox": [0, 0, 1, 1]}]
    h.classifications = [{"sha256": SHA, "model_version": "fake-classifier-v1", "common_name": "blank", "n": 1}]
    assert check_result_rows(h) == []


def test_done_job_with_unfinished_image():
    h = good()
    h.images[0]["final_category"] = None
    assert "I6 terminal_images" in names(check_terminal(h))


def test_lost_image_in_running_job():
    h = good()
    h.jobs[0]["status"] = "running"
    h.images[0]["final_category"] = None
    h.tasks[0]["state"] = "FAILED"
    assert "I6 terminal_images" in names(check_terminal(h))


def test_running_job_with_every_image_final():
    h = good()
    h.jobs[0]["status"] = "running"
    assert names(check_terminal(h)) == ["I7 job_finishes"]


def test_cancelled_jobs_are_ignored():
    h = good()
    h.jobs[0]["status"] = "cancelled"
    h.images[0]["final_category"] = None
    assert check_terminal(h) == []


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *_a):
        pass

    def fetchall(self):
        return self.rows


class _FakeConn:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self):
        return _FakeCursor(self.rows)


def test_live_sampler_needs_two_consecutive_sightings():
    rows = [("t1", "detect-a", "DEAD", 9000, 3000)]
    s = LiveSampler(lambda: _FakeConn(rows))
    s.sample()
    assert s.violations == {}
    s.sample()
    assert list(s.violations) == ["t1"]


def test_live_sampler_suppressed_while_coordinator_down():
    rows = [("t1", "detect-a", "DEAD", 9000, 3000)]
    s = LiveSampler(lambda: _FakeConn(rows))
    s.suppress(60)
    s.sample()
    s.sample()
    assert s.violations == {}


# ------------------------------------------------------------------------------ live (opt-in)

@pytest.mark.skipif(not os.environ.get("WB_DATABASE_URL"), reason="set WB_DATABASE_URL to check a live stack")
def test_live_database_has_no_violations():
    import psycopg

    from checker import load_history

    with psycopg.connect(os.environ["WB_DATABASE_URL"]) as conn:
        res = check_history(load_history(conn))
    assert res["violations"] == []


@pytest.mark.skipif(not os.environ.get("WB_FAULTS_TARGET"), reason="set WB_FAULTS_TARGET=<checkout> to run a fault run")
def test_fault_matrix_smoke(tmp_path):
    here = Path(__file__).resolve().parent
    cmd = [sys.executable, str(here / "faults.py"), "--target", os.environ["WB_FAULTS_TARGET"], "--runs", "1",
           "--faults-per-run", "3", "--images", "400", "--out", str(tmp_path), "--no-summary"]
    assert subprocess.run(cmd).returncode == 0
