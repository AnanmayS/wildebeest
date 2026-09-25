"""Recovery distribution: SIGKILL busy fake-backend worker containers, measure kill → re-claim.

Runs the regular `detector`/`classifier` services with the fake backend (FAKE_MODEL_DELAY_MS,
default 300 ms, so workers are busy nearly all the time), keeps a backlog of sample jobs, and
repeatedly:
  1. picks an ALIVE detect worker that holds a lease (GET /workers → currentTaskIds),
  2. kills it with `POST /workers/:id/kill` (SIGKILL through the coordinator's Docker socket),
  3. waits until every task taken away from it has been claimed by another worker,
  4. removes the dead container and scales the pool back up, then waits a few seconds.

All timestamps come from task_events (Postgres clock), so there is no host/VM clock skew:
  kill      = the victim's `worker_killed` event
  detected  = its `worker_died` event (heartbeat timeout, or Docker event in the new code)
  requeued  = the last `reassigned`/`lease_expired`/`released` event for its tasks
  reclaimed = for each such task, the first later `claimed` event; the max over its tasks.
A kill that lands between tasks (nothing leased) is recorded as `idle` and not counted.
"""

from __future__ import annotations

import csv
import time
from dataclasses import dataclass
from pathlib import Path

from stack import Stack, log


@dataclass
class RecoveryConfig:
    kills: int = 22
    detectors: int = 4
    classifiers: int = 2
    fake_delay_ms: int = 300
    settle_s: float = 3.0
    timeout_s: float = 60.0
    source: str = "sample"  # sample | synthetic


FIELDS = ["kill", "worker_id", "tasks", "via", "detect_ms", "requeue_ms", "reclaim_ms", "total_ms", "note"]


def _ms(a, b) -> float | None:
    return None if a is None or b is None else round((b - a) * 1000, 1)


def measure_one(stack: Stack, worker_id: str, timeout_s: float) -> dict:
    """Waits for the victim's tasks to be re-claimed and returns the timing breakdown."""
    deadline = time.time() + timeout_s
    while True:
        kill = stack.sql("""select id, extract(epoch from at)::float8 from task_events
                             where worker_id = %s and type = 'worker_killed' order by id desc limit 1""", (worker_id,))
        if kill:
            break
        if time.time() > deadline:
            return {"note": "no worker_killed event"}
        time.sleep(0.2)
    kill_id, kill_at = kill[0]
    while True:
        died = stack.sql("""select extract(epoch from at)::float8, detail from task_events
                             where worker_id = %s and type = 'worker_died' and id > %s order by id limit 1""",
                         (worker_id, kill_id))
        moved = stack.sql("""select task_id, extract(epoch from at)::float8, id from task_events
                              where worker_id = %s and type in ('reassigned', 'lease_expired', 'released')
                                and id > %s and task_id is not null""", (worker_id, kill_id))
        reclaimed = []
        for task_id, _at, ev_id in moved:
            c = stack.sql("""select extract(epoch from at)::float8 from task_events
                              where task_id = %s and type = 'claimed' and id > %s order by id limit 1""", (task_id, ev_id))
            if c:
                reclaimed.append(c[0][0])
        if died and moved and len(reclaimed) == len(moved):
            break
        if died and not moved and time.time() > deadline - timeout_s + 20:
            # Dead and nothing to re-claim 20 s after the kill: it was between tasks.
            return {"note": "idle", "detect_ms": _ms(kill_at, died[0][0])}
        if time.time() > deadline:
            return {"note": "timeout", "detect_ms": _ms(kill_at, died[0][0]) if died else None}
        time.sleep(0.25)
    died_at, detail = died[0]
    via = (detail or {}).get("via", "heartbeat")
    requeued_at = max(m[1] for m in moved)
    reclaimed_at = max(reclaimed)
    return {"tasks": len(moved), "via": via, "detect_ms": _ms(kill_at, died_at),
            "requeue_ms": _ms(kill_at, requeued_at), "reclaim_ms": _ms(requeued_at, reclaimed_at),
            "total_ms": _ms(kill_at, reclaimed_at), "note": ""}


def _pending_detect(stack: Stack) -> int:
    return stack.sql("select count(*) from tasks where stage = 'detect' and state = 'PENDING'")[0][0]


def run_recovery(stack: Stack, cfg: RecoveryConfig, out: Path) -> list[dict]:
    out.mkdir(parents=True, exist_ok=True)
    stack.reset_state()
    stack.api("POST", "/admin/clear-cache")
    env = {"FAKE_MODEL_DELAY_MS": str(cfg.fake_delay_ms)}
    stack.submit_tasks(4000, cfg.source)
    stack.scale_workers(cfg.detectors, cfg.classifiers, **env)
    stack.wait_workers(cfg.detectors, cfg.classifiers)
    time.sleep(3)

    rows: list[dict] = []
    attempts = 0
    while sum(1 for r in rows if r.get("total_ms") is not None) < cfg.kills and attempts < cfg.kills * 3:
        attempts += 1
        if _pending_detect(stack) < 300:
            stack.api("POST", "/admin/clear-cache")
            stack.submit_tasks(2000, cfg.source)
        busy = [w for w in stack.alive_workers("detect") if w.get("currentTaskIds")]
        if not busy:
            time.sleep(0.5)
            continue
        victim = busy[attempts % len(busy)]
        stack.api("POST", f"/workers/{victim['id']}/kill")
        r = {"kill": attempts, "worker_id": victim["id"], **measure_one(stack, victim["id"], cfg.timeout_s)}
        rows.append(r)
        log(f"  kill {attempts}: {victim['id']} -> total {r.get('total_ms')} ms "
            f"(detect {r.get('detect_ms')}, requeue {r.get('requeue_ms')}, reclaim {r.get('reclaim_ms')}, "
            f"via {r.get('via')}) {r.get('note', '')}")
        # The dead container stays around as "exited"; remove it (so Compose doesn't restart the
        # same worker ID) and bring the pool back to size.
        stack.docker("rm", "-f", victim["containerId"])
        stack.scale_workers(cfg.detectors, cfg.classifiers, **env)
        stack.wait_workers(cfg.detectors, cfg.classifiers)
        with (out / "recovery.csv").open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        time.sleep(cfg.settle_s)
    stack.cancel_running_jobs()
    stack.remove_worker_containers()
    return rows
