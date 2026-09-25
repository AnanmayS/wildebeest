"""Closed-loop burn-down: how many tasks/s the control plane sustains as worker loops are added.

One point = (task time, N worker loops, trial):
  1. Reset the stack (no jobs, no workers, empty tables and queues) and clear the result cache.
  2. Submit K detect tasks up front: `POST /jobs/synthetic` (synthetic mode) or sample jobs of
     ≤ 2,000 real images (sample mode). K is sized from what earlier points measured so the run
     lasts about `duration` seconds.
  3. Start N swarm loops (fake handler: sleep task_ms, return no detections, so every task
     finalises one image and no classify tasks exist).
  4. Stop when every task is done or `max_duration` passed; cancel what is left.
  5. Read the SUCCEEDED tasks' started_at/finished_at/worker_id from Postgres and compute
     steady-state throughput (10th–90th percentile of completion times), lease-time
     percentiles, per-worker cycle time, per-task overhead and Little's law.
"""

from __future__ import annotations

import csv
import json
import math
import time
from dataclasses import dataclass
from pathlib import Path

from analysis import pct, task_metrics
from stack import Stack, log


@dataclass
class CeilingConfig:
    mode: str = "sample"
    task_ms: tuple[float, ...] = (0, 5, 50)
    workers: tuple[int, ...] = (1, 2, 4, 8, 16, 32, 64)
    trials: int = 3
    duration: float = 15.0
    max_duration: float = 60.0
    min_tasks: int = 300
    max_tasks: int = 20000
    per_container: int = 8
    quiet_cpu_pct: float = 150.0   # wait for other containers to drop below this before a point
    resume: bool = False
    retries: int = 2               # re-run a point whose neighbours averaged > 1.5 × that during it


FIELDS = ["mode", "task_ms", "workers", "trial", "submitted", "tasks", "window_s", "window_tasks", "throughput",
          "p50_ms", "p99_ms", "cycle_p50_ms", "cycle_mean_ms", "gap_p50_ms", "overhead_ms", "lease_overhead_p50_ms",
          "littles_L", "littles_ratio", "leased_L", "active_workers", "handler_ms", "wall_s", "other_cpu_pct",
          "host_load1", "timings_p50"]


class RateEstimator:
    """Guesses the next point's throughput from earlier ones, to size K."""

    def __init__(self) -> None:
        self.seen: dict[tuple[float, int], float] = {}
        self.overhead_ms = 5.0

    def record(self, task_ms: float, n: int, x: float, overhead_ms: float | None) -> None:
        if x and not math.isnan(x):
            self.seen[(task_ms, n)] = x
        if n == 1 and overhead_ms and not math.isnan(overhead_ms):
            self.overhead_ms = max(1.0, overhead_ms)

    def estimate(self, task_ms: float, n: int) -> float:
        if (task_ms, n) in self.seen:
            return self.seen[(task_ms, n)] * 1.3
        ideal = n * 1000.0 / (task_ms + self.overhead_ms)
        same = {k[1]: v for k, v in self.seen.items() if k[0] == task_ms}
        smaller = [m for m in same if m < n]
        if smaller:
            m = max(smaller)
            ideal = min(ideal, same[m] * n / m, max(same.values()) * 1.6)
        return ideal


def _succeeded(stack: Stack, jobs: list[str]) -> int:
    return stack.sql(
        """select count(*) from tasks t join images i on i.id = t.image_id
            where i.job_id = any(%s::uuid[]) and t.state = 'SUCCEEDED'""", (jobs,))[0][0]


def run_point(stack: Stack, cfg: CeilingConfig, task_ms: float, n: int, trial: int, est: RateEstimator,
              monitor=None) -> dict:
    stack.reset_state()
    if cfg.mode == "sample":
        stack.api("POST", "/admin/clear-cache")
    k = int(min(cfg.max_tasks, max(cfg.min_tasks, est.estimate(task_ms, n) * (cfg.duration + 3))))
    jobs = stack.submit_tasks(k, cfg.mode)
    names = stack.start_swarm(n, task_ms, per_container=cfg.per_container)
    t0 = time.time()
    try:
        while True:
            time.sleep(0.5)
            done = _succeeded(stack, jobs)
            if done >= k or time.time() - t0 > cfg.max_duration:
                break
    finally:
        wall = time.time() - t0
        stack.cancel_running_jobs()
        swarm_stats = stack.stop_swarms(names)

    has_timings = "timings" in stack.columns("tasks")
    rows = stack.sql(
        f"""select extract(epoch from t.started_at)::float8, extract(epoch from t.finished_at)::float8, t.worker_id
                   {', t.timings' if has_timings else ''}
              from tasks t join images i on i.id = t.image_id
             where i.job_id = any(%s::uuid[]) and t.state = 'SUCCEEDED' and t.stage = 'detect'
               and t.started_at is not null and t.finished_at is not null""", (jobs,))
    handler_ms = swarm_stats.get("handlerMeanMs")
    m = task_metrics([(r[0], r[1], r[2]) for r in rows], task_ms, n, handler_ms)
    timings = {}
    if has_timings:
        tim = [r[3] for r in rows if r[3]]
        for key in sorted({k for t in tim for k in t}):
            vals = [float(t[key]) for t in tim if isinstance(t.get(key), (int, float))]
            if vals:
                timings[key] = round(pct(vals, 50), 3)
    row = {"mode": cfg.mode, "task_ms": task_ms, "workers": n, "trial": trial, "submitted": k,
           "wall_s": round(wall, 2), "timings_p50": json.dumps(timings) if timings else "", **m,
           **(monitor.window(t0, t0 + wall) if monitor else {})}
    est.record(task_ms, n, m.get("throughput", float("nan")), m.get("overhead_ms"))
    log(f"  ceiling {task_ms:>4g} ms  N={n:<3} trial {trial}: {m.get('throughput', float('nan')):8.1f} tasks/s "
        f"(K={k}, done {m['tasks']}, lease p50 {m.get('p50_ms') or float('nan'):.1f} ms, "
        f"cycle {m.get('cycle_mean_ms', float('nan')):.1f} ms, Little {m.get('littles_ratio', float('nan')):.2f}, "
        f"other containers {row.get('other_cpu_pct')}% CPU)")
    return row


def run_ceiling(stack: Stack, cfg: CeilingConfig, out: Path) -> list[dict]:
    out.mkdir(parents=True, exist_ok=True)
    path = out / "ceiling.csv"
    rows: list[dict] = load_rows(path) if cfg.resume else []
    done = {(int(r["trial"]), float(r["task_ms"]), int(r["workers"])) for r in rows}
    est = RateEstimator()
    for r in rows:
        try:
            est.record(float(r["task_ms"]), int(r["workers"]), float(r["throughput"]), float(r["overhead_ms"]))
        except (TypeError, ValueError):
            pass
    if rows:
        log(f"resuming: {len(rows)} points already in {path}")
    from stack import ContentionMonitor

    monitor = ContentionMonitor(stack.project)
    # Trials are the outer loop so slow drift (host load, table growth) spreads over every point.
    for trial in range(1, cfg.trials + 1):
        for task_ms in cfg.task_ms:
            for n in cfg.workers:
                if (trial, float(task_ms), n) in done:
                    continue
                try:
                    for attempt in range(cfg.retries + 1):
                        monitor.wait_quiet(cfg.quiet_cpu_pct)
                        row = run_point(stack, cfg, task_ms, n, trial, est, monitor)
                        other = row.get("other_cpu_pct") or 0
                        if other <= cfg.quiet_cpu_pct * 1.5 or attempt == cfg.retries:
                            break
                        log(f"  neighbours averaged {other:.0f}% CPU during that point; repeating it")
                    rows.append(row)
                except Exception as e:  # keep the sweep going; the gap shows up in the CSV
                    log(f"  ceiling {task_ms} ms N={n} trial {trial} FAILED: {e}")
                with path.open("w", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
                    w.writeheader()
                    w.writerows(rows)
    monitor.stop()
    return rows


def load_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))
