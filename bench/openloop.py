"""Open-loop latency: tasks arrive on a fixed schedule whatever the system is doing.

For each offered rate (a fraction of the closed-loop maximum) the generator computes the
intended arrival time of every task up front (evenly spaced, 1/rate apart) and every 10 ms
submits whatever is due, from a thread pool so a slow submission never delays the next one.
Latency is measured from the task's *intended* arrival time to its image being finalised
(Postgres `images.finalized_at`, corrected for the host↔VM clock offset). Measuring from the
intended time, not from when the request was actually sent, avoids coordinated omission:
if the system stalls, the tasks that should have arrived during the stall are charged for it.

Arrivals:
  - synthetic mode: one `POST /jobs/synthetic {count}` per 10 ms tick. Images in one tick share
    the tick's earliest intended time (over-states latency by at most one tick).
  - sample mode (old code has no synthetic jobs): one `POST /jobs` multipart upload per tick with
    tiny unique JPEG payloads (so the content-hash cache never short-circuits them), named after
    their sequence number, which gives every image its exact intended time.
"""

from __future__ import annotations

import csv
import io
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from analysis import pct
from stack import Stack, log

TICK_S = 0.010


def tiny_jpeg() -> bytes:
    from PIL import Image

    buf = io.BytesIO()
    Image.new("RGB", (8, 8), (120, 140, 90)).save(buf, "JPEG")
    return buf.getvalue()


@dataclass
class OpenLoopConfig:
    mode: str = "sample"
    task_ms: float = 5
    workers: int = 8
    fractions: tuple[float, ...] = (0.5, 0.8, 1.0, 1.1)
    duration: float = 20.0
    warmup: float = 4.0
    drain_timeout: float = 90.0


FIELDS = ["mode", "task_ms", "workers", "fraction", "offered_rate", "achieved_rate", "arrivals", "measured",
          "completed", "incomplete", "submit_errors", "p50_ms", "p90_ms", "p99_ms", "p999_ms", "max_ms",
          "submit_lag_p99_ms", "submit_p50_ms", "wait_p50_ms", "service_p50_ms", "other_cpu_pct", "host_load1"]


def run_rate(stack: Stack, cfg: OpenLoopConfig, rate: float, fraction: float) -> tuple[dict, list[float]]:
    stack.reset_state()
    if cfg.mode == "sample":
        stack.api("POST", "/admin/clear-cache")
    names = stack.start_swarm(cfg.workers, cfg.task_ms)
    stack.wait_workers(cfg.workers)
    offset = stack.clock_offset_s()
    base = tiny_jpeg()

    n = int(rate * cfg.duration)
    t0 = time.time() + 0.5
    intended = t0 + np.arange(n) / rate
    batches: list[dict] = []
    lock = threading.Lock()
    pool = ThreadPoolExecutor(max_workers=48)

    def submit(seqs: list[int]) -> None:
        sent = time.time()
        rec = {"seqs": seqs, "sent": sent, "job": None, "error": None}
        try:
            if cfg.mode == "synthetic":
                rec["job"] = stack.api("POST", "/jobs/synthetic", {"count": len(seqs), "stage": "detect"}, timeout=60)["jobId"]
            else:
                names_ = [f"ol{s:07d}.jpg" for s in seqs]
                payloads = [base + os.urandom(16) for _ in seqs]  # trailing bytes after EOI: unique sha, still a JPEG
                rec["job"] = stack.upload_job(names_, payloads, timeout=60)
        except Exception as e:  # counted, not fatal: an arrival that could not be submitted
            rec["error"] = str(e)[:200]
        with lock:
            batches.append(rec)

    i = 0
    while i < n:
        now = time.time()
        j = int(np.searchsorted(intended, now, side="right"))
        if j > i:
            pool.submit(submit, list(range(i, j)))
            i = j
        time.sleep(max(0.0, min(TICK_S, intended[min(i, n - 1)] - time.time())) or 0.001)
    pool.shutdown(wait=True)

    jobs = [b["job"] for b in batches if b["job"]]
    deadline = time.time() + cfg.drain_timeout
    while time.time() < deadline:
        left = stack.sql("select count(*) from images where job_id = any(%s::uuid[]) and final_category is null", (jobs,))[0][0]
        if left == 0:
            break
        time.sleep(0.5)
    stack.cancel_running_jobs()
    stack.stop_swarms(names)

    rows = stack.sql(
        """select i.job_id::text, i.original_name, extract(epoch from i.finalized_at)::float8,
                  extract(epoch from i.created_at)::float8, extract(epoch from t.started_at)::float8,
                  extract(epoch from t.finished_at)::float8
             from images i left join tasks t on t.image_id = i.id and t.stage = 'detect'
            where i.job_id = any(%s::uuid[])""", (jobs,))
    first_of_job = {b["job"]: float(intended[b["seqs"][0]]) for b in batches if b["job"]}
    lat, done_times, incomplete = [], [], 0
    submit_ms, wait_ms, service_ms = [], [], []
    lo, hi = t0 + cfg.warmup, t0 + cfg.duration
    for job, name, fin, created, started, finished in rows:
        if cfg.mode == "sample" and name and name.startswith("ol"):
            t_int = float(intended[int(name[2:9])])
        else:
            t_int = first_of_job[job]
        if not (lo <= t_int <= hi):
            continue
        if fin is None:
            incomplete += 1
            continue
        lat.append((fin - offset - t_int) * 1000)
        done_times.append(fin - offset)
        submit_ms.append((created - offset - t_int) * 1000)  # intended arrival → image row committed
        if started and finished:
            wait_ms.append((started - created) * 1000)        # dispatcher + ready queue + claim
            service_ms.append((finished - started) * 1000)    # lease held: task + complete
    # Unfinished arrivals count as "longer than the drain timeout" in the percentiles.
    lat_all = lat + [float("inf")] * incomplete
    errors = sum(len(b["seqs"]) for b in batches if b["error"])
    lag = [(b["sent"] - float(intended[b["seqs"][0]])) * 1000 for b in batches]
    achieved = (len(done_times) - 1) / (max(done_times) - min(done_times)) if len(done_times) > 1 else float("nan")

    def p(q):
        v = pct(lat_all, q) if lat_all else None
        return None if v is None or not np.isfinite(v) else round(v, 2)

    row = {"mode": cfg.mode, "task_ms": cfg.task_ms, "workers": cfg.workers, "fraction": fraction,
           "offered_rate": round(rate, 2), "achieved_rate": round(achieved, 2), "arrivals": n,
           "measured": len(lat_all), "completed": len(lat), "incomplete": incomplete, "submit_errors": errors,
           "p50_ms": p(50), "p90_ms": p(90), "p99_ms": p(99), "p999_ms": p(99.9),
           "max_ms": round(max(lat), 2) if lat else None, "submit_lag_p99_ms": round(pct(lag, 99) or 0, 2),
           "submit_p50_ms": round(pct(submit_ms, 50) or 0, 2), "wait_p50_ms": round(pct(wait_ms, 50) or 0, 2),
           "service_p50_ms": round(pct(service_ms, 50) or 0, 2)}
    log(f"  open-loop {fraction:.0%} of max = {rate:.0f}/s: achieved {achieved:.0f}/s, p50 {row['p50_ms']} ms, "
        f"p99 {row['p99_ms']} ms, p99.9 {row['p999_ms']} ms, incomplete {incomplete}, errors {errors}")
    return row, lat


def run_openloop(stack: Stack, cfg: OpenLoopConfig, max_rate: float, out: Path, quiet_cpu_pct: float = 150) -> list[dict]:
    from stack import ContentionMonitor

    out.mkdir(parents=True, exist_ok=True)
    monitor = ContentionMonitor(stack.project)
    rows, hist = [], {}
    for f in cfg.fractions:
        monitor.wait_quiet(quiet_cpu_pct)
        t_start = time.time()
        try:
            row, lat = run_rate(stack, cfg, max_rate * f, f)
        except Exception as e:
            log(f"  open-loop {f:.0%} FAILED: {e}")
            continue
        row.update(monitor.window(t_start, time.time()))
        rows.append(row)
        hist[f] = lat
        with (out / "openloop.csv").open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(rows)
    monitor.stop()
    np.savez_compressed(out / "openloop_latencies.npz", **{f"f{int(k * 100)}": np.asarray(v) for k, v in hist.items()})
    return rows
