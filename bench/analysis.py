"""Statistics for the benchmark: steady-state windows, confidence intervals, USL fit, Little's law."""

from __future__ import annotations

import math
import random
from collections import defaultdict
from typing import Iterable, Sequence

import numpy as np
from scipy import optimize, stats


def pct(values: Sequence[float], q: float) -> float | None:
    """Exact percentile (linear interpolation), or None for no data."""
    if len(values) == 0:
        return None
    return float(np.percentile(np.asarray(values, dtype=float), q))


def mean_ci95(values: Sequence[float]) -> tuple[float, float]:
    """Mean and half-width of the 95% t-interval (0 half-width for n < 2)."""
    xs = [v for v in values if v is not None and not math.isnan(v)]
    if not xs:
        return float("nan"), float("nan")
    m = float(np.mean(xs))
    if len(xs) < 2:
        return m, 0.0
    se = float(np.std(xs, ddof=1)) / math.sqrt(len(xs))
    return m, float(stats.t.ppf(0.975, len(xs) - 1) * se)


def steady_window(finish_times: Sequence[float], lo: float = 0.10, hi: float = 0.90) -> tuple[float, float]:
    """Time window between the lo-th and hi-th quantile of completion times.

    Excludes warm-up (workers registering, the first dispatcher ticks) and the tail (the last
    stragglers, graceful shutdown), which is how River-style burn-down numbers are usually taken.
    """
    ts = sorted(finish_times)
    if len(ts) < 10:
        return (ts[0], ts[-1]) if ts else (0.0, 0.0)
    return ts[int(lo * (len(ts) - 1))], ts[int(hi * (len(ts) - 1))]


def task_metrics(rows: list[tuple[float, float, str]], task_ms: float, workers: int,
                 handler_ms: float | None = None) -> dict:
    """Throughput, lease time, per-worker cycle time, overhead and Little's law for one run.

    rows: (started_at, finished_at, worker_id) in seconds for every SUCCEEDED task.
    handler_ms: measured mean time inside the fake handler (sleep overshoots in containers);
    overhead is cycle time minus this, falling back to the nominal task time.
    """
    work_ms = handler_ms if handler_ms is not None else task_ms
    if len(rows) < 10:
        return {"tasks": len(rows), "throughput": float("nan")}
    t_lo, t_hi = steady_window([r[1] for r in rows])
    inwin = [r for r in rows if t_lo <= r[1] <= t_hi]
    span = max(t_hi - t_lo, 1e-9)
    x = (len(inwin) - 1) / span if len(inwin) > 1 else float("nan")
    lease_ms = [(r[1] - r[0]) * 1000 for r in inwin]

    # Per-worker cycle: time between the starts of consecutive tasks of the same worker loop
    # (claim + work + complete + whatever idle waiting there was in between).
    by_worker: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for s, f, w in rows:
        by_worker[w].append((s, f))
    cycles, gaps = [], []
    for seq in by_worker.values():
        seq.sort()
        for (s0, f0), (s1, _f1) in zip(seq, seq[1:]):
            if t_lo <= s0 and s1 <= t_hi:
                cycles.append((s1 - s0) * 1000)
                gaps.append((s1 - f0) * 1000)
    mean_cycle = float(np.mean(cycles)) if cycles else float("nan")
    mean_lease = float(np.mean(lease_ms)) if lease_ms else float("nan")
    active = len({r[2] for r in inwin})
    return {
        "tasks": len(rows),
        "window_s": round(span, 3),
        "window_tasks": len(inwin),
        "throughput": x,
        "p50_ms": pct(lease_ms, 50),
        "p99_ms": pct(lease_ms, 99),
        "cycle_p50_ms": pct(cycles, 50),
        "cycle_mean_ms": mean_cycle,
        "gap_p50_ms": pct(gaps, 50),
        "overhead_ms": mean_cycle - work_ms if cycles else float("nan"),
        "lease_overhead_p50_ms": (pct(lease_ms, 50) or 0) - work_ms,
        "handler_ms": work_ms,
        # Little's law: in a closed loop every worker loop always holds exactly one task
        # "in the system" (claiming, working or completing), so X * mean cycle must equal N.
        "littles_L": x * mean_cycle / 1000 if cycles else float("nan"),
        "littles_ratio": (x * mean_cycle / 1000) / workers if cycles and workers else float("nan"),
        "leased_L": x * mean_lease / 1000,
        "active_workers": active,
    }


# ---------------------------------------------------------------------------------------------
# Universal Scalability Law
# ---------------------------------------------------------------------------------------------

def usl(n, lam, alpha, beta):
    n = np.asarray(n, dtype=float)
    return lam * n / (1 + alpha * (n - 1) + beta * n * (n - 1))


def _fit(ns: Sequence[float], xs: Sequence[float]) -> tuple[float, float, float]:
    ns = np.asarray(ns, float)
    xs = np.asarray(xs, float)
    lam0 = float(xs[ns == ns.min()].mean() / ns.min())
    popt, _ = optimize.curve_fit(
        usl, ns, xs, p0=[lam0, 0.05, 0.001],
        bounds=([0, 0, 0], [np.inf, 1.0, 1.0]), maxfev=20000,
    )
    return float(popt[0]), float(popt[1]), float(popt[2])


def fit_usl(trials: dict[int, list[float]], boot: int = 500, seed: int = 1) -> dict | None:
    """Fits X(N) = λN / (1 + α(N−1) + βN(N−1)) to every trial point.

    trials: workers -> list of per-trial throughputs. Needs ≥ 3 distinct N (Gunther asks for ≥ 6).
    95% CIs come from a bootstrap that resamples trials within each N.
    """
    ns = sorted(n for n, v in trials.items() if v)
    if len(ns) < 3:
        return None
    all_n = [n for n in ns for _ in trials[n]]
    all_x = [x for n in ns for x in trials[n]]
    try:
        lam, a, b = _fit(all_n, all_x)
    except (RuntimeError, ValueError):
        return None
    rng = random.Random(seed)
    samples = []
    for _ in range(boot):
        bn, bx = [], []
        for n in ns:
            for _ in trials[n]:
                bn.append(n)
                bx.append(rng.choice(trials[n]))
        try:
            samples.append(_fit(bn, bx))
        except (RuntimeError, ValueError):
            continue
    ci = {}
    if samples:
        arr = np.asarray(samples)
        for i, k in enumerate(("lambda", "alpha", "beta")):
            ci[k] = [float(np.percentile(arr[:, i], 2.5)), float(np.percentile(arr[:, i], 97.5))]
    pred = usl(all_n, lam, a, b)
    ss_res = float(np.sum((np.asarray(all_x) - pred) ** 2))
    ss_tot = float(np.sum((np.asarray(all_x) - np.mean(all_x)) ** 2)) or 1e-9
    n_star = math.sqrt((1 - a) / b) if b > 0 and a < 1 else float("inf")
    return {
        "lambda": lam, "alpha": a, "beta": b, "ci95": ci, "r2": 1 - ss_res / ss_tot,
        "n_points": len(ns), "n_samples": len(all_x),
        "n_star": n_star, "x_max": float(usl([n_star], lam, a, b)[0]) if math.isfinite(n_star) else None,
    }


def summarize_points(rows: Iterable[dict], key: str = "throughput") -> dict[tuple, dict]:
    """Groups ceiling rows by (task_ms, workers) → mean ± CI of each metric."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        groups[(float(r["task_ms"]), int(r["workers"]))].append(r)
    out = {}
    for k, rs in sorted(groups.items()):
        agg = {"trials": len(rs)}
        for m in ("throughput", "p50_ms", "p99_ms", "cycle_p50_ms", "cycle_mean_ms", "overhead_ms",
                  "littles_ratio", "lease_overhead_p50_ms", "other_cpu_pct"):
            vals = [float(r[m]) for r in rs if r.get(m) not in (None, "", "nan")]
            vals = [v for v in vals if not math.isnan(v)]
            agg[m], agg[m + "_ci"] = mean_ci95(vals) if vals else (float("nan"), float("nan"))
        out[k] = agg
    return out
