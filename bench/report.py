"""Charts, results.md and benchmarks/summary.json from the CSVs a run wrote."""

from __future__ import annotations

import csv
import json
import math
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from analysis import fit_usl, mean_ci95, pct, summarize_points, usl  # noqa: E402
from stack import REPO, machine_description  # noqa: E402

# Reference categorical palette (dataviz skill), slots 1-3 validate all-pairs in light mode.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def _read(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def _f(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def _style(ax, title: str, xlabel: str, ylabel: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.figure.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=INK, fontsize=12, pad=12)
    ax.set_xlabel(xlabel, color=INK2)
    ax.set_ylabel(ylabel, color=INK2)
    ax.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2)


def analyse(out: Path, label: str) -> dict:
    """Aggregates one run directory into results.json (and returns it)."""
    ceiling = _read(out / "ceiling.csv")
    points = summarize_points(ceiling)
    task_ms = sorted({k[0] for k in points})
    series = []
    for t in task_ms:
        pts = []
        for (tm, n), agg in points.items():
            if tm != t:
                continue
            pts.append({"workers": n, "throughput": agg["throughput"], "throughput_ci": agg["throughput_ci"],
                        "p50Ms": agg["p50_ms"], "p99Ms": agg["p99_ms"], "trials": agg["trials"],
                        "cycleMs": agg["cycle_mean_ms"], "overheadMs": agg["overhead_ms"],
                        "littlesRatio": agg["littles_ratio"], "otherCpuPct": agg["other_cpu_pct"]})
        series.append({"taskMs": t, "points": sorted(pts, key=lambda p: p["workers"])})

    usl_fits = {}
    for t in task_ms:
        trials: dict[int, list[float]] = {}
        for r in ceiling:
            if _f(r["task_ms"]) == t and not math.isnan(_f(r["throughput"])):
                trials.setdefault(int(r["workers"]), []).append(_f(r["throughput"]))
        fit = fit_usl(trials)
        if fit:
            usl_fits[str(t)] = fit

    # Per-task overhead: at N=1 a worker loop's cycle is claim + task + complete, so
    # cycle − task time is the whole orchestration cost of one task.
    n1 = [_f(r["overhead_ms"]) for r in ceiling if int(r["workers"]) == 1 and not math.isnan(_f(r["overhead_ms"]))]
    n1_by_t = {t: mean_ci95([_f(r["overhead_ms"]) for r in ceiling
                             if int(r["workers"]) == 1 and _f(r["task_ms"]) == t]) for t in task_ms}
    lease_ovh = [_f(r["lease_overhead_p50_ms"]) for r in ceiling if int(r["workers"]) == 1]
    timings = [json.loads(r["timings_p50"]) for r in ceiling if r.get("timings_p50")]
    littles = [_f(r["littles_ratio"]) for r in ceiling if not math.isnan(_f(r["littles_ratio"]))]

    rec = _read(out / "recovery.csv")
    rec_ok = [r for r in rec if r.get("total_ms") not in (None, "")]
    tot = [_f(r["total_ms"]) for r in rec_ok]

    def dist(key):
        v = [_f(r[key]) for r in rec_ok if r.get(key) not in (None, "")]
        return {"p50": pct(v, 50), "p95": pct(v, 95), "max": max(v) if v else None} if v else None

    ol = _read(out / "openloop.csv")
    res = {
        "label": label,
        "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "machine": machine_description(),
        "mode": ceiling[0]["mode"] if ceiling else None,
        "ceiling": {"taskMs": task_ms, "series": series, "usl": usl_fits},
        "overhead": {
            "perTaskMs": mean_ci95(n1)[0] if n1 else None,
            "perTaskMsCi": mean_ci95(n1)[1] if n1 else None,
            "byTaskMs": {str(t): {"mean": v[0], "ci95": v[1]} for t, v in n1_by_t.items()},
            "leaseOverheadP50Ms": mean_ci95(lease_ovh)[0] if lease_ovh else None,
            "workerTimingsP50": timings[-1] if timings else None,
        },
        "littles": {"meanRatio": float(np.mean(littles)) if littles else None,
                    "min": min(littles) if littles else None, "max": max(littles) if littles else None},
        "recovery": {
            "samples": len(rec_ok), "kills": len(rec), "idle": sum(1 for r in rec if r.get("note") == "idle"),
            "p50Ms": pct(tot, 50), "p95Ms": pct(tot, 95), "maxMs": max(tot) if tot else None,
            "meanMs": float(np.mean(tot)) if tot else None,
            "via": sorted({r.get("via") for r in rec_ok if r.get("via")}),
            "detect": dist("detect_ms"), "requeue": dist("requeue_ms"), "reclaim": dist("reclaim_ms"),
        },
        "openloop": ol,
    }
    (out / "results.json").write_text(json.dumps(res, indent=2, default=float))
    return res


def charts(out: Path, res: dict) -> None:
    ceiling = res["ceiling"]
    if ceiling["series"]:
        fig, ax = plt.subplots(figsize=(8, 5), dpi=150)
        for i, s in enumerate(ceiling["series"]):
            c = SERIES[i % len(SERIES)]
            ns = [p["workers"] for p in s["points"]]
            xs = [p["throughput"] for p in s["points"]]
            ci = [p["throughput_ci"] or 0 for p in s["points"]]
            ax.errorbar(ns, xs, yerr=ci, color=c, linewidth=2, marker="o", markersize=6, capsize=3,
                        label=f"{s['taskMs']:g} ms task")
            if ns:
                ax.annotate(f"{s['taskMs']:g} ms", (ns[-1], xs[-1]), xytext=(6, 0), textcoords="offset points",
                            color=INK2, fontsize=9, va="center")
            fit = ceiling["usl"].get(str(s["taskMs"]))
            if fit and ns:
                grid = np.geomspace(min(ns), max(ns), 100)
                ax.plot(grid, usl(grid, fit["lambda"], fit["alpha"], fit["beta"]), color=c, linewidth=1,
                        linestyle="--", alpha=0.8)
        ax.set_xscale("log", base=2)
        ax.set_xticks(sorted({p["workers"] for s in ceiling["series"] for p in s["points"]}))
        ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        ax.set_ylim(bottom=0)
        _style(ax, f"Orchestration ceiling ({res['label']}): tasks/s vs worker loops",
               "worker loops (N, log scale)", "tasks/s (mean ± 95% CI; dashed = USL fit)")
        ax.legend(frameon=False, labelcolor=INK2)
        fig.tight_layout()
        fig.savefig(out / "ceiling.png")
        plt.close(fig)

    ol = res.get("openloop") or []
    if ol:
        fig, ax = plt.subplots(figsize=(8, 5), dpi=150)
        off = [_f(r["offered_rate"]) for r in ol]
        for i, (k, name) in enumerate((("p50_ms", "p50"), ("p99_ms", "p99"), ("p999_ms", "p99.9"))):
            ys = [_f(r[k]) for r in ol]
            ax.plot(off, ys, color=SERIES[i], linewidth=2, marker="o", markersize=6, label=name)
            ax.annotate(name, (off[-1], ys[-1]), xytext=(6, 0), textcoords="offset points", color=INK2, fontsize=9)
        ax.set_yscale("log")
        for r in ol:
            ax.annotate(f"{_f(r['fraction']):.0%}", (_f(r["offered_rate"]), ax.get_ylim()[0]), xytext=(0, 4),
                        textcoords="offset points", color=INK2, fontsize=8, ha="center")
        _style(ax, f"Open-loop latency vs offered load ({res['label']}, {ol[0]['task_ms']} ms tasks, "
                   f"{ol[0]['workers']} loops)", "offered load (tasks/s)", "latency from intended arrival (ms, log)")
        ax.legend(frameon=False, labelcolor=INK2)
        fig.tight_layout()
        fig.savefig(out / "openloop.png")
        plt.close(fig)

    rec = _read(out / "recovery.csv")
    tot = sorted(_f(r["total_ms"]) for r in rec if r.get("total_ms") not in (None, ""))
    if tot:
        fig, ax = plt.subplots(figsize=(8, 4), dpi=150)
        ys = np.arange(1, len(tot) + 1) / len(tot)
        ax.step(tot, ys, where="post", color=SERIES[0], linewidth=2)
        ax.plot(tot, ys, "o", color=SERIES[0], markersize=4)
        for q in (50, 95):
            v = pct(tot, q)
            ax.axvline(v, color=INK2, linewidth=1, linestyle=":")
            ax.annotate(f"p{q} {v / 1000:.2f} s", (v, 0.05 if q == 50 else 0.15), xytext=(4, 0),
                        textcoords="offset points", color=INK2, fontsize=9)
        ax.set_xlim(left=0)
        _style(ax, f"Recovery after SIGKILL ({res['label']}, {len(tot)} kills): kill → all tasks re-claimed",
               "ms", "fraction of kills")
        fig.tight_layout()
        fig.savefig(out / "recovery.png")
        plt.close(fig)


def _fmt(v, d=1) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "–"
    return f"{v:,.{d}f}"


def markdown(out: Path, res: dict) -> None:
    L = [f"# Orchestration ceiling — {res['label']}", "",
         f"Generated {res['generatedAt']} on {res['machine']}. Task source: **{res['mode']}** mode, fake handler "
         "(sleep for the task time, return no detections so every task finalises one image). "
         "Throughput is the steady state between the 10th and 90th percentile completion; "
         "± is the 95% t-interval over trials. Latency columns are lease time (claim-confirm → complete committed). "
         "The last column is the mean CPU of *other* projects' containers on the shared Docker VM during the point "
         "(100 = one core): the machine is shared, and a busy neighbour shows up here.",
         ""]
    if (out / "ceiling.png").exists():
        L += ["![ceiling](ceiling.png)", ""]
    for s in res["ceiling"]["series"]:
        L += [f"### {s['taskMs']:g} ms tasks", "",
              "| Worker loops | Trials | Tasks/s | ± 95% CI | Lease p50 (ms) | Lease p99 (ms) | Cycle (ms) | Overhead/task (ms) | Little's law X·W/N | Other containers' CPU (%) |",
              "|---|---|---|---|---|---|---|---|---|---|"]
        for p in s["points"]:
            L.append(f"| {p['workers']} | {p['trials']} | {_fmt(p['throughput'])} | {_fmt(p['throughput_ci'])} | "
                     f"{_fmt(p['p50Ms'], 2)} | {_fmt(p['p99Ms'], 2)} | {_fmt(p['cycleMs'], 2)} | "
                     f"{_fmt(p['overheadMs'], 2)} | {_fmt(p['littlesRatio'], 3)} | {_fmt(p.get('otherCpuPct'), 0)} |")
        L.append("")
    if res["ceiling"]["usl"]:
        L += ["### Universal Scalability Law fit", "",
              "X(N) = λN / (1 + α(N−1) + βN(N−1)), least squares over every trial; CIs from 500 bootstrap resamples.", "",
              "| Task (ms) | Points | λ (tasks/s per loop) | α (contention) | β (crosstalk) | R² | Peak N* | X(N*) |",
              "|---|---|---|---|---|---|---|---|"]
        for t, f in res["ceiling"]["usl"].items():
            ci = f.get("ci95", {})

            def c(k, d):
                return f"{_fmt(f[k], d)} [{_fmt(ci[k][0], d)}, {_fmt(ci[k][1], d)}]" if k in ci else _fmt(f[k], d)
            L.append(f"| {float(t):g} | {f['n_points']} | {c('lambda', 1)} | {c('alpha', 4)} | {c('beta', 6)} | "
                     f"{_fmt(f['r2'], 3)} | {_fmt(f['n_star'], 1)} | {_fmt(f['x_max'])} |")
        L.append("")
    o = res["overhead"]
    L += ["### Per-task overhead", "",
          f"- One worker loop, cycle time minus the measured handler time (claim → work → complete → next claim; "
          f"`time.sleep` overshoots by ~1.4 ms in these containers, so the handler's actual time is subtracted): "
          f"**{_fmt(o['perTaskMs'], 2)} ms** ± {_fmt(o['perTaskMsCi'], 2)} (all task times); by task time: "
          + ", ".join(f"{float(t):g} ms → {_fmt(v['mean'], 2)} ± {_fmt(v['ci95'], 2)}" for t, v in o["byTaskMs"].items()) + ".",
          f"- Inside the lease (claim-confirm committed → complete committed, minus task time), p50: "
          f"{_fmt(o['leaseOverheadP50Ms'], 2)} ms.",
          f"- Worker-reported timings (tasks.timings, p50 of the last run): {o['workerTimingsP50'] or 'not recorded by this code'}.",
          f"- Little's law (X × mean cycle ÷ N, should be 1.0 in a closed loop): mean {_fmt(res['littles']['meanRatio'], 3)}, "
          f"range {_fmt(res['littles']['min'], 3)}–{_fmt(res['littles']['max'], 3)}.", ""]
    ol = res.get("openloop") or []
    if ol:
        L += ["### Open loop (latency from intended arrival, no coordinated omission)", ""]
        if (out / "openloop.png").exists():
            L += ["![open loop](openloop.png)", ""]
        L += ["| Offered | Offered (tasks/s) | Achieved (tasks/s) | p50 (ms) | p90 (ms) | p99 (ms) | p99.9 (ms) | Max (ms) | Unfinished | Submit errors |",
              "|---|---|---|---|---|---|---|---|---|---|"]
        for r in ol:
            L.append(f"| {_f(r['fraction']):.0%} | {r['offered_rate']} | {r['achieved_rate']} | {r['p50_ms']} | {r['p90_ms']} | "
                     f"{r['p99_ms']} | {r['p999_ms']} | {r['max_ms']} | {r['incomplete']} | {r['submit_errors']} |")
        L += ["", "Where the median task spends its time (p50 of each hop, ms):", "",
              "| Offered | intended arrival → image row committed | → claimed (dispatch + queue) | → completed (lease) |",
              "|---|---|---|---|"]
        for r in ol:
            L.append(f"| {_f(r['fraction']):.0%} | {r.get('submit_p50_ms', '–')} | {r.get('wait_p50_ms', '–')} | "
                     f"{r.get('service_p50_ms', '–')} |")
        L.append("")
    rc = res["recovery"]
    if rc["samples"]:
        L += ["### Recovery after SIGKILL", ""]
        if (out / "recovery.png").exists():
            L += ["![recovery](recovery.png)", ""]
        L += [f"{rc['samples']} kills of busy fake-backend detector containers via `POST /workers/:id/kill` "
              f"({rc['idle']} more landed between tasks and are not counted). Detection via: {', '.join(rc['via']) or '–'}.", "",
              "| | p50 (ms) | p95 (ms) | max (ms) |", "|---|---|---|---|",
              f"| **kill → all tasks re-claimed** | **{_fmt(rc['p50Ms'], 0)}** | **{_fmt(rc['p95Ms'], 0)}** | **{_fmt(rc['maxMs'], 0)}** |"]
        for k, name in (("detect", "kill → worker marked dead"), ("requeue", "kill → tasks requeued"),
                        ("reclaim", "requeued → re-claimed")):
            d = rc.get(k)
            if d:
                L.append(f"| {name} | {_fmt(d['p50'], 0)} | {_fmt(d['p95'], 0)} | {_fmt(d['max'], 0)} |")
        L.append("")
    (out / "results.md").write_text("\n".join(L) + "\n")


# ---------------------------------------------------------------------------------------------
# benchmarks/summary.json (contract shape, docs/CONTRACTS.md "GET /benchmarks")
# ---------------------------------------------------------------------------------------------

def _num(v, d=1):
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return None
    return round(float(v), d)


def write_summary(bench_dir: Path = REPO / "benchmarks") -> dict:
    """Merges before/after ceiling results, the real-model sweep and the fault matrix."""
    def load(p: Path):
        return json.loads(p.read_text()) if p.exists() else None

    before = load(bench_dir / "ceiling" / "before" / "results.json")
    after = load(bench_dir / "ceiling" / "after" / "results.json")
    faults = (load(bench_dir / "faults" / "after" / "results.json") or load(bench_dir / "faults" / "before" / "results.json")
              or load(bench_dir / "faults" / "results.json"))
    cur = after or before

    ceiling = {"taskMs": [], "series": [], "usl": {"taskMs": 0, "lambda": None, "alpha": None, "beta": None}}
    if cur:
        ceiling["taskMs"] = [_num(t, 1) if t % 1 else int(t) for t in cur["ceiling"]["taskMs"]]
        ceiling["series"] = [
            {"taskMs": int(s["taskMs"]) if s["taskMs"] % 1 == 0 else s["taskMs"],
             "points": [{"workers": p["workers"], "throughput": _num(p["throughput"]),
                         "p50Ms": _num(p["p50Ms"], 2), "p99Ms": _num(p["p99Ms"], 2)} for p in s["points"]]}
            for s in cur["ceiling"]["series"]]
        fit = cur["ceiling"]["usl"].get("0.0") or cur["ceiling"]["usl"].get("0")
        if fit:
            ceiling["usl"] = {"taskMs": 0, "lambda": _num(fit["lambda"], 2), "alpha": _num(fit["alpha"], 5),
                              "beta": _num(fit["beta"], 7)}

    real = []
    for r in _read(bench_dir / "results.csv"):
        real.append({"detectors": int(r["detectors"]), "classifiers": int(r["classifiers"]),
                     "throughput": _num(_f(r["throughput_img_s"]), 2)})

    def rec(res):
        if not res or not res["recovery"]["samples"]:
            return {"p50Ms": None, "p95Ms": None, "samples": 0}
        r = res["recovery"]
        return {"p50Ms": _num(r["p50Ms"], 0), "p95Ms": _num(r["p95Ms"], 0), "samples": r["samples"]}

    def ovh(res):
        return {"perTaskMs": _num(res["overhead"]["perTaskMs"], 2) if res else None}

    summary = {
        "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "machine": (cur or {}).get("machine") or machine_description(),
        "ceiling": ceiling,
        "real": {"points": real},
        "recovery": {"before": rec(before), "after": rec(after)},
        "overhead": {"before": ovh(before), "after": ovh(after)},
        "faults": {"runs": faults["runs"], "faultsInjected": faults["faultsInjected"], "violations": faults["violations"]}
        if faults else {"runs": 0, "faultsInjected": 0, "violations": 0},
    }
    (bench_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def build(out: Path, label: str) -> dict:
    res = analyse(out, label)
    charts(out, res)
    markdown(out, res)
    return res
