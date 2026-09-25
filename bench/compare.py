"""benchmarks/ceiling/results.md: the before/after comparison across run directories."""

from __future__ import annotations

import json
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from report import INK2, SERIES, _fmt, _style  # noqa: E402


def _load(d: Path):
    p = d / "results.json"
    return json.loads(p.read_text()) if p.exists() else None


def write(root: Path) -> None:
    if not root.is_dir():
        return
    runs = [(d.name, r) for d in sorted(root.iterdir()) if d.is_dir() and (r := _load(d))]
    order = {"before": 0, "after": 1}
    runs.sort(key=lambda x: order.get(x[0], 2))
    if not runs:
        return
    L = ["# Orchestration ceiling: before vs after", "",
         "Each run directory has its own `results.md` with every point, CI, USL fit, open-loop table and recovery "
         "breakdown. Reproduce with `bench/run.py` (see docs/decisions/b-bench.md).", ""]

    # Throughput per (task time, N): one column per run.
    task_ms = sorted({s["taskMs"] for _, r in runs for s in r["ceiling"]["series"]})
    for t in task_ms:
        ns = sorted({p["workers"] for _, r in runs for s in r["ceiling"]["series"] if s["taskMs"] == t for p in s["points"]})
        L += [f"### Tasks/s at {t:g} ms tasks (mean ± 95% CI)", "",
              "| Worker loops | " + " | ".join(name for name, _ in runs) + " |",
              "|---|" + "---|" * len(runs)]
        for n in ns:
            cells = []
            for _, r in runs:
                p = next((p for s in r["ceiling"]["series"] if s["taskMs"] == t for p in s["points"] if p["workers"] == n), None)
                cells.append(f"{_fmt(p['throughput'])} ± {_fmt(p['throughput_ci'])}" if p else "–")
            L.append(f"| {n} | " + " | ".join(cells) + " |")
        L.append("")

    L += ["### Headline numbers", "", "| | " + " | ".join(n for n, _ in runs) + " |", "|---|" + "---|" * len(runs)]

    def row(name, f):
        L.append(f"| {name} | " + " | ".join(f(r) for _, r in runs) + " |")

    def peak(r, t):
        pts = [p for s in r["ceiling"]["series"] if s["taskMs"] == t for p in s["points"]]
        if not pts:
            return "–"
        p = max(pts, key=lambda p: p["throughput"] if not math.isnan(p["throughput"]) else -1)
        return f"{_fmt(p['throughput'])} (N={p['workers']})"

    for t in task_ms:
        row(f"Peak tasks/s, {t:g} ms tasks", lambda r, t=t: peak(r, t))

    def usl0(r):
        f = r["ceiling"]["usl"].get("0.0")
        return f"λ={_fmt(f['lambda'])}, α={_fmt(f['alpha'], 4)}, β={_fmt(f['beta'], 6)}" if f else "–"

    row("USL fit, 0 ms", usl0)
    row("Per-task overhead (ms, N=1)", lambda r: f"{_fmt(r['overhead']['perTaskMs'], 2)} ± {_fmt(r['overhead']['perTaskMsCi'], 2)}")
    row("Recovery p50 / p95 / max (ms)", lambda r: (f"{_fmt(r['recovery']['p50Ms'], 0)} / {_fmt(r['recovery']['p95Ms'], 0)} / "
                                                 f"{_fmt(r['recovery']['maxMs'], 0)} (n={r['recovery']['samples']})")
        if r["recovery"]["samples"] else "–")
    row("Little's law ratio (mean)", lambda r: _fmt(r["littles"]["meanRatio"], 3))
    row("Task source", lambda r: str(r.get("mode")))
    L.append("")

    fig, ax = plt.subplots(figsize=(8, 5), dpi=150)
    styles = ["--", "-", ":"]
    for ri, (name, r) in enumerate(runs):
        for si, s in enumerate(r["ceiling"]["series"]):
            ns = [p["workers"] for p in s["points"]]
            xs = [p["throughput"] for p in s["points"]]
            ax.plot(ns, xs, color=SERIES[si % 3], linestyle=styles[ri % 3], linewidth=2, marker="o", markersize=5,
                    label=f"{name}, {s['taskMs']:g} ms")
    ax.set_xscale("log", base=2)
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.set_ylim(bottom=0)
    _style(ax, "Orchestration ceiling: before vs after", "worker loops (N, log scale)", "tasks/s")
    ax.legend(frameon=False, labelcolor=INK2, fontsize=8)
    fig.tight_layout()
    fig.savefig(root / "ceiling_compare.png")
    plt.close(fig)
    L[4:4] = ["![before vs after](ceiling_compare.png)", ""]
    (root / "results.md").write_text("\n".join(L) + "\n")
