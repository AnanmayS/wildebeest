"""Turn benchmarks/results.csv + extra.json into throughput.png and results.md."""
import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

OUT = Path(__file__).resolve().parent.parent / "benchmarks"

rows = list(csv.DictReader(open(OUT / "results.csv")))
extra = json.loads((OUT / "extra.json").read_text())
workers = [int(r["detectors"]) for r in rows]
throughput = [float(r["throughput_img_s"]) for r in rows]

fig, ax = plt.subplots(figsize=(7, 4.2), dpi=150)
base = throughput[0] / workers[0]
ax.plot(workers, [base * w for w in workers], linestyle="--", color="#9ca3af", label="linear scaling")
ax.plot(workers, throughput, marker="o", linewidth=2.5, color="#16a34a", label="measured")
for w, t in zip(workers, throughput):
    ax.annotate(f"{t:.1f}", (w, t), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=9)
ax.set_xlabel("Detector workers (classifiers ≈ 1 per 3 detectors)")
ax.set_ylabel("Throughput (images/sec)")
ax.set_title(f"ForgeGrid throughput vs workers ({rows[0]['images']} images, CPU)")
ax.set_xticks(workers)
ax.set_ylim(bottom=0)
ax.grid(alpha=0.25)
ax.spines[["top", "right"]].set_visible(False)
ax.legend(frameon=False)
fig.tight_layout()
fig.savefig(OUT / "throughput.png")

lines = [
    "# Benchmark results",
    "",
    f"{rows[0]['images']}-image Snapshot Serengeti sample, cache cleared before each run. "
    "Latency is per-image processing time (detect + classify task time), excluding queue wait.",
    "",
    "![Throughput vs workers](throughput.png)",
    "",
    "| Detectors | Classifiers | Total (s) | Throughput (img/s) | Speedup | p50 (ms) | p95 (ms) | Peak detector RSS (MiB) | Peak classifier RSS (MiB) | Min host RAM free |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
]
for r in rows:
    lines.append(
        f"| {r['detectors']} | {r['classifiers']} | {r['total_s']} | {r['throughput_img_s']} | {r['speedup']}× "
        f"| {r['p50_ms']} | {r['p95_ms']} | {r['peak_detector_mib']} | {r['peak_classifier_mib']} | {r.get('host_min_free_pct', '')}% |"
    )
lines += [
    "",
    f"- **Cache-hit rerun:** {extra['cacheHits']}/{extra['images']} images served from the content-hash cache "
    f"in {extra['cacheRerunMs'] / 1000:.2f} s.",
    f"- **Recovery after SIGKILL:** {extra['tasksReclaimed']} in-flight task(s) of a killed detector were reclaimed "
    f"by live workers {extra['recoveryMs'] / 1000:.1f} s after the kill "
    "(bounded by WORKER_TIMEOUT_MS = 6 s plus the 1 s reaper tick).",
    "",
]
(OUT / "results.md").write_text("\n".join(lines))
print(f"wrote {OUT / 'throughput.png'} and {OUT / 'results.md'}")
