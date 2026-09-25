#!/usr/bin/env python3
"""Orchestration-ceiling benchmark: one command per code version.

    .venv/bin/python bench/run.py --target <checkout> --mode synthetic --out benchmarks/ceiling/after

Brings up <checkout>'s Compose stack as its own project (fake backend, benchmark ports), runs
  ceiling   closed-loop burn-down at every (task time, N) with ≥ 3 trials   → ceiling.csv
  openloop  fixed arrival rates at fractions of the measured max             → openloop.csv
  recovery  ≥ 20 SIGKILLs of busy worker containers                         → recovery.csv
then writes charts, results.md and results.json into --out, regenerates benchmarks/summary.json
and benchmarks/ceiling/results.md (before vs after), and tears the stack down.

`--mode sample` works on any version (sample jobs + /admin/clear-cache); `--mode synthetic` needs
POST /jobs/synthetic. `--mode auto` picks synthetic when the endpoint exists.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ceiling import CeilingConfig, load_rows, run_ceiling  # noqa: E402
from openloop import OpenLoopConfig, run_openloop  # noqa: E402
from recovery import RecoveryConfig, run_recovery  # noqa: E402
from report import build, write_summary  # noqa: E402
from stack import REPO, Stack, log  # noqa: E402
import compare  # noqa: E402


def floats(s: str) -> tuple[float, ...]:
    return tuple(float(x) for x in s.split(",") if x)


def ints(s: str) -> tuple[int, ...]:
    return tuple(int(x) for x in s.split(",") if x)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target", type=Path, default=REPO, help="checkout whose docker-compose.yml to run (default: this repo)")
    ap.add_argument("--project", help="Compose project name (default: wb-b for this repo, wb-before otherwise)")
    ap.add_argument("--worker-tag", help="WORKER_TAG for the worker image (default: b / before)")
    ap.add_argument("--label", help="name used in charts and summary (default: basename of --out)")
    ap.add_argument("--mode", choices=["sample", "synthetic", "auto"], default="auto")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--suites", default="ceiling,openloop,recovery")
    ap.add_argument("--task-ms", type=floats, default=(0, 5, 50))
    ap.add_argument("--workers", type=ints, default=(1, 2, 4, 8, 16, 32, 64))
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--duration", type=float, default=15, help="target seconds per ceiling point")
    ap.add_argument("--max-tasks", type=int, default=None, help="cap on tasks per point (default 20k sample / 200k synthetic)")
    ap.add_argument("--per-container", type=int, default=8, help="worker loops per swarm container")
    ap.add_argument("--ol-task-ms", type=float, default=5)
    ap.add_argument("--ol-workers", type=int, default=None, help="default: N with the best throughput at --ol-task-ms")
    ap.add_argument("--ol-fractions", type=floats, default=(0.5, 0.8, 1.0, 1.1))
    ap.add_argument("--ol-duration", type=float, default=20)
    ap.add_argument("--kills", type=int, default=22)
    ap.add_argument("--recovery-delay-ms", type=int, default=300)
    ap.add_argument("--quiet-cpu", type=float, default=150,
                    help="before each point, wait until other projects' containers use less than this much CPU "
                         "(%% of one core); points run while they averaged > 1.5x this are repeated (up to --retries)")
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--resume", action="store_true", help="keep ceiling.csv rows already in --out and run only the missing points")
    ap.add_argument("--env", action="append", default=[], metavar="KEY=VALUE",
                    help="extra Compose variable, e.g. --env CLAIM_MODE=postgres (repeatable)")
    ap.add_argument("--no-build", action="store_true", help="don't rebuild images")
    ap.add_argument("--keep-up", action="store_true", help="leave the stack running afterwards")
    ap.add_argument("--report-only", action="store_true", help="only rebuild charts/results from existing CSVs")
    args = ap.parse_args(argv)

    out = args.out if args.out.is_absolute() else (Path.cwd() / args.out)
    out = out.resolve()
    label = args.label or out.name
    suites = set(args.suites.split(","))
    if args.report_only:
        build(out, label)
        write_summary()
        compare.write(REPO / "benchmarks" / "ceiling")
        return 0

    target = args.target.resolve()
    is_repo = target == REPO
    stack = Stack(target, args.project or ("wb-b" if is_repo else "wb-before"),
                  args.worker_tag or ("b" if is_repo else "before"),
                  extra_env=dict(kv.split("=", 1) for kv in args.env))
    started = time.time()
    try:
        stack.up_infra(build=not args.no_build)
        if not args.no_build:
            stack.compose("build", "detector")
        mode = args.mode
        if mode == "auto":
            mode = "synthetic" if stack.has_endpoint("POST", "/jobs/synthetic", {"count": 0}) else "sample"
        log(f"mode: {mode}")
        max_tasks = args.max_tasks or (20000 if mode == "sample" else 200000)

        if "ceiling" in suites:
            run_ceiling(stack, CeilingConfig(mode=mode, task_ms=args.task_ms, workers=args.workers, trials=args.trials,
                                             duration=args.duration, max_tasks=max_tasks,
                                             per_container=args.per_container, quiet_cpu_pct=args.quiet_cpu,
                                             retries=args.retries, resume=args.resume), out)
        if "openloop" in suites:
            rows = [r for r in load_rows(out / "ceiling.csv") if float(r["task_ms"]) == args.ol_task_ms and r["throughput"]]
            by_n: dict[int, list[float]] = {}
            for r in rows:
                by_n.setdefault(int(r["workers"]), []).append(float(r["throughput"]))
            if not by_n:
                log("open loop skipped: no closed-loop result at that task time")
            else:
                best_n = max(by_n, key=lambda n: sum(by_n[n]) / len(by_n[n]))
                x_max = max(sum(v) / len(v) for v in by_n.values())
                n = args.ol_workers or best_n
                log(f"open loop: {args.ol_task_ms} ms tasks, {n} loops, max {x_max:.1f} tasks/s")
                run_openloop(stack, OpenLoopConfig(mode=mode, task_ms=args.ol_task_ms, workers=n,
                                                   fractions=args.ol_fractions, duration=args.ol_duration), x_max, out,
                             quiet_cpu_pct=args.quiet_cpu)
        if "recovery" in suites:
            run_recovery(stack, RecoveryConfig(kills=args.kills, fake_delay_ms=args.recovery_delay_ms), out)
    finally:
        if not args.keep_up:
            stack.down()
    build(out, label)
    write_summary()
    compare.write(REPO / "benchmarks" / "ceiling")
    log(f"done in {(time.time() - started) / 60:.1f} min → {out}/results.md, benchmarks/summary.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
