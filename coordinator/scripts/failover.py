#!/usr/bin/env python3
"""Coordinator failover test (docs/decisions/h-ha.md).

Runs against a live Compose stack with two coordinator replicas behind coordinator-lb and fake
workers. Each run starts a synthetic job, injects one fault into the *leader* mid-job, and checks:

  kill       docker kill -s KILL the leader. Failover time = kill → the new leader's
             `leader_elected` event, which is written after its first full sweep (DB clock on both
             ends). The killed replica is started again afterwards and must rejoin as a follower.
  stop       docker stop (SIGTERM) the leader: it resigns and announces it, so a follower takes over
             in one election round instead of after the TTL. Started again afterwards.
  pause      docker pause the leader for LEASE + 3 s (past its lease), then unpause. A follower
             takes over; the woken leader's in-flight/next leader-only statements must all be
             rejected by the term guard (`leader_fenced` events > 0) and none may commit.
  terminate  pg_terminate_backend every Postgres connection of the leader (its election
             connection and its pool). It may keep or lose the lead; the job must not notice.

After every run: the job finishes, the invariant checker (tests/invariants/checker.py when
present) finds no violation in the job's history, and the stale-term audit finds no event
stamped with an older leader term after an event of a newer one (a write accepted from a
deposed leader).

Usage (Python with psycopg + requests, e.g. the repo's .venv):
  python coordinator/scripts/failover.py --runs 12 --scenarios kill,pause,terminate \
      --project wb-ha --api http://localhost:43000 --pg postgresql://wildebeest:wildebeest@localhost:45432/wildebeest
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import requests
from psycopg.rows import dict_row

SERVICES = {"coord-1": "coordinator", "coord-2": "coordinator-2"}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    i = min(len(s) - 1, max(0, math.ceil(p / 100 * len(s)) - 1))  # nearest rank
    return round(s[i], 1)


class Stack:
    def __init__(self, args):
        self.project = args.project
        self.api = args.api.rstrip("/")
        self.pg_url = args.pg
        self.conn = psycopg.connect(self.pg_url, autocommit=True, row_factory=dict_row)

    # ---- helpers -----------------------------------------------------------------------------

    def q(self, sql: str, params=None) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() if cur.description else []

    def one(self, sql: str, params=None) -> dict:
        return self.q(sql, params)[0]

    def container(self, holder: str) -> str:
        return f"{self.project}-{SERVICES[holder]}-1"

    def docker(self, *args: str, check: bool = True) -> str:
        r = subprocess.run(["docker", *args], capture_output=True, text=True)
        if check and r.returncode != 0:
            raise RuntimeError(f"docker {' '.join(args)}: {r.stderr.strip()}")
        return r.stdout.strip()

    def state(self, name: str) -> str:
        return self.docker("inspect", "-f", "{{.State.Status}}", name, check=False)

    def leader(self) -> dict:
        return self.one("select holder, instance, term, expires_at > now() as valid from coordinator_leader where id = 1")

    def db_now(self):
        return self.one("select clock_timestamp() as t")["t"]

    def wait(self, what: str, fn, timeout: float, every: float = 0.1):
        deadline = time.time() + timeout
        while True:
            v = fn()
            if v:
                return v
            if time.time() > deadline:
                raise TimeoutError(f"timed out waiting for {what}")
            time.sleep(every)

    # ---- stack state -------------------------------------------------------------------------

    def ensure_healthy(self) -> dict:
        """Both replicas running and heartbeating, a valid leader, the balancer seeing both."""
        for holder in SERVICES:
            name = self.container(holder)
            st = self.state(name)
            if st == "paused":
                self.docker("unpause", name)
            elif st in ("exited", "dead", "created"):
                self.docker("start", name)
        self.wait("two live replicas and a leader", lambda: (
            self.one("select count(distinct id)::int as n from coordinator_nodes where last_seen_at > now() - interval '2 seconds'")["n"] == 2
            and self.leader()["valid"]), 60, 0.25)
        self.wait("the API through the balancer", lambda: requests.get(f"{self.api}/healthz", timeout=2).ok, 30, 0.25)
        time.sleep(2.5)  # the balancer's health checks (rise 2 × 1 s) put a restarted replica back
        return self.leader()

    def start_job(self, count: int) -> str:
        r = requests.post(f"{self.api}/jobs/synthetic", json={"count": count}, timeout=30)
        r.raise_for_status()
        return r.json()["jobId"]

    def job_done(self, job_id: str) -> dict | None:
        row = self.q("select status from jobs where id = %s", (job_id,))
        return row[0] if row and row[0]["status"] == "done" else None

    def stale_term_writes(self) -> int:
        """Events stamped with a leader term lower than one already recorded before them."""
        return self.one(
            """select count(*)::int as n from (
                 select leader_term, max(leader_term) over (order by id rows between unbounded preceding and 1 preceding) as prev
                   from task_events where leader_term is not null) x
                where leader_term < prev""")["n"]

    def max_event_id(self) -> int:
        return self.one("select coalesce(max(id), 0)::bigint as m from task_events")["m"]


def load_checker(path: str | None):
    candidates = [path] if path else []
    here = Path(__file__).resolve()
    candidates += [str(here.parents[2] / "tests" / "invariants")]
    for c in candidates:
        if c and Path(c, "checker.py").exists():
            sys.path.insert(0, c)
            import checker  # type: ignore

            return checker
    return None


def run_once(stack: Stack, scenario: str, args, checker) -> dict:
    before = stack.ensure_healthy()
    old_term, holder = before["term"], before["holder"]
    target = stack.container(holder)
    start_id = stack.max_event_id()
    job = stack.start_job(args.tasks)
    time.sleep(args.fault_after)
    out: dict = {"scenario": scenario, "job": job, "leader": holder, "term": old_term}

    t0 = stack.db_now()
    if scenario == "kill":
        stack.docker("kill", "-s", "KILL", target)
    elif scenario == "stop":
        stack.docker("stop", "-t", "10", target)
    elif scenario == "pause":
        stack.docker("pause", target)
    elif scenario == "terminate":
        n = stack.one(
            "select count(pg_terminate_backend(pid))::int as n from pg_stat_activity where application_name = %s",
            (f"wildebeest-coordinator:{holder}",))["n"]
        out["terminatedBackends"] = n

    if scenario in ("kill", "stop", "pause"):
        elected = stack.wait("a new leader's first sweep", lambda: stack.q(
            """select at, detail from task_events where type = 'leader_elected' and id > %s
                and (detail->>'term')::bigint > %s order by id limit 1""", (start_id, old_term)), 30, 0.05)[0]
        out["failoverMs"] = round((elected["at"] - t0).total_seconds() * 1000, 1)
        out["newLeader"] = elected["detail"]["holder"]
        out["newTerm"] = elected["detail"]["term"]
        out["leaderlessMs"] = elected["detail"].get("leaderlessMs")
        out["reconcileMs"] = elected["detail"].get("reconcileMs")
        log(f"  {scenario}: {holder} (term {old_term}) → {out['newLeader']} (term {out['newTerm']}) in {out['failoverMs']} ms")
    if scenario == "pause":
        # Keep it frozen past its lease, then wake it: it must try, and be refused.
        remaining = args.pause_s - (time.time() - t0.timestamp())
        if remaining > 0:
            time.sleep(remaining)
        stack.docker("unpause", target)
        time.sleep(3)
        fenced = stack.q("select detail from task_events where type = 'leader_fenced' and id > %s", (start_id,))
        out["fencedAttempts"] = len(fenced)
        log(f"  pause: woke {holder} after {args.pause_s:.0f}s; {len(fenced)} leader-only statement(s) rejected by the term guard")
    if scenario == "terminate":
        time.sleep(3)
        after = stack.leader()
        out["termAfter"] = after["term"]
        out["leaderAfter"] = after["holder"]
        log(f"  terminate: {out['terminatedBackends']} backends of {holder} killed; leader now {after['holder']} term {after['term']}")

    stack.wait(f"job {job} to finish", lambda: stack.job_done(job), args.job_timeout, 0.5)
    out["jobMs"] = round((stack.one("select finished_at - created_at as d from jobs where id = %s", (job,))["d"]).total_seconds() * 1000)
    if scenario in ("kill", "stop"):
        stack.docker("start", target)

    out["staleTermWrites"] = stack.stale_term_writes()
    if checker is not None:
        with psycopg.connect(stack.pg_url) as c:
            h = checker.load_history(c, [job])
        # The checker understands speculative copies (`speculated` events) itself.
        report = checker.check_history(h)
        out["violations"] = report["violations"]
        out["stats"] = {k: report["stats"][k] for k in ("tasks", "failedImages", "staleRejected", "reassigned", "leaseExpired", "reexecutions")}
    failed = stack.one(
        """select count(*) filter (where i.final_category = 'failed')::int as failed, count(*)::int as images
             from images i where i.job_id = %s""", (job,))
    out.update(failedImages=failed["failed"], images=failed["images"])
    ok = out["staleTermWrites"] == 0 and not out.get("violations") and failed["failed"] == 0
    out["ok"] = ok
    log(f"  job done in {out['jobMs']} ms; stale-term writes {out['staleTermWrites']}, "
        f"violations {len(out.get('violations', []))}, failed images {failed['failed']} → {'OK' if ok else 'FAIL'}")
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--runs", type=int, default=10, help="runs per scenario")
    p.add_argument("--scenarios", default="kill,pause,terminate")
    p.add_argument("--tasks", type=int, default=1500, help="synthetic tasks per run's job")
    p.add_argument("--fault-after", type=float, default=2.0, help="seconds into the job before the fault")
    p.add_argument("--pause-s", type=float, default=8.0, help="how long the leader stays frozen (> lease TTL)")
    p.add_argument("--job-timeout", type=float, default=180)
    p.add_argument("--project", default=os.environ.get("COMPOSE_PROJECT_NAME", "wb-ha"))
    p.add_argument("--api", default=f"http://localhost:{os.environ.get('COORDINATOR_PORT', '43000')}")
    p.add_argument("--pg", default=f"postgresql://wildebeest:wildebeest@localhost:{os.environ.get('POSTGRES_HOST_PORT', '45432')}/wildebeest")
    p.add_argument("--checker", default=None, help="directory containing checker.py")
    p.add_argument("--out", default=None, help="write the per-run results and summary as JSON here")
    args = p.parse_args()

    checker = load_checker(args.checker)
    log(f"invariant checker: {'tests/invariants/checker.py' if checker else 'not found (built-in checks only)'}")
    stack = Stack(args)
    results = []
    scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    for i in range(args.runs):
        for s in scenarios:
            log(f"run {i + 1}/{args.runs} · {s}")
            try:
                results.append(run_once(stack, s, args, checker))
            except Exception as e:  # a run that couldn't finish is a failure, not a crash
                log(f"  ERROR: {e}")
                results.append({"scenario": s, "ok": False, "error": str(e)})

    summary: dict = {"runs": len(results), "ok": sum(1 for r in results if r.get("ok")), "scenarios": {}}
    for s in scenarios:
        rs = [r for r in results if r["scenario"] == s]
        fo = [r["failoverMs"] for r in rs if "failoverMs" in r]
        entry = {
            "runs": len(rs),
            "ok": sum(1 for r in rs if r.get("ok")),
            "failoverMs": {"p50": pct(fo, 50), "p95": pct(fo, 95), "min": min(fo) if fo else None, "max": max(fo) if fo else None,
                           "mean": round(statistics.mean(fo), 1) if fo else None},
            "staleTermWrites": sum(r.get("staleTermWrites", 0) for r in rs),
            "violations": sum(len(r.get("violations", [])) for r in rs),
            "failedImages": sum(r.get("failedImages", 0) for r in rs),
            "jobMs": {"p50": pct([r["jobMs"] for r in rs if "jobMs" in r], 50)},
        }
        if s == "pause":
            fa = [r.get("fencedAttempts", 0) for r in rs]
            entry["fencedAttempts"] = {"total": sum(fa), "runsWithAttempts": sum(1 for x in fa if x > 0)}
        if s == "terminate":
            entry["leadershipChanged"] = sum(1 for r in rs if r.get("termAfter") not in (None, r.get("term")))
        summary["scenarios"][s] = entry
    print(json.dumps(summary, indent=2, default=str))
    if args.out:
        Path(args.out).write_text(json.dumps({"summary": summary, "results": results}, indent=2, default=str))
    sys.exit(0 if summary["ok"] == summary["runs"] else 1)


if __name__ == "__main__":
    main()
