#!/usr/bin/env python3
"""Fault matrix: run fake-backend jobs while injecting a seeded schedule of faults, then check invariants.

    .venv/bin/python tests/invariants/faults.py --target . --runs 12 --seed 42

Each run: fresh state → one sample job (fake backend, workers routed through toxiproxy) →
`--faults-per-run` faults drawn from a seeded deck, one after another with seeded gaps →
wait for the job to finish → check every invariant in checker.py over that job's history.
The live sampler checks I5 (no stuck leases) once a second throughout.

Fault types:
  kill         SIGKILL a busy worker container (POST /workers/:id/kill), then replace it
  pause        `docker pause` a busy worker for 20 s (> LEASE_MS 15 s), then unpause: the
               SIGSTOP / GC-pause case fencing exists for; its late result must get 409
  latency      toxiproxy latency 1500 ± 500 ms for 10 s on worker→coordinator or worker→Redis
  reset        toxiproxy reset_peer for 5 s on worker→coordinator or worker→Redis
  minio        stop MinIO for 30 s, then start it
  redis        FLUSHALL + SIGKILL + start Redis (data loss; the dispatcher must rebuild the queues)
  coordinator  SIGKILL + start the coordinator container

The schedule (fault order, gaps, victims' positions) is a pure function of --seed; the exact
interleaving with the system is not, since Docker and the workers run in real time.
Output: <out>/results.json, <out>/results.md, <out>/runs/run-XX.json, and benchmarks/summary.json.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "bench"))
sys.path.insert(0, str(HERE))

import psycopg  # noqa: E402
import requests  # noqa: E402

from checker import LiveSampler, Violation, check_history, load_history  # noqa: E402
from stack import Stack, log  # noqa: E402

FAULTS = ["kill", "pause", "latency", "reset", "minio", "redis", "coordinator"]
TOXI = "http://localhost:38474"


def build_schedule(seed: int, runs: int, per_run: int, types: list[str] = FAULTS) -> list[list[dict]]:
    """Deck of fault types, reshuffled whenever it runs out, so every type appears evenly."""
    rng = random.Random(seed)
    deck: list[str] = []
    out = []
    for _ in range(runs):
        faults = []
        for _ in range(per_run):
            if not deck:
                deck = list(types)
                rng.shuffle(deck)
            faults.append({"type": deck.pop(), "gap_s": round(rng.uniform(4, 8), 1),
                           "pick": rng.random(), "proxy": rng.choice(["coordinator", "redis"])})
        out.append(faults)
    return out


class Toxiproxy:
    def __init__(self, url: str = TOXI) -> None:
        self.url = url
        self.http = requests.Session()

    def wait(self, timeout_s: float = 60) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                if self.http.get(self.url + "/version", timeout=2).ok:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.5)
        raise TimeoutError("toxiproxy API not reachable")

    def setup(self) -> None:
        for name, listen, upstream in (("coordinator", "0.0.0.0:8666", "coordinator:3000"),
                                       ("redis", "0.0.0.0:8679", "redis:6379")):
            self.http.delete(f"{self.url}/proxies/{name}", timeout=5)
            r = self.http.post(f"{self.url}/proxies", json={"name": name, "listen": listen, "upstream": upstream,
                                                            "enabled": True}, timeout=5)
            r.raise_for_status()

    def add(self, proxy: str, name: str, type_: str, attributes: dict) -> None:
        r = self.http.post(f"{self.url}/proxies/{proxy}/toxics", json={
            "name": name, "type": type_, "stream": "downstream", "toxicity": 1.0, "attributes": attributes}, timeout=5)
        r.raise_for_status()

    def clear(self) -> None:
        for proxy in ("coordinator", "redis"):
            try:
                toxics = self.http.get(f"{self.url}/proxies/{proxy}/toxics", timeout=5).json()
                for t in toxics:
                    self.http.delete(f"{self.url}/proxies/{proxy}/toxics/{t['name']}", timeout=5)
            except requests.RequestException:
                pass


class FaultRunner:
    def __init__(self, stack: Stack, args) -> None:
        self.stack = stack
        self.args = args
        self.toxi = Toxiproxy()
        self.env = {"FAKE_MODEL_DELAY_MS": str(args.fake_delay_ms)}
        self.sampler: LiveSampler | None = None
        self._local = threading.local()

    # ---------------------------------------------------------------- helpers

    def conn(self):
        c = getattr(self._local, "conn", None)
        if c is None or c.closed:
            c = psycopg.connect(self.stack.dsn, autocommit=True, connect_timeout=3)
            self._local.conn = c
        return c

    def api(self, method: str, path: str, body=None, tries: int = 30):
        """Retries only while the coordinator is unreachable (e.g. mid-restart). A POST is not
        retried after a timeout or an HTTP error, since it may already have taken effect."""
        for i in range(tries):
            try:
                return self.stack.api(method, path, body, timeout=10)
            except requests.ConnectionError:
                if i == tries - 1:
                    raise
            except requests.Timeout:
                if method != "GET" or i == tries - 1:
                    raise
            time.sleep(1)

    def busy_worker(self, pick: float) -> dict | None:
        busy = sorted((w for w in self.api("GET", "/workers")["workers"]
                       if w["status"] == "ALIVE" and w.get("currentTaskIds")), key=lambda w: w["id"])
        return busy[int(pick * len(busy))] if busy else None

    def diagnose(self, job: str) -> str:
        """Where are the unfinished tasks? (Postgres state + which Redis list holds each ID.)"""
        rows = self.stack.sql("""select t.id::text, t.state, t.stage, t.queued, t.lease_epoch, t.worker_id
                                   from tasks t join images i on i.id = t.image_id
                                  where i.job_id = %s and t.state not in ('SUCCEEDED', 'FAILED', 'CANCELLED')""", (job,))
        where: dict[str, list[str]] = {}
        for key in ["queue:detect", "queue:classify"] + self.stack.redis_cmd("--scan", "--pattern", "processing:*").split():
            for tid in self.stack.redis_cmd("LRANGE", key, "0", "-1").split():
                where.setdefault(tid, []).append(key)
        alive = {w["id"] for w in self.api("GET", "/workers")["workers"] if w["status"] == "ALIVE"}
        parts = []
        for tid, state, stage, queued, epoch, wid in rows[:10]:
            lists = where.get(tid, [])
            tag = ", ".join(f"{k} ({'ALIVE' if k.split(':', 1)[1] in alive else 'not alive'})" if k.startswith("processing:")
                            else k for k in lists) or "in no Redis list"
            parts.append(f"{tid[:8]} {stage} {state} queued={queued} epoch={epoch} → {tag}")
        # Keep the workers' last log lines as evidence (the containers are removed before the next run).
        tails = {}
        for svc in ("detector", "classifier"):
            for cid in self.stack.container_ids(svc):
                out = self.stack.docker("logs", "--tail", "15", cid)
                tails[cid[:12]] = (out.stdout + out.stderr).splitlines()[-15:]
        self.last_worker_logs = tails
        return f"{len(rows)} unfinished: " + "; ".join(parts)

    def restore_pool(self) -> None:
        self.stack.scale_workers(self.args.detectors, self.args.classifiers, **self.env)

    # ---------------------------------------------------------------- faults

    def inject(self, f: dict) -> dict:
        t = f["type"]
        rec = {"type": t, "start": time.time()}
        if t == "kill":
            w = self.busy_worker(f["pick"])
            if w is None:
                rec["skipped"] = "no busy worker"
            else:
                self.api("POST", f"/workers/{w['id']}/kill")
                rec["target"] = w["id"]
                time.sleep(1)
                self.stack.docker("rm", "-f", w["containerId"])
                self.restore_pool()
        elif t == "pause":
            w = self.busy_worker(f["pick"])
            if w is None:
                rec["skipped"] = "no busy worker"
            else:
                rec["target"] = w["id"]
                self.stack.docker("pause", w["containerId"])
                try:
                    time.sleep(self.args.pause_s)
                finally:
                    self.stack.docker("unpause", w["containerId"])
        elif t == "latency":
            rec["target"] = f["proxy"]
            self.toxi.add(f["proxy"], "lat", "latency", {"latency": 1500, "jitter": 500})
            try:
                time.sleep(10)
            finally:
                self.toxi.clear()
        elif t == "reset":
            rec["target"] = f["proxy"]
            self.toxi.add(f["proxy"], "rst", "reset_peer", {"timeout": 0})
            try:
                time.sleep(5)
            finally:
                self.toxi.clear()
        elif t == "minio":
            cid = self.stack.service_container("minio")
            self.stack.docker("stop", "-t", "1", cid)
            try:
                time.sleep(30)
            finally:
                self.stack.docker("start", cid)
        elif t == "redis":
            cid = self.stack.service_container("redis")
            self.stack.docker("exec", cid, "redis-cli", "FLUSHALL")
            self.stack.docker("kill", cid)
            time.sleep(1)
            self.stack.docker("start", cid)
        elif t == "coordinator":
            cid = self.stack.service_container("coordinator")
            self.sampler.suppress(120)
            self.stack.docker("kill", cid)
            time.sleep(1)
            self.stack.docker("start", cid)
            self.stack.wait_healthy(120)
            # Nothing can be reaped while the coordinator is down; its startup reconciliation then
            # gives every worker a fresh grace period. Resume I5 checks after that grace.
            self.sampler.suppressed_until = time.time() + 12
        rec["end"] = time.time()
        rec["duration_s"] = round(rec["end"] - rec["start"], 1)
        return rec

    # ---------------------------------------------------------------- one run

    def run(self, idx: int, faults: list[dict]) -> dict:
        a = self.args
        st = self.stack
        st.reset_state()
        self.toxi.clear()
        self.api("POST", "/admin/clear-cache")
        # Not through self.api: a retry after a client timeout would create a second job (the first
        # sample job on a fresh stack hashes and uploads every image, which can take a while).
        job = st.api("POST", "/jobs/sample", {"size": a.images, "countryCode": "TZA"}, timeout=600)["jobId"]
        self.restore_pool()
        st.wait_workers(a.detectors, a.classifiers)
        cfg = self.api("GET", "/config")
        self.sampler = LiveSampler(self.conn, worker_timeout_ms=int(cfg.get("workerTimeoutMs", 6000)))
        stop = threading.Event()

        def sample_loop():
            while not stop.is_set():
                self.sampler.sample()
                stop.wait(1.0)

        th = threading.Thread(target=sample_loop, daemon=True)
        th.start()
        injected = []
        t_start = time.time()
        try:
            time.sleep(5)
            for f in faults:
                rec = self.inject(f)
                injected.append(rec)
                log(f"  run {idx}: {rec['type']:<11} {rec.get('target', '') or ''} {rec.get('skipped', '')} ({rec['duration_s']} s)")
                time.sleep(f["gap_s"])
            faults_end = time.time()
            status = None
            while time.time() - faults_end < a.finish_timeout_s:
                try:
                    status = st.job(job)["status"]
                except Exception:
                    status = None
                if status in ("done", "cancelled"):
                    break
                time.sleep(1)
            finish_s = round(time.time() - faults_end, 1)
            time.sleep(2.5)  # let I5 see the settled state for a couple more samples
        finally:
            stop.set()
            th.join(timeout=5)
            self.toxi.clear()
            for cid in st.container_ids("detector") + st.container_ids("classifier"):
                st.docker("unpause", cid)

        hist = load_history(self.conn(), [job])
        res = check_history(hist)
        violations = res["violations"] + [v.as_dict() for v in self.sampler.violations.values()]
        if status != "done":
            violations.append(Violation("I7 job_finishes", job,
                                        f"job {status} {a.finish_timeout_s:.0f} s after the last fault; "
                                        + self.diagnose(job)).as_dict())
        stats = res["stats"]
        out = {"run": idx, "jobId": job, "images": a.images, "faults": injected,
               "faultsInjected": sum(1 for r in injected if not r.get("skipped")),
               "jobStatus": status, "finishAfterFaultsS": finish_s, "wallS": round(time.time() - t_start, 1),
               "stats": stats, "unverifiableCompletions": res["unverifiableCompletions"],
               "liveSamples": self.sampler.samples, "violations": violations,
               "workerLogs": getattr(self, "last_worker_logs", None) if status != "done" else None}
        self.last_worker_logs = None
        log(f"  run {idx}: job {status} {finish_s} s after faults; {len(violations)} violations; "
            f"stale_rejected {stats['staleRejected']}, reassigned {stats['reassigned']}, "
            f"failed images {stats['failedImages']}, re-executions {stats['reexecutions']}")
        if not (violations and self.args.keep_up):  # keep the evidence around when asked to
            st.remove_worker_containers()
        return out


def write_report(out: Path, runs: list[dict], args, machine: str) -> dict:
    total_faults = sum(r["faultsInjected"] for r in runs)
    total_viol = sum(len(r["violations"]) for r in runs)
    by_type: dict[str, int] = {}
    for r in runs:
        for f in r["faults"]:
            if not f.get("skipped"):
                by_type[f["type"]] = by_type.get(f["type"], 0) + 1
    by_inv: dict[str, int] = {}
    for r in runs:
        for v in r["violations"]:
            by_inv[v["invariant"]] = by_inv.get(v["invariant"], 0) + 1
    summary = {"runs": len(runs), "faultsInjected": total_faults, "violations": total_viol,
               "byFault": by_type, "byInvariant": by_inv, "seed": args.seed, "target": str(args.target),
               "project": args.project, "machine": machine,
               "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "totals": {k: sum(r["stats"][k] for r in runs) for k in
                          ("staleRejected", "reassigned", "leaseExpired", "failedImages", "reexecutions", "tasks", "images")},
               "runsDetail": [{k: r[k] for k in ("run", "faultsInjected", "jobStatus", "finishAfterFaultsS")}
                              | {"violations": len(r["violations"]), **r["stats"]} for r in runs]}
    (out / "results.json").write_text(json.dumps(summary, indent=2))
    L = [f"# Fault matrix — {Path(args.target).name if str(args.target) != str(REPO) else 'current code'}", "",
         f"Generated {summary['generatedAt']} on {machine}. Seed {args.seed}, {args.images}-image fake-backend sample job per run "
         f"({args.detectors} detectors + {args.classifiers} classifiers, {args.fake_delay_ms} ms fake model), "
         "workers routed through toxiproxy. Checker: tests/invariants/checker.py.", "",
         "| Runs | Faults injected | Violations |", "|---|---|---|",
         f"| {len(runs)} | {total_faults} | **{total_viol}** |", "",
         "Faults by type: " + ", ".join(f"{k} {v}" for k, v in sorted(by_type.items())) + ".", ""]
    if by_inv:
        L += ["Violations by invariant: " + ", ".join(f"{k}: {v}" for k, v in sorted(by_inv.items())) + ".", ""]
    L += ["| Run | Faults | Job | Finished (s after last fault) | stale_rejected (fenced) | reassigned | lease_expired | "
          "Failed images | Re-executions | Violations |", "|---|---|---|---|---|---|---|---|---|---|"]
    for r in runs:
        fl = ", ".join(f["type"] + (" (skipped)" if f.get("skipped") else "") for f in r["faults"])
        s = r["stats"]
        L.append(f"| {r['run']} | {fl} | {r['jobStatus']} | {r['finishAfterFaultsS']} | {s['staleRejected']} | "
                 f"{s['reassigned']} | {s['leaseExpired']} | {s['failedImages']} | {s['reexecutions']} | {len(r['violations'])} |")
    L.append("")
    viol = [(r["run"], v) for r in runs for v in r["violations"]]
    if viol:
        L += ["## Violations", "", "| Run | Invariant | Subject | Detail |", "|---|---|---|---|"]
        for run, v in viol[:200]:
            L.append(f"| {run} | {v['invariant']} | `{v['subject']}` | {v['detail']} |")
        if len(viol) > 200:
            L.append(f"| … | {len(viol) - 200} more in runs/*.json | | |")
        L.append("")
    L += ["Columns: *stale_rejected* = late results fenced off by the epoch check (409); *re-executions* = claims "
          "beyond one per finished task (work redone after a fault); *failed images* = images finalised `failed` "
          "after running out of attempts (terminal, so not a violation, but each is healthy work lost to an "
          "infrastructure fault)."]
    (out / "results.md").write_text("\n".join(L) + "\n")
    return summary


def write_index(root: Path) -> None:
    """benchmarks/faults/results.md: one row per code version that has been through the matrix."""
    rows = []
    for d in sorted(p for p in root.iterdir() if p.is_dir()):
        f = d / "results.json"
        if f.exists():
            data = json.loads(f.read_text())
            if "byInvariant" in data:
                rows.append((d.name, data))
    if not rows:
        return
    L = ["# Fault matrix", "",
         "Jepsen-lite: seeded fault schedules against a running fake-backend job, then an offline check of the "
         "coordinator's own history (tests/invariants/checker.py). Per-run tables are in each directory's results.md.", "",
         "| Code | Runs | Faults injected | Violations | Violations by invariant | Fenced late results | Failed images | Re-executions |",
         "|---|---|---|---|---|---|---|---|"]
    for name, s in rows:
        inv = ", ".join(f"{k}: {v}" for k, v in sorted(s["byInvariant"].items())) or "–"
        t = s["totals"]
        L.append(f"| [{name}]({name}/results.md) | {s['runs']} | {s['faultsInjected']} | **{s['violations']}** | {inv} | "
                 f"{t['staleRejected']} | {t['failedImages']} | {t['reexecutions']} |")
    (root / "results.md").write_text("\n".join(L) + "\n")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target", type=Path, default=REPO)
    ap.add_argument("--project")
    ap.add_argument("--worker-tag")
    ap.add_argument("--runs", type=int, default=12)
    ap.add_argument("--faults-per-run", type=int, default=5)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--images", type=int, default=1200)
    ap.add_argument("--detectors", type=int, default=3)
    ap.add_argument("--classifiers", type=int, default=2)
    ap.add_argument("--fake-delay-ms", type=int, default=200)
    ap.add_argument("--pause-s", type=float, default=20)
    ap.add_argument("--finish-timeout-s", type=float, default=300)
    ap.add_argument("--out", type=Path, help="default: benchmarks/faults/after for this repo, benchmarks/faults/before otherwise")
    ap.add_argument("--from-run", type=int, default=1, help="skip the schedule's first runs (to replay one)")
    ap.add_argument("--only", help=f"comma-separated subset of fault types ({','.join(FAULTS)})")
    ap.add_argument("--env", action="append", default=[], metavar="KEY=VALUE",
                    help="extra Compose variable, e.g. --env CLAIM_MODE=postgres (repeatable)")
    ap.add_argument("--no-build", action="store_true")
    ap.add_argument("--keep-up", action="store_true")
    ap.add_argument("--no-summary", action="store_true", help="don't update benchmarks/summary.json")
    args = ap.parse_args(argv)
    args.target = args.target.resolve()
    is_repo = args.target == REPO
    args.project = args.project or ("wb-b" if is_repo else "wb-before")
    stack = Stack(args.target, args.project, args.worker_tag or ("b" if is_repo else "before"),
                  compose_files=[REPO / "docker-compose.faults.yml"],
                  extra_env=dict(kv.split("=", 1) for kv in args.env))
    out = (args.out or (REPO / "benchmarks" / "faults" / ("after" if is_repo else "before"))).resolve()
    (out / "runs").mkdir(parents=True, exist_ok=True)
    from stack import machine_description

    runner = FaultRunner(stack, args)
    runs = []
    try:
        stack.up_infra(build=not args.no_build, services=("postgres", "redis", "minio", "coordinator", "toxiproxy"))
        runner.toxi.wait()
        runner.toxi.setup()
        schedule = build_schedule(args.seed, args.runs, args.faults_per_run,
                                  args.only.split(",") if args.only else FAULTS)
        for i, faults in enumerate(schedule, 1):
            if i < args.from_run:
                continue
            log(f"fault run {i}/{args.runs}: {', '.join(f['type'] for f in faults)}")
            r = runner.run(i, faults)
            runs.append(r)
            (out / "runs" / f"run-{i:02d}.json").write_text(json.dumps(r, indent=2, default=str))
            write_report(out, runs, args, machine_description())
    finally:
        if not args.keep_up:
            stack.down()
    summary = write_report(out, runs, args, machine_description())
    write_index(out.parent)
    if not args.no_summary:
        from report import write_summary

        write_summary()
    log(f"fault matrix: {summary['runs']} runs, {summary['faultsInjected']} faults, {summary['violations']} violations")
    return 0 if summary["violations"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
