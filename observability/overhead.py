"""Tracing overhead: the same synthetic workload with tracing off, on, and head-sampled.

Closed loop, fake handler, one swarm container of N worker loops (worker/wildebeest_worker/swarm.py,
the benchmark harness's fake worker; mounted read-only from --swarm). Per trial: truncate the
tables, recreate the coordinator with the configuration's OTEL_* env, start the swarm with the same
env, run a warm-up job, then a measured synthetic job. Trials are interleaved across
configurations (the order rotates each trial) so drift on the shared Docker VM hits all of them.

Measured per run:
  tasks/s        completions between the 10th and 90th percentile of finished_at
  overhead ms    N × 1000 / tasks/s − the swarm's measured mean handler time
  create ms      the POST /jobs/synthetic call (producer spans + traceparent writes when tracing is on)
  coord µs/task  coordinator CPU (cgroup usage_usec, both HA replicas) from job created to done ÷ tasks
  swarm µs/task  the same for the swarm container (all worker loops)
  lgtm µs/task   the same for the otel-lgtm container (collector + Tempo + Prometheus ingesting)
  spans/task     spans the collector accepted over the job ÷ tasks (lgtm's otelcol metrics)

Usage (stack already up with --profile observability, env as in docs/decisions/o-observability.md):
  python observability/overhead.py --swarm ../../worker/wildebeest_worker/swarm.py --trials 3
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import requests

REPO = Path(__file__).resolve().parents[1]
PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "wb-obs")
API = f"http://localhost:{os.environ.get('COORDINATOR_PORT', '53000')}"
DSN = f"postgresql://wildebeest:wildebeest@localhost:{os.environ.get('POSTGRES_HOST_PORT', '55432')}/wildebeest"
PROM = f"http://localhost:{os.environ.get('PROMETHEUS_PORT', '53390')}"

CONFIGS = {
    "off": {"OTEL_EXPORTER_OTLP_ENDPOINT": ""},
    "on": {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://lgtm:4318", "OTEL_TRACES_SAMPLER": "parentbased_always_on"},
    "sampled-10%": {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://lgtm:4318",
                    "OTEL_TRACES_SAMPLER": "parentbased_traceidratio", "OTEL_TRACES_SAMPLER_ARG": "0.1"},
    # Attribution: tracing in one tier only (the other keeps OTEL_EXPORTER_OTLP_ENDPOINT empty).
    "coordinator-only": {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://lgtm:4318", "_workers_off": "1"},
    "workers-only": {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://lgtm:4318", "_coordinator_off": "1"},
}
FAKE = {"MODEL_BACKEND": "fake", "DETECTOR_MODEL_VERSION": "fake-detector-v1",
        "CLASSIFIER_MODEL_VERSION": "fake-classifier-v1"}


def log(msg: str) -> None:
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def env_for(config: str) -> dict:
    e = dict(os.environ)
    e.update(FAKE)
    e.update({"OTEL_TRACES_SAMPLER": "parentbased_always_on", "OTEL_TRACES_SAMPLER_ARG": "1.0"})
    e.update({k: v for k, v in CONFIGS[config].items() if not k.startswith("_")})
    return e


def compose(*args: str, env: dict, check: bool = True) -> str:
    cmd = ["docker", "compose", "-p", PROJECT, "-f", str(REPO / "docker-compose.yml"), *args]
    out = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if check and out.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}: {out.stderr[-800:]}")
    return out.stdout


def container_ids(service: str) -> list[str]:
    out = subprocess.run(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={PROJECT}",
                          "--filter", f"label=com.docker.compose.service={service}",
                          "--filter", "label=com.docker.compose.oneoff=False"], capture_output=True, text=True)
    return out.stdout.split()


def container_id(service: str) -> str:
    return container_ids(service)[0]


# Coordinator replicas (HA: coordinator + coordinator-2 behind coordinator-lb); CPU is summed.
COORDINATORS = ["coordinator", "coordinator-2"]


def cpu_usec(container: str) -> int:
    out = subprocess.run(["docker", "exec", container, "cat", "/sys/fs/cgroup/cpu.stat"], capture_output=True, text=True)
    for line in out.stdout.splitlines():
        if line.startswith("usage_usec"):
            return int(line.split()[1])
    raise RuntimeError(f"no cpu.stat in {container}: {out.stderr}")


def accepted_spans() -> float:
    try:
        r = requests.get(f"{PROM}/api/v1/query", params={"query": "sum(otelcol_receiver_accepted_spans_total)"}, timeout=5)
        res = r.json()["data"]["result"]
        return float(res[0]["value"][1]) if res else 0.0
    except Exception:
        return float("nan")


def wait_healthy(timeout: float = 90) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{API}/healthz", timeout=2).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(0.5)
    raise RuntimeError("coordinator not healthy")


def db():
    return psycopg.connect(DSN, autocommit=True)


def truncate() -> None:
    with db() as c:
        c.execute("truncate jobs, images, tasks, detection_results, classification_results, workers, task_events "
                  "restart identity cascade")


def alive_workers() -> int:
    with db() as c:
        return c.execute("select count(*) from workers where status = 'ALIVE'").fetchone()[0]


def job_done(job_id: str, timeout: float) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with db() as c:
            left = c.execute("select count(*) from images where job_id = %s and final_category is null", (job_id,)).fetchone()[0]
        if left == 0:
            return
        time.sleep(0.25)
    raise RuntimeError(f"job {job_id} not done in {timeout}s")


def throughput(job_id: str) -> tuple[float, int]:
    with db() as c:
        rows = [r[0] for r in c.execute(
            "select extract(epoch from t.finished_at)::float8 from tasks t join images i on i.id = t.image_id "
            "where i.job_id = %s and t.state = 'SUCCEEDED' order by 1", (job_id,))]
    n = len(rows)
    lo, hi = int(n * 0.1), int(n * 0.9)
    return (hi - lo) / max(1e-9, rows[hi] - rows[lo]), n


def synthetic(count: int) -> str:
    r = requests.post(f"{API}/jobs/synthetic", json={"count": count, "stage": "detect"}, timeout=120)
    r.raise_for_status()
    return r.json()["jobId"]


def run_once(config: str, workers: int, task_ms: float, count: int, swarm: Path) -> dict:
    env = env_for(config)
    flags = CONFIGS[config]
    truncate()
    coord_env = dict(env, OTEL_EXPORTER_OTLP_ENDPOINT="") if flags.get("_coordinator_off") else env
    compose("up", "-d", "--no-deps", "--force-recreate", *COORDINATORS, env=coord_env)
    wait_healthy()
    name = f"{PROJECT}-ovh-swarm"
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    compose("run", "-d", "--no-deps", "--name", name, "-v", f"{swarm}:/app/wildebeest_worker/swarm.py:ro",
            "-e", f"SWARM_WORKERS={workers}", "-e", f"SWARM_TASK_MS={task_ms}", "-e", "SWARM_DETECTIONS=empty",
            "-e", "SWARM_NAME=ovh",
            *(["-e", "OTEL_EXPORTER_OTLP_ENDPOINT="] if flags.get("_workers_off") else []), "detector", "python", "-m", "wildebeest_worker.swarm", env=env)
    try:
        deadline = time.time() + 60
        while alive_workers() < workers:
            if time.time() > deadline:
                raise RuntimeError("swarm did not register")
            time.sleep(0.3)
        warm = synthetic(max(200, count // 20))
        job_done(warm, 120)
        coords = [c for svc in COORDINATORS for c in container_ids(svc)]
        lgtm = container_id("lgtm")
        coord_cpu = lambda: sum(cpu_usec(c) for c in coords)
        t0 = time.perf_counter()
        job = synthetic(count)  # tracing on: producer spans + traceparent writes happen here
        create_ms = (time.perf_counter() - t0) * 1000
        c0, s0, l0, sp0 = coord_cpu(), cpu_usec(name), cpu_usec(lgtm), accepted_spans()
        job_done(job, 600)
        c1, s1, l1 = coord_cpu(), cpu_usec(name), cpu_usec(lgtm)
        time.sleep(2.5)  # let the batch processors flush before reading the collector's counter
        sp1 = accepted_spans()
        tps, n = throughput(job)
    finally:
        subprocess.run(["docker", "stop", "-t", "15", name], capture_output=True)
        logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True).stdout
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    stats = {}
    for line in logs.splitlines():
        if line.startswith("SWARM_STATS "):
            stats = json.loads(line.removeprefix("SWARM_STATS "))
    handler_ms = stats.get("handlerMeanMs") or 0.0
    return {
        "config": config, "workers": workers, "taskMs": task_ms, "tasks": n,
        "throughput": round(tps, 1),
        "overheadMsPerTask": round(workers * 1000 / tps - handler_ms, 3),
        "handlerMeanMs": round(handler_ms, 3),
        "createMs": round(create_ms),
        "coordCpuUsPerTask": round((c1 - c0) / n, 1),
        "lgtmCpuUsPerTask": round((l1 - l0) / n, 1),
        "swarmCpuUsPerTask": round((s1 - s0) / n, 1),
        "spansPerTask": round((sp1 - sp0) / n, 2) if sp1 == sp1 and sp0 == sp0 else None,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--swarm", type=Path, required=True, help="path to worker/wildebeest_worker/swarm.py")
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--points", default="4x0:20000,16x0:40000,4x5:5000,16x5:15000",
                    help="workers x taskMs : tasks, comma separated")
    ap.add_argument("--configs", default="off,on,sampled-10%")
    ap.add_argument("--out", type=Path, default=REPO / "docs" / "observability" / "overhead-ha.json")
    args = ap.parse_args()
    swarm = args.swarm.resolve()
    points = []
    for p in args.points.split(","):
        wt, count = p.split(":")
        w, t = wt.split("x")
        points.append((int(w), float(t), int(count)))
    configs = args.configs.split(",")

    results = []
    for trial in range(args.trials):
        for workers, task_ms, count in points:
            order = configs[trial % len(configs):] + configs[:trial % len(configs)]
            for config in order:
                r = run_once(config, workers, task_ms, count, swarm)
                r["trial"] = trial + 1
                results.append(r)
                log(json.dumps(r))
                args.out.parent.mkdir(parents=True, exist_ok=True)
                args.out.write_text(json.dumps({"runs": results}, indent=1))

    # Summary: median of trials per (point, config).
    summary = []
    for workers, task_ms, _ in points:
        for config in configs:
            rs = [r for r in results if r["workers"] == workers and r["taskMs"] == task_ms and r["config"] == config]
            if not rs:
                continue
            med = lambda k: statistics.median(r[k] for r in rs if r[k] is not None) if any(r[k] is not None for r in rs) else None
            summary.append({"workers": workers, "taskMs": task_ms, "config": config,
                            "throughput": med("throughput"), "throughputTrials": [r["throughput"] for r in rs],
                            "overheadMsPerTask": med("overheadMsPerTask"), "coordCpuUsPerTask": med("coordCpuUsPerTask"),
                            "swarmCpuUsPerTask": med("swarmCpuUsPerTask"), "lgtmCpuUsPerTask": med("lgtmCpuUsPerTask"),
                            "createMs": med("createMs"), "spansPerTask": med("spansPerTask")})
    args.out.write_text(json.dumps({"summary": summary, "runs": results}, indent=1))
    for s in summary:
        print(s)


if __name__ == "__main__":
    sys.exit(main())
