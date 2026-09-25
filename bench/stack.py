"""Driving one Wildebeest Compose stack from the host: compose, HTTP API, Postgres, Docker.

Every harness in bench/ and tests/invariants/ talks to the stack through `Stack`, so the same
code runs against the frozen "before" checkout and the live repository. Only the Compose
project name and the checkout directory differ.
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import psycopg
import requests

REPO = Path(__file__).resolve().parents[1]
SWARM_PY = REPO / "worker" / "wildebeest_worker" / "swarm.py"

# Ports agreed for the benchmark projects (wb-b and wb-before; only one runs at a time).
PORTS = {
    "POSTGRES_HOST_PORT": "35432",
    "REDIS_HOST_PORT": "36379",
    "MINIO_PORT": "39000",
    "MINIO_CONSOLE_PORT": "39001",
    "COORDINATOR_PORT": "33000",
    "DASHBOARD_PORT": "38080",
}
FAKE_ENV = {
    "MODEL_BACKEND": "fake",
    "DETECTOR_MODEL_VERSION": "fake-detector-v1",
    "CLASSIFIER_MODEL_VERSION": "fake-classifier-v1",
}


def log(msg: str) -> None:
    print(time.strftime("%H:%M:%S"), msg, flush=True)


class ApiError(RuntimeError):
    def __init__(self, status: int, text: str, what: str):
        super().__init__(f"{what} -> HTTP {status}: {text[:300]}")
        self.status = status


@dataclass
class Stack:
    target: Path
    project: str
    worker_tag: str
    extra_env: dict[str, str] = field(default_factory=dict)
    compose_files: list[Path] = field(default_factory=list)  # overrides on top of target/docker-compose.yml

    def __post_init__(self) -> None:
        self.target = Path(self.target).resolve()
        self.api_url = f"http://localhost:{PORTS['COORDINATOR_PORT']}"
        self.dsn = f"postgresql://wildebeest:wildebeest@localhost:{PORTS['POSTGRES_HOST_PORT']}/wildebeest"
        self.http = requests.Session()
        self._db: psycopg.Connection | None = None
        self._swarm_seq = 0

    # ------------------------------------------------------------------ compose

    def env(self, **extra: str) -> dict[str, str]:
        e = dict(os.environ)
        e.update(PORTS)
        e.update(FAKE_ENV)
        e["COMPOSE_PROJECT_NAME"] = self.project
        e["WORKER_TAG"] = self.worker_tag
        e.update(self.extra_env)
        e.update({k: str(v) for k, v in extra.items()})
        return e

    def compose_args(self) -> list[str]:
        args = ["docker", "compose", "-p", self.project, "-f", str(self.target / "docker-compose.yml")]
        for f in self.compose_files:
            args += ["-f", str(Path(f).resolve())]
        return args

    def compose(self, *args: str, env: dict[str, str] | None = None, check: bool = True, quiet: bool = True,
                timeout: float | None = 900) -> subprocess.CompletedProcess:
        cmd = self.compose_args() + list(args)
        res = subprocess.run(cmd, cwd=self.target, env=env or self.env(), capture_output=quiet, text=True, timeout=timeout)
        if check and res.returncode != 0:
            raise RuntimeError(f"{' '.join(cmd)} failed ({res.returncode}):\n{res.stdout}\n{res.stderr}")
        return res

    def up_infra(self, build: bool = True, services: Iterable[str] = ("postgres", "redis", "minio", "coordinator")) -> None:
        log(f"[{self.project}] starting {', '.join(services)} from {self.target}")
        self.compose("up", "-d", *(["--build"] if build else []), *services)
        self.wait_healthy()

    def wait_healthy(self, timeout_s: float = 180) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                if self.http.get(self.api_url + "/healthz", timeout=2).ok:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.5)
        raise TimeoutError(f"coordinator at {self.api_url} not healthy after {timeout_s}s")

    def down(self) -> None:
        log(f"[{self.project}] docker compose down -v")
        self.stop_swarms()
        self.compose("down", "-v", "--remove-orphans", check=False)

    def scale_workers(self, detect: int, classify: int, **env: str) -> None:
        """Scales the regular (fake-backend) worker services, without recreating running ones."""
        self.compose("up", "-d", "--no-deps", "--no-recreate", "--scale", f"detector={detect}",
                     "--scale", f"classifier={classify}", "detector", "classifier", env=self.env(**env))

    def container_ids(self, service: str) -> list[str]:
        out = subprocess.run(
            ["docker", "ps", "-q", "--no-trunc", "--filter", f"label=com.docker.compose.project={self.project}",
             "--filter", f"label=com.docker.compose.service={service}",
             "--filter", "label=com.docker.compose.oneoff=False"],
            capture_output=True, text=True, check=True).stdout
        return [l for l in out.split() if l]

    def service_container(self, service: str) -> str:
        ids = self.container_ids(service)
        if not ids:
            raise RuntimeError(f"no running container for {service}")
        return ids[0]

    def remove_worker_containers(self) -> None:
        for svc in ("detector", "classifier"):
            ids = subprocess.run(
                ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={self.project}",
                 "--filter", f"label=com.docker.compose.service={svc}"],
                capture_output=True, text=True).stdout.split()
            if ids:
                subprocess.run(["docker", "rm", "-f", *ids], capture_output=True)

    # ------------------------------------------------------------------ swarm

    def start_swarm(self, workers: int, task_ms: float, stage: str = "detect", per_container: int = 8,
                    detections: str = "empty", fetch: str = "none") -> list[str]:
        """Starts `workers` fake worker loops spread over ceil(workers/per_container) containers."""
        names = []
        remaining = workers
        while remaining > 0:
            k = min(per_container, remaining)
            remaining -= k
            self._swarm_seq += 1
            name = f"{self.project}-swarm-{self._swarm_seq}"
            self.compose(
                "run", "-d", "--no-deps", "--name", name,
                "-v", f"{SWARM_PY}:/app/wildebeest_worker/swarm.py:ro",
                "-e", f"WORKER_STAGE={stage}", "-e", f"SWARM_WORKERS={k}", "-e", f"SWARM_TASK_MS={task_ms}",
                "-e", f"SWARM_DETECTIONS={detections}", "-e", f"SWARM_FETCH={fetch}", "-e", f"SWARM_NAME=sw{self._swarm_seq}",
                "detector", "python", "-m", "wildebeest_worker.swarm",
            )
            names.append(name)
        return names

    def stop_swarms(self, names: list[str] | None = None, timeout_s: int = 15) -> dict:
        """Stops swarm containers (SIGTERM: loops finish, deregister) and returns their merged
        SWARM_STATS: {"tasks": n, "handlerMeanMs": actual mean handler time}."""
        if names is None:
            out = subprocess.run(["docker", "ps", "-aq", "--filter", f"name={self.project}-swarm-"],
                                 capture_output=True, text=True).stdout.split()
            names = out
        tasks, total = 0, 0.0
        if names:
            subprocess.run(["docker", "stop", "-t", str(timeout_s), *names], capture_output=True)
            for n in names:
                logs = subprocess.run(["docker", "logs", "--tail", "20", n], capture_output=True, text=True).stdout
                for line in logs.splitlines():
                    if line.startswith("SWARM_STATS "):
                        st = json.loads(line[len("SWARM_STATS "):])
                        if st.get("handlerMeanMs") is not None:
                            tasks += st["tasks"]
                            total += st["tasks"] * st["handlerMeanMs"]
            subprocess.run(["docker", "rm", "-f", *names], capture_output=True)
        return {"tasks": tasks, "handlerMeanMs": total / tasks if tasks else None}

    # ------------------------------------------------------------------ HTTP

    def api(self, method: str, path: str, body: Any = None, timeout: float = 120, **kw) -> Any:
        resp = self.http.request(method, self.api_url + path, json=body, timeout=timeout, **kw)
        if not resp.ok:
            raise ApiError(resp.status_code, resp.text, f"{method} {path}")
        return resp.json() if resp.content else None

    def has_endpoint(self, method: str, path: str, body: Any = None) -> bool:
        try:
            r = self.http.request(method, self.api_url + path, json=body, timeout=10)
            return r.status_code != 404
        except requests.RequestException:
            return False

    def workers(self) -> list[dict]:
        return self.api("GET", "/workers")["workers"]

    def alive_workers(self, stage: str | None = None) -> list[dict]:
        return [w for w in self.workers() if w["status"] == "ALIVE" and (stage is None or w["stage"] == stage)]

    def wait_workers(self, detect: int, classify: int = 0, timeout_s: float = 120) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                alive = self.alive_workers()
                if (sum(w["stage"] == "detect" for w in alive) >= detect
                        and sum(w["stage"] == "classify" for w in alive) >= classify):
                    return
            except (requests.RequestException, ApiError):
                pass
            time.sleep(0.5)
        raise TimeoutError(f"waiting for {detect} detect + {classify} classify workers")

    def job(self, job_id: str) -> dict:
        return self.api("GET", f"/jobs/{job_id}")

    def cancel_running_jobs(self) -> None:
        try:
            for j in self.api("GET", "/jobs")["jobs"]:
                if j["status"] == "running":
                    self.api("POST", f"/jobs/{j['id']}/cancel")
        except (requests.RequestException, ApiError):
            pass

    def submit_tasks(self, count: int, mode: str) -> list[str]:
        """Creates `count` detect tasks: one synthetic job, or sample jobs of at most 2,000 images."""
        if mode == "synthetic":
            return [self.api("POST", "/jobs/synthetic", {"count": count, "stage": "detect"}, timeout=600)["jobId"]]
        jobs = []
        left = count
        while left > 0:
            k = min(2000, left)
            jobs.append(self.api("POST", "/jobs/sample", {"size": k, "countryCode": "TZA"}, timeout=600)["jobId"])
            left -= k
        return jobs

    def upload_job(self, names: list[str], payloads: list[bytes], timeout: float = 30) -> str:
        files = [("files", (n, p, "image/jpeg")) for n, p in zip(names, payloads)]
        resp = self.http.post(self.api_url + "/jobs", files=files, timeout=timeout)
        if not resp.ok:
            raise ApiError(resp.status_code, resp.text, "POST /jobs")
        return resp.json()["jobId"]

    # ------------------------------------------------------------------ Postgres

    def db(self) -> psycopg.Connection:
        if self._db is None or self._db.closed:
            self._db = psycopg.connect(self.dsn, autocommit=True)
        return self._db

    def sql(self, q: str, params: Any = None) -> list[tuple]:
        for attempt in range(3):
            try:
                with self.db().cursor() as cur:
                    cur.execute(q, params)
                    return cur.fetchall() if cur.description else []
            except psycopg.OperationalError:
                self._db = None
                if attempt == 2:
                    raise
                time.sleep(1)
        return []

    def sql_dicts(self, q: str, params: Any = None) -> list[dict]:
        with self.db().cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(q, params)
            return cur.fetchall()

    def columns(self, table: str) -> set[str]:
        return {r[0] for r in self.sql(
            "select column_name from information_schema.columns where table_schema='public' and table_name=%s", (table,))}

    def clock_offset_s(self, samples: int = 15) -> float:
        """db_clock - host_clock, from the lowest-RTT of a few round trips (Postgres runs in the Docker VM)."""
        best = None
        for _ in range(samples):
            t0 = time.time()
            (db_t,) = self.sql("select extract(epoch from clock_timestamp())::float8")[0]
            t1 = time.time()
            if best is None or (t1 - t0) < best[0]:
                best = (t1 - t0, db_t - (t0 + t1) / 2)
        return best[1]

    def reset_state(self) -> None:
        """Between runs: no running jobs, no leftover workers, empty task tables and queues.

        Equivalent to a fresh stack, without paying for a restart. Only harness-owned projects
        are ever reset like this.
        """
        self.cancel_running_jobs()
        self.stop_swarms()
        self.remove_worker_containers()
        tables = [t for t in ("task_events", "tasks", "images", "jobs", "detection_results", "classification_results")
                  if self.sql("select to_regclass(%s)", (f"public.{t}",))[0][0]]
        self.sql(f"truncate {', '.join(tables)} cascade")
        self.sql("delete from workers")
        self.sql("vacuum analyze")
        self.redis_cmd("DEL", "queue:detect", "queue:classify", "wildebeest:throttled")
        for key in self.redis_cmd("--scan", "--pattern", "processing:*").split():
            self.redis_cmd("DEL", key)

    # ------------------------------------------------------------------ Docker

    def redis_cmd(self, *args: str) -> str:
        cid = self.service_container("redis")
        return subprocess.run(["docker", "exec", cid, "redis-cli", *args], capture_output=True, text=True).stdout

    def docker(self, *args: str, check: bool = False, timeout: float = 120) -> subprocess.CompletedProcess:
        return subprocess.run(["docker", *args], capture_output=True, text=True, check=check, timeout=timeout)


class ContentionMonitor:
    """Samples, in the background, how busy the machine is with things that are not this project.

    The Docker VM and the host are shared with other projects, and a busy neighbour distorts
    every number here, so each benchmark point records the mean CPU (in % of one core) of other
    containers and the host's 1-minute load average while it ran.
    """

    def __init__(self, project: str, every_s: float = 3.0) -> None:
        import threading

        self.project = project
        self.every_s = every_s
        self.samples: list[tuple[float, float, float]] = []  # (t, other_cpu_pct, own_cpu_pct)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}"],
                                     capture_output=True, text=True, timeout=20).stdout
                other = own = 0.0
                for line in out.splitlines():
                    name, _, cpu = line.partition("\t")
                    v = float(cpu.strip().rstrip("%") or 0)
                    if name.startswith(self.project + "-"):
                        own += v
                    else:
                        other += v
                self.samples.append((time.time(), other, own))
            except Exception:
                pass
            self._stop.wait(self.every_s)

    def wait_quiet(self, max_other_pct: float, samples: int = 2, timeout_s: float = 600) -> float:
        """Blocks until `samples` consecutive fresh samples show other containers below
        max_other_pct CPU (or timeout). Returns the seconds waited."""
        start = time.time()
        announced = False
        if getattr(self, "busy_streak", False):
            timeout_s = min(timeout_s, 60)  # it stayed busy last time: don't stall every point
        while time.time() - start < timeout_s:
            fresh = [s for s in self.samples if s[0] >= start]
            if len(fresh) >= samples and all(s[1] < max_other_pct for s in fresh[-samples:]):
                self.busy_streak = False
                return time.time() - start
            if not announced and fresh and fresh[-1][1] >= max_other_pct:
                log(f"  waiting for the machine to quiet down (other containers at {fresh[-1][1]:.0f}% CPU)")
                announced = True
            time.sleep(1)
        log(f"  machine still busy after {timeout_s:.0f} s; running anyway")
        self.busy_streak = True
        return time.time() - start

    def window(self, t0: float, t1: float) -> dict:
        xs = [s for s in self.samples if t0 <= s[0] <= t1] or self.samples[-1:]
        if not xs:
            return {"other_cpu_pct": None, "host_load1": round(os.getloadavg()[0], 2)}
        return {"other_cpu_pct": round(sum(s[1] for s in xs) / len(xs), 1),
                "host_load1": round(os.getloadavg()[0], 2)}

    def stop(self) -> None:
        self._stop.set()


def machine_description() -> str:
    chip = platform.processor() or platform.machine()
    try:
        chip = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout.strip() or chip
    except OSError:
        pass
    try:
        info = json.loads(subprocess.run(["docker", "info", "--format", "{{json .}}"], capture_output=True, text=True).stdout)
        return f"{chip}, Docker Desktop {info['NCPU']} vCPU / {round(info['MemTotal'] / 2**30)} GB"
    except Exception:
        return chip


def new_run_id() -> str:
    return uuid.uuid4().hex[:8]
