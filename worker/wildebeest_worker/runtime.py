"""Worker runtime shared by both stages: register, heartbeat, claim, process, report.

Lifecycle
  1. POST /workers/register -> workerId + config (heartbeatMs, leaseMs, claimBatchSize).
  2. A heartbeat thread POSTs /workers/{id}/heartbeat every heartbeatMs with the task
     IDs we hold, which renews their leases. A 410 WORKER_DEAD means the coordinator
     already gave our tasks to someone else: drop in-flight work and re-register.
  3. Claim loop: BLMOVE queue:{stage} -> processing:{workerId} (reliable queue, so an
     ID is never only in our memory), POST /tasks/claim-confirm to get leases with
     fencing tokens (leaseEpoch), run the model, POST /tasks/{id}/complete with the
     result and the task's phase timings. 409 STALE_LEASE on complete means our lease
     was taken over: discard and move on.
  4. SIGTERM: stop claiming, finish the in-flight task, deregister, exit.
     SIGKILL needs nothing here; the coordinator's death watch / reaper recovers.

Errors are classified before they are reported (classify_error):
  infra  - MinIO/S3 unreachable, timeouts, 5xx: the task is fine, the path to it isn't.
           POST /tasks/{id}/release (no attempt spent) and open the circuit breaker:
           stop claiming, probe the dependency with backoff, keep heartbeating.
  fatal  - the input itself is bad (undecodable image, missing object): /fail with
           nonRetryable, so the task goes straight to the DLQ instead of burning retries.
  task   - anything else: /fail; the coordinator retries it after a jittered backoff.
"""

import logging
import os
import random
import resource
import signal
import socket
import sys
import threading
import time
from typing import Callable

import requests

log = logging.getLogger("wildebeest.worker")

Handler = Callable[[dict], dict]  # lease -> result (without latencyMs)

# Coordinator answers worth retrying: it is restarting or behind a proxy that is.
RETRY_STATUSES = {502, 503, 504}


class WorkerDead(Exception):
    """The coordinator declared this worker dead (410); its leases are gone."""


class InfraError(Exception):
    """A dependency (object storage, network) failed; the task itself is fine. Released, not failed."""


class NonRetryableError(Exception):
    """The task can never succeed as submitted (e.g. undecodable image). Fails straight to the DLQ."""


INFRA, FATAL, TASK = "infra", "fatal", "task"


def classify_error(e: BaseException) -> str:
    """infra | fatal | task, for an exception raised by a stage handler."""
    if isinstance(e, NonRetryableError):
        return FATAL
    if isinstance(e, InfraError):
        return INFRA
    try:
        import PIL

        if isinstance(e, (PIL.UnidentifiedImageError, PIL.Image.DecompressionBombError)):
            return FATAL
    except ImportError:  # pragma: no cover - PIL is a hard dependency
        pass
    if isinstance(e, (requests.ConnectionError, requests.Timeout, ConnectionError, TimeoutError, socket.timeout)):
        return INFRA
    try:  # raw botocore errors, in case a handler talks to S3 without going through Storage
        from botocore.exceptions import ClientError, ConnectionError as BotoConnectionError, HTTPClientError

        if isinstance(e, (BotoConnectionError, HTTPClientError)):
            return INFRA
        if isinstance(e, ClientError):
            status = int(e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) or 0)
            return INFRA if status >= 500 else TASK
    except ImportError:  # pragma: no cover
        pass
    try:
        import redis

        if isinstance(e, (redis.ConnectionError, redis.TimeoutError)):
            return INFRA
    except ImportError:  # pragma: no cover
        pass
    return TASK


def full_jitter(attempt: int, base_s: float, cap_s: float, rng: Callable[[], float] = random.random) -> float:
    """AWS "full jitter" backoff: uniform in [0, min(cap, base * 2^attempt)]."""
    return rng() * min(cap_s, base_s * (2 ** attempt))


def rss_mb() -> float:
    """Current resident memory in MB (Linux /proc), falling back to peak RSS."""
    try:
        with open("/proc/self/statm") as f:
            pages = int(f.read().split()[1])
        return round(pages * os.sysconf("SC_PAGE_SIZE") / 2**20, 1)
    except OSError:
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return round(peak / (2**20 if sys.platform == "darwin" else 2**10), 1)


def detect_runtime() -> str:
    """container | native: WORKER_RUNTIME if set, else whether we are inside Docker."""
    explicit = os.environ.get("WORKER_RUNTIME")
    if explicit in ("container", "native"):
        return explicit
    return "container" if os.path.exists("/.dockerenv") else "native"


class CircuitBreaker:
    """Stops the claim loop while a dependency is down.

    Closed: claim normally. trip() opens it after an infrastructure error. While open, the
    loop claims nothing (so it holds no leases that could expire) and calls wait_and_probe(),
    which sleeps a full-jitter backoff and then runs the probe (e.g. a HEAD on the bucket).
    A successful probe closes the breaker. The heartbeat thread is independent, so the
    coordinator keeps seeing the worker as alive the whole time.
    """

    def __init__(self, probe: Callable[[], None] | None = None, base_s: float = 0.5, cap_s: float = 30.0,
                 rng: Callable[[], float] = random.random) -> None:
        self.probe = probe
        self.base_s, self.cap_s, self.rng = base_s, cap_s, rng
        self.open_reason: str | None = None
        self.failed_probes = 0
        self.trips = 0

    @property
    def is_open(self) -> bool:
        return self.open_reason is not None

    def trip(self, reason: str) -> None:
        if not self.is_open:
            self.trips += 1
            log.warning("circuit open: %s; pausing claims and probing", reason)
        self.open_reason = reason

    def wait_and_probe(self, stop: threading.Event) -> bool:
        """One backoff + probe. Returns True once the dependency answers again."""
        stop.wait(full_jitter(self.failed_probes, self.base_s, self.cap_s, self.rng))
        if stop.is_set():
            return False
        try:
            if self.probe is not None:  # no probe: half-open, let the next task be the probe
                self.probe()
        except Exception as e:
            self.failed_probes += 1
            log.info("dependency still down (%s), probe %d", e, self.failed_probes)
            return False
        log.info("circuit closed after %d failed probe(s)", self.failed_probes)
        self.open_reason = None
        self.failed_probes = 0
        return True


class Worker:
    def __init__(
        self,
        stage: str,
        handler: Handler,
        redis_client,
        coordinator_url: str,
        http: requests.Session | None = None,
        hostname: str | None = None,
        startup_timeout_s: float = 120,
        runtime: str | None = None,
        device: str | None = None,
        io_timer=None,
        probe: Callable[[], None] | None = None,
    ) -> None:
        self.stage = stage
        self.handler = handler
        self.redis = redis_client
        self.base_url = coordinator_url.rstrip("/")
        self.http = http or requests.Session()
        self.hostname = hostname or socket.gethostname()
        self.startup_timeout_s = startup_timeout_s
        self.runtime = runtime or detect_runtime()
        self.device = device or os.environ.get("WORKER_DEVICE", "cpu")
        # Inside Docker the hostname is the short container ID; a native worker has no container.
        self.container_id = self.hostname if self.runtime == "container" else f"native-{self.hostname}"
        self.io = io_timer  # anything with fetch_ms / upload_ms / reset(); see storage.IoTimer
        self.breaker = CircuitBreaker(probe)

        self.worker_id: str | None = None
        self.heartbeat_s = int(os.environ.get("HEARTBEAT_MS", "2000")) / 1000
        self.lease_s = int(os.environ.get("LEASE_MS", "15000")) / 1000
        self.batch_size = int(os.environ.get("CLAIM_BATCH_SIZE", "1"))

        self.stopping = threading.Event()  # set by SIGTERM/SIGINT
        self.dead = threading.Event()  # set by the heartbeat thread on 410
        self.lock = threading.Lock()  # guards the fields below
        self.held_task_ids: list[str] = []  # everything we hold a lease on
        self.current_image_key: str | None = None
        self.tasks_done = 0
        self.total_latency_ms = 0.0

    # ---------------------------------------------------------------- HTTP

    def _post(self, path: str, body: dict, attempts: int = 5, budget_s: float | None = None,
              abort: threading.Event | None = None) -> requests.Response:
        """POST, retrying connection errors, timeouts and 502/503/504 with full-jitter backoff.

        Gives up after `attempts` tries, or, when `budget_s` is set, once that much time has
        passed (task reports use one lease length, so a coordinator restart shorter than a
        lease never costs a finished result). `abort` stops the retries early, e.g. once the
        coordinator has declared us dead and the result would be fenced off anyway.
        """
        deadline = time.monotonic() + budget_s if budget_s is not None else None
        attempt = 0
        while True:
            attempt += 1
            failure: Exception | None = None
            try:
                resp = self.http.post(f"{self.base_url}{path}", json=body, timeout=10)
                if resp.status_code not in RETRY_STATUSES:
                    return resp
            except (requests.ConnectionError, requests.Timeout) as e:
                failure, resp = e, None
            exhausted = (time.monotonic() >= deadline) if deadline is not None else attempt >= attempts
            if not exhausted:
                delay = full_jitter(attempt, 0.1, 2.0)
                if deadline is not None:
                    delay = min(delay, max(0.0, deadline - time.monotonic()))
                log.warning("POST %s failed (%s), retry %d", path, failure or f"HTTP {resp.status_code}", attempt)
                time.sleep(delay)
            if exhausted or (abort is not None and abort.is_set()):
                if failure is not None:
                    raise failure
                return resp

    def _report(self, path: str, body: dict) -> requests.Response:
        """complete / fail / release: retried for up to one lease length."""
        return self._post(path, body, budget_s=self.lease_s, abort=self.dead)

    # ------------------------------------------------------------ lifecycle

    def register(self) -> None:
        """Register with the coordinator, waiting for it to come up if necessary."""
        body = {"stage": self.stage, "hostname": self.hostname, "containerId": self.container_id,
                "runtime": self.runtime, "device": self.device}
        deadline = time.monotonic() + self.startup_timeout_s
        while True:
            try:
                resp = self._post("/workers/register", body, attempts=1)
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                if time.monotonic() > deadline or self.stopping.is_set():
                    raise
                log.info("coordinator not ready (%s); retrying", e)
                time.sleep(1)
        data = resp.json()
        config = data.get("config") or {}
        self.worker_id = data["workerId"]
        self.heartbeat_s = config.get("heartbeatMs", self.heartbeat_s * 1000) / 1000
        self.lease_s = config.get("leaseMs", self.lease_s * 1000) / 1000
        self.batch_size = int(config.get("claimBatchSize", self.batch_size))
        self.dead.clear()
        log.info("registered as %s (%s/%s, batch=%d, heartbeat=%.1fs)",
                 self.worker_id, self.runtime, self.device, self.batch_size, self.heartbeat_s)

    def deregister(self) -> None:
        try:
            self._post(f"/workers/{self.worker_id}/deregister", {}, attempts=2)
            log.info("deregistered %s", self.worker_id)
        except requests.RequestException as e:
            log.warning("deregister failed: %s", e)

    def heartbeat_once(self) -> None:
        with self.lock:
            body = {
                "taskIds": list(self.held_task_ids),
                "metrics": {
                    "tasksDone": self.tasks_done,
                    "avgLatencyMs": round(self.total_latency_ms / self.tasks_done) if self.tasks_done else 0,
                    "rssMb": rss_mb(),
                    "currentImageKey": self.current_image_key,
                },
            }
        resp = self._post(f"/workers/{self.worker_id}/heartbeat", body, attempts=1)
        if resp.status_code == 410:
            log.warning("coordinator says %s is dead; dropping in-flight work", self.worker_id)
            self.dead.set()
        elif not resp.ok:
            log.warning("heartbeat got HTTP %d", resp.status_code)

    def _heartbeat_loop(self) -> None:
        while not self.stopping.is_set():
            if not self.dead.is_set():  # while dead, wait for the main loop to re-register
                try:
                    self.heartbeat_once()
                except requests.RequestException as e:
                    log.warning("heartbeat failed: %s", e)
            self.stopping.wait(self.heartbeat_s)

    # ---------------------------------------------------------------- tasks

    def claim_ids(self) -> list[str]:
        """Move up to batch_size IDs from the ready queue into our processing list."""
        src, dst = f"queue:{self.stage}", f"processing:{self.worker_id}"
        first = self.redis.blmove(src, dst, 1, "LEFT", "RIGHT")
        if first is None:
            return []
        ids = [first]
        while len(ids) < self.batch_size:
            nxt = self.redis.blmove(src, dst, 0.05, "LEFT", "RIGHT")  # don't wait for a full batch
            if nxt is None:
                break
            ids.append(nxt)
        return [i.decode() if isinstance(i, bytes) else i for i in ids]

    def confirm(self, task_ids: list[str]) -> list[dict]:
        resp = self._post("/tasks/claim-confirm", {"workerId": self.worker_id, "taskIds": task_ids})
        if resp.status_code == 410:
            raise WorkerDead()
        resp.raise_for_status()
        return resp.json().get("leases", [])

    def release(self, lease: dict, reason: str) -> None:
        """Give a lease back without spending an attempt (infrastructure trouble, not the task's)."""
        task_id = lease["taskId"]
        resp = self._report(f"/tasks/{task_id}/release",
                            {"workerId": self.worker_id, "leaseEpoch": lease["leaseEpoch"], "reason": reason[:500]})
        if resp.status_code == 409:
            log.info("release for %s rejected: STALE_LEASE", task_id)

    def fail(self, lease: dict, error: str, non_retryable: bool = False) -> None:
        task_id = lease["taskId"]
        body = {"workerId": self.worker_id, "leaseEpoch": lease["leaseEpoch"], "error": error[:1000]}
        if non_retryable:
            body["nonRetryable"] = True
        resp = self._report(f"/tasks/{task_id}/fail", body)
        if resp.status_code == 409:
            log.info("fail for %s rejected: STALE_LEASE", task_id)

    def process(self, lease: dict, claim_ms: float = 0.0) -> None:
        task_id, epoch = lease["taskId"], lease["leaseEpoch"]
        with self.lock:
            self.current_image_key = lease.get("imageKey")
        if self.io is not None:
            self.io.reset()
        started = time.perf_counter()
        try:
            result = self.handler(lease)
        except Exception as e:
            self._report_error(lease, e)
            return
        finally:
            with self.lock:
                self.current_image_key = None

        latency_ms = (time.perf_counter() - started) * 1000
        # Even if the coordinator declared us dead meanwhile (a pause, a partition), report the
        # result: the lease epoch decides. If the task was reassigned, the coordinator answers 409
        # STALE_LEASE and records the rejection, which is the fencing we want to be visible. A
        # zombie gets a single try; a live worker retries for up to a lease length.
        result["latencyMs"] = round(latency_ms)
        path = f"/tasks/{task_id}/complete"
        body = {"workerId": self.worker_id, "leaseEpoch": epoch, "result": result,
                "timings": self._timings(claim_ms, latency_ms)}
        resp = self._post(path, body, attempts=1) if self.dead.is_set() else self._report(path, body)
        if resp.status_code == 409:
            log.warning("result for %s discarded: STALE_LEASE (epoch %s was superseded)", task_id, epoch)
            return
        resp.raise_for_status()
        with self.lock:
            self.tasks_done += 1
            self.total_latency_ms += latency_ms
        log.info("task %s done in %.0f ms", task_id, latency_ms)

    def _timings(self, claim_ms: float, handler_ms: float) -> dict:
        """claim RTT, then the handler split into storage reads, storage writes and the rest."""
        fetch = self.io.fetch_ms if self.io is not None else 0.0
        upload = self.io.upload_ms if self.io is not None else 0.0
        return {
            "claimMs": round(claim_ms, 1),
            "fetchMs": round(fetch, 1),
            "inferMs": round(max(0.0, handler_ms - fetch - upload), 1),
            "uploadMs": round(upload, 1),
        }

    def _report_error(self, lease: dict, e: Exception) -> None:
        task_id = lease["taskId"]
        message = f"{type(e).__name__}: {e}"
        if self.dead.is_set():
            log.info("task %s failed after we were declared dead; not reporting (%s)", task_id, message)
            return
        kind = classify_error(e)
        if kind == INFRA:
            log.warning("task %s hit an infrastructure error, releasing it: %s", task_id, message)
            self.breaker.trip(message)  # first, so we stop claiming even if the release can't be sent
            self.release(lease, message)
        elif kind == FATAL:
            log.error("task %s can never succeed, failing it permanently: %s", task_id, message)
            self.fail(lease, message, non_retryable=True)
        else:
            log.exception("task %s failed", task_id)
            self.fail(lease, message)

    def run_once(self) -> int:
        """One claim/process round. Returns how many leases were processed."""
        task_ids = self.claim_ids()
        if not task_ids:
            return 0
        started = time.perf_counter()
        leases = self.confirm(task_ids)
        claim_ms = (time.perf_counter() - started) * 1000
        with self.lock:
            self.held_task_ids = [l["taskId"] for l in leases]
        try:
            for lease in leases:
                if self.stopping.is_set() or self.dead.is_set():
                    break  # unstarted leases go back to PENDING via deregister / the reaper
                if self.breaker.is_open:
                    # A dependency went down mid-batch: hand the rest back now instead of
                    # letting their leases expire (which would cost them an attempt).
                    self.release(lease, f"circuit open: {self.breaker.open_reason}")
                else:
                    self.process(lease, claim_ms)
                with self.lock:
                    self.held_task_ids.remove(lease["taskId"])
        finally:
            with self.lock:
                self.held_task_ids = []
        return len(leases)

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)
        self.register()
        threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True).start()
        self.loop()
        self.deregister()
        log.info("worker %s exited cleanly", self.worker_id)

    def loop(self) -> None:
        """Claim and process until SIGTERM."""
        while not self.stopping.is_set():
            try:
                if self.dead.is_set():
                    self.register()  # new workerId; the old one's leases were reassigned
                    continue
                if self.breaker.is_open:
                    self.breaker.wait_and_probe(self.stopping)  # claim nothing until it closes
                    continue
                self.run_once()
            except WorkerDead:
                self.dead.set()
            except requests.RequestException as e:
                log.warning("coordinator call failed: %s", e)
                time.sleep(1)
            except Exception:  # e.g. Redis blip; keep the worker alive
                log.exception("claim loop error")
                time.sleep(1)

    def _on_signal(self, signum, _frame) -> None:
        log.info("received %s: finishing in-flight task, then exiting", signal.Signals(signum).name)
        self.stopping.set()
