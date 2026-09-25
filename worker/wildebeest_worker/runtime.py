"""Worker runtime shared by both stages: register, heartbeat, claim, process, report.

Lifecycle
  1. POST /workers/register -> workerId + config (heartbeatMs, leaseMs, claimMode, claim batch
     floor/cap).
  2. A heartbeat thread POSTs /workers/{id}/heartbeat every heartbeatMs with every task ID we
     hold (running, waiting, or finished but not yet reported), which renews their leases. A 410
     WORKER_DEAD means the coordinator already gave our tasks to someone else: drop in-flight
     work and re-register.
  3. Claim: hybrid mode moves up to k IDs from queue:{stage} to processing:{workerId} in one
     MULTI/EXEC (reliable queue: an ID is never only in our memory), then POST
     /tasks/claim-confirm for leases with fencing tokens (leaseEpoch). Postgres mode long-polls
     POST /tasks/claim instead. k ≈ round trip ÷ service time, measured online (ClaimSizer).
  4. Process the held leases in order. With prefetch, the next image downloads while the
     current one runs. Results are buffered and reported in one request (complete, or
     complete-batch for several) that also asks for the next leases (`next`), so a busy
     worker makes one coordinator round trip per batch, not two per task. 409 STALE_LEASE
     (or status "stale" in a batch) means our lease was taken over: discard and move on.
  5. SIGTERM / stop(): stop claiming, finish the in-flight task, report, deregister, exit.
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
import math
import os
import random
import resource
import signal
import socket
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
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


class ClaimSizer:
    """Claim batch size k ≈ round trip ÷ service time (RabbitMQ's prefetch rule), measured online.

    A synchronous worker pays one coordinator round trip per claim/complete exchange, so with k
    tasks per exchange the orchestration cost per task is RTT / k. Choosing k = ceil(RTT / service)
    keeps that at or below one service time; for real models (RTT ~5 ms, service ~700 ms) it gives
    k = 1, for 0 ms fake tasks it runs to the cap. Both inputs are EWMAs; k stays at the floor until
    both have been measured, and floor == cap pins it.
    """

    def __init__(self, alpha: float = 0.2) -> None:
        self.alpha = alpha
        self.rtt_ms: float | None = None
        self.service_ms: float | None = None

    def _ewma(self, old: float | None, new: float) -> float:
        return new if old is None else old + self.alpha * (new - old)

    def observe_rtt(self, ms: float) -> None:
        self.rtt_ms = self._ewma(self.rtt_ms, max(0.0, ms))

    def observe_service(self, ms: float) -> None:
        self.service_ms = self._ewma(self.service_ms, max(0.0, ms))

    def size(self, floor: int, cap: int) -> int:
        floor = max(1, floor)
        if cap <= floor or self.rtt_ms is None or self.service_ms is None:
            return floor
        return max(floor, min(cap, math.ceil(self.rtt_ms / max(self.service_ms, 0.05))))


@dataclass
class Finished:
    """A completed task whose result waits in the batch buffer (its lease is still held)."""

    item: dict
    latency_ms: float
    at: float  # monotonic time it finished


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
        prefetch: Callable[[dict], None] | None = None,
        container_id: str | None = None,
    ) -> None:
        self.stage = stage
        self.handler = handler
        self.redis = redis_client
        self.base_url = coordinator_url.rstrip("/")
        self.http = http or requests.Session()
        # Worker IDs are {stage}-{hostname}; WORKER_HOSTNAME lets two native workers share a host.
        self.hostname = hostname or os.environ.get("WORKER_HOSTNAME") or socket.gethostname()
        self.startup_timeout_s = startup_timeout_s
        self.runtime = runtime or detect_runtime()
        self._device = device
        # Inside Docker the hostname is the short container ID; a native worker has no container.
        self.container_id = (container_id or os.environ.get("WORKER_CONTAINER_ID")
                             or (self.hostname if self.runtime == "container" else f"native-{self.hostname}"))
        self.io = io_timer  # anything with fetch_ms / upload_ms / reset(); see storage.IoTimer
        self.breaker = CircuitBreaker(probe)
        # Depth-1 prefetch: download/decode the next leased image while the current one runs.
        self.prefetch = prefetch
        self.prefetch_depth = int(os.environ.get("PREFETCH", "1")) if prefetch else 0

        self.worker_id: str | None = None
        self.heartbeat_s = int(os.environ.get("HEARTBEAT_MS", "2000")) / 1000
        self.lease_s = int(os.environ.get("LEASE_MS", "15000")) / 1000
        self.claim_mode = "hybrid"
        self.batch_size = int(os.environ.get("CLAIM_BATCH_SIZE", "1"))  # floor of the adaptive batch
        self.max_batch = self.batch_size  # cap; the coordinator sends maxClaimBatch at register
        self.sizer = ClaimSizer()
        # Results are buffered and sent as one complete-batch: flushed after COMPLETE_BATCH tasks
        # (0 = auto: the current claim batch), after COMPLETE_FLUSH_MS, or when we run out of work.
        self.complete_batch = int(os.environ.get("COMPLETE_BATCH", "0"))
        self.flush_s = int(os.environ.get("COMPLETE_FLUSH_MS", "50")) / 1000

        self.stopping = threading.Event()  # set by SIGTERM/SIGINT or stop()
        self.dead = threading.Event()  # set by the heartbeat thread on 410
        self.lock = threading.Lock()  # guards the fields below
        self.backlog: deque[tuple[dict, float]] = deque()  # leased, not started: (lease, claimMs)
        self.current: dict | None = None  # the lease being processed
        self.finished: list[Finished] = []  # done, result not yet accepted by the coordinator
        self.current_image_key: str | None = None
        self.tasks_done = 0
        self.total_latency_ms = 0.0

    @property
    def device(self) -> str:
        # Read late: with DEVICE=auto the model sets WORKER_DEVICE while it loads.
        return self._device or os.environ.get("WORKER_DEVICE", "cpu")

    @property
    def held_task_ids(self) -> list[str]:
        """Every task we hold a lease on: the one running, the ones waiting, the unreported ones."""
        with self.lock:
            ids = [self.current["taskId"]] if self.current else []
            ids += [lease["taskId"] for lease, _ in self.backlog]
            ids += [f.item["taskId"] for f in self.finished]
        return ids

    def claim_batch(self) -> int:
        """k: tasks per claim (≈ RTT ÷ service time, between the coordinator's floor and cap)."""
        return self.sizer.size(self.batch_size, self.max_batch)

    def window(self) -> int:
        """Leases to hold: a claim batch, and with prefetch at least one beyond the running task."""
        return max(self.claim_batch(), 1 + self.prefetch_depth)

    # ---------------------------------------------------------------- HTTP

    def _post(self, path: str, body: dict, attempts: int = 5, budget_s: float | None = None,
              abort: threading.Event | None = None, timeout: float = 10) -> requests.Response:
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
                resp = self.http.post(f"{self.base_url}{path}", json=body, timeout=timeout)
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
        self.max_batch = int(config.get("maxClaimBatch", self.batch_size))
        self.claim_mode = config.get("claimMode", "hybrid")
        with self.lock:  # a new incarnation holds nothing: the old one's leases were released
            self.backlog.clear()
            self.finished.clear()
        self.dead.clear()
        log.info("registered as %s (%s/%s, %s claims, batch %d..%d, prefetch %d, heartbeat %.1fs)",
                 self.worker_id, self.runtime, self.device, self.claim_mode, self.batch_size, self.max_batch,
                 self.prefetch_depth, self.heartbeat_s)

    def deregister(self) -> None:
        try:
            self._post(f"/workers/{self.worker_id}/deregister", {}, attempts=2)
            log.info("deregistered %s", self.worker_id)
        except requests.RequestException as e:
            log.warning("deregister failed: %s", e)

    def heartbeat_once(self) -> None:
        held = self.held_task_ids
        with self.lock:
            body = {
                "taskIds": held,
                "metrics": {
                    "tasksDone": self.tasks_done,
                    "avgLatencyMs": round(self.total_latency_ms / self.tasks_done) if self.tasks_done else 0,
                    "rssMb": rss_mb(),
                    "currentImageKey": self.current_image_key,
                    # The coordinator sizes queue:detect from its workers' claim windows.
                    "claimBatch": self.window(),
                },
            }
        # Two tries: one immediate retry covers a stale pooled connection (seen through Docker
        # Desktop's port forwarding), which would otherwise cost a whole heartbeat interval.
        resp = self._post(f"/workers/{self.worker_id}/heartbeat", body, attempts=2)
        if resp.status_code == 410:
            log.warning("coordinator says %s is dead; dropping in-flight work", self.worker_id)
            self.dead.set()
        elif not resp.ok:
            log.warning("heartbeat got HTTP %d", resp.status_code)
        else:
            mode = resp.json().get("claimMode")
            if mode in ("hybrid", "postgres") and mode != self.claim_mode:
                log.info("coordinator switched to %s claims", mode)
                self.claim_mode = mode  # read by the main loop on its next claim

    def _heartbeat_loop(self) -> None:
        while not self.stopping.is_set():
            if not self.dead.is_set():  # while dead, wait for the main loop to re-register
                try:
                    self.heartbeat_once()
                except requests.RequestException as e:
                    log.warning("heartbeat failed: %s", e)
            self.stopping.wait(self.heartbeat_s)

    # ---------------------------------------------------------------- claiming

    def claim_ids(self, k: int | None = None) -> list[str]:
        """Hybrid mode: move up to k IDs from the ready queue into our processing list.

        One MULTI/EXEC round trip moves up to k at once (atomic, like a Lua script, but needs no
        scripting engine); only an empty queue costs a second, blocking BLMOVE (≤ 1 s) for the first.
        """
        k = k or self.window()
        src, dst = f"queue:{self.stage}", f"processing:{self.worker_id}"
        ids = self._move(src, dst, k)
        if not ids:
            first = self.redis.blmove(src, dst, 1, "LEFT", "RIGHT")
            if first is None:
                return []
            ids = [first] + (self._move(src, dst, k - 1) if k > 1 else [])
        return [i.decode() if isinstance(i, bytes) else i for i in ids]

    def _move(self, src: str, dst: str, k: int) -> list:
        pipe = self.redis.pipeline(transaction=True)
        for _ in range(k):
            pipe.lmove(src, dst, "LEFT", "RIGHT")
        return [i for i in pipe.execute() if i is not None]

    def confirm(self, task_ids: list[str]) -> list[dict]:
        resp = self._post("/tasks/claim-confirm", {"workerId": self.worker_id, "taskIds": task_ids})
        if resp.status_code == 410:
            raise WorkerDead()
        resp.raise_for_status()
        return resp.json().get("leases", [])

    def claim_postgres(self, k: int, wait_ms: int = 1000) -> list[dict]:
        """Postgres mode: one long-poll; the coordinator leases up to k with SKIP LOCKED."""
        resp = self._post("/tasks/claim", {"workerId": self.worker_id, "stage": self.stage, "max": k,
                                           "waitMs": wait_ms}, timeout=10 + wait_ms / 1000)
        resp.raise_for_status()
        return resp.json().get("leases", [])

    def acquire(self) -> int:
        """Fill the backlog with a fresh claim; returns how many leases we got."""
        k = self.window()
        started = time.perf_counter()
        if self.claim_mode == "postgres":
            leases = self.claim_postgres(k)
        else:
            task_ids = self.claim_ids(k)
            if not task_ids:
                return 0
            started = time.perf_counter()  # the blocking wait for work isn't claim overhead
            leases = self.confirm(task_ids)
        self._add_leases(leases, (time.perf_counter() - started) * 1000)
        return len(leases)

    def _add_leases(self, leases: list[dict], request_ms: float) -> None:
        """Queue leases locally; each is charged its share of the request that delivered it."""
        if not leases:
            return
        self.sizer.observe_rtt(request_ms)
        share = request_ms / len(leases)
        with self.lock:
            self.backlog.extend((lease, share) for lease in leases)

    # ---------------------------------------------------------------- reporting

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

    def _flush_due(self) -> bool:
        with self.lock:
            if not self.finished:
                return False
            n = self.complete_batch or self.claim_batch()
            return (not self.backlog or len(self.finished) >= n
                    or time.monotonic() - self.finished[0].at >= self.flush_s)

    def flush(self) -> None:
        """Report buffered results: one complete (or complete-batch) request, asking for the next
        leases in the same request (`next`) so the backlog refills without a separate claim.

        Even if the coordinator declared us dead meanwhile (a pause, a partition), the results are
        posted: the lease epoch decides. A task that was reassigned is answered STALE_LEASE and
        recorded, which is the fencing we want to be visible. A zombie gets a single try and asks
        for nothing; a live worker retries for up to a lease length.
        """
        with self.lock:
            batch = list(self.finished)
            want = self.window() - len(self.backlog) - (1 if self.current else 0)
        if not batch:
            return
        zombie = self.dead.is_set()
        refill = 0 if (zombie or self.stopping.is_set() or self.breaker.is_open) else max(0, want)
        if len(batch) == 1:
            path = f"/tasks/{batch[0].item['taskId']}/complete"
            body = {"workerId": self.worker_id, **{k: v for k, v in batch[0].item.items() if k != "taskId"}}
        else:
            path = "/tasks/complete-batch"
            body = {"workerId": self.worker_id, "items": [f.item for f in batch]}
        if refill:
            body["next"] = refill

        started = time.perf_counter()
        try:
            resp = self._post(path, body, attempts=1) if zombie else self._report(path, body)
        finally:
            with self.lock:  # sent (or given up on): these leases are no longer ours to renew
                del self.finished[:len(batch)]
        request_ms = (time.perf_counter() - started) * 1000

        if len(batch) == 1:
            if resp.status_code == 409:
                statuses = ["stale"]
            else:
                resp.raise_for_status()
                statuses = ["ok"]
        else:
            resp.raise_for_status()
            by_id = {r["taskId"]: r["status"] for r in resp.json().get("results", [])}
            statuses = [by_id.get(f.item["taskId"], "invalid") for f in batch]

        for f, status in zip(batch, statuses):
            if status == "ok":
                with self.lock:
                    self.tasks_done += 1
                    self.total_latency_ms += f.latency_ms
                log.debug("task %s done in %.0f ms", f.item["taskId"], f.latency_ms)
            else:
                log.warning("result for %s discarded: %s (epoch %s)", f.item["taskId"],
                            "STALE_LEASE" if status == "stale" else status, f.item["leaseEpoch"])
        leases = resp.json().get("leases", []) if resp.ok else []
        if leases and not (self.dead.is_set() or self.stopping.is_set()):
            self._add_leases(leases, request_ms)
        elif refill:
            self.sizer.observe_rtt(request_ms)

    # ---------------------------------------------------------------- running tasks

    def execute(self, lease: dict, claim_ms: float = 0.0) -> None:
        """Run one task. A result joins the report buffer; an error is reported at once."""
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
        self.sizer.observe_service(latency_ms)
        result["latencyMs"] = round(latency_ms)
        item = {"taskId": lease["taskId"], "leaseEpoch": lease["leaseEpoch"], "result": result,
                "timings": self._timings(claim_ms, latency_ms)}
        with self.lock:
            self.finished.append(Finished(item, latency_ms, time.monotonic()))

    def _timings(self, claim_ms: float, handler_ms: float) -> dict:
        """claim RTT share, then the handler split into storage reads, storage writes and the rest.
        With prefetch, fetchMs is only the part of the download the task actually waited for."""
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

    def _prefetch_next(self) -> None:
        """Start downloading the next leased image while the current one runs (read-only, so it
        can't break fencing: a lease we lose meanwhile just wastes the download)."""
        if not self.prefetch:
            return
        with self.lock:
            nxt = self.backlog[0][0] if self.backlog else None
        if nxt is not None:
            try:
                self.prefetch(nxt)
            except Exception as e:  # an optimisation must never cost a task
                log.debug("prefetch of %s failed: %s", nxt.get("imageKey"), e)

    def run_once(self) -> int:
        """Claim if we hold nothing, then work through the held leases, reporting as we go.
        Leases that arrive with a report (complete + next) are processed in the same call.
        Returns how many leases were taken off the backlog (processed or released)."""
        with self.lock:
            empty = not self.backlog
        if empty and self.acquire() == 0:
            return 0
        taken = 0
        try:
            while not (self.stopping.is_set() or self.dead.is_set()):
                with self.lock:
                    if not self.backlog:
                        break
                    lease, claim_ms = self.backlog.popleft()
                    self.current = lease
                taken += 1
                if self.breaker.is_open:
                    # A dependency went down mid-batch: hand the rest back now instead of
                    # letting their leases expire (which would cost them an attempt).
                    self.release(lease, f"circuit open: {self.breaker.open_reason}")
                else:
                    self._prefetch_next()
                    self.execute(lease, claim_ms)
                with self.lock:
                    self.current = None
                if self._flush_due():
                    self.flush()
            self.flush()  # stopping, dead, or out of work: report what we have (a zombie once)
        except Exception:
            with self.lock:
                self.finished.clear()  # unsendable: those leases expire and the tasks rerun
            raise
        finally:
            with self.lock:
                self.current = None
                if self.stopping.is_set() or self.dead.is_set():
                    # Unstarted leases: deregister (or the reaper, or re-registration) returns them.
                    self.backlog.clear()
        return taken

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)
        self.register()
        threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True).start()
        self.loop()
        self.deregister()
        log.info("worker %s exited cleanly", self.worker_id)

    def stop(self) -> None:
        """Finish the in-flight task, report, deregister and return from run()."""
        self.stopping.set()

    def loop(self) -> None:
        """Claim and process until stopped."""
        while not self.stopping.is_set():
            try:
                if self.dead.is_set():
                    self.register()  # same workerId; the old incarnation's leases were reassigned
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
        self.stop()
