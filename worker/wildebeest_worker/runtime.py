"""Worker runtime shared by both stages: register, heartbeat, claim, process, report.

Lifecycle
  1. POST /workers/register -> workerId + config (heartbeatMs, claimBatchSize).
  2. A heartbeat thread POSTs /workers/{id}/heartbeat every heartbeatMs with the task
     IDs we hold, which renews their leases. A 410 WORKER_DEAD means the coordinator
     already gave our tasks to someone else: drop in-flight work and re-register.
  3. Claim loop: BLMOVE queue:{stage} -> processing:{workerId} (reliable queue, so an
     ID is never only in our memory), POST /tasks/claim-confirm to get leases with
     fencing tokens (leaseEpoch), run the model, POST /tasks/{id}/complete.
     409 STALE_LEASE on complete means our lease was taken over: discard and move on.
  4. SIGTERM: stop claiming, finish the in-flight task, deregister, exit.
     SIGKILL needs nothing here; the coordinator's heartbeat/lease reaper recovers.
"""

import logging
import os
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


class WorkerDead(Exception):
    """The coordinator declared this worker dead (410); its leases are gone."""


def rss_mb() -> float:
    """Current resident memory in MB (Linux /proc), falling back to peak RSS."""
    try:
        with open("/proc/self/statm") as f:
            pages = int(f.read().split()[1])
        return round(pages * os.sysconf("SC_PAGE_SIZE") / 2**20, 1)
    except OSError:
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return round(peak / (2**20 if sys.platform == "darwin" else 2**10), 1)


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
    ) -> None:
        self.stage = stage
        self.handler = handler
        self.redis = redis_client
        self.base_url = coordinator_url.rstrip("/")
        self.http = http or requests.Session()
        self.hostname = hostname or socket.gethostname()
        self.startup_timeout_s = startup_timeout_s

        self.worker_id: str | None = None
        self.heartbeat_s = int(os.environ.get("HEARTBEAT_MS", "2000")) / 1000
        self.batch_size = int(os.environ.get("CLAIM_BATCH_SIZE", "1"))

        self.stopping = threading.Event()  # set by SIGTERM/SIGINT
        self.dead = threading.Event()  # set by the heartbeat thread on 410
        self.lock = threading.Lock()  # guards the fields below
        self.held_task_ids: list[str] = []  # everything we hold a lease on
        self.current_image_key: str | None = None
        self.tasks_done = 0
        self.total_latency_ms = 0.0

    # ---------------------------------------------------------------- HTTP

    def _post(self, path: str, body: dict, attempts: int = 5) -> requests.Response:
        """POST with a few retries on connection errors (not on HTTP error codes)."""
        delay = 0.2
        for attempt in range(1, attempts + 1):
            try:
                return self.http.post(f"{self.base_url}{path}", json=body, timeout=10)
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt == attempts:
                    raise
                log.warning("POST %s failed (%s), retry %d/%d", path, e, attempt, attempts - 1)
                time.sleep(delay)
                delay = min(delay * 2, 2.0)
        raise AssertionError("unreachable")

    # ------------------------------------------------------------ lifecycle

    def register(self) -> None:
        """Register with the coordinator, waiting for it to come up if necessary."""
        body = {"stage": self.stage, "hostname": self.hostname, "containerId": self.hostname}
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
        self.batch_size = int(config.get("claimBatchSize", self.batch_size))
        self.dead.clear()
        log.info("registered as %s (batch=%d, heartbeat=%.1fs)", self.worker_id, self.batch_size, self.heartbeat_s)

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

    def process(self, lease: dict) -> None:
        task_id, epoch = lease["taskId"], lease["leaseEpoch"]
        with self.lock:
            self.current_image_key = lease.get("imageKey")
        started = time.perf_counter()
        try:
            result = self.handler(lease)
        except Exception as e:  # model or storage failure: report it, the coordinator retries
            log.exception("task %s failed", task_id)
            if not self.dead.is_set():
                resp = self._post(f"/tasks/{task_id}/fail",
                                  {"workerId": self.worker_id, "leaseEpoch": epoch, "error": f"{type(e).__name__}: {e}"[:1000]})
                if resp.status_code == 409:
                    log.info("fail for %s rejected: STALE_LEASE", task_id)
            return
        finally:
            with self.lock:
                self.current_image_key = None

        latency_ms = (time.perf_counter() - started) * 1000
        if self.dead.is_set():
            log.info("discarding result for %s: worker was declared dead", task_id)
            return
        result["latencyMs"] = round(latency_ms)
        resp = self._post(f"/tasks/{task_id}/complete",
                          {"workerId": self.worker_id, "leaseEpoch": epoch, "result": result})
        if resp.status_code == 409:
            log.warning("result for %s discarded: STALE_LEASE (epoch %s was superseded)", task_id, epoch)
            return
        resp.raise_for_status()
        with self.lock:
            self.tasks_done += 1
            self.total_latency_ms += latency_ms
        log.info("task %s done in %.0f ms", task_id, latency_ms)

    def run_once(self) -> int:
        """One claim/process round. Returns how many leases were processed."""
        task_ids = self.claim_ids()
        if not task_ids:
            return 0
        leases = self.confirm(task_ids)
        with self.lock:
            self.held_task_ids = [l["taskId"] for l in leases]
        try:
            for lease in leases:
                if self.stopping.is_set() or self.dead.is_set():
                    break  # unstarted leases go back to PENDING via deregister / the reaper
                self.process(lease)
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
