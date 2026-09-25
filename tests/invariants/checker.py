"""Jepsen-lite invariant checker for Wildebeest: reads Postgres after (and during) a run.

The history is what the coordinator itself recorded: `tasks`, `task_events` (claimed /
succeeded / reassigned / stale_rejected ...), `images`, `jobs`, the result tables and `workers`.
Every check is a pure function over plain rows, so the unit tests can feed it hand-written
histories (tests/invariants/test_checker.py) and the fault runner feeds it the real database.

Invariants (from reports/Wildebeest distributed systems improvements.md, item #6):
  I1 single_success      at most one `succeeded` event per task; a SUCCEEDED task has exactly one;
                         no `claimed` event after a task succeeded (it must never run again)
  I2 result_rows         exactly one result row per (sha256, model_version), every finalised image
                         has the rows its category needs, and its category agrees with them
  I3 fenced_completion   every accepted completion carries the task's latest claimed epoch
  I4 epochs_increase     claimed epochs strictly increase per task; tasks.lease_epoch == last claim
  I5 no_stuck_lease      no task LEASED on a worker silent for > timeout + 2 reaper ticks, or on a
                         DEAD worker > 2 ticks after it was marked dead (sampled live)
  I6 terminal_images     every image of a finished job has a terminal category, and no image is
                         left without a final category and without a live task
  I7 job_finishes        no job stuck `running` once faults stop (and none `running` with every image final)
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable

ANIMAL_CONF_THRESHOLD = 0.2
TERMINAL = {"empty", "animal", "human", "vehicle", "failed"}


@dataclass
class Violation:
    invariant: str
    subject: str
    detail: str

    def as_dict(self) -> dict:
        return {"invariant": self.invariant, "subject": self.subject, "detail": self.detail}


@dataclass
class History:
    """Plain rows, as dicts. Event `detail` is the decoded jsonb."""
    tasks: list[dict] = field(default_factory=list)      # id, image_id, stage, state, lease_epoch, attempts
    events: list[dict] = field(default_factory=list)     # id, task_id, worker_id, type, at, detail
    images: list[dict] = field(default_factory=list)     # id, job_id, sha256, final_category, species_common_name
    jobs: list[dict] = field(default_factory=list)       # id, status
    detections: list[dict] = field(default_factory=list)  # sha256, model_version, detections, n
    classifications: list[dict] = field(default_factory=list)  # sha256, model_version, common_name, n
    detector_version: str = "fake-detector-v1"
    classifier_version: str = "fake-classifier-v1"


# ---------------------------------------------------------------------------------------------
# pure checks
# ---------------------------------------------------------------------------------------------

def _epoch(ev: dict) -> int | None:
    d = ev.get("detail") or {}
    v = d.get("leaseEpoch", d.get("epoch"))
    return int(v) if v is not None else None


def check_single_success(h: History) -> list[Violation]:
    out = []
    by_task: dict[str, list[dict]] = defaultdict(list)
    for e in h.events:
        if e.get("task_id"):
            by_task[e["task_id"]].append(e)
    for t in h.tasks:
        evs = sorted(by_task.get(t["id"], []), key=lambda e: e["id"])
        succ = [e for e in evs if e["type"] == "succeeded"]
        if len(succ) > 1:
            out.append(Violation("I1 single_success", t["id"], f"{len(succ)} succeeded events"))
        if t["state"] == "SUCCEEDED" and len(succ) == 0:
            out.append(Violation("I1 single_success", t["id"], "task SUCCEEDED without a succeeded event"))
        if succ and t["state"] != "SUCCEEDED":
            out.append(Violation("I1 single_success", t["id"], f"succeeded event but task state {t['state']}"))
        if succ:
            later = [e for e in evs if e["type"] == "claimed" and e["id"] > succ[0]["id"]]
            if later:
                out.append(Violation("I1 single_success", t["id"],
                                     f"claimed again after success (epoch {_epoch(later[0])})"))
    return out


def check_epochs(h: History) -> tuple[list[Violation], int]:
    """I3 + I4. Returns violations and the number of completions whose epoch could not be checked."""
    out, unverifiable = [], 0
    by_task: dict[str, list[dict]] = defaultdict(list)
    for e in h.events:
        if e.get("task_id") and e["type"] in ("claimed", "succeeded"):
            by_task[e["task_id"]].append(e)
    tasks = {t["id"]: t for t in h.tasks}
    for tid, evs in by_task.items():
        evs.sort(key=lambda e: e["id"])
        last_claim: int | None = None
        for e in evs:
            ep = _epoch(e)
            if e["type"] == "claimed":
                if ep is None:
                    unverifiable += 1
                    continue
                if last_claim is not None and ep <= last_claim:
                    out.append(Violation("I4 epochs_increase", tid, f"claimed epoch {ep} after epoch {last_claim}"))
                last_claim = ep if last_claim is None else max(last_claim, ep)
            else:  # succeeded
                if ep is None or last_claim is None:
                    unverifiable += 1
                    continue
                if ep != last_claim:
                    out.append(Violation("I3 fenced_completion", tid,
                                         f"completion accepted with epoch {ep}, latest claim was {last_claim}"))
        t = tasks.get(tid)
        if t is not None and last_claim is not None and t.get("lease_epoch") is not None and t["lease_epoch"] != last_claim:
            out.append(Violation("I4 epochs_increase", tid,
                                 f"tasks.lease_epoch={t['lease_epoch']} but last claimed epoch {last_claim}"))
    return out, unverifiable


def categorize(detections: Iterable[dict], threshold: float = ANIMAL_CONF_THRESHOLD) -> str:
    dets = list(detections or [])

    def hit(label):
        return any(d.get("label") == label and float(d.get("conf", 0)) >= threshold for d in dets)
    if hit("animal"):
        return "animal"
    if hit("human"):
        return "human"
    if hit("vehicle"):
        return "vehicle"
    return "empty"


def check_result_rows(h: History) -> list[Violation]:
    out = []
    for r in h.detections:
        if r.get("n", 1) != 1:
            out.append(Violation("I2 result_rows", f"{r['sha256']}/{r['model_version']}", f"{r['n']} detection rows"))
    for r in h.classifications:
        if r.get("n", 1) != 1:
            out.append(Violation("I2 result_rows", f"{r['sha256']}/{r['model_version']}", f"{r['n']} classification rows"))
    det = {r["sha256"]: r for r in h.detections if r["model_version"] == h.detector_version}
    cls = {r["sha256"]: r for r in h.classifications if r["model_version"] == h.classifier_version}
    for img in h.images:
        cat = img.get("final_category")
        if cat is None or cat == "failed":
            continue
        d = det.get(img["sha256"])
        if d is None:
            out.append(Violation("I2 result_rows", img["id"], f"final '{cat}' but no detection row"))
            continue
        expect = categorize(d.get("detections") or [])
        if expect == "animal":
            c = cls.get(img["sha256"])
            if c is None:
                out.append(Violation("I2 result_rows", img["id"], f"animal detection, final '{cat}', no classification row"))
            elif cat == "empty" and (c.get("common_name") or "").lower() != "blank":
                out.append(Violation("I2 result_rows", img["id"], "final 'empty' but classifier did not say blank"))
            elif cat == "animal" and img.get("species_common_name") != c.get("common_name"):
                out.append(Violation("I2 result_rows", img["id"],
                                     f"species {img.get('species_common_name')!r} != stored {c.get('common_name')!r}"))
            elif cat not in ("animal", "empty"):
                out.append(Violation("I2 result_rows", img["id"], f"animal detection but final '{cat}'"))
        elif cat != expect:
            out.append(Violation("I2 result_rows", img["id"], f"final '{cat}' but stored detections say '{expect}'"))
    return out


def check_terminal(h: History) -> list[Violation]:
    out = []
    jobs = {j["id"]: j for j in h.jobs}
    live_task_images = {t["image_id"] for t in h.tasks if t["state"] in ("PENDING", "LEASED")}
    for img in h.images:
        job = jobs.get(img["job_id"])
        if job is None or job["status"] == "cancelled":
            continue
        cat = img.get("final_category")
        if cat is not None and cat not in TERMINAL:
            out.append(Violation("I6 terminal_images", img["id"], f"unknown category {cat!r}"))
        if job["status"] == "done" and cat is None:
            out.append(Violation("I6 terminal_images", img["id"], "job done but image has no final category"))
        if job["status"] == "running" and cat is None and img["id"] not in live_task_images:
            out.append(Violation("I6 terminal_images", img["id"], "image lost: no final category and no PENDING/LEASED task"))
    finals = defaultdict(lambda: [0, 0])
    for img in h.images:
        finals[img["job_id"]][0] += 1
        finals[img["job_id"]][1] += img.get("final_category") is not None
    for jid, (n, done) in finals.items():
        j = jobs.get(jid)
        if j and j["status"] == "running" and n == done:
            out.append(Violation("I7 job_finishes", jid, "every image final but job still running"))
    image_job = {i["id"]: i["job_id"] for i in h.images}
    for t in h.tasks:
        j = jobs.get(image_job.get(t["image_id"]))
        if j and j["status"] == "done" and t["state"] in ("PENDING", "LEASED"):
            out.append(Violation("I6 terminal_images", t["id"], f"task {t['state']} in a finished job"))
    return out


def check_history(h: History) -> dict:
    v = []
    v += check_single_success(h)
    ep, unverifiable = check_epochs(h)
    v += ep
    v += check_result_rows(h)
    v += check_terminal(h)
    counts: dict[str, int] = defaultdict(int)
    for e in h.events:
        counts[e["type"]] += 1
    return {
        "violations": [x.as_dict() for x in v],
        "unverifiableCompletions": unverifiable,
        "stats": {
            "tasks": len(h.tasks), "images": len(h.images),
            "failedImages": sum(1 for i in h.images if i.get("final_category") == "failed"),
            "staleRejected": counts.get("stale_rejected", 0),
            "reassigned": counts.get("reassigned", 0), "leaseExpired": counts.get("lease_expired", 0),
            "claims": counts.get("claimed", 0), "succeeded": counts.get("succeeded", 0),
            "reexecutions": max(0, counts.get("claimed", 0) - sum(1 for t in h.tasks if t["state"] in ("SUCCEEDED", "FAILED"))),
        },
    }


# ---------------------------------------------------------------------------------------------
# reading Postgres
# ---------------------------------------------------------------------------------------------

def load_history(conn, job_ids: list[str] | None = None, detector_version: str = "fake-detector-v1",
                 classifier_version: str = "fake-classifier-v1") -> History:
    """Reads the history of the given jobs (default: every job) from a psycopg connection."""
    from psycopg.rows import dict_row

    with conn.cursor(row_factory=dict_row) as cur:
        if job_ids is None:
            cur.execute("select id::text from jobs")
            job_ids = [r["id"] for r in cur.fetchall()]
        cur.execute("select id::text, status from jobs where id = any(%s::uuid[])", (job_ids,))
        jobs = cur.fetchall()
        cur.execute("""select id::text, job_id::text, sha256, final_category, species_common_name
                         from images where job_id = any(%s::uuid[])""", (job_ids,))
        images = cur.fetchall()
        cur.execute("""select t.id::text, t.image_id::text, t.stage, t.state, t.lease_epoch, t.attempts
                         from tasks t join images i on i.id = t.image_id where i.job_id = any(%s::uuid[])""", (job_ids,))
        tasks = cur.fetchall()
        cur.execute("""select e.id, e.task_id::text, e.worker_id, e.type, e.at, e.detail
                         from task_events e
                        where e.task_id in (select t.id from tasks t join images i on i.id = t.image_id
                                             where i.job_id = any(%s::uuid[]))
                        order by e.id""", (job_ids,))
        events = cur.fetchall()
        shas = list({i["sha256"] for i in images})
        cur.execute("""select sha256, model_version, (array_agg(detections))[1] as detections, count(*) as n
                         from detection_results where sha256 = any(%s) group by 1, 2""", (shas,))
        dets = cur.fetchall()
        cur.execute("""select sha256, model_version, (array_agg(common_name))[1] as common_name, count(*) as n
                         from classification_results where sha256 = any(%s) group by 1, 2""", (shas,))
        clss = cur.fetchall()
    return History(tasks=tasks, events=events, images=images, jobs=jobs, detections=dets, classifications=clss,
                   detector_version=detector_version, classifier_version=classifier_version)


STUCK_SQL = """
select t.id::text, t.worker_id, w.status,
       extract(epoch from now() - w.last_heartbeat_at) * 1000 as silent_ms,
       extract(epoch from now() - w.dead_at) * 1000 as dead_ms
  from tasks t left join workers w on w.id = t.worker_id
 where t.state = 'LEASED'
   and (w.id is null
        or w.last_heartbeat_at < now() - make_interval(secs => %(silent)s)
        or (w.status <> 'ALIVE' and coalesce(w.dead_at, w.last_heartbeat_at) < now() - make_interval(secs => %(ticks)s)))
"""


class LiveSampler:
    """I5, sampled once a second while faults run: leases held by workers that are gone.

    A lease on a worker that has not heartbeated for timeout + 2 reaper ticks, or on a worker
    marked DEAD/STOPPED more than 2 ticks ago, means the reaper failed to recover it. Samples
    are skipped while the coordinator itself is down (nothing can reap then) and for a grace
    period after it comes back.
    """

    def __init__(self, conn_factory, worker_timeout_ms: int = 6000, reap_ms: int = 1000, confirm: int = 2) -> None:
        self.conn_factory = conn_factory
        self.silent_s = (worker_timeout_ms + 2 * reap_ms) / 1000
        self.ticks_s = 2 * reap_ms / 1000
        self.confirm = confirm
        self.suppressed_until = 0.0
        self.seen: dict[str, int] = defaultdict(int)
        self.violations: dict[str, Violation] = {}
        self.samples = 0

    def suppress(self, seconds: float) -> None:
        self.suppressed_until = max(self.suppressed_until, time.time() + seconds)

    def sample(self) -> None:
        if time.time() < self.suppressed_until:
            self.seen.clear()
            return
        try:
            conn = self.conn_factory()
            with conn.cursor() as cur:
                cur.execute(STUCK_SQL, {"silent": self.silent_s, "ticks": self.ticks_s})
                rows = cur.fetchall()
        except Exception:
            return
        self.samples += 1
        now_ids = set()
        for tid, wid, status, silent_ms, dead_ms in rows:
            now_ids.add(tid)
            self.seen[tid] += 1
            # Seen on `confirm` consecutive samples: one sample can race a reaper tick in flight.
            if self.seen[tid] >= self.confirm and tid not in self.violations:
                self.violations[tid] = Violation(
                    "I5 no_stuck_lease", tid,
                    f"LEASED on {wid} ({status}), silent {silent_ms and round(silent_ms)} ms, dead {dead_ms and round(dead_ms)} ms")
        for tid in list(self.seen):
            if tid not in now_ids:
                del self.seen[tid]


def as_json(obj: Any) -> Any:
    if isinstance(obj, Violation):
        return obj.as_dict()
    return obj
