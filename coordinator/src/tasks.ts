import type pg from "pg";
import { config } from "./config.js";
import { getPool, query, tx, type Db } from "./db.js";
import {
  isThrottled,
  kickDetectInBackground,
  pushAt,
  pushNew,
  pushNow,
  waitForWork,
  finishLockThreshold,
  type Stage,
} from "./dispatcher.js";
import { hub, recordEvents, type EventInput, type EventRow } from "./events.js";
import * as traces from "./otel.js";
import { metrics } from "./prom.js";
import { getRedis, keys } from "./redis.js";
import { telemetry } from "./telemetry.js";
import { finalizeImage, maybeFinishJob, type Detection } from "./results.js";
import { offerIgnored, serviceTimes } from "./speculation.js";

// Task state machine (PRD 6.1):
//
//   PENDING --claim--> LEASED --complete--> SUCCEEDED
//   LEASED --fail / lease lost--> PENDING (attempts+1)  or  FAILED (attempts >= MAX)
//   LEASED --fail {nonRetryable}--> FAILED
//   LEASED --release / deregister / lost to our own kill or pause--> PENDING (no attempt)
//   FAILED --redrive (DLQ)--> PENDING (counters reset)
//
// Retry accounting (docs/decisions/p1-coordinator.md): `attempts` = task_errors + lease_losses and
// is the only counter checked against MAX_ATTEMPTS. `releases` counts free returns: infrastructure
// errors the worker reported through /release, graceful exits, and leases lost because the
// coordinator itself SIGKILLed or paused the worker (chaos, the Kill/Pause buttons).
//
// Every transition is ONE guarded UPDATE: `... WHERE id = $1 AND state = '<expected>'` (plus
// `AND lease_epoch = $n` for anything a worker sends). If the row isn't in the expected state the
// UPDATE touches zero rows and the caller loses cleanly, so two actors can never both "win" a
// transition, whatever order their requests arrive in.
//
// Fencing: each claim bumps lease_epoch and hands the new value to the worker. complete/fail must
// echo it. A worker that was presumed dead (GC pause, network stall) and comes back later holds an
// old epoch, so its late result matches zero rows → 409 STALE_LEASE, and the result from the
// worker that took over the task is the one that stays.
//
// Hot path (P2, docs/decisions/p2-hotpath.md): a claim is one statement (lease + `claimed` events
// + lease details), a completion is one statement (wb_complete in migration 006: fenced update,
// result, finalisation, events, job check), and both can be batched: complete-batch completes
// many tasks in one statement, and `next: k` claims the next k in the same request.
//
// Speculation (P3, docs/decisions/p3-speculation.md, migration 007): a LEASED task may also have
// one speculative copy (task_attempts), valid while the lease it shadows is still the task's lease.
// The first result to commit wins; the other attempt's report gets `already_done` (409
// ALREADY_DONE), not STALE_LEASE. If the lease is lost while the copy is healthy, the copy is
// promoted to be the lease instead of the task being requeued.

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const isUuid = (s: unknown): s is string => typeof s === "string" && UUID_RE.test(s);

export class ValidationError extends Error {}

export interface Lease {
  taskId: string;
  leaseEpoch: number;
  stage: Stage;
  imageKey: string;
  sha256: string;
  countryCode: string;
  detections: Detection[] | null;
  /** A speculative copy of a task another worker holds (additive; workers treat it like any lease). */
  speculative?: boolean;
  /** W3C trace context for the worker's process span (null when the task isn't traced). */
  traceparent: string | null;
}

/** Side effects to run once a transaction has committed. */
interface Effects {
  events: EventInput[];
  jobs: Set<string>;
}

/** Writes the collected task_events inside the transaction; they are published after commit. */
function commitEffects(db: Db, fx: Effects): Promise<EventRow[]> {
  return recordEvents(db, fx.events);
}

function publish(rows: EventRow[], jobs: Iterable<string>) {
  hub.publishEvents(rows);
  for (const j of jobs) hub.jobChanged(j);
}

async function lremProcessing(workerId: string, taskIds: string[]) {
  if (!workerId || taskIds.length === 0) return;
  const pipe = getRedis().pipeline();
  for (const id of taskIds) pipe.lrem(keys.processing(workerId), 0, id);
  await pipe.exec();
}

// ---------------------------------------------------------------------------------------------
// claim
// ---------------------------------------------------------------------------------------------

/**
 * The one statement every claim path shares: lease the picked tasks (PENDING → LEASED, epoch + 1,
 * only for an ALIVE worker of the task's stage), log a `claimed` event per lease, and return what
 * the worker needs to run them. `pick` is a query yielding candidate `id`s; the guarded UPDATE
 * decides which of them are really leased.
 *
 * Parameters: $1 worker, $2 lease ms, $3 default country, $4 detector model version,
 * $5 claimed straight from Postgres (then pushed_at = eligible_at = now: there was no push step),
 * then the pick's own parameters from $6.
 */
function leaseSql(pick: string) {
  return `
    with picked as (${pick}),
    leased as (
      update tasks t
         set state = 'LEASED', lease_epoch = greatest(t.lease_epoch, coalesce(t.spec_epoch, 0)) + 1, worker_id = $1,
             lease_expires_at = now() + ($2::int * interval '1 millisecond'),
             started_at = now(), queued = false,
             pushed_at = case when $5::boolean then now() else t.pushed_at end,
             eligible_at = case when $5::boolean then now() else t.eligible_at end
        from picked
       where t.id = picked.id and t.state = 'PENDING'
         and exists (select 1 from workers w where w.id = $1 and w.status = 'ALIVE' and w.stage = t.stage)
      returning t.id, t.lease_epoch, t.stage, t.image_id, t.enqueued_at, t.traceparent
    ),
    logged as (
      insert into task_events (task_id, worker_id, type, detail)
      select id, $1, 'claimed', jsonb_build_object('leaseEpoch', lease_epoch) from leased
    )
    select l.id, l.lease_epoch, l.stage, i.sha256, i.object_key, coalesce(j.country_code, $3) as country_code,
           d.detections, l.traceparent
      from leased l
      join images i on i.id = l.image_id
      join jobs j on j.id = i.job_id
      left join detection_results d on l.stage = 'classify' and d.sha256 = i.sha256 and d.model_version = $4
     order by l.enqueued_at, l.id`;
}

/** Candidates for hybrid mode: the IDs the worker (or complete+next) took from Redis. */
const PICK_IDS = `select unnest($6::uuid[]) as id`;

/**
 * Candidates for postgres mode: retries (tasks that ran before) first, oldest first, then new work
 * in creation order, locked with SKIP LOCKED so concurrent claimers never wait on each other.
 * New detect work is not admitted while backpressure is on ($8). $6 stage, $7 max.
 */
const PICK_POSTGRES = `
  with retries as (
    select id from tasks
     where state = 'PENDING' and queued = false and stage = $6 and started_at is not null
       and (not_before is null or not_before <= now())
     order by pending_at
     limit $7
     for update skip locked
  ),
  fresh as (
    select id from tasks
     where state = 'PENDING' and queued = false and stage = $6 and started_at is null and $8::boolean
     order by enqueued_at
     limit greatest(0, $7 - (select count(*) from retries))
     for update skip locked
  )
  select id from retries union all select id from fresh`;

async function lease(workerId: string, pick: string, fromPostgres: boolean, pickParams: unknown[]): Promise<Lease[]> {
  const { rows } = await query(leaseSql(pick), [
    workerId,
    config.leaseMs,
    config.defaultCountry,
    config.detectorModelVersion,
    fromPostgres,
    ...pickParams,
  ]);
  return rows.map((r) => ({
    taskId: r.id,
    leaseEpoch: r.lease_epoch,
    stage: r.stage,
    imageKey: r.object_key,
    sha256: r.sha256,
    countryCode: r.country_code,
    detections: r.stage === "classify" ? (r.detections ?? []) : null,
    traceparent: r.traceparent ?? null,
  }));
}

/** Telemetry, dashboard, and (push mode) refilling queue:detect behind a detect claim. */
function afterClaim(workerId: string, leases: Lease[]) {
  if (leases.length === 0) return;
  traces.leased(workerId, leases, config.claimMode); // opens the attempt spans, sets lease.traceparent
  telemetry.recordClaimed(leases.map((l) => l.taskId), workerId);
  hub.workersChanged();
  if (config.claimMode === "hybrid" && leases.some((l) => l.stage === "detect")) kickDetectInBackground();
}

/**
 * Hybrid mode: PENDING → LEASED for each ID the worker BLMOVEd. Only tasks still PENDING, of the
 * worker's own stage, and only while the worker is ALIVE, get leased; everything else is silently
 * skipped (that is what makes duplicate queue entries harmless).
 */
export async function claimConfirm(workerId: string, taskIds: string[]): Promise<Lease[]> {
  const ids = [...new Set(taskIds.filter(isUuid))];
  if (ids.length === 0) return [];
  const leases = await lease(workerId, PICK_IDS, false, [ids]);
  // IDs that weren't PENDING may be speculative copies offered to this worker (spec:{workerId}).
  const leasedIds = new Set(leases.map((l) => l.taskId));
  const copies =
    leases.length < ids.length ? await leaseCopies(workerId, ids.filter((id) => !leasedIds.has(id))) : [];
  for (const c of copies) leasedIds.add(c.taskId);

  // The IDs are now either leased (tracked in Postgres) or not ours to run; either way they
  // leave the processing list. Any ID we refused that is still waiting to run goes back.
  await lremProcessing(workerId, ids);
  const refused = ids.filter((id) => !leasedIds.has(id));
  if (refused.length > 0) await requeueWaiting(refused);
  afterClaim(workerId, leases);
  return orderLike(ids, [...leases, ...copies]);
}

/**
 * Leases the speculative copies offered to this worker (all of them, or only `taskIds`):
 * offered → running, with the copy's own epoch and lease, only while the lease it shadows is
 * still the task's lease, never on the worker holding that lease, and only for an ALIVE worker of
 * the task's stage. One statement, which also writes the `speculated` event.
 */
export async function leaseCopies(workerId: string, taskIds: string[] | null): Promise<Lease[]> {
  if (taskIds !== null && taskIds.length === 0) return [];
  const { rows } = await query(
    `with ok as (
       select t.id, t.stage, t.image_id, t.worker_id as original_worker, t.lease_epoch as original_epoch,
              a.epoch, a.detail, (extract(epoch from (now() - t.started_at)) * 1000)::int as age_ms, t.traceparent
         from task_attempts a join tasks t on t.id = a.task_id
        where a.worker_id = $1 and a.state = 'offered' and ($2::uuid[] is null or a.task_id = any($2::uuid[]))
          and t.state = 'LEASED' and t.lease_epoch = a.shadow_epoch and t.worker_id <> $1
          and exists (select 1 from workers w where w.id = $1 and w.status = 'ALIVE' and w.stage = t.stage)
        for update of t skip locked
     ),
     started as (
       update task_attempts a set state = 'running', started_at = now(),
                                  lease_expires_at = now() + ($3::int * interval '1 millisecond')
         from ok where a.task_id = ok.id and a.epoch = ok.epoch and a.state = 'offered'
       returning a.task_id
     ),
     logged as (
       insert into task_events (task_id, worker_id, type, detail)
       select ok.id, $1, 'speculated',
              coalesce(ok.detail, '{}'::jsonb) || jsonb_build_object(
                'stage', ok.stage, 'speculativeWorker', $1, 'epoch', ok.epoch,
                'originalWorker', ok.original_worker, 'originalEpoch', ok.original_epoch, 'ageMs', ok.age_ms)
         from ok join started s on s.task_id = ok.id
       returning id, at, type, task_id, worker_id, detail
     )
     select ok.id, ok.epoch, ok.stage, i.sha256, i.object_key, coalesce(j.country_code, $4) as country_code,
            d.detections, ok.traceparent, (select coalesce(jsonb_agg(to_jsonb(l)), '[]'::jsonb) from logged l) as events
       from ok
       join started s on s.task_id = ok.id
       join images i on i.id = ok.image_id
       join jobs j on j.id = i.job_id
       left join detection_results d on ok.stage = 'classify' and d.sha256 = i.sha256 and d.model_version = $5`,
    [workerId, taskIds, config.leaseMs, config.defaultCountry, config.detectorModelVersion],
  );
  if (rows.length === 0) return [];
  hub.publishEvents(
    (rows[0].events as any[]).map((e) => ({
      id: Number(e.id),
      at: new Date(e.at).toISOString(),
      type: e.type,
      taskId: e.task_id,
      workerId: e.worker_id,
      detail: e.detail,
    })),
  );
  hub.workersChanged();
  const copies: Lease[] = rows.map((r) => ({
    taskId: r.id,
    leaseEpoch: r.epoch,
    stage: r.stage,
    imageKey: r.object_key,
    sha256: r.sha256,
    countryCode: r.country_code,
    detections: r.stage === "classify" ? (r.detections ?? []) : null,
    speculative: true,
    traceparent: r.traceparent ?? null,
  }));
  traces.leased(workerId, copies, "speculative"); // its own attempt span in the task's trace
  return copies;
}

const orderLike = (ids: string[], leases: Lease[]) => {
  const pos = new Map(ids.map((id, i) => [id, i]));
  return [...leases].sort((a, b) => pos.get(a.taskId)! - pos.get(b.taskId)!);
};

/**
 * Puts back IDs whose task is PENDING and marked queued (i.e. it should be in a ready queue).
 * They were at the head of the queue when the worker took them, so they go back to the head.
 */
export async function requeueWaiting(taskIds: string[]): Promise<string[]> {
  const ids = taskIds.filter(isUuid);
  if (ids.length === 0) return [];
  const { rows } = await query(
    `select id, stage from tasks where id = any($1::uuid[]) and state = 'PENDING' and queued`,
    [ids],
  );
  if (rows.length === 0) return [];
  const pipe = getRedis().pipeline();
  for (const r of [...rows].reverse()) pipe.lpush(keys.queue(r.stage), r.id);
  await pipe.exec();
  return rows.map((r) => r.id as string);
}

/** Postgres mode: lease up to `max` tasks of a stage straight from the table (no Redis). */
async function leaseFromPostgres(workerId: string, stage: Stage, max: number): Promise<Lease[]> {
  const admitNew = stage === "classify" || !isThrottled();
  const leases = await lease(workerId, PICK_POSTGRES, true, [stage, max, admitNew]);
  afterClaim(workerId, leases);
  return leases;
}

/**
 * Claims up to `max` more tasks of `stage` for a worker, in the same request that completed its
 * previous ones (complete-and-claim-next). Hybrid: LPOP up to max IDs from the ready queue in one
 * atomic command and lease them; IDs we can't lease go back to the head. The coordinator is the
 * consumer here, so the IDs skip the processing list: if this process dies between the LPOP and
 * the lease, the rows are still PENDING and the startup rebuild re-pushes them.
 */
export async function claimNext(workerId: string, stage: Stage, max: number): Promise<Lease[]> {
  const k = Math.min(Math.floor(max), config.maxClaimBatch);
  if (!(k > 0)) return [];
  if (config.claimMode === "postgres") return leaseFromPostgres(workerId, stage, k);

  const ids = (await getRedis().lpop(keys.queue(stage), k)) ?? [];
  if (ids.length === 0) return [];
  let leases: Lease[];
  try {
    leases = await lease(workerId, PICK_IDS, false, [ids.filter(isUuid)]);
  } catch (err) {
    await requeueWaiting(ids).catch(() => {});
    throw err;
  }
  const leasedIds = new Set(leases.map((l) => l.taskId));
  const refused = ids.filter((id) => !leasedIds.has(id));
  if (refused.length > 0) await requeueWaiting(refused);
  afterClaim(workerId, leases);
  return orderLike(ids, leases);
}

/**
 * POST /tasks/claim (postgres mode): lease up to `max` tasks, waiting up to `waitMs` for work.
 * The wait ends early when new work of this stage is committed (dispatcher.wake), and re-checks
 * every 250 ms anyway (retry backoffs ending, other coordinators). `gone()` reports that the
 * client hung up, so the caller can hand back leases nobody will run.
 */
export async function claimTasks(
  workerId: string,
  stage: Stage,
  max: number,
  waitMs: number,
  gone: () => boolean = () => false,
): Promise<Lease[]> {
  const k = Math.max(1, Math.min(Math.floor(max) || 1, config.maxClaimBatch));
  const deadline = Date.now() + Math.max(0, Math.min(Number(waitMs) || 0, config.claimWaitMaxMs));
  for (;;) {
    let leases = await leaseFromPostgres(workerId, stage, k);
    // Nothing to hand out: exactly when a speculative copy may have been offered to this worker.
    if (leases.length === 0) leases = await leaseCopies(workerId, null);
    const left = deadline - Date.now();
    if (leases.length > 0 || left <= 0 || gone()) return leases;
    await waitForWork(stage, Math.min(250, left));
  }
}

// ---------------------------------------------------------------------------------------------
// complete
// ---------------------------------------------------------------------------------------------

/**
 * `already_done`: the report's attempt was valid but the task's other attempt (speculative copy
 * or original) committed its result first. Not a fencing event: no `stale_rejected`, not counted.
 */
export type Outcome =
  | { status: "ok"; leases?: Lease[] }
  | { status: "stale" }
  | { status: "already_done" }
  | { status: "not_found" };
export type ItemStatus = "ok" | "stale" | "already_done" | "invalid" | "not_found";

function num(v: unknown, what: string): number {
  const n = Number(v);
  if (!Number.isFinite(n)) throw new ValidationError(`${what} must be a number`);
  return n;
}

function parseDetections(detections: unknown): Detection[] {
  if (!Array.isArray(detections)) throw new ValidationError("result.detections must be an array");
  return detections.map((d: any, i: number) => {
    if (!d || typeof d.label !== "string") throw new ValidationError(`detections[${i}].label must be a string`);
    const bbox = Array.isArray(d.bbox) ? d.bbox.map((x: unknown) => num(x, `detections[${i}].bbox`)) : [];
    return { label: d.label, conf: num(d.conf, `detections[${i}].conf`), bbox };
  });
}

function parseClassification(result: any) {
  const str = (v: unknown) => (typeof v === "string" && v !== "" ? v : null);
  return {
    label: str(result.label),
    commonName: str(result.commonName),
    confidence: result.confidence == null ? null : num(result.confidence, "confidence"),
    cropKey: str(result.cropKey),
    raw: result.raw ?? null,
  };
}

let warnedVersion = false;
function checkModelVersion(result: any) {
  const reported = result?.modelVersion;
  if (reported === undefined || warnedVersion) return;
  if (reported !== config.detectorModelVersion && reported !== config.classifierModelVersion) {
    warnedVersion = true; // once per process: a misconfigured worker would otherwise log per task
    console.warn(`[tasks] a result reports modelVersion=${String(reported)}; stored under the coordinator's versions`);
  }
}

/** Worker-measured phase timings sent with `complete` (CONTRACTS.md "Worker protocol v2"). */
export interface WorkerTimings {
  claimMs: number;
  fetchMs: number;
  inferMs: number;
  uploadMs: number;
}

/** Telemetry must never cost a result: anything malformed is simply not stored. */
export function parseTimings(raw: unknown): WorkerTimings | null {
  if (!raw || typeof raw !== "object") return null;
  const r = raw as Record<string, unknown>;
  const out = {} as WorkerTimings;
  for (const k of ["claimMs", "fetchMs", "inferMs", "uploadMs"] as const) {
    const v = r[k] === undefined ? 0 : Number(r[k]);
    if (!Number.isFinite(v) || v < 0) return null;
    out[k] = Math.round(v * 10) / 10;
  }
  return out;
}

export interface CompleteItem {
  taskId: unknown;
  leaseEpoch: unknown;
  result: unknown;
  timings?: unknown;
}

/** What wb_complete receives per item: validated in Node, so the statement never sees junk. */
interface NormalisedItem {
  taskId: string;
  leaseEpoch: number;
  detections: Detection[] | null;
  classification: ReturnType<typeof parseClassification>;
  timings: WorkerTimings | null;
}

/**
 * The task's stage isn't known here (no read before the write), so both shapes are parsed: a
 * result with a `detections` array is a DetectResult, and every result object also yields the
 * classification fields. wb_complete marks a detect task without detections `invalid`.
 */
function normalise(item: CompleteItem): NormalisedItem {
  const epoch = Number(item.leaseEpoch);
  if (!Number.isInteger(epoch)) throw new ValidationError("leaseEpoch must be an integer");
  const result = item.result as any;
  if (!result || typeof result !== "object") throw new ValidationError("result must be an object");
  checkModelVersion(result);
  return {
    taskId: item.taskId as string,
    leaseEpoch: epoch,
    detections: "detections" in result ? parseDetections(result.detections) : null,
    classification: parseClassification(result),
    timings: parseTimings(item.timings),
  };
}

const ms = (d: Date | null | undefined) => (d ? new Date(d).getTime() : null);

interface Timeline {
  pending_at: Date;
  not_before: Date | null;
  eligible_at: Date | null;
  pushed_at: Date | null;
  started_at: Date;
}

/**
 * Turns the timestamps the fenced UPDATE returned plus the worker's own timings into one
 * waterfall sample (definitions in docs/CONTRACTS.md, system.timings):
 *   ready    = became PENDING, or its retry backoff ended
 *   eligible = the dispatcher could have pushed it (not held back by the queue target/backpressure)
 *   dispatchWaitMs = pushed − eligible                   (orchestration: push latency)
 *   queueWaitMs    = (eligible − ready) + (claimed − pushed)   (backlog in Postgres + wait in Redis)
 *   totalMs        = complete handled − ready
 */
function timingSample(stage: Stage, row: Timeline, worker: WorkerTimings | null, completeMs: number, end: number) {
  const ready = Math.max(ms(row.pending_at)!, ms(row.not_before) ?? 0);
  const pushed = ms(row.pushed_at);
  const eligible = Math.min(pushed ?? Infinity, Math.max(ready, ms(row.eligible_at) ?? ready));
  const claimed = ms(row.started_at)!;
  return {
    at: end,
    stage,
    dispatchWaitMs: pushed !== null ? Math.max(0, pushed - eligible) : 0,
    queueWaitMs: pushed !== null ? Math.max(0, eligible - ready) + Math.max(0, claimed - pushed) : 0,
    claimMs: worker?.claimMs ?? 0,
    fetchMs: worker?.fetchMs ?? 0,
    inferMs: worker?.inferMs ?? 0,
    uploadMs: worker?.uploadMs ?? 0,
    completeMs,
    totalMs: Math.max(0, end - ready),
    serviceMs: Math.max(0, end - claimed),
  };
}

interface CompleteRow extends Timeline {
  task_id: string | null;
  status: ItemStatus | "job_done";
  stage: Stage | null;
  job_id: string | null;
  finalised: boolean;
  classify_task_id: string | null;
  event_id: string | null;
  event_at: Date | null;
  event_type: string | null;
  event_detail: Record<string, any> | null;
}

export interface BatchResult {
  results: Array<{ taskId: string; status: ItemStatus; error?: string }>;
  leases: Lease[];
}

/**
 * LEASED → SUCCEEDED for a batch of tasks, each fenced on its own lease_epoch, in ONE statement
 * (wb_complete). A stale, unknown or malformed item never fails the batch: it just gets its own
 * status. After the commit: push new classify tasks, publish events and telemetry, and, when
 * `next` > 0, claim the worker's next tasks (complete-and-claim-next).
 */
export async function completeTasks(
  workerId: string,
  items: CompleteItem[],
  next = 0,
  opts: { single?: boolean } = {},
): Promise<BatchResult> {
  const startedAt = performance.now();
  const results = new Map<string, { status: ItemStatus; error?: string }>();
  const valid: NormalisedItem[] = [];
  for (const item of items) {
    const taskId = String(item?.taskId ?? "");
    if (!isUuid(taskId)) {
      results.set(taskId, { status: "not_found" });
      continue;
    }
    try {
      valid.push(normalise(item));
    } catch (err) {
      if (!(err instanceof ValidationError)) throw err;
      results.set(taskId, { status: "invalid", error: err.message });
    }
  }

  let rows: CompleteRow[] = [];
  if (valid.length > 0) {
    ({ rows } = await query<CompleteRow>(`select * from wb_complete($1, $2::jsonb, $3, $4, $5, $6)`, [
      workerId,
      JSON.stringify(valid),
      config.detectorModelVersion,
      config.classifierModelVersion,
      config.animalConfThreshold,
      finishLockThreshold(),
    ]));
  }
  // The coordinator's handling time of the statement, shared out over the items it completed.
  const end = Date.now();
  const completeMs = Math.round(((performance.now() - startedAt) / Math.max(1, valid.length)) * 10) / 10;

  const timingsById = new Map(valid.map((v) => [v.taskId, v.timings]));
  const epochById = new Map(valid.map((v) => [v.taskId, v.leaseEpoch]));
  const events: EventRow[] = [];
  const jobs = new Set<string>();
  const newClassify: string[] = [];
  let finalised = 0;
  let stage: Stage | null = null;
  for (const r of rows) {
    if (r.job_id) jobs.add(r.job_id);
    if (r.event_id !== null) {
      events.push({
        id: Number(r.event_id),
        at: new Date(r.event_at!).toISOString(),
        type: r.event_type!,
        taskId: r.task_id,
        // stale_rejected: the sender; speculation_won/wasted: the attempt whose result won.
        workerId: r.event_type === "stale_rejected" ? workerId : (r.event_detail?.winner ?? null),
        detail: r.event_detail,
      });
    }
    if (r.status === "job_done") continue;
    const taskId = r.task_id!;
    if (r.status === "invalid") {
      results.set(taskId, { status: "invalid", error: "result.detections must be an array" });
      continue;
    }
    results.set(taskId, { status: r.status });
    if (r.status === "stale") {
      telemetry.recordStale(taskId, workerId, epochById.get(taskId)!, r.event_detail?.currentEpoch);
    } else if (r.status === "ok") {
      stage = r.stage;
      if (r.finalised) finalised++;
      if (r.classify_task_id) newClassify.push(r.classify_task_id);
      const sample = timingSample(r.stage!, r, timingsById.get(taskId) ?? null, completeMs, end);
      telemetry.recordCompletion(sample, taskId);
      // Per-worker service times drive straggler detection and probation (speculation.ts).
      serviceTimes.record(workerId, r.stage!, sample.serviceMs, end);
    }
  }
  telemetry.recordFinalized(finalised);
  await traces.completed(workerId, items, rows, startedAt); // before the push: stamps new classify tasks

  if (newClassify.length > 0) {
    // Outbox: the rows committed with queued=false; if this push is lost, the repair sweep does it.
    await pushNew(newClassify).catch((err) => console.error(`[tasks] classify push failed: ${err.message}`));
  }
  publish(events, jobs);
  hub.workersChanged();

  let leases: Lease[] = [];
  // A single complete that wasn't accepted answers 409, which carries no leases: claiming for it
  // would strand them (LEASED to a worker that never hears of them) until their lease expired.
  if (next > 0 && !(opts.single && rows.every((r) => r.status !== "ok"))) {
    stage ??= await workerStage(workerId);
    if (stage) leases = await claimNext(workerId, stage, next);
  }
  return {
    results: items.map((item) => {
      const taskId = String(item?.taskId ?? "");
      return { taskId, ...(results.get(taskId) ?? { status: "not_found" as const }) };
    }),
    leases,
  };
}

async function workerStage(workerId: string): Promise<Stage | null> {
  const { rows } = await query(`select stage from workers where id = $1 and status = 'ALIVE'`, [workerId]);
  return rows[0]?.stage ?? null;
}

/**
 * POST /tasks/:id/complete: one item through completeTasks. A malformed result is a 400
 * (ValidationError) and keeps the lease, as before.
 */
export async function completeTask(
  taskId: string,
  workerId: string,
  leaseEpoch: unknown,
  result: unknown,
  rawTimings?: unknown,
  next = 0,
): Promise<Outcome> {
  if (!isUuid(taskId)) return { status: "not_found" };
  try {
    normalise({ taskId, leaseEpoch, result, timings: rawTimings });
  } catch (err) {
    // Cold path only: an unknown task is a 404 whatever the body; a known one gets the 400.
    const { rowCount } = await query(`select 1 from tasks where id = $1`, [taskId]);
    if (!rowCount) return { status: "not_found" };
    throw err;
  }
  const { results, leases } = await completeTasks(
    workerId,
    [{ taskId, leaseEpoch, result, timings: rawTimings }],
    next,
    { single: true },
  );
  const [r] = results;
  if (r.status === "invalid") throw new ValidationError(r.error ?? "invalid result");
  if (r.status === "ok") return next > 0 ? { status: "ok", leases } : { status: "ok" };
  return { status: r.status };
}

// ---------------------------------------------------------------------------------------------
// fail / retry
// ---------------------------------------------------------------------------------------------

interface RetriedRow {
  id: string;
  stage: string;
  state: "PENDING" | "FAILED";
  attempts: number;
  image_id: string;
}

/**
 * A fail/release whose epoch isn't the task's lease. It may come from the task's running
 * speculative copy: the copy is dropped and the lease carries on, nothing charged (if the error is
 * real, the original will hit it too). A report from the attempt that lost a race gets
 * `already_done`. Anything else is fenced off, as before.
 */
async function endCopyOrReject(
  taskId: string,
  workerId: string,
  epoch: number,
  action: "fail" | "release",
  message: string,
): Promise<Outcome> {
  const events = await tx(async (c) => {
    const { rows: t } = await c.query(
      `select state, lease_epoch, spec_epoch, stage from tasks where id = $1 and spec_epoch = $2 for update`,
      [taskId, epoch],
    );
    if (t.length === 0 || t[0].state !== "LEASED") return null;
    const { rows } = await c.query(
      `update task_attempts set state = 'dropped', finished_at = now(), end_reason = $4
        where task_id = $1 and epoch = $2 and state = 'running' and shadow_epoch = $3
        returning worker_id`,
      [taskId, epoch, t[0].lease_epoch, `${action}: ${message}`.slice(0, 500)],
    );
    if (rows.length === 0) return null;
    return recordEvents(c, [
      action === "release"
        ? { type: "released", taskId, workerId, detail: { reason: message, stage: t[0].stage, speculative: true } }
        : { type: "failed", taskId, workerId, detail: { final: false, error: message, stage: t[0].stage, speculative: true } },
    ]);
  });
  if (events !== null) {
    publish(events, []);
    hub.workersChanged();
    return { status: "ok" };
  }
  const { rows } = await query(`select wb_late_outcome($1, $2) as outcome`, [taskId, epoch]);
  if (rows[0].outcome === "already_done") return { status: "already_done" };
  return rejectStale(taskId, workerId, epoch, action);
}

/** Records a rejected (fenced-off) fail/release and tells the caller whether the task exists. */
async function rejectStale(taskId: string, workerId: string, leaseEpoch: number, action: string): Promise<Outcome> {
  const { rows } = await query(`select state, lease_epoch, worker_id from tasks where id = $1`, [taskId]);
  if (rows.length === 0) return { status: "not_found" };
  const events = await recordEvents(getPool(), [
    {
      type: "stale_rejected",
      taskId,
      workerId,
      detail: { action, leaseEpoch, currentEpoch: rows[0].lease_epoch, state: rows[0].state, holder: rows[0].worker_id },
    },
  ]);
  publish(events, []);
  telemetry.recordStale(taskId, workerId, leaseEpoch, rows[0].lease_epoch);
  return { status: "stale" };
}

/**
 * After a task has been moved to PENDING/FAILED by a guarded UPDATE, finalise the images of
 * tasks that ran out of attempts (as 'failed') and finish their jobs if that was the last image.
 */
export async function finalizeFailed(c: pg.PoolClient, failed: RetriedRow[], fx: Effects) {
  const jobIds = new Set<string>();
  let finalised = 0;
  for (const t of failed) {
    const jobId = await finalizeImage(c, t.image_id, "failed");
    if (jobId) {
      jobIds.add(jobId);
      finalised++;
    }
  }
  telemetry.recordFinalized(finalised);
  for (const jobId of [...jobIds].sort()) {
    fx.jobs.add(jobId);
    const done = await maybeFinishJob(c, jobId);
    if (done) fx.events.push(done);
  }
}

let random = Math.random;

/** Test hook: make the backoff jitter deterministic. Pass nothing to restore Math.random. */
export function setJitterSource(fn?: () => number) {
  random = fn ?? Math.random;
}

/**
 * Worker-reported task error: counts as an attempt. A retryable error goes back to PENDING with a
 * full-jitter backoff (not_before = now + random(0, min(cap, base * 2^previousErrors))), so a task
 * that fails fast doesn't bounce straight back onto the same broken path. A non-retryable one
 * (e.g. an undecodable image) goes straight to FAILED, i.e. to the DLQ.
 */
export async function failTask(
  taskId: string,
  workerId: string,
  leaseEpoch: number,
  error: unknown,
  nonRetryable = false,
): Promise<Outcome> {
  if (!isUuid(taskId)) return { status: "not_found" };
  const epoch = Number(leaseEpoch);
  if (!Number.isInteger(epoch)) throw new ValidationError("leaseEpoch must be an integer");
  const message = String(error ?? "unknown error").slice(0, 2000);

  const fx: Effects = { events: [], jobs: new Set() };
  const outcome = await tx(async (c) => {
    const { rows } = await c.query<RetriedRow & { worker_id: string; retry_in_ms: number | null }>(
      `with old as (select id, worker_id, task_errors from tasks where id = $1),
       decided as (
         select old.*, (not $5::boolean and t.attempts + 1 < $3) as retry
           from old join tasks t on t.id = old.id
       )
       update tasks t
          set attempts = t.attempts + 1,
              task_errors = t.task_errors + 1,
              state = case when d.retry then 'PENDING' else 'FAILED' end,
              finished_at = case when d.retry then null else now() end,
              pending_at = now(),
              not_before = case when d.retry
                                then now() + ($6::float8 * least($7::float8, $8::float8 * power(2, d.task_errors)))
                                             * interval '1 millisecond' end,
              queued = false, worker_id = null, lease_expires_at = null, error = $4
         from decided d
        where t.id = d.id and t.state = 'LEASED' and t.lease_epoch = $2
        returning t.id, t.stage, t.state, t.attempts, t.image_id, d.worker_id,
                  (extract(epoch from (t.not_before - now())) * 1000)::int as retry_in_ms`,
      [taskId, epoch, config.maxAttempts, message, nonRetryable, random(), config.retryMaxMs, config.retryBaseMs],
    );
    if (rows.length === 0) return null;
    const t = rows[0];
    metrics.failed(t.stage, t.state === "FAILED");
    const { rows: imgs } = await c.query(`select job_id from images where id = $1`, [t.image_id]);
    fx.jobs.add(imgs[0].job_id);
    fx.events.push({
      type: "failed",
      taskId,
      workerId: t.worker_id,
      detail: {
        final: t.state === "FAILED",
        attempts: t.attempts,
        error: message,
        nonRetryable,
        ...(t.retry_in_ms !== null ? { retryInMs: t.retry_in_ms } : {}),
      },
    });
    if (t.state === "FAILED") await finalizeFailed(c, [t], fx);
    return { events: await commitEffects(c, fx), retryInMs: t.state === "PENDING" ? (t.retry_in_ms ?? 0) : null };
  });

  if (outcome === null) return endCopyOrReject(taskId, workerId, epoch, "fail", message);
  publish(outcome.events, fx.jobs);
  hub.workersChanged();
  // Re-dispatch when the backoff ends (push mode), not on whichever sweep comes after it.
  if (outcome.retryInMs !== null) {
    if (outcome.retryInMs <= 0) await pushNow([taskId]);
    else pushAt(taskId, outcome.retryInMs);
  }
  return { status: "ok" };
}

/**
 * Worker-reported infrastructure error (MinIO down, a timeout): the task is fine, the path to it
 * isn't. Back to PENDING without spending an attempt; fenced like complete/fail.
 */
export async function releaseTask(taskId: string, workerId: string, leaseEpoch: number, reason: unknown): Promise<Outcome> {
  if (!isUuid(taskId)) return { status: "not_found" };
  const epoch = Number(leaseEpoch);
  if (!Number.isInteger(epoch)) throw new ValidationError("leaseEpoch must be an integer");
  const why = String(reason ?? "released").slice(0, 500);

  const rows = await tx(async (c) => {
    const { rows } = await c.query(
      `update tasks set state = 'PENDING', releases = releases + 1, pending_at = now(),
                        queued = false, worker_id = null, lease_expires_at = null
        where id = $1 and state = 'LEASED' and lease_epoch = $2
        returning stage`,
      [taskId, epoch],
    );
    if (rows.length === 0) return null;
    return recordEvents(c, [{ type: "released", taskId, workerId, detail: { reason: why, stage: rows[0].stage } }]);
  });

  if (rows === null) return endCopyOrReject(taskId, workerId, epoch, "release", why);
  publish(rows, []);
  hub.workersChanged();
  await pushNow([taskId]); // no backoff: it should run on a healthy worker at once
  return { status: "ok" };
}

export interface Requeued {
  id: string;
  stage: string;
  oldWorker: string | null;
  workerGone: boolean;
  /** True when the coordinator itself killed or paused the worker: no attempt was charged. */
  induced: boolean;
  /** The lost attempt (for its trace span): its epoch, claim time and the task's trace context. */
  leaseEpoch?: number;
  startedAt?: Date | null;
  traceparent?: string | null;
}

/**
 * Reaper / death-watch transition: LEASED tasks whose lease expired, or whose worker is no longer
 * ALIVE, go back to PENDING or, once attempts run out, to FAILED. One statement; rows a concurrent
 * complete/claim is holding are skipped (SKIP LOCKED) and picked up next pass.
 *
 * - `workerIds` limits the sweep to those workers (the death path recovers exactly the dead ones).
 * - `graceMs` extends every lease by the reaper's own recent stall, so a coordinator that froze
 *   doesn't blame workers for heartbeats it failed to process.
 * - A lease lost because the coordinator itself killed or paused the worker is not the task's
 *   fault: it costs a release, not an attempt. A kill ends the incarnation, so every lease that
 *   incarnation held is ours (killed_at >= registered_at; a task can be claimed in the few hundred
 *   ms between our stamp and the container actually dying). A pause covers tasks started before
 *   it ended (started_at <= paused_until, for a pause of this incarnation).
 */
export async function requeueLostLeases(
  opts: { workerIds?: string[]; graceMs?: number } = {},
): Promise<{ requeued: number; failed: number; tasks: Requeued[] }> {
  const fx: Effects = { events: [], jobs: new Set() };
  const graceMs = Math.max(0, Math.round(opts.graceMs ?? 0));
  const result = await tx(async (c) => {
    // A lost lease with a healthy speculative copy running: the copy becomes the lease, nothing
    // is requeued (and those tasks no longer match the requeue below).
    const promoted = await promoteCopies(c, { graceMs, workerIds: opts.workerIds ?? null });
    const perWorker = new Map<string, number>();
    for (const p of promoted) {
      fx.events.push(promotionEvent(p));
      if (p.worker_gone && p.old_worker) perWorker.set(p.old_worker, (perWorker.get(p.old_worker) ?? 0) + 1);
    }

    const { rows } = await c.query<
      RetriedRow & {
        old_worker: string | null;
        worker_gone: boolean;
        induced: boolean;
        lease_epoch: number;
        started_at: Date | null;
        traceparent: string | null;
      }
    >(
      `with lost as (
         select t.id, t.worker_id as old_worker, (w.status is distinct from 'ALIVE') as worker_gone,
                coalesce(w.killed_at >= w.registered_at, false)
                  or coalesce(w.paused_at >= w.registered_at and t.started_at <= w.paused_until, false) as induced
           from tasks t left join workers w on w.id = t.worker_id
          where t.state = 'LEASED'
            and ($3::text[] is null or t.worker_id = any($3::text[]))
            and (t.lease_expires_at < now() - ($2::int * interval '1 millisecond')
                 or w.status is distinct from 'ALIVE')
          for update of t skip locked
       ),
       decided as (
         select lost.*, (lost.induced or t.attempts + 1 < $1) as retry
           from lost join tasks t on t.id = lost.id
       )
       update tasks t
          set attempts     = t.attempts + (not d.induced)::int,
              lease_losses = t.lease_losses + (not d.induced)::int,
              releases     = t.releases + d.induced::int,
              state = case when d.retry then 'PENDING' else 'FAILED' end,
              finished_at = case when d.retry then null else now() end,
              error = case when d.retry then t.error
                           else 'lease lost (attempt ' || (t.attempts + 1) || '/' || $1 || ')'
                                || coalesce('; last task error: ' || t.error, '') end,
              pending_at = now(),
              queued = false, worker_id = null, lease_expires_at = null
         from decided d
        where t.id = d.id and t.state = 'LEASED'
        returning t.id, t.stage, t.state, t.attempts, t.image_id, d.old_worker, d.worker_gone, d.induced,
                  t.lease_epoch, t.started_at, t.traceparent`,
      [config.maxAttempts, graceMs, opts.workerIds ?? null],
    );
    if (rows.length === 0 && promoted.length === 0) {
      return { requeued: 0, failed: 0, tasks: [] as Requeued[], events: [] as EventRow[] };
    }

    for (const t of rows) {
      if (t.state === "FAILED") {
        fx.events.push({
          type: "failed",
          taskId: t.id,
          workerId: t.old_worker,
          detail: { final: true, attempts: t.attempts, error: "max attempts exceeded" },
        });
      } else {
        fx.events.push({
          type: t.worker_gone ? "reassigned" : "lease_expired",
          taskId: t.id,
          workerId: t.old_worker,
          detail: { attempts: t.attempts, stage: t.stage, charged: !t.induced },
        });
      }
      if (t.worker_gone && t.old_worker) perWorker.set(t.old_worker, (perWorker.get(t.old_worker) ?? 0) + 1);
    }
    for (const [workerId, n] of perWorker) {
      await c.query(`update workers set reassigned_count = reassigned_count + $2 where id = $1`, [workerId, n]);
    }
    const { rows: jobs } = await c.query(`select distinct job_id from images where id = any($1::uuid[])`, [
      rows.map((r) => r.image_id),
    ]);
    for (const j of jobs) fx.jobs.add(j.job_id);
    const failed = rows.filter((r) => r.state === "FAILED");
    await finalizeFailed(c, failed, fx);
    const tasks: Requeued[] = rows
      .filter((r) => r.state === "PENDING")
      .map((r) => ({
        id: r.id,
        stage: r.stage,
        oldWorker: r.old_worker,
        workerGone: r.worker_gone,
        induced: r.induced,
        leaseEpoch: r.lease_epoch,
        startedAt: r.started_at,
        traceparent: r.traceparent,
      }));
    return { requeued: tasks.length, failed: failed.length, tasks, events: await commitEffects(c, fx) };
  });
  publish(result.events, fx.jobs);
  traces.requeued(result.tasks);
  metrics.leasesLost(result.tasks);
  const dropped = await dropDeadCopies({ graceMs, workerIds: opts.workerIds ?? null });
  if (result.requeued + result.failed + result.events.length + dropped > 0) hub.workersChanged();
  return { requeued: result.requeued, failed: result.failed, tasks: result.tasks };
}

interface PromotedRow {
  id: string;
  stage: string;
  old_worker: string | null;
  old_epoch: number;
  new_worker: string;
  epoch: number;
  worker_gone: boolean;
}

/**
 * Speculated tasks whose lease is lost (expired, its worker not ALIVE, or `released` by it) while
 * their copy is healthy (running, its worker ALIVE, its own lease fresh): the copy becomes the
 * lease. tasks.worker_id/lease_epoch/lease_expires_at/started_at take the copy's values, so from
 * here on it is an ordinary lease (renewed, completed, fenced exactly like one) and the old lease's
 * epoch is fenced off. Not charged: the task never went back to PENDING.
 */
async function promoteCopies(
  c: pg.PoolClient,
  opts: { graceMs: number; workerIds: string[] | null; released?: boolean },
): Promise<PromotedRow[]> {
  const { rows } = await c.query<PromotedRow>(
    `with lost as (
       select t.id, t.stage, t.worker_id as old_worker, t.lease_epoch as old_epoch,
              (w.status is distinct from 'ALIVE') as worker_gone,
              a.epoch, a.worker_id as new_worker, a.started_at as copy_started, a.lease_expires_at as copy_expires
         from tasks t
         join task_attempts a on a.task_id = t.id and a.state = 'running' and a.shadow_epoch = t.lease_epoch
         join workers sw on sw.id = a.worker_id and sw.status = 'ALIVE'
         left join workers w on w.id = t.worker_id
        where t.state = 'LEASED' and t.spec_epoch is not null
          and ($2::text[] is null or t.worker_id = any($2::text[]))
          and ($3::boolean
               or t.lease_expires_at < now() - ($1::int * interval '1 millisecond')
               or w.status is distinct from 'ALIVE')
          and a.lease_expires_at >= now() - ($1::int * interval '1 millisecond')
        for update of t, a skip locked
     ),
     promoted as (
       update tasks t set worker_id = l.new_worker, lease_epoch = l.epoch, lease_expires_at = l.copy_expires,
                          started_at = l.copy_started
         from lost l where t.id = l.id
     ),
     marked as (
       update task_attempts a
          set state = 'promoted',
              end_reason = case when $3::boolean then 'original released its lease'
                                when l.worker_gone then 'original worker died'
                                else 'original lease expired' end
         from lost l where a.task_id = l.id and a.epoch = l.epoch
     )
     select id, stage, old_worker, old_epoch, new_worker, epoch, worker_gone from lost`,
    [opts.graceMs, opts.workerIds, opts.released ?? false],
  );
  return rows;
}

function promotionEvent(p: PromotedRow): EventInput {
  return {
    type: p.worker_gone ? "reassigned" : "lease_expired",
    taskId: p.id,
    workerId: p.old_worker,
    detail: {
      stage: p.stage,
      promoted: true,
      to: p.new_worker,
      epoch: p.epoch,
      previousEpoch: p.old_epoch,
      charged: false,
    },
  };
}

/**
 * Housekeeping for attempts that can no longer win: running copies whose lease expired or whose
 * worker is not ALIVE, copies whose task moved on under them (the lease they shadow ended), and
 * offers not taken within SPECULATE_OFFER_TTL_MS. They become 'dropped' (their reports get
 * STALE_LEASE). Correctness doesn't depend on this sweep (validity is checked against the task
 * row), but it frees those workers to count as idle, and it is what fences a copy whose worker was
 * declared dead. SKIP LOCKED: a row a completion holds is dealt with next pass.
 */
export async function dropDeadCopies(opts: { graceMs?: number; workerIds?: string[] | null } = {}): Promise<number> {
  const { rows } = await query<{ worker_id: string; reason: string }>(
    `with gone as (
       select a.task_id, a.epoch,
              case when w.status is distinct from 'ALIVE' then 'worker not alive'
                   when not (t.state = 'LEASED' and t.lease_epoch = a.shadow_epoch) then 'task moved on'
                   when a.state = 'offered' then 'offer not taken'
                   else 'lease expired' end as reason
         from task_attempts a
         join tasks t on t.id = a.task_id
         left join workers w on w.id = a.worker_id
        where a.state in ('offered', 'running')
          and ($2::text[] is null or a.worker_id = any($2::text[]))
          and (w.status is distinct from 'ALIVE'
               or not (t.state = 'LEASED' and t.lease_epoch = a.shadow_epoch)
               or (a.state = 'running' and a.lease_expires_at < now() - ($1::int * interval '1 millisecond'))
               or (a.state = 'offered' and a.offered_at < now() - ($3::int * interval '1 millisecond')))
        for update of a skip locked
     )
     update task_attempts a set state = 'dropped', finished_at = now(), end_reason = g.reason
       from gone g where a.task_id = g.task_id and a.epoch = g.epoch
     returning a.worker_id, g.reason`,
    [Math.max(0, Math.round(opts.graceMs ?? 0)), opts.workerIds ?? null, config.speculateOfferTtlMs],
  );
  for (const r of rows) if (r.reason === "offer not taken") offerIgnored(r.worker_id);
  return rows.length;
}

/**
 * Graceful release (deregister, or a worker ID re-registering after a restart): the worker's
 * leases go back to PENDING without costing an attempt. Their epochs are bumped on the next claim,
 * so anything this worker still sends for them is fenced off.
 */
export async function releaseWorkerTasks(workerId: string, reason: string): Promise<number> {
  const fx: Effects = { events: [], jobs: new Set() };
  const rows = await tx(async (c) => {
    // A lease with a running speculative copy hands over to the copy instead of being requeued.
    const promoted = await promoteCopies(c, { graceMs: 0, workerIds: [workerId], released: true });
    fx.events.push(
      ...promoted.map((p) => ({
        type: "released",
        taskId: p.id,
        workerId,
        detail: { reason, stage: p.stage, promoted: true, to: p.new_worker, epoch: p.epoch },
      })),
    );
    const { rows } = await c.query(
      `update tasks set state = 'PENDING', releases = releases + 1, pending_at = now(),
                        queued = false, worker_id = null, lease_expires_at = null
        where worker_id = $1 and state = 'LEASED'
        returning id, stage`,
      [workerId],
    );
    fx.events.push(...rows.map((r) => ({ type: "released", taskId: r.id, workerId, detail: { reason, stage: r.stage } })));
    // This worker's own copies and offers end with it (its next incarnation holds nothing).
    await c.query(
      `update task_attempts set state = 'dropped', finished_at = now(), end_reason = $2
        where worker_id = $1 and state in ('offered', 'running')`,
      [workerId, reason],
    );
    return { events: await commitEffects(c, fx), released: rows.map((r) => r.id as string) };
  });
  publish(rows.events, []);
  await pushNow(rows.released);
  return rows.released.length;
}

/**
 * Write-behind for tasks.complete_ms: the coordinator's handling time of a complete is only known
 * after its transaction commits, so it is buffered in memory and written here in one statement
 * per second instead of costing every completion a second round trip.
 */
export async function flushCompleteTimings(): Promise<number> {
  const rows = telemetry.drainCompleteMs();
  if (rows.length === 0) return 0;
  await query(
    `update tasks t set complete_ms = v.ms
       from unnest($1::uuid[], $2::real[]) as v(id, ms)
      where t.id = v.id`,
    [rows.map((r) => r[0]), rows.map((r) => r[1])],
  );
  return rows.length;
}
