import type pg from "pg";
import { config } from "./config.js";
import { getPool, query, tx, type Db } from "./db.js";
import { hub, recordEvents, type EventInput, type EventRow } from "./events.js";
import { getRedis, keys } from "./redis.js";
import {
  categorize,
  enqueueClassify,
  finalizeImage,
  getClassification,
  maybeFinishJob,
  storeClassification,
  storeDetection,
  type Detection,
} from "./results.js";

// Task state machine (PRD 6.1):
//
//   PENDING --claim--> LEASED --complete--> SUCCEEDED
//   LEASED --lease expired / worker dead / fail--> PENDING (attempts+1)  or  FAILED (attempts >= MAX)
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

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const isUuid = (s: unknown): s is string => typeof s === "string" && UUID_RE.test(s);

export class ValidationError extends Error {}

export interface Lease {
  taskId: string;
  leaseEpoch: number;
  stage: "detect" | "classify";
  imageKey: string;
  sha256: string;
  countryCode: string;
  detections: Detection[] | null;
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
 * PENDING → LEASED for each ID the worker BLMOVEd. Only tasks still PENDING, of the worker's own
 * stage, and only while the worker is ALIVE, get leased; everything else is silently skipped
 * (that is what makes duplicate queue entries harmless).
 */
export async function claimConfirm(workerId: string, taskIds: string[]): Promise<Lease[]> {
  const ids = [...new Set(taskIds.filter(isUuid))];
  if (ids.length === 0) return [];

  const { leases, rows } = await tx(async (c) => {
    const { rows: leased } = await c.query(
      `update tasks t
          set state = 'LEASED', lease_epoch = t.lease_epoch + 1, worker_id = $1,
              lease_expires_at = now() + ($2::int * interval '1 millisecond'),
              started_at = now(), queued = false
        where t.id = any($3::uuid[]) and t.state = 'PENDING'
          and exists (select 1 from workers w where w.id = $1 and w.status = 'ALIVE' and w.stage = t.stage)
        returning t.id, t.lease_epoch, t.stage, t.image_id`,
      [workerId, config.leaseMs, ids],
    );
    if (leased.length === 0) return { leases: [] as Lease[], rows: [] as EventRow[] };

    const { rows: info } = await c.query(
      `select i.id, i.sha256, i.object_key, coalesce(j.country_code, $2) as country_code, d.detections
         from images i
         join jobs j on j.id = i.job_id
         left join detection_results d on d.sha256 = i.sha256 and d.model_version = $3
        where i.id = any($1::uuid[])`,
      [leased.map((r) => r.image_id), config.defaultCountry, config.detectorModelVersion],
    );
    const byImage = new Map(info.map((r) => [r.id, r]));
    const leases: Lease[] = leased.map((r) => {
      const img = byImage.get(r.image_id);
      return {
        taskId: r.id,
        leaseEpoch: r.lease_epoch,
        stage: r.stage,
        imageKey: img.object_key,
        sha256: img.sha256,
        countryCode: img.country_code,
        detections: r.stage === "classify" ? (img.detections ?? []) : null,
      };
    });
    const rows = await recordEvents(
      c,
      leases.map((l) => ({ type: "claimed", taskId: l.taskId, workerId, detail: { leaseEpoch: l.leaseEpoch } })),
    );
    return { leases, rows };
  });

  // The IDs are now either leased (tracked in Postgres) or not ours to run; either way they
  // leave the processing list. Any ID we refused that is still waiting to run goes back.
  await lremProcessing(workerId, ids);
  const leasedIds = new Set(leases.map((l) => l.taskId));
  const refused = ids.filter((id) => !leasedIds.has(id));
  if (refused.length > 0) await requeueWaiting(refused);

  publish(rows, []);
  if (leases.length > 0) hub.workersChanged();
  return leases;
}

/** RPUSHes back IDs whose task is PENDING and marked queued (i.e. it should be in a ready queue). */
export async function requeueWaiting(taskIds: string[]): Promise<number> {
  const ids = taskIds.filter(isUuid);
  if (ids.length === 0) return 0;
  const { rows } = await query(
    `select id, stage from tasks where id = any($1::uuid[]) and state = 'PENDING' and queued`,
    [ids],
  );
  if (rows.length === 0) return 0;
  const pipe = getRedis().pipeline();
  for (const r of rows) pipe.rpush(keys.queue(r.stage), r.id);
  await pipe.exec();
  return rows.length;
}

// ---------------------------------------------------------------------------------------------
// complete
// ---------------------------------------------------------------------------------------------

export type Outcome = { status: "ok" } | { status: "stale" } | { status: "not_found" };

function num(v: unknown, what: string): number {
  const n = Number(v);
  if (!Number.isFinite(n)) throw new ValidationError(`${what} must be a number`);
  return n;
}

function parseDetections(result: any): Detection[] {
  if (!result || !Array.isArray(result.detections)) throw new ValidationError("result.detections must be an array");
  return result.detections.map((d: any, i: number) => {
    if (!d || typeof d.label !== "string") throw new ValidationError(`detections[${i}].label must be a string`);
    const bbox = Array.isArray(d.bbox) ? d.bbox.map((x: unknown) => num(x, `detections[${i}].bbox`)) : [];
    return { label: d.label, conf: num(d.conf, `detections[${i}].conf`), bbox };
  });
}

function parseClassification(result: any) {
  if (!result || typeof result !== "object") throw new ValidationError("result must be an object");
  const str = (v: unknown) => (typeof v === "string" && v !== "" ? v : null);
  return {
    label: str(result.label),
    commonName: str(result.commonName),
    confidence: result.confidence == null ? null : num(result.confidence, "confidence"),
    cropKey: str(result.cropKey),
    raw: result.raw ?? null,
  };
}

function checkModelVersion(stage: string, reported: unknown) {
  const expected = stage === "detect" ? config.detectorModelVersion : config.classifierModelVersion;
  if (reported !== undefined && reported !== expected) {
    console.warn(`[tasks] ${stage} result reports modelVersion=${String(reported)}, storing under ${expected}`);
  }
}

/** Records a rejected (fenced-off) complete/fail and tells the caller whether the task exists. */
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
  await lremProcessing(workerId, [taskId]);
  return { status: "stale" };
}

/**
 * LEASED → SUCCEEDED, fenced on lease_epoch, then stores the result and moves the image forward
 * (finalise it, or create its classify task) in the same transaction.
 */
export async function completeTask(taskId: string, workerId: string, leaseEpoch: number, result: unknown): Promise<Outcome> {
  if (!isUuid(taskId)) return { status: "not_found" };
  const epoch = Number(leaseEpoch);
  if (!Number.isInteger(epoch)) throw new ValidationError("leaseEpoch must be an integer");

  // Validate before touching state, so a malformed body doesn't consume the lease.
  const { rows: peek } = await query(`select stage from tasks where id = $1`, [taskId]);
  if (peek.length === 0) return { status: "not_found" };
  const stage: "detect" | "classify" = peek[0].stage;
  const parsed = stage === "detect" ? parseDetections(result) : parseClassification(result);
  checkModelVersion(stage, (result as any)?.modelVersion);

  const fx: Effects = { events: [], jobs: new Set() };
  const outcome = await tx(async (c) => {
    const { rows } = await c.query(
      `update tasks set state = 'SUCCEEDED', finished_at = now(), lease_expires_at = null, queued = false
        where id = $1 and state = 'LEASED' and lease_epoch = $2
        returning image_id, worker_id`,
      [taskId, epoch],
    );
    if (rows.length === 0) return null;
    const { image_id: imageId, worker_id: holder } = rows[0];
    const { rows: imgs } = await c.query(`select sha256, job_id from images where id = $1`, [imageId]);
    const { sha256, job_id: jobId } = imgs[0];
    fx.jobs.add(jobId);

    let finalised: string | null = null;
    if (stage === "detect") {
      const stored = await storeDetection(c, sha256, parsed as Detection[]);
      const category = categorize(stored);
      if (category === "animal") {
        // Another job may already have classified this exact photo.
        const cached = await getClassification(c, sha256);
        if (cached) {
          finalised = await finalizeImage(c, imageId, "animal", cached);
        } else {
          const classifyId = await enqueueClassify(c, imageId);
          if (classifyId) fx.events.push({ type: "enqueued", taskId: classifyId, detail: { stage: "classify" } });
        }
      } else {
        finalised = await finalizeImage(c, imageId, category);
      }
    } else {
      const stored = await storeClassification(c, sha256, parsed as ReturnType<typeof parseClassification>);
      finalised = await finalizeImage(c, imageId, "animal", stored);
    }

    await c.query(`update workers set tasks_completed = tasks_completed + 1 where id = $1`, [holder]);
    fx.events.push({ type: "succeeded", taskId, workerId: holder, detail: { stage, leaseEpoch: epoch } });
    if (finalised) {
      const done = await maybeFinishJob(c, finalised);
      if (done) fx.events.push(done);
    }
    return commitEffects(c, fx);
  });

  if (outcome === null) return rejectStale(taskId, workerId, epoch, "complete");
  await lremProcessing(workerId, [taskId]);
  publish(outcome, fx.jobs);
  hub.workersChanged();
  return { status: "ok" };
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
 * After a task has been moved to PENDING/FAILED by a guarded UPDATE, finalise the images of
 * tasks that ran out of attempts (as 'failed') and finish their jobs if that was the last image.
 */
export async function finalizeFailed(c: pg.PoolClient, failed: RetriedRow[], fx: Effects) {
  const jobIds = new Set<string>();
  for (const t of failed) {
    const jobId = await finalizeImage(c, t.image_id, "failed");
    if (jobId) jobIds.add(jobId);
  }
  for (const jobId of [...jobIds].sort()) {
    fx.jobs.add(jobId);
    const done = await maybeFinishJob(c, jobId);
    if (done) fx.events.push(done);
  }
}

/** Worker-reported failure: counts as an attempt, like a lost lease. */
export async function failTask(taskId: string, workerId: string, leaseEpoch: number, error: unknown): Promise<Outcome> {
  if (!isUuid(taskId)) return { status: "not_found" };
  const epoch = Number(leaseEpoch);
  if (!Number.isInteger(epoch)) throw new ValidationError("leaseEpoch must be an integer");
  const message = String(error ?? "unknown error").slice(0, 2000);

  const fx: Effects = { events: [], jobs: new Set() };
  const outcome = await tx(async (c) => {
    const { rows } = await c.query<RetriedRow & { worker_id: string }>(
      `update tasks t
          set attempts = t.attempts + 1,
              state = case when t.attempts + 1 < $3 then 'PENDING' else 'FAILED' end,
              finished_at = case when t.attempts + 1 < $3 then null else now() end,
              queued = false, worker_id = null, lease_expires_at = null, error = $4
         from (select id, worker_id from tasks where id = $1) old
        where t.id = $1 and t.id = old.id and t.state = 'LEASED' and t.lease_epoch = $2
        returning t.id, t.stage, t.state, t.attempts, t.image_id, old.worker_id`,
      [taskId, epoch, config.maxAttempts, message],
    );
    if (rows.length === 0) return null;
    const t = rows[0];
    const { rows: imgs } = await c.query(`select job_id from images where id = $1`, [t.image_id]);
    fx.jobs.add(imgs[0].job_id);
    fx.events.push({
      type: "failed",
      taskId,
      workerId: t.worker_id,
      detail: { final: t.state === "FAILED", attempts: t.attempts, error: message },
    });
    if (t.state === "FAILED") await finalizeFailed(c, [t], fx);
    return commitEffects(c, fx);
  });

  if (outcome === null) return rejectStale(taskId, workerId, epoch, "fail");
  await lremProcessing(workerId, [taskId]);
  publish(outcome, fx.jobs);
  hub.workersChanged();
  return { status: "ok" };
}

/**
 * Reaper transition: every LEASED task whose lease expired or whose worker is no longer ALIVE
 * goes back to PENDING (attempts+1) or to FAILED once attempts run out. One statement; rows a
 * concurrent complete/claim is holding are skipped (SKIP LOCKED) and picked up next pass.
 */
export async function requeueLostLeases(): Promise<{ requeued: number; failed: number }> {
  const fx: Effects = { events: [], jobs: new Set() };
  const result = await tx(async (c) => {
    const { rows } = await c.query<RetriedRow & { old_worker: string | null; worker_gone: boolean }>(
      `with lost as (
         select t.id, t.worker_id as old_worker, (w.status is distinct from 'ALIVE') as worker_gone
           from tasks t left join workers w on w.id = t.worker_id
          where t.state = 'LEASED' and (t.lease_expires_at < now() or w.status is distinct from 'ALIVE')
          for update of t skip locked
       )
       update tasks t
          set attempts = t.attempts + 1,
              state = case when t.attempts + 1 < $1 then 'PENDING' else 'FAILED' end,
              finished_at = case when t.attempts + 1 < $1 then null else now() end,
              error = case when t.attempts + 1 < $1 then t.error
                           else 'lease lost ' || (t.attempts + 1) || ' times' end,
              queued = false, worker_id = null, lease_expires_at = null
         from lost
        where t.id = lost.id and t.state = 'LEASED'
        returning t.id, t.stage, t.state, t.attempts, t.image_id, lost.old_worker, lost.worker_gone`,
      [config.maxAttempts],
    );
    if (rows.length === 0) return { requeued: 0, failed: 0, events: [] as EventRow[] };

    const perWorker = new Map<string, number>();
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
          detail: { attempts: t.attempts, stage: t.stage },
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
    return { requeued: rows.length - failed.length, failed: failed.length, events: await commitEffects(c, fx) };
  });
  publish(result.events, fx.jobs);
  if (result.requeued + result.failed > 0) hub.workersChanged();
  return { requeued: result.requeued, failed: result.failed };
}

/**
 * Graceful release (deregister, or a worker ID re-registering after a restart): the worker's
 * leases go back to PENDING without costing an attempt. Their epochs are bumped on the next claim,
 * so anything this worker still sends for them is fenced off.
 */
export async function releaseWorkerTasks(workerId: string, reason: string): Promise<number> {
  const fx: Effects = { events: [], jobs: new Set() };
  const rows = await tx(async (c) => {
    const { rows } = await c.query(
      `update tasks set state = 'PENDING', queued = false, worker_id = null, lease_expires_at = null
        where worker_id = $1 and state = 'LEASED'
        returning id, stage`,
      [workerId],
    );
    fx.events.push(...rows.map((r) => ({ type: "released", taskId: r.id, workerId, detail: { reason, stage: r.stage } })));
    return commitEffects(c, fx);
  });
  publish(rows, []);
  return rows.length;
}
