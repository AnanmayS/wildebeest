// Per-image tracing (docs/decisions/o-observability.md). One trace per task, following the
// OpenTelemetry messaging conventions:
//
//   create detect            PRODUCER  job creation (root; its traceparent is stored on the task row)
//   ├─ lease detect          SERVER    one per attempt: claimed → settled or lost (epoch, worker)
//   │   └─ process detect    CONSUMER  the worker (fetch / infer / upload / settle children)
//   │       └─ settle detect CLIENT    the worker's complete/fail/release request
//   │           └─ complete detect  SERVER  per task, also inside a batched complete
//   ├─ requeue detect        INTERNAL  reaper / death watch put it back (reassigned, lease_expired)
//   ├─ lease detect          SERVER    attempt 2, higher epoch …
//   ├─ lease detect          SERVER    a speculative copy (P3): its own attempt and epoch, same trace
//   └─ create classify       PRODUCER  under the detect completion that created it
//
// The trace context of a task lives in `tasks.traceparent` (migration 009), not in Redis or in
// process memory, so a task that loses its worker stays in one trace: attempt 1's lease span ends
// with "lease lost", the requeue follows, and attempt 2 runs under a lease with a higher epoch.
//
// Attempt spans are stateless across replicas (coordinator HA): the span ID of a task's PRODUCER
// span and of each attempt's lease span are derived from (task ID, epoch). A claim only computes
// the attempt's context and hands it to the worker in the lease (`traceparent`); whichever replica
// sees the attempt end (complete, fail, release, or the leader's requeue) emits the lease span
// then, with the claim time as its start. The lease span is the coordinator's record of an attempt
// on purpose: a SIGKILLed worker never exports its own open span, but the coordinator closes the
// attempt when it requeues it.
//
// Every exported function returns on its first line when tracing is off.

import { createHash } from "node:crypto";
import {
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  context,
  trace,
  type Attributes,
  type Context,
  type Link,
  type SpanContext,
} from "@opentelemetry/api";
import { query, type Db } from "./db.js";
import { tracingEnabled, withSpanId } from "./otel-sdk.js";

const TRACEPARENT = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;
const ZEROS = /^0+$/;

/** W3C traceparent → span context (null if absent or malformed). */
export function parseTraceparent(tp: unknown): SpanContext | null {
  if (typeof tp !== "string") return null;
  const m = TRACEPARENT.exec(tp);
  if (!m || ZEROS.test(m[1]) || ZEROS.test(m[2])) return null;
  return { traceId: m[1], spanId: m[2], traceFlags: parseInt(m[3], 16) & 1, isRemote: true };
}

export function formatTraceparent(sc: SpanContext): string {
  return `00-${sc.traceId}-${sc.spanId}-${(sc.traceFlags & 0xff).toString(16).padStart(2, "0")}`;
}

/** Deterministic span IDs: any replica can parent under or close a span another one started. */
function derivedSpanId(taskId: string, what: string | number): string {
  const id = createHash("sha256").update(`${taskId}/${what}`).digest("hex").slice(0, 16);
  return ZEROS.test(id) ? "0000000000000001" : id;
}
export const producerSpanId = (taskId: string) => derivedSpanId(taskId, "create");
export const attemptSpanId = (taskId: string, epoch: number) => derivedSpanId(taskId, epoch);

const contextOf = (sc: SpanContext): Context => trace.setSpanContext(ROOT_CONTEXT, sc);
const tracer = () => trace.getTracer("wildebeest-coordinator");

/** A span context in the task's trace (from any context of that trace) with the given span ID. */
const inTrace = (trace0: SpanContext, spanId: string): SpanContext => ({
  traceId: trace0.traceId,
  spanId,
  traceFlags: trace0.traceFlags,
  isRemote: true,
});

/** OTel messaging attributes. The destination is the stage (the queue name template). */
function messaging(operation: string, type: string, stage: string, taskId: string): Attributes {
  return {
    "messaging.system": "wildebeest",
    "messaging.operation.name": operation,
    "messaging.operation.type": type,
    "messaging.destination.name": stage,
    "messaging.message.id": taskId,
    "wildebeest.task_id": taskId,
    "wildebeest.stage": stage,
  };
}

// ---------------------------------------------------------------------------------------------
// Attempts
// ---------------------------------------------------------------------------------------------

/**
 * What this replica knows about attempts it claimed and hasn't seen end: only an enrichment
 * (claim path, speculative flag, the other attempt of a speculated task), never required, since
 * another replica may settle the attempt. Bounded like a cache.
 */
interface Hint {
  epoch: number;
  stage: string;
  workerId: string;
  speculative: boolean;
  via: string;
  claimedAt: number;
  trace: SpanContext;
}
const hints = new Map<string, Hint[]>();
let hintCount = 0;
const MAX_HINTS = 100_000;
const MAX_HINT_AGE_MS = 15 * 60_000;

function forget(taskId: string, epoch?: number) {
  const list = hints.get(taskId);
  if (!list) return;
  const keep = epoch === undefined ? [] : list.filter((h) => h.epoch !== epoch);
  hintCount -= list.length - keep.length;
  if (keep.length) hints.set(taskId, keep);
  else hints.delete(taskId);
}

function pruneHints(now: number) {
  for (const [taskId, list] of hints) {
    if (hintCount <= MAX_HINTS && now - list[0].claimedAt < MAX_HINT_AGE_MS) break; // oldest first
    forget(taskId);
  }
}

interface AttemptEnd {
  taskId: string;
  epoch: number;
  stage: string;
  workerId: string | null;
  /** Any context of the task's trace (flags carry the sampling decision). */
  trace: SpanContext;
  /** Claim time; the span is emitted now, starting then. */
  claimedAt: number | Date | null | undefined;
  event: string;
  attrs?: Attributes;
  error?: string;
}

/** Emits an attempt's lease span, under the task's PRODUCER span, now that the attempt is over. */
function emitAttempt(a: AttemptEnd) {
  const hint = hints.get(a.taskId)?.find((h) => h.epoch === a.epoch);
  const start = a.claimedAt ? new Date(a.claimedAt).getTime() : (hint?.claimedAt ?? Date.now());
  const span = withSpanId(attemptSpanId(a.taskId, a.epoch), () =>
    tracer().startSpan(
      `lease ${a.stage}`,
      {
        kind: SpanKind.SERVER,
        startTime: Math.min(start, Date.now()),
        attributes: {
          ...messaging("lease", "receive", a.stage, a.taskId),
          "messaging.client.id": a.workerId ?? hint?.workerId ?? "",
          "wildebeest.worker_id": a.workerId ?? hint?.workerId ?? "",
          "wildebeest.lease_epoch": a.epoch,
          "wildebeest.speculative": hint?.speculative ?? false,
          ...(hint ? { "wildebeest.claim_via": hint.via } : {}),
        },
      },
      contextOf(inTrace(a.trace, producerSpanId(a.taskId))),
    ),
  );
  span.addEvent(a.event, a.attrs ?? {});
  if (a.error) span.setStatus({ code: SpanStatusCode.ERROR, message: a.error });
  span.end();
  forget(a.taskId, a.epoch);
}

// ---------------------------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------------------------

/**
 * Task creation: a PRODUCER span per task (each its own trace, so the sampler decides per image)
 * and its traceparent written to the row inside the creating transaction, before any push.
 * Unsampled contexts are stored too, so the worker's parent-based sampler agrees.
 */
export async function stampNewTasks(db: Db, tasks: Array<{ id: string; stage: string }>, jobId: string | null) {
  if (!tracingEnabled() || tasks.length === 0) return;
  const t = tracer();
  const job = jobId
    ? t.startSpan("create job", { attributes: { "wildebeest.job_id": jobId, "wildebeest.tasks": tasks.length } }, ROOT_CONTEXT)
    : null;
  const links: Link[] = job ? [{ context: job.spanContext() }] : [];
  const ids: string[] = [];
  const tps: string[] = [];
  for (const task of tasks) {
    const span = withSpanId(producerSpanId(task.id), () =>
      t.startSpan(
        `create ${task.stage}`,
        {
          kind: SpanKind.PRODUCER,
          root: true,
          links,
          attributes: { ...messaging("create", "create", task.stage, task.id), ...(jobId ? { "wildebeest.job_id": jobId } : {}) },
        },
        ROOT_CONTEXT,
      ),
    );
    span.end();
    ids.push(task.id);
    tps.push(formatTraceparent(span.spanContext()));
  }
  job?.end();
  for (let i = 0; i < ids.length; i += 20_000) {
    await db.query(
      `update tasks t set traceparent = v.tp from unnest($1::uuid[], $2::text[]) as v(id, tp) where t.id = v.id`,
      [ids.slice(i, i + 20_000), tps.slice(i, i + 20_000)],
    );
  }
}

/** Synthetic jobs are created by one SQL statement; stamp their tasks afterwards (same tx). */
export async function stampJobTasks(db: Db, jobId: string) {
  if (!tracingEnabled()) return;
  const { rows } = await db.query<{ id: string; stage: string }>(
    `select t.id, t.stage from tasks t join images i on i.id = t.image_id where i.job_id = $1 order by t.enqueued_at`,
    [jobId],
  );
  await stampNewTasks(db, rows, jobId);
}

interface LeaseLike {
  taskId: string;
  leaseEpoch: number;
  stage: string;
  speculative?: boolean;
  traceparent?: string | null;
}

/**
 * Claim (every path: claim-confirm, /tasks/claim, complete + next, speculative copies): the lease
 * handed to the worker carries the context of this attempt's lease span, so its process span nests
 * under the attempt. The span itself is emitted when the attempt ends (emitAttempt).
 */
export function leased(workerId: string, leases: LeaseLike[], via: string) {
  if (!tracingEnabled()) return;
  const now = Date.now();
  for (const l of leases) {
    const stored = parseTraceparent(l.traceparent);
    if (!stored) continue;
    const ctx = inTrace(stored, attemptSpanId(l.taskId, l.leaseEpoch));
    l.traceparent = formatTraceparent(ctx);
    if (!(ctx.traceFlags & 1)) continue; // unsampled: nothing will be emitted, nothing to remember
    const hint = { epoch: l.leaseEpoch, stage: l.stage, workerId, speculative: Boolean(l.speculative), via, claimedAt: now, trace: ctx };
    const list = hints.get(l.taskId);
    if (list) list.push(hint);
    else hints.set(l.taskId, [hint]);
    hintCount++;
  }
  if (hintCount > MAX_HINTS || (hintCount > 0 && now - hints.values().next().value![0].claimedAt > MAX_HINT_AGE_MS)) pruneHints(now);
}

interface CompleteRowLike {
  task_id: string | null;
  status: string;
  stage: string | null;
  started_at?: Date | null;
  classify_task_id: string | null;
  event_detail: Record<string, any> | null;
}

/**
 * Completions (single or batched): a `complete {stage}` span per task in that task's own trace
 * (under the worker's settle span when the item carries its traceparent) and, for an accepted
 * write, the winning attempt's lease span. A fenced-off (stale) write is an error on its own span.
 * Classify tasks created by these completions get their PRODUCER span and traceparent here,
 * before they are pushed.
 */
export async function completed(workerId: string, items: unknown[], rows: CompleteRowLike[], startedAt: number) {
  if (!tracingEnabled() || rows.length === 0) return;
  // Wall clock, like every other span (performance.timeOrigin drifts from Date.now() over hours).
  const startTime = Date.now() - (performance.now() - startedAt);
  const byId = new Map<string, any>();
  for (const it of items as any[]) if (it && typeof it === "object") byId.set(String(it.taskId), it);
  const active = trace.getActiveSpan();
  const relevant = rows.filter((r) => r.task_id && (r.status === "ok" || r.status === "stale" || r.status === "already_done"));

  // The task's trace: from the worker's settle context, the report's own context (single
  // complete), this replica's claim, or, for an untraced worker on another replica, the row.
  const traceOf = new Map<string, SpanContext>();
  const missing: string[] = [];
  for (const r of relevant) {
    const item = byId.get(r.task_id!);
    const sc =
      parseTraceparent(item?.traceparent) ??
      (items.length === 1 && active ? active.spanContext() : undefined) ??
      hints.get(r.task_id!)?.[0]?.trace;
    if (sc) traceOf.set(r.task_id!, sc);
    else missing.push(r.task_id!);
  }
  if (missing.length > 0) {
    const { rows: tp } = await query<{ id: string; traceparent: string }>(
      `select id, traceparent from tasks where id = any($1::uuid[]) and traceparent is not null`,
      [missing],
    ).catch(() => ({ rows: [] as Array<{ id: string; traceparent: string }> }));
    for (const t of tp) {
      const sc = parseTraceparent(t.traceparent);
      if (sc) traceOf.set(t.id, sc);
    }
  }

  const created: Array<{ id: string; parent: SpanContext }> = [];
  for (const r of relevant) {
    const taskId = r.task_id!;
    const sc = traceOf.get(taskId);
    if (!sc) continue;
    const item = byId.get(taskId);
    const epoch = Number(item?.leaseEpoch);
    const fromWorker = parseTraceparent(item?.traceparent);
    const parent =
      active && active.spanContext().traceId === sc.traceId
        ? context.active() // a single traced request: under its http server span
        : contextOf(fromWorker ?? inTrace(sc, attemptSpanId(taskId, epoch)));
    const stage = r.stage ?? hints.get(taskId)?.[0]?.stage ?? "task";
    const accepted = r.status === "ok";
    const span = tracer().startSpan(
      `complete ${stage}`,
      {
        kind: SpanKind.SERVER,
        startTime,
        attributes: {
          ...messaging("complete", "settle", stage, taskId),
          "wildebeest.worker_id": workerId,
          "wildebeest.lease_epoch": epoch,
          "wildebeest.write.accepted": accepted,
          "wildebeest.batch_size": items.length,
        },
      },
      parent,
    );
    if (accepted) {
      emitAttempt({ taskId, epoch, stage, workerId, trace: sc, claimedAt: r.started_at, event: "completed", attrs: { "wildebeest.worker_id": workerId } });
      // Speculation: first commit wins; another attempt this replica claimed is moot from here on.
      for (const h of [...(hints.get(taskId) ?? [])]) {
        emitAttempt({
          taskId, epoch: h.epoch, stage: h.stage, workerId: h.workerId, trace: sc, claimedAt: h.claimedAt,
          event: "lost_race", attrs: { "wildebeest.winner_epoch": epoch, "wildebeest.winner_worker": workerId },
        });
      }
      if (r.classify_task_id) created.push({ id: r.classify_task_id, parent: span.spanContext() });
    } else if (r.status === "already_done") {
      span.addEvent("already_done", { "wildebeest.lease_epoch": epoch });
      span.setAttribute("wildebeest.report.status", "already_done");
    } else {
      const current = r.event_detail?.currentEpoch;
      span.addEvent("stale_rejected", {
        "wildebeest.lease_epoch": epoch,
        "wildebeest.current_epoch": Number(current ?? -1),
        "wildebeest.worker_id": workerId,
      });
      span.setStatus({ code: SpanStatusCode.ERROR, message: `STALE_LEASE: epoch ${epoch} fenced off (current ${current})` });
    }
    span.end();
  }

  if (created.length > 0) {
    const ids: string[] = [];
    const tps: string[] = [];
    for (const c of created) {
      const span = withSpanId(producerSpanId(c.id), () =>
        tracer().startSpan(
          "create classify",
          { kind: SpanKind.PRODUCER, attributes: messaging("create", "create", "classify", c.id) },
          contextOf(c.parent),
        ),
      );
      span.end();
      ids.push(c.id);
      tps.push(formatTraceparent(span.spanContext()));
    }
    await query(
      `update tasks t set traceparent = v.tp from unnest($1::uuid[], $2::text[]) as v(id, tp)
        where t.id = v.id and t.traceparent is null`,
      [ids, tps],
    ).catch((err) => console.error(`[otel] classify traceparent write failed: ${err.message}`));
  }
}

/**
 * Runs a single-task report (complete/fail/release) under the context the worker sent in the body,
 * unless the http instrumentation already continued that trace from the header.
 */
export function withReportContext<T>(traceparent: unknown, fn: () => T): T {
  if (!tracingEnabled()) return fn();
  const sc = parseTraceparent(traceparent);
  if (!sc || trace.getActiveSpan()?.spanContext().traceId === sc.traceId) return fn();
  return context.with(contextOf(sc), fn);
}

/** /fail and /release that were accepted: the attempt ends. */
export async function settled(
  taskId: string,
  leaseEpoch: unknown,
  action: "fail" | "release",
  status: string,
  traceparent: unknown,
  detail: Attributes = {},
) {
  if (!tracingEnabled() || status !== "ok") return;
  const epoch = Number(leaseEpoch);
  const hint = hints.get(taskId)?.find((h) => h.epoch === epoch);
  let sc = parseTraceparent(traceparent) ?? hint?.trace ?? null;
  let stage = hint?.stage ?? null;
  if (!sc || !stage) {
    const { rows } = await query<{ traceparent: string | null; stage: string }>(
      `select traceparent, stage from tasks where id = $1`,
      [taskId],
    ).catch(() => ({ rows: [] as Array<{ traceparent: string | null; stage: string }> }));
    sc ??= parseTraceparent(rows[0]?.traceparent);
    stage ??= rows[0]?.stage ?? "task";
  }
  if (!sc) return;
  const error = action === "fail" ? String(detail["wildebeest.error"] ?? "task failed") : undefined;
  emitAttempt({ taskId, epoch, stage, workerId: hint?.workerId ?? null, trace: sc, claimedAt: hint?.claimedAt, event: action === "fail" ? "failed" : "released", attrs: detail, error });
}

/** Death path (recovery.ts): remember how each worker was found dead, for the requeue spans. */
const deaths = new Map<string, { via: string; detectMs: number; at: number }>();
export function workersDied(list: Array<{ workerId: string; via: string; detectMs: number }>) {
  if (!tracingEnabled()) return;
  const now = Date.now();
  for (const d of list) deaths.set(d.workerId, { via: d.via, detectMs: d.detectMs, at: now });
  for (const [id, d] of deaths) if (now - d.at > 60_000) deaths.delete(id);
}

interface RequeuedLike {
  id: string;
  stage: string;
  oldWorker: string | null;
  workerGone: boolean;
  induced: boolean;
  leaseEpoch?: number;
  startedAt?: Date | null;
  traceparent?: string | null;
}

/**
 * Reaper / death watch requeue (the leader): the lost attempt's lease span is emitted ending in
 * error ("lease lost"), and a `requeue` span under the task's PRODUCER span records the
 * reassignment, so attempt 1, the requeue and attempt 2 read top to bottom in one trace.
 */
export function requeued(tasks: RequeuedLike[]) {
  if (!tracingEnabled() || tasks.length === 0) return;
  for (const t of tasks) {
    const sc = parseTraceparent(t.traceparent);
    if (!sc) continue;
    const death = t.oldWorker ? deaths.get(t.oldWorker) : undefined;
    const event = t.workerGone ? "reassigned" : "lease_expired";
    const attrs: Attributes = {
      "wildebeest.reason": t.workerGone ? "worker_dead" : "lease_expired",
      "wildebeest.worker_id": t.oldWorker ?? "",
      "wildebeest.charged": !t.induced,
      ...(death ? { "wildebeest.death.via": death.via, "wildebeest.death.detect_ms": death.detectMs } : {}),
    };
    if (t.leaseEpoch) {
      for (const h of [...(hints.get(t.id) ?? [])]) {
        // A speculative copy this replica leased can't outlive the lease it shadowed.
        if (h.epoch !== t.leaseEpoch) emitAttempt({ taskId: t.id, epoch: h.epoch, stage: h.stage, workerId: h.workerId, trace: sc, claimedAt: h.claimedAt, event: "dropped", attrs });
      }
      const why = t.workerGone ? `lease lost: ${t.oldWorker} died${death ? ` (${death.via})` : ""}` : "lease expired";
      emitAttempt({ taskId: t.id, epoch: t.leaseEpoch, stage: t.stage, workerId: t.oldWorker, trace: sc, claimedAt: t.startedAt, event, attrs, error: why });
    }
    const span = tracer().startSpan(
      `requeue ${t.stage}`,
      { attributes: { ...messaging("requeue", "send", t.stage, t.id), ...attrs } },
      contextOf(inTrace(sc, producerSpanId(t.id))),
    );
    span.addEvent(event, attrs);
    span.end();
  }
}

/** Test hook. */
export function resetTraceState() {
  hints.clear();
  hintCount = 0;
  deaths.clear();
}
