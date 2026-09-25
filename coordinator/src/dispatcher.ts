import { config } from "./config.js";
import { getPool, query } from "./db.js";
import { hub, recordEvents } from "./events.js";
import { getRedis, keys } from "./redis.js";
import { telemetry } from "./telemetry.js";

// Dispatch: getting PENDING task IDs in front of workers.
//
// CLAIM_MODE=hybrid (default): Postgres is the source of truth, Redis lists are the ready queues.
// A task is created PENDING with queued=false; pushing it flips queued=true and RPUSHes/LPUSHes
// the ID. Holding detect tasks back in Postgres is what makes backpressure possible.
//
//   DISPATCH_MODE=push (default): whoever commits a transaction that makes tasks PENDING pushes
//   them right after the commit (an outbox without a table: the row itself, queued=false, is the
//   outbox entry):
//     job created            → classify tasks pushed, detect queue topped up
//     detect completed       → its new classify task pushed (tail)
//     requeue / release / redrive / deregister → pushed to the head (pushNow)
//     retry backoff ends     → pushed by a timer (pushAt)
//     a worker claimed detect work → detect queue topped up (kickDetect, single-flight)
//   The 200 ms tick is a repair sweep: it pushes whatever is still queued=false (a crash between
//   commit and push, a failed Redis call) and runs throttle hysteresis.
//
//   DISPATCH_MODE=tick: the P1 behaviour, the tick does all dispatching (kept for A/B runs).
//
// CLAIM_MODE=postgres: there is no ready queue. Workers long-poll POST /tasks/claim, which leases
// straight from Postgres; "pushing" becomes waking the long-polls that wait for that stage.
//
// Detect admission: queue:detect is kept at detectQueueTarget(), which scales with the live detect
// workers' claim windows, and nothing new is admitted while the classify stage is throttled.

export type Stage = "detect" | "classify";
const STAGES: Stage[] = ["detect", "classify"];

let throttled = false;
/** Start of the previous detect top-up: a task ready before it but not pushed by it was held back. */
let lastTopUpAt: Date | null = null;
/** Refreshed every tick (and when workers come and go); cheap aggregates the hot path reads. */
let stats = { leased: 0, alive: 0, detectWindow: 0 };
/** Test hook: behave as if the process died between a commit and its push. */
let eventPushesSuspended = false;

export const isThrottled = () => throttled;
const pushMode = () => config.dispatchMode === "push";
const hybrid = () => config.claimMode === "hybrid";

/** Test hook: forget the in-memory throttle and sweep state. */
export function resetDispatcherState() {
  throttled = false;
  lastTopUpAt = null;
  stats = { leased: 0, alive: 0, detectWindow: 0 };
  eventPushesSuspended = false;
  for (const t of retryTimers) clearTimeout(t);
  retryTimers.clear();
}

/** Test hook: drop every post-commit push, as a crash right after COMMIT would. */
export function suspendEventPushes(suspended = true) {
  eventPushesSuspended = suspended;
}

// ---------------------------------------------------------------------------------------------
// Sizing
// ---------------------------------------------------------------------------------------------

/**
 * How many detect IDs queue:detect should hold: every live detect worker can take a full claim
 * window without waiting for a refill, twice over (a refill happens right after each claim, the
 * factor 2 covers the refill's own latency). DETECT_QUEUE_TARGET > 0 pins it.
 */
export function detectQueueTarget(): number {
  if (config.detectQueueTarget > 0) return config.detectQueueTarget;
  return Math.max(config.detectQueueMin, 2 * stats.detectWindow);
}

/**
 * How many unfinalised images a job may have left before a finalisation bothers to lock the job
 * row (wb_finish_job). Images can only be finishing concurrently if they are in flight, so the
 * number of leases (plus one batch per worker, for leases taken since the last tick) bounds it.
 */
export function finishLockThreshold(): number {
  return Math.max(1, stats.leased + stats.alive);
}

export async function refreshStats() {
  const { rows } = await query<{ leased: number; alive: number; detect_window: number }>(
    `select (select count(*)::int from tasks where state = 'LEASED') as leased,
            (select count(*)::int from workers where status = 'ALIVE') as alive,
            (select coalesce(sum(least($2::int, greatest(1,
                      case when metrics->>'claimBatch' ~ '^[0-9]+$' then (metrics->>'claimBatch')::int else $1 end))), 0)::int
               from workers where status = 'ALIVE' and stage = 'detect') as detect_window`,
    [config.claimBatchSize, 2 * config.maxClaimBatch],
  );
  stats = { leased: rows[0].leased, alive: rows[0].alive, detectWindow: rows[0].detect_window };
}

// ---------------------------------------------------------------------------------------------
// Pushing (hybrid mode)
// ---------------------------------------------------------------------------------------------

/** When the current push could first have happened, for eligible_at (see pushPending). */
interface Window {
  at: Date;
  previousAt: Date;
}

/** Pushes already-marked rows to Redis; on failure un-marks them so the repair sweep retries. */
async function pushRows(rows: Array<{ id: string; stage: string }>, position: "head" | "tail"): Promise<number> {
  if (rows.length === 0) return 0;
  const byStage = new Map<string, string[]>();
  for (const r of rows) byStage.set(r.stage, [...(byStage.get(r.stage) ?? []), r.id]);
  try {
    const pipe = getRedis().pipeline();
    for (const [stage, ids] of byStage) {
      // LPUSH reverses its arguments, so push the reversed list to keep the head in order.
      if (position === "head") pipe.lpush(keys.queue(stage), ...[...ids].reverse());
      else pipe.rpush(keys.queue(stage), ...ids);
    }
    const results = (await pipe.exec()) ?? [];
    const failed = results.find(([err]) => err);
    if (failed) throw failed[0];
  } catch (err) {
    await query(`update tasks set queued = false where id = any($1::uuid[]) and state = 'PENDING'`, [
      rows.map((r) => r.id),
    ]);
    throw err;
  }
  telemetry.recordPushed(rows.length);
  return rows.length;
}

/**
 * Marks up to `limit` oldest PENDING, not-yet-queued, dispatchable tasks of a stage as queued and
 * pushes them. New tasks go to the tail (RPUSH). Retries (tasks that already ran: their worker
 * died, the lease was lost, it failed or was released) go to the head (LPUSH), so recovered work
 * runs next instead of waiting behind a full queue. A task in its retry backoff waits.
 *
 * eligible_at (for dispatchWaitMs): with a `window`, a task that became ready after the previous
 * push attempt started could not have been pushed earlier, so it was eligible from the moment it
 * was ready; one that was ready before and not pushed then was held back by the queue target or
 * backpressure (backlog), and counts as eligible only from now. Without a window nothing held it
 * back: eligible = ready.
 */
async function pushPending(stage: Stage, limit: number, window: Window | null, retries = false): Promise<number> {
  if (limit <= 0) return 0;
  const { rows } = await query<{ id: string; stage: string }>(
    `with pushed as (
       update tasks set queued = true, pushed_at = now(),
                        eligible_at = case when $4::timestamptz is null or greatest(pending_at, not_before) >= $4
                                           then greatest(pending_at, not_before) else $5 end
        where id in (select id from tasks
                      where state = 'PENDING' and stage = $1 and queued = false
                        and (started_at is not null) = $3
                        and (not_before is null or not_before <= now())
                      order by enqueued_at, id
                      limit $2
                      for update skip locked)
        returning id, stage, enqueued_at
     )
     select id, stage from pushed order by enqueued_at, id`,
    [stage, limit, retries, window?.previousAt ?? null, window?.at ?? null],
  );
  return pushRows(rows, retries ? "head" : "tail");
}

/** Guarded push of specific tasks: only those still PENDING, unqueued and past their backoff. */
async function pushIds(taskIds: string[], position: "head" | "tail"): Promise<number> {
  if (taskIds.length === 0) return 0;
  if (!hybrid()) {
    wake();
    return taskIds.length;
  }
  const { rows } = await query<{ id: string; stage: string }>(
    `update tasks set queued = true, pushed_at = now(), eligible_at = greatest(pending_at, not_before)
      where id = any($1::uuid[]) and state = 'PENDING' and queued = false
        and (not_before is null or not_before <= now())
      returning id, stage`,
    [taskIds],
  );
  return pushRows(rows, position);
}

/**
 * Recovered or returned work, pushed to the head of its queue right after the transaction that
 * requeued it committed (reaper, death watch, release, redrive, deregister). Retries bypass the
 * queue target and backpressure: that work was admitted before. Active in both dispatch modes.
 */
export function pushNow(taskIds: string[]): Promise<number> {
  return pushIds(taskIds, "head");
}

/** Newly created classify tasks, pushed to the tail after their commit (push mode). */
export async function pushNew(taskIds: string[]): Promise<number> {
  if (!pushMode() || eventPushesSuspended) return 0;
  return pushIds(taskIds, "tail");
}

const retryTimers = new Set<NodeJS.Timeout>();

/** A task failed and is backing off until `delayMs` from now: push it when the backoff ends. */
export function pushAt(taskId: string, delayMs: number) {
  if (!pushMode()) return;
  const timer = setTimeout(() => {
    retryTimers.delete(timer);
    pushNow([taskId]).catch((err) => console.error(`[dispatcher] retry push failed: ${err.message}`));
  }, Math.max(0, delayMs) + 5);
  timer.unref?.();
  retryTimers.add(timer);
}

/**
 * After a job's transaction committed: push its classify tasks (cache hits that only need stage 2)
 * and top up the detect queue. Bounded like the sweep, so a million-task job isn't pushed at once.
 */
export async function afterJobCreated(): Promise<void> {
  if (!pushMode() || eventPushesSuspended) return;
  if (!hybrid()) return wake();
  await pushPending("classify", 1000, null);
  await kickDetect();
}

// ---------------------------------------------------------------------------------------------
// Detect top-up: single-flight, coalescing
// ---------------------------------------------------------------------------------------------

let topUp: Promise<number> | null = null;
let topUpAgain = false;

async function topUpDetectOnce(): Promise<number> {
  if (throttled) return 0;
  const depth = await getRedis().llen(keys.queue("detect"));
  const at = new Date();
  const window: Window = { at, previousAt: lastTopUpAt ?? at };
  lastTopUpAt = at;
  return pushPending("detect", detectQueueTarget() - depth, window);
}

/**
 * Tops queue:detect up to detectQueueTarget(). At most one top-up runs at a time; kicks that
 * arrive meanwhile coalesce into one more pass, so a burst of claims costs a couple of
 * statements, not one each. Returns the number of IDs this run pushed.
 */
export function kickDetect(): Promise<number> {
  if (!hybrid()) {
    wake(["detect"]);
    return Promise.resolve(0);
  }
  if (topUp) {
    topUpAgain = true;
    return topUp;
  }
  topUp = (async () => {
    let pushed = 0;
    do {
      topUpAgain = false;
      pushed += await topUpDetectOnce();
    } while (topUpAgain);
    return pushed;
  })().finally(() => {
    topUp = null;
  });
  return topUp;
}

/** Fire-and-forget top-up after a detect claim (push mode). */
export function kickDetectInBackground() {
  if (!pushMode() || eventPushesSuspended) return;
  kickDetect().catch((err) => console.error(`[dispatcher] detect top-up failed: ${err.message}`));
}

/** Test hook: resolves once no background top-up is running. */
export async function dispatchIdle() {
  while (topUp) await topUp.catch(() => 0);
}

// ---------------------------------------------------------------------------------------------
// Waking long-polls (postgres mode)
// ---------------------------------------------------------------------------------------------

const waiters: Record<Stage, Set<() => void>> = { detect: new Set(), classify: new Set() };

/** Other replicas' long-polls are woken through the cluster bus (cluster.ts). */
let wakeRelay: ((stages: Stage[]) => void) | null = null;
export function setWakeRelay(fn: ((stages: Stage[]) => void) | null) {
  wakeRelay = fn;
}

/** Wakes every POST /tasks/claim long-poll waiting on these stages. */
export function wake(stages: Stage[] = STAGES, fromPeer = false) {
  if (!fromPeer) wakeRelay?.(stages);
  for (const s of stages) {
    const ws = [...waiters[s]];
    waiters[s].clear();
    for (const w of ws) w();
  }
}

/** Resolves on the next wake() for this stage, or after `ms`. */
export function waitForWork(stage: Stage, ms: number): Promise<void> {
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer);
      waiters[stage].delete(done);
      resolve();
    };
    const timer = setTimeout(done, Math.max(0, ms));
    waiters[stage].add(done);
  });
}

// ---------------------------------------------------------------------------------------------
// Queue depth, backpressure, rebuild
// ---------------------------------------------------------------------------------------------

const DEPTH_CAP = 100_000;

/**
 * Ready work per stage: the Redis list length (hybrid), or the PENDING rows waiting to be claimed
 * (postgres; counted up to DEPTH_CAP so a million-task backlog stays a cheap index scan).
 */
export async function queueDepths(): Promise<Record<Stage, number>> {
  if (hybrid()) {
    const [d, c] = await Promise.all([getRedis().llen(keys.queue("detect")), getRedis().llen(keys.queue("classify"))]);
    return { detect: d, classify: c };
  }
  const { rows } = await query<{ stage: Stage; n: number }>(
    `select s.stage, (select count(*)::int from (select 1 from tasks t
                                                  where t.state = 'PENDING' and t.stage = s.stage and t.queued = false
                                                  limit $1) x) as n
       from unnest(array['detect', 'classify']) as s(stage)`,
    [DEPTH_CAP],
  );
  return Object.fromEntries(rows.map((r) => [r.stage, r.n])) as Record<Stage, number>;
}

/**
 * Backpressure with hysteresis: engage above the high-water mark, release only below the
 * low-water mark, so the flag doesn't flap while the classify queue hovers near one threshold.
 */
async function updateThrottle(classifyQueue: number) {
  let changed: "throttled" | "unthrottled" | null = null;
  if (!throttled && classifyQueue > config.classifyQueueHighWater) changed = "throttled";
  else if (throttled && classifyQueue < config.classifyQueueLowWater) changed = "unthrottled";
  if (!changed) return;

  // Postgres holds the flag every replica reads (syncSharedState); on the leader this write is
  // fenced like any other leader-only statement.
  await query(`update coordinator_leader set throttled = $1 where id = 1`, [changed === "throttled"]);
  throttled = changed === "throttled";
  if (throttled) await getRedis().set(keys.throttled, "1");
  else await getRedis().del(keys.throttled);
  console.log(
    `[dispatcher] ${changed}: classify queue=${classifyQueue} (high=${config.classifyQueueHighWater}, low=${config.classifyQueueLowWater})`,
  );
  const events = await recordEvents(getPool(), [
    {
      type: changed,
      detail: { classifyQueue, highWater: config.classifyQueueHighWater, lowWater: config.classifyQueueLowWater },
    },
  ]);
  hub.publishEvents(events);
  hub.throttleChanged(throttled, classifyQueue);
}

/**
 * Rebuilds the ready queues from Postgres, the source of truth: clear them and mark every PENDING
 * task unqueued, so the next dispatch re-pushes exactly the tasks that still need to run. Runs at
 * election of a new leader, when a coordinator replica disappears (it may have popped IDs it never
 * leased), and whenever Redis turns out to have lost its data (restart without persistence,
 * FLUSHALL). IDs a worker already moved may end up queued twice; claim-confirm makes that harmless.
 *
 * The queues-built marker is deleted together with the queues and set again only after the
 * Postgres half committed. So if this run stops half way (a deposed leader whose UPDATE is fenced
 * off after its DEL went through, a crash), the marker is missing and the leader's next sweep
 * rebuilds again: a DEL can never strand IDs that Postgres still counts as queued.
 */
export async function rebuildQueues(reason: string): Promise<number> {
  const redis = getRedis();
  await redis.del(keys.queue("detect"), keys.queue("classify"), keys.throttled, keys.queuesBuilt);
  lastTopUpAt = null;
  const { rowCount } = await query(`update tasks set queued = false where state = 'PENDING' and queued`);
  await query(`update coordinator_leader set throttled = false where id = 1 and throttled`);
  throttled = false;
  await redis.set(keys.queuesBuilt, new Date().toISOString());
  if (rowCount) console.log(`[dispatcher] rebuilt queues (${reason}): ${rowCount} pending tasks will be re-dispatched`);
  return rowCount ?? 0;
}

/**
 * Leader, every few seconds (hybrid mode): rows Postgres counts as queued (PENDING, queued=true)
 * whose ID is in no Redis list any more are marked unqueued, so the repair sweep pushes them again.
 * That happens when an ID was popped (complete-and-claim-next's LPOP) and the lease statement, and
 * then the put-back, failed with their connection (a terminated backend) or died with their
 * replica. The rebuild after a lost replica covers the second case; this covers the first, which
 * otherwise waited for the next rebuild. Only rows pushed more than `minAgeMs` ago are considered,
 * and every list is read in one MULTI (an atomic snapshot); an ID that is between a pop and its
 * lease at that very moment is re-pushed as a duplicate, which claim-confirm makes harmless.
 */
export async function repairLostQueued(minAgeMs = 5000): Promise<number> {
  if (!hybrid()) return 0;
  const { rows } = await query<{ id: string }>(
    `select id from tasks where state = 'PENDING' and queued
        and pushed_at < now() - ($1::int * interval '1 millisecond')
      limit 10000`,
    [minAgeMs],
  );
  if (rows.length === 0) return 0;
  const { rows: workers } = await query<{ id: string }>(
    `select id from workers where status = 'ALIVE' or greatest(last_heartbeat_at, dead_at) > now() - interval '15 minutes'`,
  );
  const multi = getRedis().multi().lrange(keys.queue("detect"), 0, -1).lrange(keys.queue("classify"), 0, -1);
  for (const w of workers) multi.lrange(keys.processing(w.id), 0, -1).lrange(keys.spec(w.id), 0, -1);
  const present = new Set<string>();
  for (const [err, ids] of (await multi.exec()) ?? []) {
    if (err) throw err;
    for (const id of ids as string[]) present.add(id);
  }
  const lost = rows.map((r) => r.id).filter((id) => !present.has(id));
  if (lost.length === 0) return 0;
  const { rowCount } = await query(
    `update tasks set queued = false where id = any($1::uuid[]) and state = 'PENDING' and queued`,
    [lost],
  );
  if (rowCount) console.warn(`[dispatcher] ${rowCount} queued task(s) were in no Redis list; re-dispatching them`);
  return rowCount ?? 0;
}

/**
 * A replica that is not the leader keeps its copy of the cluster-wide dispatcher state fresh: the
 * throttle flag the leader decided (read by detect top-ups and Postgres-mode claims on every
 * replica) and the worker/lease counts that size the detect queue and the finish-lock threshold.
 */
export async function syncSharedState() {
  await refreshStats();
  await loadThrottle();
}

/** The throttle flag as the leader last decided it. */
export async function loadThrottle() {
  const { rows } = await query<{ throttled: boolean }>(`select throttled from coordinator_leader where id = 1`);
  throttled = rows[0]?.throttled ?? false;
}

/**
 * One tick. In push mode everything it pushes is a repair: work the post-commit path should have
 * pushed but didn't (a crash between commit and push, a Redis error), plus detect admission after
 * backpressure lifts. In tick mode it is the dispatcher.
 */
export async function dispatchOnce(): Promise<{ detect: number; classify: number; retried: number; throttled: boolean }> {
  await refreshStats();
  if (!hybrid()) {
    await updateThrottle((await queueDepths()).classify);
    telemetry.recordSweep(0);
    wake(); // long-polls re-check: retry backoffs may have ended
    return { detect: 0, classify: 0, retried: 0, throttled };
  }

  const redis = getRedis();
  const now = new Date();
  const sweep: Window = { at: now, previousAt: lastTopUpAt ?? now };
  if (!(await redis.exists(keys.queuesBuilt))) await rebuildQueues("redis data lost");
  await updateThrottle(await redis.llen(keys.queue("classify")));

  // Recovered work is never held back: it was already admitted before its worker died.
  const retried =
    (await pushPending("classify", 1000, sweep, true)) + (await pushPending("detect", 1000, sweep, true));
  // Stage 2 is never held back: draining it is what relieves the pressure.
  const classify = await pushPending("classify", 1000, sweep);
  const detect = await kickDetect();

  telemetry.recordSweep(retried + classify + detect);
  return { detect, classify, retried, throttled };
}
