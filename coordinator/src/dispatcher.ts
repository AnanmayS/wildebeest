import { config } from "./config.js";
import { getPool, query } from "./db.js";
import { hub, recordEvents } from "./events.js";
import { getRedis, keys } from "./redis.js";
import { telemetry } from "./telemetry.js";

// The dispatcher is the only code that pushes task IDs into the Redis ready queues. Tasks are
// created in Postgres as PENDING with queued=false; each tick moves some of them into Redis and
// flips queued=true. Holding stage 1 tasks back in Postgres is what makes backpressure possible.

let throttled = false;
/** Start of the previous sweep: a task ready before it but not pushed by it was being held back. */
let lastSweepAt: Date | null = null;

export const isThrottled = () => throttled;

/** Test hook: forget the in-memory throttle and sweep state. */
export function resetDispatcherState() {
  throttled = false;
  lastSweepAt = null;
}

/** The current sweep's window, for eligible_at (see pushPending). */
interface Sweep {
  at: Date;
  previousAt: Date;
}

/** Pushes already-claimed-for-dispatch rows to Redis; on failure un-marks them so a later sweep retries. */
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
 * pushes them. New tasks go to the tail (RPUSH). Retries — tasks a worker already started before
 * it died, lost its lease or failed — go to the head (LPUSH), so recovered work runs next instead
 * of waiting behind a full queue. A task in its retry backoff (not_before in the future) waits.
 *
 * eligible_at: a task that became ready after the previous sweep started could not have been
 * pushed earlier than now, so it was eligible from the moment it was ready (the wait is tick
 * delay: orchestration). A task that was already ready at the previous sweep and wasn't pushed
 * then was held back by the queue target or backpressure (backlog), so it counts as eligible
 * only from this sweep.
 */
async function pushPending(stage: "detect" | "classify", limit: number, sweep: Sweep, retries = false): Promise<number> {
  if (limit <= 0) return 0;
  const { rows } = await query<{ id: string; stage: string }>(
    `with pushed as (
       update tasks set queued = true, pushed_at = now(),
                        eligible_at = case when greatest(pending_at, not_before) >= $4
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
    [stage, limit, retries, sweep.previousAt, sweep.at],
  );
  return pushRows(rows, retries ? "head" : "tail");
}

/**
 * Pushes specific tasks right now, to the head of their queue, without waiting for the next
 * dispatcher tick. Used for recovered work straight after the transaction that requeued it
 * committed. Guarded like the sweep (PENDING, not yet queued, not in backoff), so a task the
 * sweep pushed first is not pushed twice.
 */
export async function pushNow(taskIds: string[]): Promise<number> {
  if (taskIds.length === 0) return 0;
  const { rows } = await query<{ id: string; stage: string }>(
    `update tasks set queued = true, pushed_at = now(), eligible_at = greatest(pending_at, not_before)
      where id = any($1::uuid[]) and state = 'PENDING' and queued = false
        and (not_before is null or not_before <= now())
      returning id, stage`,
    [taskIds],
  );
  return pushRows(rows, "head");
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

  throttled = changed === "throttled";
  if (throttled) await getRedis().set(keys.throttled, "1");
  else await getRedis().del(keys.throttled);
  console.log(
    `[dispatcher] ${changed}: queue:classify=${classifyQueue} (high=${config.classifyQueueHighWater}, low=${config.classifyQueueLowWater})`,
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
 * startup and whenever Redis turns out to have lost its data (restart without persistence,
 * FLUSHALL). IDs a worker already moved may end up queued twice; claim-confirm makes that harmless.
 */
export async function rebuildQueues(reason: string) {
  const redis = getRedis();
  await redis.del(keys.queue("detect"), keys.queue("classify"), keys.throttled);
  throttled = false;
  const { rowCount } = await query(`update tasks set queued = false where state = 'PENDING' and queued`);
  await redis.set(keys.queuesBuilt, new Date().toISOString());
  if (rowCount) console.log(`[dispatcher] rebuilt queues (${reason}): ${rowCount} pending tasks will be re-dispatched`);
}

export async function dispatchOnce(): Promise<{ detect: number; classify: number; retried: number; throttled: boolean }> {
  const redis = getRedis();
  const sweep: Sweep = { at: new Date(), previousAt: lastSweepAt ?? new Date() };
  lastSweepAt = sweep.at;
  if (!(await redis.exists(keys.queuesBuilt))) await rebuildQueues("redis data lost");
  await updateThrottle(await redis.llen(keys.queue("classify")));

  // Recovered work is never held back: it was already admitted before its worker died.
  const retried =
    (await pushPending("classify", 1000, sweep, true)) + (await pushPending("detect", 1000, sweep, true));

  // Stage 2 is never held back: draining it is what relieves the pressure.
  const classify = await pushPending("classify", 1000, sweep);

  let detect = 0;
  if (!throttled) {
    const depth = await redis.llen(keys.queue("detect"));
    detect = await pushPending("detect", config.detectQueueTarget - depth, sweep);
  }
  telemetry.recordSweep(retried + classify + detect);
  return { detect, classify, retried, throttled };
}
