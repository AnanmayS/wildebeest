import { config } from "./config.js";
import { getPool, query } from "./db.js";
import { hub, recordEvents } from "./events.js";
import { getRedis, keys } from "./redis.js";

// The dispatcher is the only code that pushes task IDs into the Redis ready queues. Tasks are
// created in Postgres as PENDING with queued=false; each tick moves some of them into Redis and
// flips queued=true. Holding stage 1 tasks back in Postgres is what makes backpressure possible.

let throttled = false;

export const isThrottled = () => throttled;

/** Test hook: forget the in-memory throttle state. */
export function resetDispatcherState() {
  throttled = false;
}

/**
 * Marks up to `limit` oldest PENDING, not-yet-queued tasks of a stage as queued and pushes them.
 * New tasks go to the tail (RPUSH). Retries — tasks a worker already started before it died or
 * lost its lease — go to the head (LPUSH), so recovered work runs next instead of waiting behind
 * a full queue. If the push fails, the flag is reverted so the next tick retries.
 */
async function pushPending(stage: "detect" | "classify", limit: number, retries = false): Promise<number> {
  if (limit <= 0) return 0;
  const { rows } = await query<{ id: string }>(
    `with pushed as (
       update tasks set queued = true
        where id in (select id from tasks
                      where state = 'PENDING' and stage = $1 and queued = false
                        and (started_at is not null) = $3
                      order by enqueued_at, id
                      limit $2
                      for update skip locked)
        returning id, enqueued_at
     )
     select id from pushed order by enqueued_at, id`,
    [stage, limit, retries],
  );
  if (rows.length === 0) return 0;
  const ids = rows.map((r) => r.id);
  try {
    if (retries) await getRedis().lpush(keys.queue(stage), ...ids.reverse());
    else await getRedis().rpush(keys.queue(stage), ...ids);
  } catch (err) {
    await query(`update tasks set queued = false where id = any($1::uuid[]) and state = 'PENDING'`, [ids]);
    throw err;
  }
  return ids.length;
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
  if (!(await redis.exists(keys.queuesBuilt))) await rebuildQueues("redis data lost");
  await updateThrottle(await redis.llen(keys.queue("classify")));

  // Recovered work is never held back: it was already admitted before its worker died.
  const retried = (await pushPending("classify", 1000, true)) + (await pushPending("detect", 1000, true));

  // Stage 2 is never held back: draining it is what relieves the pressure.
  const classify = await pushPending("classify", 1000);

  let detect = 0;
  if (!throttled) {
    const depth = await redis.llen(keys.queue("detect"));
    detect = await pushPending("detect", config.detectQueueTarget - depth);
  }
  return { detect, classify, retried, throttled };
}
