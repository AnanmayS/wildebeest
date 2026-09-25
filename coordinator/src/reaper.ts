import { config } from "./config.js";
import { getPool, query, tx } from "./db.js";
import { hub, recordEvents } from "./events.js";
import { getRedis, keys } from "./redis.js";
import { maybeFinishJob } from "./results.js";
import { requeueLostLeases } from "./tasks.js";
import { drainProcessingList } from "./workers.js";

// Failure detection and recovery, run every second (CONTRACTS.md "Reaper"):
//   1. ALIVE workers silent for WORKER_TIMEOUT_MS → DEAD.
//   2. LEASED tasks with an expired lease or a non-ALIVE worker → PENDING (attempts+1) or FAILED.
//   3. Processing lists of non-ALIVE workers (IDs BLMOVEd but never claim-confirmed) → back to
//      their ready queue.
//   4. Safety net: any running job with no unfinished images is marked done.
// Recovery time (DEAD mark / kill → every reassigned task re-claimed) is derived from the
// task_events these steps write; see metrics.ts.

export async function markDeadWorkers(): Promise<string[]> {
  const { rows } = await query(
    `update workers set status = 'DEAD', dead_at = now()
      where status = 'ALIVE' and last_heartbeat_at < now() - ($1::int * interval '1 millisecond')
      returning id, stage, extract(epoch from (now() - last_heartbeat_at)) * 1000 as silent_ms`,
    [config.workerTimeoutMs],
  );
  if (rows.length === 0) return [];
  const events = await recordEvents(
    getPool(),
    rows.map((w) => ({ type: "worker_died", workerId: w.id, detail: { stage: w.stage, silentMs: Math.round(w.silent_ms) } })),
  );
  const pipe = getRedis().pipeline();
  for (const w of rows) pipe.del(keys.alive(w.id));
  await pipe.exec();
  for (const w of rows) console.log(`[reaper] worker ${w.id} marked DEAD (silent ${Math.round(w.silent_ms)} ms)`);
  hub.publishEvents(events);
  hub.workersChanged();
  return rows.map((w) => w.id);
}

/** Drains processing lists of recently seen non-ALIVE workers. */
export async function drainDeadProcessingLists(): Promise<number> {
  const { rows } = await query<{ id: string }>(
    `select id from workers
      where status <> 'ALIVE' and greatest(last_heartbeat_at, dead_at) > now() - interval '15 minutes'`,
  );
  if (rows.length === 0) return 0;
  const pipe = getRedis().pipeline();
  for (const w of rows) pipe.llen(keys.processing(w.id));
  const lens = (await pipe.exec()) ?? [];
  let requeued = 0;
  for (let i = 0; i < rows.length; i++) {
    if (Number(lens[i]?.[1] ?? 0) > 0) requeued += await drainProcessingList(rows[i].id);
  }
  return requeued;
}

export async function finishCompletedJobs(): Promise<number> {
  const { rows } = await query<{ id: string }>(
    `select j.id from jobs j
      where j.status <> 'done'
        and not exists (select 1 from images i where i.job_id = j.id and i.final_category is null)`,
  );
  let finished = 0;
  for (const { id } of rows) {
    const events = await tx(async (c) => {
      const done = await maybeFinishJob(c, id);
      return done ? recordEvents(c, [done]) : [];
    });
    if (events.length > 0) {
      finished++;
      hub.publishEvents(events);
      hub.jobChanged(id);
    }
  }
  return finished;
}

export async function reapOnce() {
  const dead = await markDeadWorkers();
  const { requeued, failed } = await requeueLostLeases();
  const drained = await drainDeadProcessingLists();
  const finishedJobs = await finishCompletedJobs();
  return { dead, requeued, failed, drained, finishedJobs };
}
