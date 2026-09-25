import { config } from "./config.js";
import { query, tx } from "./db.js";
import { pushNow } from "./dispatcher.js";
import { hub, recordEvents } from "./events.js";
import { markSilentWorkersDead } from "./recovery.js";
import { getRedis, keys } from "./redis.js";
import { maybeFinishJob } from "./results.js";
import { requeueLostLeases } from "./tasks.js";
import { drainProcessingList } from "./workers.js";

// Failure detection backstop and lease hygiene, run every second (CONTRACTS.md "Reaper"):
//   1. ALIVE workers silent for WORKER_TIMEOUT_MS → DEAD, their work requeued (recovery.ts, the
//      same path the Docker event watcher uses, which usually gets there first).
//   2. LEASED tasks with an expired lease (or a non-ALIVE worker) → PENDING or FAILED, and pushed
//      to the head of their queue straight away.
//   3. Processing lists of non-ALIVE workers (IDs BLMOVEd but never claim-confirmed) → back to
//      their ready queue.
//   4. Safety net: any running job with no unfinished images is marked done. Finalisations only
//      lock the job row near the end of a job (wb_finish_job), so this sweep is what finishes a
//      job when two "last" images raced past the lock threshold (done ≤ 1 s late).
//
// Self-awareness (Lifeguard): if the reaper itself was late — an event-loop stall, a slow Postgres
// round trip — heartbeats that arrived during the stall may not have been processed either, so the
// reaper extends every worker's grace by its own recent stall instead of convicting them for it.

/** Measures how late a periodic loop runs, and turns recent lateness into grace. */
export class StallMeter {
  private last: number | null = null;
  private stalls: Array<{ at: number; ms: number }> = [];

  constructor(
    private readonly intervalMs: number,
    /** How long a stall keeps counting: long enough to cover a heartbeat timeout or a lease. */
    private readonly windowMs: number,
    /** Lateness below this is normal scheduling jitter plus the tick's own work. */
    private readonly slackMs = 250,
  ) {}

  /** Call at the start of every tick. Returns the stall observed since the previous tick. */
  tick(at = Date.now()): number {
    let stall = 0;
    if (this.last !== null) stall = Math.max(0, at - this.last - this.intervalMs - this.slackMs);
    this.last = at;
    if (stall > 0) this.stalls.push({ at, ms: stall });
    this.stalls = this.stalls.filter((s) => s.at >= at - this.windowMs);
    return stall;
  }

  /** Total stall within the window: added to the heartbeat timeout and to every lease. */
  graceMs(at = Date.now()): number {
    return this.stalls.filter((s) => s.at >= at - this.windowMs).reduce((n, s) => n + s.ms, 0);
  }

  /** Forget history, e.g. when this replica (re)gains the lead: the gap since it last reaped isn't a stall. */
  reset() {
    this.last = null;
    this.stalls = [];
  }

  stats(at = Date.now()) {
    const recent = this.stalls.filter((s) => s.at >= at - this.windowMs);
    return { graceMs: this.graceMs(at), stalls: recent.length, maxStallMs: Math.max(0, ...recent.map((s) => s.ms)) };
  }
}

export const reaperStalls = new StallMeter(
  config.reapIntervalMs,
  Math.max(config.workerTimeoutMs, config.leaseMs),
);

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
    if (Number(lens[i]?.[1] ?? 0) > 0) requeued += (await drainProcessingList(rows[i].id)).length;
  }
  return requeued;
}

export async function finishCompletedJobs(): Promise<number> {
  const { rows } = await query<{ id: string }>(
    `select j.id from jobs j
      where j.status = 'running'
        and not exists (select 1 from images i where i.job_id = j.id and i.final_category is null)`,
  );
  let finished = 0;
  for (const { id } of rows) {
    const events = await tx(async (c) => {
      const done = await maybeFinishJob(c, id, 0); // nothing left unfinalised: lock and finish
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

/**
 * One reaper pass. `graceMs` defaults to the reaper loop's own recent stall (0 when the loop isn't
 * running, e.g. in tests that call this directly).
 */
export async function reapOnce(graceMs = reaperStalls.graceMs()) {
  const deaths = await markSilentWorkersDead(graceMs);
  const leases = await requeueLostLeases({ graceMs });
  const pushed = await pushNow(leases.tasks.map((t) => t.id));
  const drained = await drainDeadProcessingLists();
  const finishedJobs = await finishCompletedJobs();
  return {
    dead: deaths.dead,
    requeued: deaths.requeued + leases.requeued,
    failed: deaths.failed + leases.failed,
    drained: deaths.drained + drained,
    pushed: deaths.pushed + pushed,
    finishedJobs,
    graceMs,
  };
}
