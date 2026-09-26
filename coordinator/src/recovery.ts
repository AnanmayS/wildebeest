import { config } from "./config.js";
import { getPool, query } from "./db.js";
import { pushNow } from "./dispatcher.js";
import { hub, recordEvents } from "./events.js";
import { getRedis, keys } from "./redis.js";
import { requeueLostLeases } from "./tasks.js";
import { workersDied } from "./otel.js";
import { telemetry, type DeathVia } from "./telemetry.js";
import { drainProcessingList } from "./workers.js";

// One recovery path for every way we learn a worker is gone:
//
//   Docker `die`/`oom` event ──┐
//   docker ps reconciliation ──┼──> mark DEAD ─> worker_died event ─> requeue its leases
//   heartbeat timeout (reaper)─┘      (guarded      (detail.via,       + drain its processing list
//                                      on ALIVE)     detail.detectMs)  ─> LPUSH the IDs right away
//
// Marking is a guarded UPDATE (`status = 'ALIVE'`), so when the event and the reaper race, exactly
// one of them wins and the other is a no-op. A false positive is harmless: the worker's next
// heartbeat gets 410, anything it sends is fenced off by the bumped lease epoch, and it re-registers.

interface DeadRow {
  id: string;
  stage: string;
  registered_at: Date;
  last_heartbeat_at: Date;
  killed_at: Date | null;
  paused_at: Date | null;
  dead_at: Date;
}

/** What Docker told us about a death, when it told us anything. */
export interface DeathEvidence {
  /** When the container exited (the event's timestamp). */
  diedAt?: Date | null;
  exitCode?: number | null;
  action?: string;
  /** Found by reconciling against `docker ps` rather than from the event stream. */
  reconciled?: boolean;
}

export interface RecoveryOutcome {
  dead: string[];
  requeued: number;
  failed: number;
  drained: number;
  pushed: number;
}

const NONE: RecoveryOutcome = { dead: [], requeued: 0, failed: 0, drained: 0, pushed: 0 };

const RETURNING = `returning id, stage, registered_at, last_heartbeat_at, killed_at, paused_at, dead_at`;

/** Docker says these workers' containers died. */
export async function recoverWorkers(
  workerIds: string[],
  via: DeathVia,
  evidence: (workerId: string) => DeathEvidence = () => ({}),
): Promise<RecoveryOutcome> {
  if (workerIds.length === 0) return NONE;
  const { rows } = await query<DeadRow>(
    `update workers set status = 'DEAD', dead_at = now() where id = any($1::text[]) and status = 'ALIVE' ${RETURNING}`,
    [workerIds],
  );
  return handleDeaths(rows, via, evidence);
}

/**
 * The heartbeat backstop: ALIVE workers silent for longer than WORKER_TIMEOUT_MS (+ the reaper's
 * own recent stall, see StallMeter) are dead.
 */
export async function markSilentWorkersDead(graceMs = 0): Promise<RecoveryOutcome> {
  const { rows } = await query<DeadRow>(
    `update workers set status = 'DEAD', dead_at = now()
      where status = 'ALIVE' and last_heartbeat_at < now() - ($1::int * interval '1 millisecond') ${RETURNING}`,
    [config.workerTimeoutMs + Math.max(0, Math.round(graceMs))],
  );
  return handleDeaths(rows, "heartbeat", () => ({}));
}

/**
 * Best known moment of death. A kill or pause we sent to *this incarnation* beats the container's
 * exit time, which beats the last heartbeat (the latest moment it was certainly alive).
 */
function deathTime(w: DeadRow, ev: DeathEvidence): { at: Date; killedAt: Date | null } {
  const ours = (t: Date | null) => (t && t >= w.registered_at ? new Date(t) : null);
  const killedAt = ours(w.killed_at);
  const pausedAt = ours(w.paused_at);
  return { at: killedAt ?? pausedAt ?? ev.diedAt ?? new Date(w.last_heartbeat_at), killedAt };
}

async function handleDeaths(
  rows: DeadRow[],
  via: DeathVia,
  evidence: (workerId: string) => DeathEvidence,
): Promise<RecoveryOutcome> {
  if (rows.length === 0) return NONE;
  const ids = rows.map((w) => w.id);

  const facts = rows.map((w) => {
    const ev = evidence(w.id);
    const { at, killedAt } = deathTime(w, ev);
    const detectedAt = new Date(w.dead_at);
    return { w, ev, killedAt, startMs: at.getTime(), detectedAt, detectMs: Math.max(0, detectedAt.getTime() - at.getTime()) };
  });
  workersDied(facts.map((f) => ({ workerId: f.w.id, via, detectMs: f.detectMs }))); // for the requeue spans

  const died = await recordEvents(
    getPool(),
    facts.map((f) => ({
      type: "worker_died",
      workerId: f.w.id,
      detail: {
        stage: f.w.stage,
        via,
        detectMs: f.detectMs,
        silentMs: Math.max(0, f.detectedAt.getTime() - new Date(f.w.last_heartbeat_at).getTime()),
        // Split of detectMs for docker events: kill → container exit is Docker's share,
        // exit → DEAD mark (exitToDeadMs) is ours.
        ...(f.ev.diedAt ? { exitedAt: f.ev.diedAt.toISOString(), exitToDeadMs: f.detectedAt.getTime() - f.ev.diedAt.getTime() } : {}),
        ...(f.ev.exitCode != null ? { exitCode: f.ev.exitCode } : {}),
        ...(f.ev.action ? { dockerAction: f.ev.action } : {}),
        ...(f.ev.reconciled ? { reconciled: true } : {}),
      },
    })),
  );
  hub.publishEvents(died);
  const pipe = getRedis().pipeline();
  for (const id of ids) pipe.del(keys.alive(id));
  await pipe.exec();

  // Leases first (one statement for all of them), then IDs BLMOVEd but never claim-confirmed
  // (the drain puts those back on the queue itself).
  const leases = await requeueLostLeases({ workerIds: ids });
  const drainedByWorker = new Map<string, string[]>();
  for (const id of ids) drainedByWorker.set(id, await drainProcessingList(id));
  const requeuedAt = new Date();

  // The recovery records open before the requeued leases are pushed: a worker can claim a pushed
  // ID within a millisecond, and a claim nobody was waiting for would leave the record open
  // forever. (Drained IDs, pushed by the drain, and claims handled on another replica are matched
  // by the telemetry's recent-claims memory instead.)
  for (const f of facts) {
    const taskIds = [
      ...leases.tasks.filter((t) => t.oldWorker === f.w.id).map((t) => t.id),
      ...(drainedByWorker.get(f.w.id) ?? []),
    ];
    telemetry.recordRecovery({
      workerId: f.w.id,
      killedAt: f.killedAt,
      startMs: f.startMs,
      detectedAt: f.detectedAt,
      via,
      requeuedAt,
      taskIds,
    });
    console.log(
      `[recovery] ${f.w.id} DEAD via ${via}${f.ev.reconciled ? " (reconciled)" : ""} after ${f.detectMs} ms; ` +
        `${taskIds.length} task(s) requeued to the queue head`,
    );
  }
  const pushed = await pushNow(leases.tasks.map((t) => t.id));
  hub.workersChanged();
  const drained = [...drainedByWorker.values()].reduce((n, l) => n + l.length, 0);
  return { dead: ids, requeued: leases.requeued, failed: leases.failed, drained, pushed };
}
