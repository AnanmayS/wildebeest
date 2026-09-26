import { config } from "./config.js";
import { query } from "./db.js";
import { dispatchOnce, loadThrottle, rebuildQueues, repairLostQueued, resetOrphanSightings, syncSharedState } from "./dispatcher.js";
import { checkInvariants } from "./invariants.js";
import { reapOnce, reaperStalls } from "./reaper.js";
import { speculateOnce } from "./speculation.js";
import { flushCompleteTimings } from "./tasks.js";

/** Runs fn every intervalMs, never overlapping itself; returns a stop function. */
export function every(name: string, intervalMs: number, fn: () => Promise<unknown>) {
  let stopped = false;
  let timer: NodeJS.Timeout | null = null;
  const run = async () => {
    try {
      await fn();
    } catch (err) {
      console.error(`[${name}] ${(err as Error).message}`);
    }
    if (!stopped) timer = setTimeout(run, intervalMs);
  };
  timer = setTimeout(run, intervalMs);
  return () => {
    stopped = true;
    if (timer) clearTimeout(timer);
  };
}

// With coordinator HA (docs/decisions/h-ha.md) the loops split in two:
//
//   every replica   invariants (read-only), the complete_ms write-behind for completions this
//                   replica handled, and, while not leading, a refresh of the cluster-wide
//                   dispatcher state the leader decides (throttle flag, worker/lease counts).
//   leader only     the dispatch repair sweep with throttle hysteresis, the reaper (dead workers,
//                   lost leases, the job-finish sweep), straggler speculation, replica-loss detection. Started inside the
//                   leader's fence (ha.ts), so every statement they run is term-guarded.

export function startReplicaLoops(isLeader: () => boolean) {
  const stops = [
    every("invariants", config.invariantIntervalMs, checkInvariants),
    every("timings", 1000, flushCompleteTimings),
    every("sync", 500, async () => {
      if (!isLeader()) await syncSharedState();
    }),
  ];
  return () => stops.forEach((s) => s());
}

export function startLeaderLoops(opts: { self: string }) {
  reaperStalls.reset();
  resetOrphanSightings(); // sightings from an earlier term of this process may be stale
  const stops = [
    every("dispatcher", config.dispatchIntervalMs, dispatchOnce),
    every("reaper", config.reapIntervalMs, async () => {
      const stall = reaperStalls.tick();
      if (stall >= 1000) console.warn(`[reaper] ran ${stall} ms late; extending worker grace to ${reaperStalls.graceMs()} ms`);
      return reapOnce();
    }),
    // Per-worker service-time history lives in this process: a new leader starts with none.
    every("speculation", config.speculateIntervalMs, speculateOnce),
    every("queued-audit", config.queuedAuditMs, () => repairLostQueued()),
    every("replicas", config.leaderRenewMs, async () => {
      const lost = await forgetLostReplicas(opts.self);
      if (lost.length > 0) await rebuildQueues(`replica lost: ${lost.join(", ")}`);
    }),
  ];
  return () => stops.forEach((s) => s());
}

/**
 * Coordinator replicas that stopped heartbeating their row for longer than the lease TTL. The
 * caller rebuilds the ready queues afterwards: IDs a lost replica had LPOPed for
 * complete-and-claim-next but not leased yet are gone from Redis while Postgres still counts them
 * as queued. Forget first, rebuild second, so whatever a forgotten replica lost is covered.
 */
export async function forgetLostReplicas(selfInstance: string): Promise<string[]> {
  const { rows } = await query<{ id: string; instance: string }>(
    `delete from coordinator_nodes
      where instance <> $1 and last_seen_at < now() - ($2::int * interval '1 millisecond')
      returning id, instance`,
    [selfInstance, config.leaderTtlMs],
  );
  return rows.map((r) => `${r.id} (${r.instance.slice(0, 8)})`);
}

/**
 * What a newly elected leader does before its first sweep (it used to run at every coordinator
 * start; a follower's start must not touch shared state, so now only an election runs it):
 *  - rebuild the ready queues from Postgres (the previous leader, or a replica that died with it,
 *    may have popped IDs it never leased; see forgetLostReplicas);
 *  - adopt the throttle flag the previous leader left in Postgres;
 *  - cold start only: give workers and leases a grace period, so the coordinator's own downtime
 *    isn't mistaken for every worker dying at once. "Cold" = nobody has processed a heartbeat for
 *    two heartbeat intervals. After an ordinary failover the surviving replicas kept serving
 *    heartbeats and renewing leases, so no grace is due and dead workers are still caught on time.
 */
export async function reconcileAsLeader(selfInstance: string, term: number) {
  const forgotten = await forgetLostReplicas(selfInstance);
  const rebuilt = await rebuildQueues(`leader elected (term ${term})`);
  await loadThrottle();
  const { rows } = await query<{ cold: boolean }>(
    `select coalesce(max(last_heartbeat_at) < now() - ($1::int * interval '1 millisecond'), false) as cold
       from workers where status = 'ALIVE'`,
    [2 * config.heartbeatMs],
  );
  const coldStart = rows[0].cold;
  if (coldStart) {
    await query(`update workers set last_heartbeat_at = now() where status = 'ALIVE'`);
    await query(
      `update tasks set lease_expires_at = greatest(lease_expires_at, now() + ($1::int * interval '1 millisecond'))
        where state = 'LEASED'`,
      [config.leaseMs],
    );
  }
  return { forgotten, rebuilt, coldStart };
}
