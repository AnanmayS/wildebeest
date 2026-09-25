import { startChaos } from "./chaos.js";
import { ClusterBus } from "./cluster.js";
import { config } from "./config.js";
import { FencedError, getPool, nodeId, withFence } from "./db.js";
import { startDeathWatch, stopDeathWatch, unpauseLeftovers } from "./deathwatch.js";
import { dispatchOnce } from "./dispatcher.js";
import { hub, recordEvents, type EventInput } from "./events.js";
import { Elector, setElector, type Election } from "./leader.js";
import { every, reconcileAsLeader, startLeaderLoops, startReplicaLoops } from "./loops.js";
import { reapOnce } from "./reaper.js";

// Wires one coordinator replica into the cluster (docs/decisions/h-ha.md):
//
//   every replica  serves the whole HTTP API and its WebSocket clients, heartbeats its row in
//                  coordinator_nodes, joins the cluster bus, runs the replica loops.
//   the leader     additionally runs, inside its fence: the election reconciliation (queue rebuild,
//                  cold-start grace), the dispatcher repair sweep + throttle, the reaper, the Docker
//                  death watch, chaos, replica-loss detection, leftover-pause cleanup.
//
// Being deposed stops the leader-only work at once; statements already in flight fail the guard.

export interface LeaderWork {
  /**
   * Starts the leader-only work for `e` (already running inside its fence). Everything it starts
   * is handed to `onStop`, which runs it when the term ends (at once if it already has).
   */
  lead(e: Election, elector: Elector, onStop: (stop: () => void) => void): Promise<void>;
}

/** The real leader-only work. */
export const coordinatorLeaderWork: LeaderWork = {
  async lead(e, elector, onStop) {
    const { fence } = e;
    const acquiredAt = Date.now();
    const reconciled = await reconcileAsLeader(fence.instance, fence.term);
    if (!elector.holds(fence)) return;

    onStop(startLeaderLoops({ self: fence.instance }));
    onStop(every("leftover-pauses", config.leaderTtlMs, () => unpauseLeftovers()));
    onStop(startChaos());
    const watch = await startDeathWatch().catch((err) => {
      console.error(`[deathwatch] failed to start: ${err.message}`);
      return null;
    });
    onStop(() => stopDeathWatch(watch));
    if (!elector.holds(fence)) return;

    // The first sweep, then the record of the takeover: `leader_elected.at` is when the new leader
    // had done its first full sweep, which is where the failover test stops the clock.
    const first = { dispatch: await dispatchOnce(), reap: await reapOnce() };
    const p = e.previous;
    const events: EventInput[] = [];
    if (p && p.holder) {
      events.push({
        type: "leader_lost",
        detail: {
          holder: p.holder,
          term: p.term,
          reason: p.resigned ? "resigned" : p.holder === fence.holder ? "lease lapsed (same replica)" : "lease expired",
          renewedAt: p.renewedAt,
          expiresAt: p.expiresAt,
        },
      });
    }
    events.push({
      type: "leader_elected",
      detail: {
        holder: fence.holder,
        instance: fence.instance,
        term: fence.term,
        since: e.since.toISOString(),
        previousHolder: p?.holder ?? null,
        previousTerm: p?.term ?? null,
        previousRenewedAt: p?.renewedAt ?? null,
        // From the previous leader's last renewal to our acquisition: how long nobody led.
        leaderlessMs: p?.renewedAt ? Math.max(0, e.since.getTime() - new Date(p.renewedAt).getTime()) : null,
        reconcileMs: Date.now() - acquiredAt,
        rebuilt: reconciled.rebuilt,
        forgottenReplicas: reconciled.forgotten,
        coldStart: reconciled.coldStart,
        firstSweep: { pushed: first.dispatch.detect + first.dispatch.classify + first.dispatch.retried, dead: first.reap.dead.length, requeued: first.reap.requeued },
      },
    });
    hub.publishEvents(await recordEvents(getPool(), events));
  },
};

export interface HaNode {
  elector: Elector;
  bus: ClusterBus | null;
  stop(): Promise<void>;
}

/**
 * Starts this replica's part in the cluster. The first election round has run when this resolves
 * (so the replica's row exists before it serves requests), but a won election's reconciliation
 * continues in the background.
 */
export async function startHa(
  opts: { id?: string; work?: LeaderWork; bus?: boolean; replicaLoops?: boolean; databaseUrl?: string; redisUrl?: string } = {},
): Promise<HaNode> {
  const work = opts.work ?? coordinatorLeaderWork;
  let stops: Array<() => void> = [];
  const stopAll = () => {
    const s = stops;
    stops = [];
    for (const fn of s) fn();
  };

  const elector: Elector = new Elector(opts.id ?? nodeId(), {
    ttlMs: config.leaderTtlMs,
    renewMs: config.leaderRenewMs,
    databaseUrl: opts.databaseUrl,
    onElected: (e) =>
      withFence(e.fence, async () => {
        const onStop = (fn: () => void) => (elector.holds(e.fence) ? stops.push(fn) : fn());
        try {
          await work.lead(e, elector, onStop);
        } catch (err) {
          if (err instanceof FencedError || !elector.holds(e.fence)) return; // deposed meanwhile
          // Any doubt about the leader's start-up: give the term up; the next round elects again.
          console.error(`[ha] leader start-up failed: ${(err as Error).message}`);
          elector.stepDown(`leader start-up failed: ${(err as Error).message}`, { resign: true });
        }
      }),
    onDeposed: () => stopAll(),
  });
  setElector(elector);

  const bus = opts.bus === false ? null : new ClusterBus(elector.instance, { onResigned: () => elector.kick(), redisUrl: opts.redisUrl });
  await bus?.start();
  const stopReplica = opts.replicaLoops === false ? () => {} : startReplicaLoops(() => elector.isLeader());
  await elector.start();

  return {
    elector,
    bus,
    async stop() {
      stopReplica();
      const wasLeader = elector.isLeader();
      await elector.stop(); // resigns if leading (onDeposed stops the leader work)
      if (wasLeader) await bus?.announceResignation().catch(() => {});
      await bus?.stop();
      setElector(null);
    },
  };
}
