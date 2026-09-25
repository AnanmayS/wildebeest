// Coordinator HA as the page shows it: which replica leads (and in which term), which one stands
// by, which one is gone, and the story of the latest failover. GET /cluster is the live source;
// `system.leader` and the leader_* events fill in when it is missing.

import { useRef } from 'react';
import type { Cluster, SystemSnapshot, TaskEvent } from './types';

export type CoordRole = 'leader' | 'standby' | 'down';

export interface CoordNode { id: string; role: CoordRole; term: number | null }

export interface HaView {
  leader: { id: string; term: number; since: string } | null;
  nodes: CoordNode[];
  /** The leader stopped renewing its lease: a failover is under way. */
  leaderSilent: { id: string; term: number; forMs: number; leaseMs: number | null } | null;
}

/** A replica whose row is this much older than the freshest row has stopped heartbeating. */
const NODE_STALE_MS = 3000;
/** Keep showing a replica that vanished (the leader forgets rows silent for the lease TTL). */
const REMEMBER_MS = 10 * 60_000;
/** Renewals happen every second and /cluster is polled every 2 s: no change for this long = silent. */
const SILENT_AFTER_MS = 2600;

export function useHaView(cluster: Cluster | null, system: SystemSnapshot | null, now: number): HaView | null {
  const seen = useRef(new Map<string, number>()); // replica id -> last time the browser saw it alive
  const renewal = useRef<{ key: string; changedAt: number } | null>(null);

  const leader = cluster?.leader ?? (system?.leader ? { ...system.leader, renewedAt: null, expiresAt: null, valid: true } : null);
  if (!leader && !cluster) return null;

  const nodes: CoordNode[] = [];
  if (cluster) {
    const freshest = Math.max(0, ...cluster.nodes.map((n) => Date.parse(n.lastSeenAt)));
    for (const n of cluster.nodes) {
      const alive = freshest - Date.parse(n.lastSeenAt) < NODE_STALE_MS;
      if (alive) seen.current.set(n.id, now);
      const isLeader = leader?.id === n.id && n.role === 'leader';
      nodes.push({ id: n.id, role: !alive ? 'down' : isLeader ? 'leader' : 'standby', term: n.term });
    }
    for (const [id, at] of seen.current) {
      if (nodes.some((n) => n.id === id)) continue;
      if (now - at > REMEMBER_MS) seen.current.delete(id);
      else nodes.push({ id, role: 'down', term: null });
    }
    nodes.sort((a, b) => a.id.localeCompare(b.id));
  } else if (leader) {
    nodes.push({ id: leader.id, role: 'leader', term: leader.term });
  }

  // A lease that stopped being renewed, seen from the browser: renewedAt frozen, or already invalid.
  let leaderSilent: HaView['leaderSilent'] = null;
  if (cluster?.leader) {
    const key = `${cluster.leader.id}/${cluster.leader.term}/${cluster.leader.renewedAt}`;
    if (renewal.current?.key !== key) renewal.current = { key, changedAt: now };
    const frozenFor = now - renewal.current.changedAt;
    if (!cluster.leader.valid || frozenFor > SILENT_AFTER_MS) {
      const leaseMs = Date.parse(cluster.leader.expiresAt) - Date.parse(cluster.leader.renewedAt);
      leaderSilent = {
        id: cluster.leader.id, term: cluster.leader.term,
        // Since the last renewal by the browser's clock (same host or NTP), at least as long as we watched it stall.
        forMs: Math.max(frozenFor, now - Date.parse(cluster.leader.renewedAt), 1000), leaseMs: Number.isFinite(leaseMs) ? leaseMs : null,
      };
      const n = nodes.find((x) => x.id === cluster.leader!.id);
      if (n) n.role = 'down';
    }
  }

  return {
    leader: leader ? { id: leader.id, term: leader.term, since: leader.since } : null,
    nodes,
    leaderSilent,
  };
}

export interface Failover {
  at: number;
  from: string;
  fromTerm: number | null;
  to: string;
  term: number;
  reason: string | null;
  /** Previous leader's last renewal → new leader done with its first sweep. */
  totalMs: number | null;
  leaderlessMs: number | null;
  reconcileMs: number | null;
  /** Statements a deposed leader tried after it lost its term, refused by the term guard. */
  fencedWrites: number;
  firstSweep: { pushed?: number; dead?: number; requeued?: number } | null;
}

const num = (v: unknown) => (typeof v === 'number' && Number.isFinite(v) ? v : null);

/** The newest takeover in the event log (events are newest first). A cold start is not a failover. */
export function latestFailover(events: TaskEvent[]): Failover | null {
  const elected = events.find((e) => e.type === 'leader_elected' && e.detail?.previousHolder);
  if (!elected) return null;
  const d = elected.detail!;
  const at = Date.parse(elected.at);
  const prevRenewed = typeof d.previousRenewedAt === 'string' ? Date.parse(d.previousRenewedAt) : NaN;
  const fromTerm = num(d.previousTerm);
  const lost = events.find((e) => e.type === 'leader_lost' && num(e.detail?.term) === fromTerm);
  const fencedWrites = events.filter((e) => e.type === 'leader_fenced' && (fromTerm == null || num(e.detail?.term) === fromTerm)).length;
  return {
    at,
    from: String(d.previousHolder),
    fromTerm,
    to: String(d.holder ?? elected.workerId ?? '?'),
    term: num(d.term) ?? 0,
    reason: typeof lost?.detail?.reason === 'string' ? lost.detail.reason : null,
    totalMs: Number.isFinite(prevRenewed) ? Math.max(0, at - prevRenewed) : num(d.leaderlessMs),
    leaderlessMs: num(d.leaderlessMs),
    reconcileMs: num(d.reconcileMs),
    fencedWrites,
    firstSweep: (d.firstSweep as Failover['firstSweep']) ?? null,
  };
}
