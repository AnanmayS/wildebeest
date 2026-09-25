// The latest failure and how the system recovered from it: from the v2 `system.recovery`
// records when the coordinator has them, otherwise pieced together from the event log.

import type { SystemSnapshot, TaskEvent } from './types';

export interface RecoveryView {
  workerId: string;
  killedAt: number;
  detectedAt: number | null;
  via?: string;
  requeuedAt: number | null;
  reclaimedAt: number | null;
  reclaimedBy?: string | null;
  tasks: number;
  totalMs: number | null;
  source: 'system' | 'events';
}

const t = (iso: string | null | undefined) => (iso ? Date.parse(iso) : null);

export function latestRecovery(system: SystemSnapshot | null, events: TaskEvent[]): RecoveryView | null {
  const records = system?.recovery ?? [];
  if (records.length) {
    const r = records.reduce((a, b) => (Date.parse(b.detectedAt) > Date.parse(a.detectedAt) ? b : a));
    return {
      workerId: r.workerId, killedAt: Date.parse(r.killedAt), detectedAt: t(r.detectedAt), via: r.via,
      requeuedAt: t(r.requeuedAt), reclaimedAt: t(r.reclaimedAt), reclaimedBy: r.reclaimedBy, tasks: r.tasks,
      totalMs: r.totalMs, source: 'system',
    };
  }
  return fromEvents(events);
}

/** v1 fallback: kill -> died -> reassigned from the event log (no reclaim time available). */
function fromEvents(events: TaskEvent[]): RecoveryView | null {
  const died = events.find((e) => e.type === 'worker_died' && e.workerId);
  if (!died) return null;
  const diedAt = Date.parse(died.at);
  const killed = events.find((e) => e.type === 'worker_killed' && e.workerId === died.workerId && Date.parse(e.at) <= diedAt);
  const reassigned = events.filter((e) => e.type === 'reassigned' && e.workerId === died.workerId && Date.parse(e.at) >= diedAt);
  const killedAt = killed ? Date.parse(killed.at) : diedAt;
  const requeuedAt = reassigned.length ? Math.min(...reassigned.map((e) => Date.parse(e.at))) : null;
  return {
    workerId: died.workerId!, killedAt, detectedAt: diedAt, via: (died.detail?.via as string) ?? 'heartbeat',
    requeuedAt, reclaimedAt: null, tasks: reassigned.length, totalMs: null, source: 'events',
  };
}
