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
  /** P3: the dead worker's task had a speculative copy, which took over instead of a requeue. */
  promotedTo?: string;
  /** Docker events: container exit -> DEAD mark (the coordinator's share of detection). */
  exitToDeadMs?: number;
  /** Reclaim time read by the dashboard from system.leases, because the record never closed. */
  observed?: boolean;
}

const t = (iso: string | null | undefined) => (iso ? Date.parse(iso) : null);
/** Timestamps come from two coordinator replicas and Postgres: allow this much disagreement. */
const CLOCK_SLACK_MS = 250;

/** Every lease the dashboard has seen in system.leases: task id -> [{ worker, epoch, claim time }]. */
export type LeaseSightings = Map<string, { workerId: string; epoch: number; at: number }[]>;

/** Which failures are worth telling: by default, every one. */
export interface RecoveryFilter {
  /** Only failures detected at or after this time (ms). */
  since?: number;
  /** Only this worker's failures. */
  workerId?: string;
  /** Skip deaths nobody caused: a heartbeat timeout with no kill or pause behind it. */
  causedOnly?: boolean;
}

/** How long before a heartbeat-detected death a kill or pause still counts as its cause (a 20 s pause + 6 s timeout fits). */
const CAUSE_WINDOW_MS = 120_000;

/**
 * Was this death something we can tell a story about? A kill or pause (from the coordinator's record or its
 * events), or a container Docker saw exit. A worker that simply went quiet (a native process stopped during a
 * restart, a laptop asleep) is not: it stays in the event log only.
 */
function wasCaused(workerId: string, detectedAt: number, via: string | undefined, killedAt: string | null | undefined, events: TaskEvent[]): boolean {
  if (killedAt || via === 'docker_event') return true;
  return events.some((e) =>
    (e.type === 'worker_killed' || e.type === 'worker_paused') && e.workerId === workerId
    && Date.parse(e.at) <= detectedAt && detectedAt - Date.parse(e.at) < CAUSE_WINDOW_MS);
}

export function latestRecovery(system: SystemSnapshot | null, events: TaskEvent[], sightings?: LeaseSightings, filter: RecoveryFilter = {}): RecoveryView | null {
  const keep = (workerId: string, detectedAt: number, via: string | undefined, killedAt?: string | null) =>
    (filter.since == null || detectedAt >= filter.since)
    && (filter.workerId == null || workerId === filter.workerId)
    && (!filter.causedOnly || wasCaused(workerId, detectedAt, via, killedAt, events));
  const records = (system?.recovery ?? []).filter((r) => keep(r.workerId, Date.parse(r.detectedAt), r.via, r.killedAt));
  if (!records.length) return fromEvents(events, sightings, keep);
  const r = records.reduce((a, b) => (Date.parse(b.detectedAt) > Date.parse(a.detectedAt) ? b : a));
  const detectedAt = Date.parse(r.detectedAt);
  const died = events.find((e) => e.type === 'worker_died' && e.workerId === r.workerId && Math.abs(Date.parse(e.at) - detectedAt) < 2000);
  const detectMs = typeof died?.detail?.detectMs === 'number' ? died.detail.detectMs : null;
  // killedAt is null for a death we didn't cause (and some pauses): start the clock at the best known moment.
  const killedAt = t(r.killedAt) ?? (detectMs != null ? detectedAt - detectMs : detectedAt);
  const lost = events.filter((e) =>
    (e.type === 'reassigned' || e.type === 'lease_expired') && e.workerId === r.workerId && e.taskId
    && Date.parse(e.at) >= killedAt - 1000 && Date.parse(e.at) <= detectedAt + 30_000);
  const promoted = lost.find((e) => e.detail?.promoted);
  const requeuedEvents = lost.filter((e) => !e.detail?.promoted);
  // The requeue commit is when its `reassigned` events were written; the record's requeuedAt is taken
  // after the push, by which time a worker may already have claimed the task.
  const firstRequeue = requeuedEvents.length ? Math.min(...requeuedEvents.map((e) => Date.parse(e.at))) : null;
  const requeuedAt = firstRequeue != null && r.requeuedAt ? Math.min(firstRequeue, Date.parse(r.requeuedAt)) : t(r.requeuedAt) ?? firstRequeue;

  let reclaimedAt = t(r.reclaimedAt);
  let reclaimedBy = r.reclaimedBy;
  let totalMs = r.totalMs;
  let observed = false;
  // A reclaim can't precede the requeue. Seen live after a pause: reclaimedAt 25 s before detectedAt (and
  // before the pause itself), totalMs 0. Treat such a record as not closed and use the lease sightings.
  if (reclaimedAt != null && reclaimedAt < (requeuedAt ?? detectedAt) - CLOCK_SLACK_MS) {
    reclaimedAt = null;
    reclaimedBy = null;
    totalMs = null;
  }
  if (reclaimedAt == null && r.tasks > 0 && sightings && requeuedEvents.length) {
    // The coordinator didn't close the record (its claim can race the record being opened): use the
    // claim times the dashboard read off system.leases, if it saw every requeued task leased again.
    const seen = requeuedEvents.map((e) => sightings.get(e.taskId!)?.find((x) => x.workerId !== r.workerId && x.at >= killedAt));
    if (seen.every(Boolean)) {
      const last = seen.reduce((a, b) => (b!.at > a!.at ? b : a))!;
      reclaimedAt = Math.max(last.at, requeuedAt ?? last.at);
      reclaimedBy = last.workerId;
      totalMs = reclaimedAt - killedAt;
      observed = true;
    }
  }
  return {
    promotedTo: typeof promoted?.detail?.to === 'string' ? promoted.detail.to : undefined,
    workerId: r.workerId, killedAt, detectedAt, via: r.via,
    exitToDeadMs: typeof died?.detail?.exitToDeadMs === 'number' ? died.detail.exitToDeadMs : undefined,
    requeuedAt, reclaimedAt, reclaimedBy, tasks: r.tasks, totalMs, source: 'system', observed,
  };
}

/**
 * From the event log: a v1 coordinator, or a v2 replica restarted since the failure (its
 * `recovery` records are in memory). kill/pause -> died -> reassigned, reclaim from lease sightings.
 */
function fromEvents(
  events: TaskEvent[],
  sightings: LeaseSightings | undefined,
  keep: (workerId: string, detectedAt: number, via: string | undefined) => boolean,
): RecoveryView | null {
  const died = events.find((e) => e.type === 'worker_died' && e.workerId && keep(e.workerId, Date.parse(e.at), e.detail?.via as string | undefined));
  if (!died) return null;
  const diedAt = Date.parse(died.at);
  const cause = events.find((e) => (e.type === 'worker_killed' || e.type === 'worker_paused') && e.workerId === died.workerId && Date.parse(e.at) <= diedAt);
  const reassigned = events.filter((e) => (e.type === 'reassigned' || e.type === 'lease_expired') && e.workerId === died.workerId && Date.parse(e.at) >= diedAt - 1000);
  const detectMs = typeof died.detail?.detectMs === 'number' ? died.detail.detectMs : null;
  const killedAt = cause ? Date.parse(cause.at) : detectMs != null ? diedAt - detectMs : diedAt;
  const requeuedAt = reassigned.length ? Math.min(...reassigned.map((e) => Date.parse(e.at))) : null;
  const seen = sightings && reassigned.length
    ? reassigned.map((e) => sightings.get(e.taskId ?? '')?.find((x) => x.workerId !== died.workerId && x.at >= killedAt))
    : [];
  const all = seen.length > 0 && seen.every(Boolean);
  const last = all ? seen.reduce((a, b) => (b!.at > a!.at ? b : a))! : null;
  const reclaimedAt = last ? Math.max(last.at, requeuedAt ?? last.at) : null;
  return {
    workerId: died.workerId!, killedAt, detectedAt: diedAt, via: (died.detail?.via as string) ?? 'heartbeat',
    exitToDeadMs: typeof died.detail?.exitToDeadMs === 'number' ? died.detail.exitToDeadMs : undefined,
    requeuedAt, reclaimedAt, reclaimedBy: last?.workerId ?? null, tasks: reassigned.length,
    totalMs: reclaimedAt != null ? reclaimedAt - killedAt : null, source: 'events', observed: !!last,
  };
}
