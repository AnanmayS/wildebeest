// In-memory telemetry behind GET /system and the websocket `system` message.
//
// Everything here is cheap to record on the hot path (an array push or a counter bump) and is
// summarised at most ~2×/s when a snapshot is built. It is process-local and resets when the
// coordinator restarts; Postgres keeps the durable per-task record (tasks.pushed_at, timings,
// complete_ms, task_events).

export type Stage = "detect" | "classify";

const now = () => Date.now();

// ---------------------------------------------------------------------------------------------
// Per-second counters
// ---------------------------------------------------------------------------------------------

/** Named counters in 1 s buckets, keeping the last `seconds` seconds. */
export class SecondBuckets<K extends string> {
  private buckets = new Map<number, Record<K, number>>();

  constructor(
    private readonly names: readonly K[],
    private readonly seconds: number,
  ) {}

  add(name: K, n = 1, at = now()) {
    if (n === 0) return;
    const sec = Math.floor(at / 1000);
    let b = this.buckets.get(sec);
    if (!b) {
      b = Object.fromEntries(this.names.map((k) => [k, 0])) as Record<K, number>;
      this.buckets.set(sec, b);
      this.prune(sec);
    }
    b[name] += n;
  }

  /** Sum of one counter over the last `seconds` whole seconds (including the current one). */
  sum(name: K, seconds: number, at = now()): number {
    const end = Math.floor(at / 1000);
    let total = 0;
    for (let s = end - seconds + 1; s <= end; s++) total += this.buckets.get(s)?.[name] ?? 0;
    return total;
  }

  /** One row per second, oldest first, for the last `seconds` completed seconds. */
  series(seconds = this.seconds, at = now()): Array<{ t: number } & Record<K, number>> {
    const end = Math.floor(at / 1000) - 1; // the current second is still filling up
    const out: Array<{ t: number } & Record<K, number>> = [];
    for (let s = end - seconds + 1; s <= end; s++) {
      const b = this.buckets.get(s);
      out.push({ t: s * 1000, ...(Object.fromEntries(this.names.map((k) => [k, b?.[k] ?? 0])) as Record<K, number>) });
    }
    return out;
  }

  private prune(currentSec: number) {
    for (const s of this.buckets.keys()) if (s <= currentSec - this.seconds - 1) this.buckets.delete(s);
  }

  clear() {
    this.buckets.clear();
  }
}

// ---------------------------------------------------------------------------------------------
// Per-task timing samples (the overhead waterfall)
// ---------------------------------------------------------------------------------------------

export const TIMING_KEYS = [
  "dispatchWaitMs",
  "queueWaitMs",
  "claimMs",
  "fetchMs",
  "inferMs",
  "uploadMs",
  "completeMs",
  "totalMs",
] as const;
export type TimingKey = (typeof TIMING_KEYS)[number];

export interface TimingSample extends Record<TimingKey, number> {
  at: number;
  stage: Stage;
  /** Coordinator-observed service time: claimed → complete handled. */
  serviceMs: number;
}

export function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const i = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[i];
}

const round1 = (n: number) => Math.round(n * 10) / 10;

// ---------------------------------------------------------------------------------------------
// Recovery records
// ---------------------------------------------------------------------------------------------

export type DeathVia = "docker_event" | "heartbeat";

export interface RecoveryRecord {
  workerId: string;
  killedAt: string | null;
  detectedAt: string;
  via: DeathVia;
  requeuedAt: string | null;
  reclaimedAt: string | null;
  /** The worker whose claim closed the recovery (re-claimed the last outstanding task). */
  reclaimedBy: string | null;
  tasks: number;
  totalMs: number | null;
}

interface OpenRecovery {
  record: RecoveryRecord;
  startMs: number;
  outstanding: Set<string>;
}

const MAX_RECOVERY_RECORDS = 20;

// ---------------------------------------------------------------------------------------------
// The singleton
// ---------------------------------------------------------------------------------------------

class Telemetry {
  readonly timingWindowMs = 60_000;
  private readonly maxSamples = 50_000;

  /** Completions per stage and images finalised, 1 s resolution, 120 s. */
  readonly throughput = new SecondBuckets(["detect", "classify", "images"] as const, 120);
  /** Dispatcher activity. */
  readonly dispatch = new SecondBuckets(["pushed", "sweeps"] as const, 15);
  lastSweepRepaired = 0;

  private samples: TimingSample[] = [];
  /** Coordinator handling time per accepted complete, waiting to be written to tasks.complete_ms. */
  private completeMsBacklog: Array<[string, number]> = [];

  fencing = {
    staleRejected: 0,
    last: null as null | { taskId: string; workerId: string; epoch: number; currentEpoch: number; at: string },
  };

  private cacheWindow: Array<{ at: number; hits: number; total: number }> = [];

  private recoveries: OpenRecovery[] = [];
  private recoveryByTask = new Map<string, OpenRecovery>();

  // ---- recording ---------------------------------------------------------------------------

  recordCompletion(sample: TimingSample, taskId: string) {
    this.throughput.add(sample.stage, 1, sample.at);
    this.samples.push(sample);
    if (this.samples.length > this.maxSamples) this.samples.splice(0, this.samples.length - this.maxSamples);
    this.completeMsBacklog.push([taskId, sample.completeMs]);
    if (this.completeMsBacklog.length > 100_000) this.completeMsBacklog.splice(0, 50_000);
  }

  recordFinalized(n: number) {
    this.throughput.add("images", n);
  }

  recordPushed(n: number) {
    this.dispatch.add("pushed", n);
  }

  recordSweep(repaired: number) {
    this.dispatch.add("sweeps", 1);
    this.lastSweepRepaired = repaired;
  }

  recordStale(taskId: string, workerId: string, epoch: number, currentEpoch: number) {
    this.fencing.staleRejected++;
    this.fencing.last = { taskId, workerId, epoch, currentEpoch, at: new Date().toISOString() };
  }

  recordJobCreated(hits: number, total: number) {
    this.cacheWindow.push({ at: now(), hits, total });
  }

  /** Takes the backlog of (taskId, completeMs) pairs for the write-behind flush. */
  drainCompleteMs(): Array<[string, number]> {
    const out = this.completeMsBacklog;
    this.completeMsBacklog = [];
    return out;
  }

  // ---- recovery ----------------------------------------------------------------------------

  /**
   * Opens a recovery record for a dead worker. `startMs` is the best known moment of death (kill
   * request, container exit, or detection when nothing better is known); totalMs runs from there
   * until every recovered task has been claimed again.
   */
  recordRecovery(r: {
    workerId: string;
    killedAt: Date | null;
    startMs: number;
    detectedAt: Date;
    via: DeathVia;
    requeuedAt: Date;
    taskIds: string[];
  }) {
    const open: OpenRecovery = {
      startMs: r.startMs,
      outstanding: new Set(r.taskIds),
      record: {
        workerId: r.workerId,
        killedAt: r.killedAt?.toISOString() ?? null,
        detectedAt: r.detectedAt.toISOString(),
        via: r.via,
        requeuedAt: r.requeuedAt.toISOString(),
        reclaimedAt: null,
        reclaimedBy: null,
        tasks: r.taskIds.length,
        totalMs: null,
      },
    };
    this.recoveries.push(open);
    for (const id of r.taskIds) this.recoveryByTask.set(id, open);
    if (open.outstanding.size === 0) this.close(open, r.requeuedAt.getTime());
    while (this.recoveries.length > MAX_RECOVERY_RECORDS) {
      const dropped = this.recoveries.shift()!;
      for (const id of dropped.outstanding) this.recoveryByTask.delete(id);
    }
  }

  /** Called with every successful claim: closes recoveries whose last task was re-claimed. */
  recordClaimed(taskIds: string[], workerId: string, at = now()) {
    if (this.recoveryByTask.size === 0) return;
    for (const id of taskIds) {
      const open = this.recoveryByTask.get(id);
      if (!open) continue;
      this.recoveryByTask.delete(id);
      open.outstanding.delete(id);
      if (open.outstanding.size === 0) this.close(open, at, workerId);
    }
  }

  private close(open: OpenRecovery, at: number, reclaimedBy: string | null = null) {
    open.record.reclaimedBy = reclaimedBy;
    open.record.reclaimedAt = new Date(at).toISOString();
    open.record.totalMs = Math.max(0, Math.round(at - open.startMs));
  }

  /** Newest first. */
  recoveryRecords(): RecoveryRecord[] {
    return this.recoveries.map((r) => ({ ...r.record })).reverse();
  }

  // ---- summaries ---------------------------------------------------------------------------

  private windowSamples(at = now()): TimingSample[] {
    const from = at - this.timingWindowMs;
    let i = 0;
    while (i < this.samples.length && this.samples[i].at < from) i++;
    if (i > 0) this.samples.splice(0, i);
    return this.samples;
  }

  timings(at = now()) {
    const samples = this.windowSamples(at);
    const p50 = {} as Record<TimingKey, number>;
    const p95 = {} as Record<TimingKey, number>;
    for (const key of TIMING_KEYS) {
      const sorted = samples.map((s) => s[key]).sort((a, b) => a - b);
      p50[key] = round1(percentile(sorted, 50));
      p95[key] = round1(percentile(sorted, 95));
    }
    // Share of each task's service time (everything but queue wait) spent on orchestration.
    let overhead = 0;
    let service = 0;
    for (const s of samples) {
      overhead += s.claimMs + s.completeMs + s.dispatchWaitMs;
      service += Math.max(0, s.totalMs - s.queueWaitMs);
    }
    return {
      windowSec: this.timingWindowMs / 1000,
      samples: samples.length,
      p50,
      p95,
      overheadPct: service > 0 ? round1((overhead / service) * 100) : 0,
    };
  }

  p50ServiceMs(stage: Stage, at = now()): number {
    const sorted = this.windowSamples(at)
      .filter((s) => s.stage === stage)
      .map((s) => s.serviceMs)
      .sort((a, b) => a - b);
    return Math.round(percentile(sorted, 50));
  }

  cache(at = now()) {
    const from = at - 10 * 60_000;
    this.cacheWindow = this.cacheWindow.filter((c) => c.at >= from);
    const hits = this.cacheWindow.reduce((n, c) => n + c.hits, 0);
    const total = this.cacheWindow.reduce((n, c) => n + c.total, 0);
    return { hitsLast10m: hits, hitRatePct: total > 0 ? round1((hits / total) * 100) : 0 };
  }

  /** Test hook. */
  reset() {
    this.throughput.clear();
    this.dispatch.clear();
    this.lastSweepRepaired = 0;
    this.samples = [];
    this.completeMsBacklog = [];
    this.fencing = { staleRejected: 0, last: null };
    this.cacheWindow = [];
    this.recoveries = [];
    this.recoveryByTask.clear();
  }
}

export const telemetry = new Telemetry();
