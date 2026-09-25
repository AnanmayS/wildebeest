import { config } from "./config.js";
import { query } from "./db.js";
import { detectQueueTarget, isThrottled, queueDepths } from "./dispatcher.js";
import { latestInvariants, type InvariantReport } from "./invariants.js";
import { presign } from "./storage.js";
import { telemetry, type RecoveryRecord, type Stage, type TimingKey } from "./telemetry.js";

// GET /system and the websocket `system` message: the dashboard's main data. The shape is fixed by
// docs/CONTRACTS.md ("system snapshot") and must not drift. A snapshot costs four small indexed
// queries plus in-memory summaries, and is shared by every caller for SNAPSHOT_TTL_MS.

export interface SystemSnapshot {
  at: string;
  config: {
    claimMode: string;
    leaseMs: number;
    heartbeatMs: number;
    workerTimeoutMs: number;
    detectQueueTarget: number;
    classifyHighWater: number;
    classifyLowWater: number;
    modelBackend: string;
  };
  dispatcher: { mode: "tick" | "push"; pushedLast10s: number; repairSweepsLast10s: number; lastSweepRepaired: number };
  queues: { detect: number; classify: number; throttled: boolean };
  stages: Record<Stage, { workersAlive: number; inFlight: number; completedPerSec: number; p50ServiceMs: number }>;
  leases: Array<{
    taskId: string;
    workerId: string;
    stage: Stage;
    epoch: number;
    ageMs: number;
    attempt: number;
    imageUrl: string | null;
  }>;
  timings: {
    windowSec: number;
    samples: number;
    p50: Record<TimingKey, number>;
    p95: Record<TimingKey, number>;
    overheadPct: number;
  };
  recovery: RecoveryRecord[];
  fencing: {
    staleRejected: number;
    last: { taskId: string; workerId: string; epoch: number; currentEpoch: number; at: string } | null;
  };
  invariants: InvariantReport;
  throughput: Array<{ t: string; detect: number; classify: number; images: number }>;
  cache: { hitsLast10m: number; hitRatePct: number };
  speculation: { launched: number; won: number; wasted: number };
  leader: { id: string; term: number; since: string } | null;
}

const MAX_LEASES = 40;
const SNAPSHOT_TTL_MS = 400;
const STAGES: Stage[] = ["detect", "classify"];


const perSec = (n: number, seconds: number) => Math.round((n / seconds) * 100) / 100;

async function buildSnapshot(): Promise<SystemSnapshot> {
  const [depths, alive, inFlight, leaseRows, invariants] = await Promise.all([
    queueDepths(),
    query<{ stage: Stage; n: number }>(`select stage, count(*)::int as n from workers where status = 'ALIVE' group by stage`),
    query<{ stage: Stage; n: number }>(`select stage, count(*)::int as n from tasks where state = 'LEASED' group by stage`),
    query(
      `select t.id, t.worker_id, t.stage, t.lease_epoch, t.attempts, i.object_key,
              (extract(epoch from (now() - t.started_at)) * 1000)::int as age_ms
         from tasks t join images i on i.id = t.image_id
        where t.state = 'LEASED'
        order by t.started_at, t.id
        limit ${MAX_LEASES}`,
    ),
    latestInvariants(),
  ]);

  const count = (rows: Array<{ stage: Stage; n: number }>, stage: Stage) => rows.find((r) => r.stage === stage)?.n ?? 0;
  const last10 = telemetry.throughput.series(10);
  const stages = Object.fromEntries(
    STAGES.map((stage) => [
      stage,
      {
        workersAlive: count(alive.rows, stage),
        inFlight: count(inFlight.rows, stage),
        completedPerSec: perSec(
          last10.reduce((n, b) => n + b[stage], 0),
          10,
        ),
        p50ServiceMs: telemetry.p50ServiceMs(stage),
      },
    ]),
  ) as SystemSnapshot["stages"];

  const leases = await Promise.all(
    leaseRows.rows.map(async (r) => ({
      taskId: r.id as string,
      workerId: r.worker_id as string,
      stage: r.stage as Stage,
      epoch: r.lease_epoch as number,
      ageMs: Math.max(0, r.age_ms as number),
      // Which run this is: charged attempts so far plus the current one.
      attempt: (r.attempts as number) + 1,
      imageUrl: await presign(r.object_key),
    })),
  );

  return {
    at: new Date().toISOString(),
    config: {
      claimMode: config.claimMode,
      leaseMs: config.leaseMs,
      heartbeatMs: config.heartbeatMs,
      workerTimeoutMs: config.workerTimeoutMs,
      // The effective depth (it scales with live detect workers unless DETECT_QUEUE_TARGET pins it);
      // 0 in postgres claim mode, which has no ready queue.
      detectQueueTarget: config.claimMode === "hybrid" ? detectQueueTarget() : 0,
      classifyHighWater: config.classifyQueueHighWater,
      classifyLowWater: config.classifyQueueLowWater,
      modelBackend: config.modelBackend,
    },
    dispatcher: {
      mode: config.dispatchMode,
      pushedLast10s: telemetry.dispatch.sum("pushed", 10),
      repairSweepsLast10s: telemetry.dispatch.sum("sweeps", 10),
      lastSweepRepaired: telemetry.lastSweepRepaired,
    },
    // Ready work: Redis list lengths (hybrid) or claimable PENDING rows (postgres).
    queues: { detect: depths.detect, classify: depths.classify, throttled: isThrottled() },
    stages,
    leases,
    timings: telemetry.timings(),
    recovery: telemetry.recoveryRecords(),
    fencing: { staleRejected: telemetry.fencing.staleRejected, last: telemetry.fencing.last },
    invariants,
    throughput: telemetry.throughput.series(120).map((b) => ({
      t: new Date(b.t).toISOString(),
      detect: b.detect,
      classify: b.classify,
      images: b.images,
    })),
    cache: telemetry.cache(),
    speculation: { launched: 0, won: 0, wasted: 0 },
    leader: null,
  };
}

let cached: { at: number; snapshot: Promise<SystemSnapshot> } | null = null;

/** The current snapshot, rebuilt at most every SNAPSHOT_TTL_MS however many clients ask. */
export function systemSnapshot(): Promise<SystemSnapshot> {
  const now = Date.now();
  if (!cached || now - cached.at >= SNAPSHOT_TTL_MS) {
    const snapshot = buildSnapshot();
    cached = { at: now, snapshot };
    snapshot.catch(() => {
      if (cached?.snapshot === snapshot) cached = null;
    });
  }
  return cached.snapshot;
}

/** Test hook. */
export function resetSystemSnapshot() {
  cached = null;
}
