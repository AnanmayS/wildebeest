// Everything the pipeline view needs, with v1 fallbacks for each field the `system`
// snapshot would normally provide. Components below this never have to ask "is it v2?".

import type { Lease, RecoveryRecord, Stage, StageStats, SystemSnapshot, Worker } from './types';

export interface PipelineModel {
  v2: boolean;
  claimMode: string | null;
  dispatcher: { mode: string | null; pushedPerSec: number | null; sweepsPerSec: number | null; lastSweepRepaired: number | null };
  queues: { detect: number | null; classify: number | null; throttled: boolean };
  limits: { detectTarget: number; highWater: number; lowWater: number; leaseMs: number; workerTimeoutMs: number };
  stages: Record<Stage, Partial<StageStats>>;
  leasesByTask: Map<string, Lease>;
  recoveryByWorker: Map<string, RecoveryRecord>;
}

interface Inputs {
  system: SystemSnapshot | null;
  workers: Worker[];
  queues: { detect: number; classify: number } | null;
  throttled: boolean;
  config: { leaseMs: number; workerTimeoutMs: number };
}

export function pipelineModel({ system, workers, queues, throttled, config }: Inputs): PipelineModel {
  const cfg = system?.config ?? {};
  const d = system?.dispatcher;

  const recoveryByWorker = new Map<string, RecoveryRecord>();
  for (const r of system?.recovery ?? []) {
    const cur = recoveryByWorker.get(r.workerId);
    if (!cur || Date.parse(r.detectedAt) > Date.parse(cur.detectedAt)) recoveryByWorker.set(r.workerId, r);
  }

  return {
    v2: !!system,
    claimMode: cfg.claimMode ?? null,
    dispatcher: {
      mode: d?.mode ?? null,
      pushedPerSec: d ? d.pushedLast10s / 10 : null,
      sweepsPerSec: d ? d.repairSweepsLast10s / 10 : null,
      lastSweepRepaired: d?.lastSweepRepaired ?? null,
    },
    queues: {
      detect: system?.queues?.detect ?? queues?.detect ?? null,
      classify: system?.queues?.classify ?? queues?.classify ?? null,
      throttled: system?.queues?.throttled ?? throttled,
    },
    limits: {
      detectTarget: cfg.detectQueueTarget ?? 50,
      highWater: cfg.classifyHighWater ?? 500,
      lowWater: cfg.classifyLowWater ?? 200,
      leaseMs: cfg.leaseMs ?? config.leaseMs,
      workerTimeoutMs: cfg.workerTimeoutMs ?? config.workerTimeoutMs,
    },
    stages: {
      detect: system?.stages?.detect ?? stageFromWorkers(workers, 'detect'),
      classify: system?.stages?.classify ?? stageFromWorkers(workers, 'classify'),
    },
    leasesByTask: new Map((system?.leases ?? []).map((l) => [l.taskId, l])),
    recoveryByWorker,
  };
}

/** v1: what the worker list alone can tell us (no completion rate). */
function stageFromWorkers(workers: Worker[], stage: Stage): Partial<StageStats> {
  const mine = workers.filter((w) => w.stage === stage && w.status === 'ALIVE');
  const latencies = mine.map((w) => w.avgLatencyMs).filter((v): v is number => v != null).sort((a, b) => a - b);
  return {
    workersAlive: mine.length,
    inFlight: mine.filter((w) => w.state === 'busy').length,
    p50ServiceMs: latencies.length ? latencies[Math.floor(latencies.length / 2)] : undefined,
  };
}
