// Shapes from docs/CONTRACTS.md ("Dashboard API" and "WebSocket /events").

export type Category = 'empty' | 'animal' | 'human' | 'vehicle' | 'failed';

export interface JobSummary {
  id: string;
  name: string;
  status: 'running' | 'done' | 'cancelled';
  createdAt: string;
  finishedAt: string | null;
  total: number;
  processed: number;
  failed: number;
  cacheHits: number;
  categories: Record<Category, number>;
  species: { commonName: string; count: number }[];
  elapsedMs: number;
  throughput: number;
  pending: { detect: number; classify: number };
  impact: { emptyPct: number; hoursSaved: number };
  // Added after the first dashboard build: optional so older coordinators still work.
  sampleSize?: number | null;
  throttled?: boolean;
  classifyQueue?: number;
}

export interface Detection {
  label: 'animal' | 'human' | 'vehicle';
  conf: number;
  bbox: [number, number, number, number]; // normalised x, y, w, h — top-left origin
}

export interface GalleryImage {
  id: string;
  url: string;
  cropUrl: string | null;
  category: Category | null;
  commonName: string | null;
  speciesLabel: string | null;
  confidence: number | null;
  detections: Detection[];
  cacheHit: boolean;
}

export interface Worker {
  id: string;
  stage: 'detect' | 'classify';
  status: 'ALIVE' | 'DEAD' | 'STOPPED';
  state: 'idle' | 'busy' | 'dead';
  containerId: string;
  tasksCompleted: number;
  currentTaskIds: string[];
  currentImageUrl: string | null;
  avgLatencyMs: number | null;
  rssMb: number | null;
  lastHeartbeatAt: string;
  reassignedCount: number;
  registeredAt?: string;
  diedAt?: string | null;
  // v2: native (non-container) workers can't be killed or paused through Docker.
  runtime?: 'container' | 'native';
  device?: 'cpu' | 'mps' | 'cuda';
}

export type EventType =
  | 'worker_killed' | 'worker_died' | 'reassigned' | 'lease_expired' | 'stale_rejected' | 'cache_hit'
  | 'throttled' | 'unthrottled' | 'failed' | 'job_done'
  | 'worker_paused' | 'worker_resumed' | 'released' | 'speculated';

export interface TaskEvent {
  id: number;
  at: string;
  type: EventType | string;
  taskId?: string | null;
  workerId?: string | null;
  message: string;
  /** Not sent by every coordinator; e.g. worker_died carries { via, detectMs }. */
  detail?: Record<string, unknown> | null;
}

/** GET /config — coordinator settings the UI needs. */
export interface Config {
  animalConfThreshold: number;
  heartbeatMs: number;
  workerTimeoutMs: number;
  leaseMs: number;
  humanReviewSecondsPerImage: number;
}

export interface Chaos {
  enabled: boolean;
  killEverySec: number;
}

export type ServerMessage =
  | { type: 'system'; system: SystemSnapshot }
  | { type: 'job_progress'; job: JobSummary }
  | { type: 'worker_update'; workers: Worker[] }
  | { type: 'task_events'; events: TaskEvent[] }
  | { type: 'throttle'; throttled: boolean; classifyQueue: number };

// ---------------------------------------------------------------------------
// v2: GET /system and the WebSocket `system` message (docs/CONTRACTS.md, "v2 additions").
// Every field is optional on purpose: a partially upgraded coordinator must not crash the page.

export type Stage = Worker['stage'];

export interface StageStats {
  workersAlive: number;
  inFlight: number;
  completedPerSec: number;
  p50ServiceMs: number;
}

export interface Lease {
  taskId: string;
  workerId: string;
  stage: Stage;
  epoch: number;
  ageMs: number;
  attempt: number;
  imageUrl?: string | null;
}

export const TIMING_STEPS = ['dispatchWaitMs', 'queueWaitMs', 'claimMs', 'fetchMs', 'inferMs', 'uploadMs', 'completeMs'] as const;
export type TimingStep = (typeof TIMING_STEPS)[number];
export type TimingSet = Partial<Record<TimingStep | 'totalMs', number>>;

export interface RecoveryRecord {
  workerId: string;
  killedAt: string;
  detectedAt: string;
  via: 'docker_event' | 'heartbeat' | string;
  requeuedAt: string | null;
  reclaimedAt: string | null;
  tasks: number;
  totalMs: number | null;
  /** Not in the contract yet (requested): who re-claimed the task. */
  reclaimedBy?: string | null;
}

export interface SystemSnapshot {
  at: string;
  config?: Partial<{
    claimMode: string; leaseMs: number; heartbeatMs: number; workerTimeoutMs: number;
    detectQueueTarget: number; classifyHighWater: number; classifyLowWater: number; modelBackend: string;
  }>;
  dispatcher?: { mode: string; pushedLast10s: number; repairSweepsLast10s: number; lastSweepRepaired: number };
  queues?: { detect: number; classify: number; throttled: boolean };
  stages?: Partial<Record<Stage, StageStats>>;
  leases?: Lease[];
  timings?: { windowSec: number; samples: number; p50: TimingSet; p95: TimingSet; overheadPct: number };
  recovery?: RecoveryRecord[];
  fencing?: { staleRejected: number; last: { taskId: string; workerId: string; epoch: number; currentEpoch: number; at: string } | null };
  invariants?: { checkedAt: string; duplicateResults: number; stuckLeases: number; lostImages: number; ok: boolean };
  throughput?: { t: string; detect: number; classify: number; images: number }[];
  cache?: { hitsLast10m: number; hitRatePct: number };
  speculation?: { launched: number; won: number; wasted: number };
  leader?: { id: string; term: number; since: string } | null;
}

// GET /benchmarks (benchmarks/summary.json)
export interface ScalingPoint { workers: number; throughput: number; p50Ms?: number; p99Ms?: number }

export interface Benchmarks {
  generatedAt?: string;
  machine?: string;
  ceiling?: {
    taskMs: number[];
    series: { taskMs: number; points: ScalingPoint[] }[];
    usl?: { taskMs: number; lambda: number; alpha: number; beta: number };
  };
  real?: { points: { detectors: number; classifiers: number; throughput: number }[] };
  recovery?: { before?: { p50Ms: number; p95Ms: number; samples: number }; after?: { p50Ms: number; p95Ms: number; samples: number } };
  overhead?: { before?: { perTaskMs: number }; after?: { perTaskMs: number } };
  faults?: { runs: number; faultsInjected: number; violations: number };
}
