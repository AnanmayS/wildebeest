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
}

export type EventType =
  | 'worker_killed' | 'worker_died' | 'reassigned' | 'lease_expired' | 'stale_rejected' | 'cache_hit'
  | 'throttled' | 'unthrottled' | 'failed' | 'job_done';

export interface TaskEvent {
  id: number;
  at: string;
  type: EventType | string;
  taskId?: string | null;
  workerId?: string | null;
  message: string;
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
  | { type: 'job_progress'; job: JobSummary }
  | { type: 'worker_update'; workers: Worker[] }
  | { type: 'task_events'; events: TaskEvent[] }
  | { type: 'throttle'; throttled: boolean; classifyQueue: number };
