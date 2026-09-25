// Prometheus metrics at GET /metrics/prom (docs/decisions/o-observability.md). GET /metrics (JSON)
// is unchanged.
//
// RED per stage (rate, errors, duration) from the hot path, where recording is a counter bump or
// a histogram observe on a cached label child; USE-style gauges (queue depth = saturation, leases
// in flight, workers) are computed at scrape time from the cached `system` snapshot, so scraping
// adds no work to the hot path. Worker-side numbers arrive with heartbeats (workers.metrics) and
// are exported here too, so workers need no metrics endpoint of their own.
//
// No static imports of application modules: telemetry.ts imports this file, and the scrape-time
// collectors load system.ts / db.ts lazily.

import type { NextFunction, Request, Response } from "express";
import client from "prom-client";

export const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry, prefix: "wildebeest_coordinator_" });

const STAGE = ["stage"] as const;
const SECONDS = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60];

const completed = new client.Counter({
  name: "wildebeest_tasks_completed_total",
  help: "Accepted task completions (one per task; fenced-off writes are not counted)",
  labelNames: STAGE,
  registers: [registry],
});
const failures = new client.Counter({
  name: "wildebeest_task_failures_total",
  help: "Task errors reported by workers (/fail); final=true went to the DLQ",
  labelNames: ["stage", "final"] as const,
  registers: [registry],
});
const leaseLosses = new client.Counter({
  name: "wildebeest_lease_expirations_total",
  help: "Leases lost and requeued by the reaper / death watch (reason: worker_dead | lease_expired)",
  labelNames: ["stage", "reason", "charged"] as const,
  registers: [registry],
});
const stale = new client.Counter({
  name: "wildebeest_stale_write_rejections_total",
  help: "complete/fail/release requests fenced off by an older lease epoch",
  registers: [registry],
});
const recoveries = new client.Counter({
  name: "wildebeest_recoveries_total",
  help: "Worker deaths handled by the recovery path (via: docker_event | heartbeat)",
  labelNames: ["via"] as const,
  registers: [registry],
});
const recoverySeconds = new client.Histogram({
  name: "wildebeest_recovery_seconds",
  help: "Best known moment of death → every lost task claimed again",
  buckets: [0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60],
  registers: [registry],
});
const cacheHits = new client.Counter({
  name: "wildebeest_cache_hits_total",
  help: "Images answered from the content-hash result cache at job creation",
  registers: [registry],
});
const imagesSubmitted = new client.Counter({
  name: "wildebeest_images_submitted_total",
  help: "Images submitted in jobs (excluding synthetic benchmark jobs)",
  registers: [registry],
});
const serviceSeconds = new client.Histogram({
  name: "wildebeest_task_service_seconds",
  help: "Claimed → completion handled, per accepted completion",
  labelNames: STAGE,
  buckets: SECONDS,
  registers: [registry],
});
const queueWaitSeconds = new client.Histogram({
  name: "wildebeest_task_queue_wait_seconds",
  help: "Ready → claimed, minus dispatch latency (backlog in Postgres + wait in the ready queue)",
  labelNames: STAGE,
  buckets: SECONDS,
  registers: [registry],
});
const httpSeconds = new client.Histogram({
  name: "wildebeest_http_request_duration_seconds",
  help: "Coordinator handling time of the worker protocol requests",
  labelNames: ["route", "code"] as const,
  buckets: [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [registry],
});

// Label children are looked up once per stage, not per observation.
const byStage = <T>(make: (stage: string) => T) => {
  const cache = new Map<string, T>();
  return (stage: string) => {
    let c = cache.get(stage);
    if (!c) cache.set(stage, (c = make(stage)));
    return c;
  };
};
const completedFor = byStage((s) => completed.labels(s));
const serviceFor = byStage((s) => serviceSeconds.labels(s));
const queueWaitFor = byStage((s) => queueWaitSeconds.labels(s));

// ---------------------------------------------------------------------------------------------
// Hot-path hooks
// ---------------------------------------------------------------------------------------------

// HA: telemetry records replicated from another replica over the cluster bus are applied through
// the same telemetry methods that feed these counters. Each replica counts only what it handled
// itself (sum the replicas in PromQL); the bus applies remote records inside fromBus().
let applyingRemote = 0;
export function fromBus<T>(fn: () => T): T {
  applyingRemote++;
  try {
    return fn();
  } finally {
    applyingRemote--;
  }
}
const local = () => applyingRemote === 0;

export const metrics = {
  completion(s: { stage: string; serviceMs: number; queueWaitMs: number }) {
    if (!local()) return;
    completedFor(s.stage).inc();
    serviceFor(s.stage).observe(s.serviceMs / 1000);
    queueWaitFor(s.stage).observe(s.queueWaitMs / 1000);
  },
  failed(stage: string, final: boolean) {
    failures.labels(stage, String(final)).inc();
  },
  leasesLost(tasks: Array<{ stage: string; workerGone: boolean; induced: boolean }>) {
    for (const t of tasks) leaseLosses.labels(t.stage, t.workerGone ? "worker_dead" : "lease_expired", String(!t.induced)).inc();
  },
  staleRejected() {
    if (!local()) return;
    stale.inc();
  },
  recovery(via: string) {
    if (!local()) return;
    recoveries.labels(via).inc();
  },
  recovered(ms: number) {
    if (!local()) return;
    recoverySeconds.observe(ms / 1000);
  },
  jobCreated(hits: number, total: number) {
    if (!local()) return;
    cacheHits.inc(hits);
    imagesSubmitted.inc(total);
  },
};

const ROUTES: Array<[RegExp, string]> = [
  [/^\/tasks\/claim-confirm$/, "claim-confirm"],
  [/^\/tasks\/claim$/, "claim"],
  [/^\/tasks\/complete-batch$/, "complete-batch"],
  [/^\/tasks\/[^/]+\/complete$/, "complete"],
  [/^\/tasks\/[^/]+\/fail$/, "fail"],
  [/^\/tasks\/[^/]+\/release$/, "release"],
  [/^\/workers\/[^/]+\/heartbeat$/, "heartbeat"],
  [/^\/workers\/register$/, "register"],
];

/** Express middleware: handler latency of the worker protocol routes (claim, complete, ...). */
export function httpMetrics(req: Request, res: Response, next: NextFunction) {
  if (req.method === "POST" && (req.path.startsWith("/tasks/") || req.path.startsWith("/workers/"))) {
    const route = ROUTES.find(([re]) => re.test(req.path))?.[1];
    if (route) {
      const start = performance.now();
      res.on("finish", () => httpSeconds.labels(route, String(res.statusCode)).observe((performance.now() - start) / 1000));
    }
  }
  next();
}

// ---------------------------------------------------------------------------------------------
// Scrape-time gauges
// ---------------------------------------------------------------------------------------------

const snapshot = async () => (await import("./system.js")).systemSnapshot();
const STAGES = ["detect", "classify"] as const;

new client.Gauge({
  name: "wildebeest_queue_depth",
  help: "Ready tasks per stage (Redis list length in hybrid mode, claimable PENDING rows in postgres mode)",
  labelNames: STAGE,
  registers: [registry],
  async collect() {
    const s = await snapshot();
    for (const st of STAGES) this.labels(st).set(s.queues[st]);
  },
});
new client.Gauge({
  name: "wildebeest_leases_in_flight",
  help: "LEASED tasks per stage",
  labelNames: STAGE,
  registers: [registry],
  async collect() {
    const s = await snapshot();
    for (const st of STAGES) this.labels(st).set(s.stages[st].inFlight);
  },
});
new client.Gauge({
  name: "wildebeest_throttled",
  help: "1 while detect admission is paused by classify backpressure",
  registers: [registry],
  async collect() {
    this.set((await snapshot()).queues.throttled ? 1 : 0);
  },
});
new client.Gauge({
  name: "wildebeest_invariant_violations",
  help: "Live invariant check (duplicate results, stuck leases, lost images); should be 0",
  labelNames: ["kind"] as const,
  registers: [registry],
  async collect() {
    const inv = (await snapshot()).invariants as unknown as Record<string, unknown>;
    for (const k of ["duplicateResults", "stuckLeases", "lostImages"]) this.labels(k).set(Number(inv[k] ?? 0));
  },
});

// Workers: status counts plus what each live worker reported in its last heartbeat.
const workerStatus = new client.Gauge({
  name: "wildebeest_workers",
  help: "Workers per stage and status (ALIVE | DEAD | STOPPED)",
  labelNames: ["stage", "status"] as const,
  registers: [registry],
});
const W = ["worker", "stage"] as const;
const workerDone = new client.Gauge({ name: "wildebeest_worker_tasks_done", help: "Tasks completed (worker-reported)", labelNames: W, registers: [registry] });
const workerRss = new client.Gauge({ name: "wildebeest_worker_rss_bytes", help: "Worker resident memory", labelNames: W, registers: [registry] });
const workerLatency = new client.Gauge({ name: "wildebeest_worker_avg_latency_seconds", help: "Mean handler time (worker-reported)", labelNames: W, registers: [registry] });
const workerBatch = new client.Gauge({ name: "wildebeest_worker_claim_batch", help: "Leases the worker aims to hold (claim window)", labelNames: W, registers: [registry] });

async function collectWorkers() {
  const { query } = await import("./db.js");
  const [counts, alive] = await Promise.all([
    query<{ stage: string; status: string; n: number }>(`select stage, status, count(*)::int as n from workers group by stage, status`),
    query<{ id: string; stage: string; metrics: Record<string, unknown> | null }>(
      `select id, stage, metrics from workers where status = 'ALIVE'`,
    ),
  ]);
  workerStatus.reset();
  for (const st of STAGES) for (const status of ["ALIVE", "DEAD", "STOPPED"]) workerStatus.labels(st, status).set(0);
  for (const r of counts.rows) workerStatus.labels(r.stage, r.status).set(r.n);
  for (const g of [workerDone, workerRss, workerLatency, workerBatch]) g.reset(); // dead workers drop out
  for (const w of alive.rows) {
    const m = w.metrics ?? {};
    const num = (k: string) => (Number.isFinite(Number(m[k])) ? Number(m[k]) : 0);
    workerDone.labels(w.id, w.stage).set(num("tasksDone"));
    workerRss.labels(w.id, w.stage).set(num("rssMb") * 2 ** 20);
    workerLatency.labels(w.id, w.stage).set(num("avgLatencyMs") / 1000);
    workerBatch.labels(w.id, w.stage).set(num("claimBatch"));
  }
}

/** GET /metrics/prom */
export async function promHandler(_req: Request, res: Response) {
  await collectWorkers();
  res.set("Content-Type", registry.contentType);
  res.send(await registry.metrics());
}
