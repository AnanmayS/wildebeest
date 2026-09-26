import fs from "node:fs/promises";
import path from "node:path";
import express, { type NextFunction, type Request, type Response } from "express";
import multer from "multer";
import { getChaos, setChaos } from "./chaos.js";
import { config } from "./config.js";
import { getPool, query } from "./db.js";
import { ConflictError, killWorker, pauseWorker, WorkerNotFoundError } from "./docker.js";
import { listDlq, redrive } from "./dlq.js";
import { recentLogEvents } from "./events.js";
import {
  cancelJob,
  createJob,
  createSampleJob,
  createSyntheticJob,
  jobImages,
  jobSummary,
  listJobs,
  SampleUnavailableError,
  sha256Of,
} from "./jobs.js";
import { clusterStatus } from "./leader.js";
import { computeMetrics } from "./metrics.js";
import { settled, withReportContext } from "./otel.js";
import { httpMetrics, promHandler } from "./prom.js";
import { systemSnapshot } from "./system.js";
import {
  claimConfirm,
  claimTasks,
  completeTask,
  completeTasks,
  failTask,
  isUuid,
  releaseTask,
  ValidationError,
  type Outcome,
} from "./tasks.js";
import { deregisterWorker, heartbeatWithCancel, listWorkers, registerWorker } from "./workers.js";

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024, files: 5000 } });

function sendOutcome(res: Response, outcome: Outcome) {
  if (outcome.status === "ok") return res.json(outcome.leases ? { ok: true, leases: outcome.leases } : { ok: true });
  if (outcome.status === "stale") return res.status(409).json({ error: "STALE_LEASE" });
  // P3: the task's other attempt (speculative copy or original) won the race. Drop the result.
  if (outcome.status === "already_done") return res.status(409).json({ error: "ALREADY_DONE" });
  return res.status(404).json({ error: "NOT_FOUND" });
}

/** `next` on complete: how many tasks to claim in the same request (0 = none). */
function nextCount(v: unknown): number {
  const n = Math.floor(Number(v ?? 0));
  return Number.isFinite(n) && n > 0 ? Math.min(n, config.maxClaimBatch) : 0;
}

export function createApp() {
  const app = express();
  app.disable("x-powered-by");
  app.use(express.json({ limit: "5mb" }));
  app.use(httpMetrics); // claim/complete/... handler latency for /metrics/prom

  app.get("/healthz", (_req, res) => {
    res.json({ ok: true });
  });

  app.get("/config", (_req, res) => {
    res.json({
      animalConfThreshold: config.animalConfThreshold,
      heartbeatMs: config.heartbeatMs,
      workerTimeoutMs: config.workerTimeoutMs,
      leaseMs: config.leaseMs,
      humanReviewSecondsPerImage: config.humanReviewSecondsPerImage,
      // Optional (GRAFANA_PUBLIC_URL): the dashboard shows "Open traces in Grafana" when set.
      grafanaUrl: config.grafanaPublicUrl || null,
    });
  });

  // CONTRACTS.md "GET /benchmarks": the benchmark harness's summary.json, or 404 when absent.
  app.get("/benchmarks", async (_req, res) => {
    try {
      const text = await fs.readFile(path.join(config.benchmarksDir, "summary.json"), "utf8");
      res.type("application/json").send(JSON.stringify(JSON.parse(text)));
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code === "ENOENT" || code === "ENOTDIR") return res.status(404).json({ error: "NO_BENCHMARKS" });
      console.error(`[api] GET /benchmarks: ${(err as Error).message}`);
      res.status(500).json({ error: "BENCHMARKS_UNREADABLE" });
    }
  });

  // ---- jobs (dashboard) ------------------------------------------------------------------

  app.post("/jobs", upload.array("files"), async (req, res) => {
    const files = (req.files as Express.Multer.File[] | undefined) ?? [];
    if (files.length === 0) return res.status(400).json({ error: "no files uploaded (multipart field 'files')" });
    const images = files.map((f) => ({
      originalName: f.originalname,
      sha256: sha256Of(f.buffer),
      contentType: f.mimetype || "image/jpeg",
      read: async () => f.buffer,
    }));
    const { jobId } = await createJob({
      name: `upload-${files.length}`,
      countryCode: typeof req.body?.countryCode === "string" ? req.body.countryCode : null,
      images,
    });
    res.json({ jobId });
  });

  app.post("/jobs/sample", async (req, res) => {
    const size = Number(req.body?.size ?? 1000);
    if (!Number.isFinite(size) || size < 1) return res.status(400).json({ error: "size must be a positive number" });
    const { jobId } = await createSampleJob(size, req.body?.countryCode, {
      fresh: req.body?.fresh === true,
      random: req.body?.random === true,
    });
    res.json({ jobId });
  });

  app.post("/jobs/synthetic", async (req, res) => {
    res.json(await createSyntheticJob(req.body?.count, req.body?.stage ?? "detect"));
  });

  app.get("/jobs", async (_req, res) => {
    res.json({ jobs: await listJobs() });
  });

  app.get("/jobs/:id", async (req, res) => {
    const job = isUuid(req.params.id) ? await jobSummary(req.params.id) : null;
    if (!job) return res.status(404).json({ error: "NOT_FOUND" });
    res.json(job);
  });

  app.post("/jobs/:id/cancel", async (req, res) => {
    if (!isUuid(req.params.id)) return res.status(404).json({ error: "NOT_FOUND" });
    res.json({ ok: await cancelJob(req.params.id) });
  });

  app.get("/jobs/:id/images", async (req, res) => {
    if (!isUuid(req.params.id)) return res.status(404).json({ error: "NOT_FOUND" });
    const q = req.query as Record<string, string | undefined>;
    res.json(
      await jobImages(req.params.id, {
        category: q.category || undefined,
        species: q.species || undefined,
        page: q.page ? Number(q.page) : undefined,
        pageSize: q.pageSize ? Number(q.pageSize) : undefined,
      }),
    );
  });

  // ---- workers, chaos, metrics (dashboard) ---------------------------------------------------

  app.get("/workers", async (_req, res) => {
    res.json({ workers: await listWorkers() });
  });

  app.post("/workers/:id/kill", async (req, res) => {
    await killWorker(req.params.id, "api");
    res.json({ ok: true });
  });

  app.post("/workers/:id/pause", async (req, res) => {
    res.json(await pauseWorker(req.params.id, req.body?.ms ?? 20000, "api"));
  });

  app.get("/system", async (_req, res) => {
    res.json(await systemSnapshot());
  });

  app.get("/dlq", async (req, res) => {
    res.json(await listDlq(Number(req.query.limit ?? 100)));
  });

  app.post("/dlq/:taskId/redrive", async (req, res) => {
    if (!(await redrive(req.params.taskId))) return res.status(404).json({ error: "NOT_FOUND" });
    res.json({ ok: true });
  });

  app.get("/chaos", async (_req, res) => {
    res.json(await getChaos());
  });

  app.post("/chaos", async (req, res) => {
    res.json(await setChaos(req.body ?? {}));
  });

  // Which replica answered, who leads, and every live replica (HA; docs/decisions/h-ha.md).
  app.get("/cluster", async (_req, res) => {
    res.json(await clusterStatus());
  });

  app.get("/metrics", async (_req, res) => {
    res.json(await computeMetrics());
  });

  // Prometheus exposition (RED per stage, queue/lease/worker gauges); scraped by otel-lgtm.
  app.get("/metrics/prom", promHandler);

  app.get("/events", async (req, res) => {
    const limit = Math.min(1000, Math.max(1, Number(req.query.limit ?? 200) || 200));
    res.json({ events: await recentLogEvents(getPool(), limit) });
  });

  app.post("/admin/clear-cache", async (_req, res) => {
    const det = await query(`delete from detection_results`);
    const cls = await query(`delete from classification_results`);
    console.log(`[admin] cache cleared: ${det.rowCount} detection, ${cls.rowCount} classification rows`);
    res.json({ ok: true, deleted: { detection: det.rowCount ?? 0, classification: cls.rowCount ?? 0 } });
  });

  // ---- workers (internal) ----------------------------------------------------------------

  app.post("/workers/register", async (req, res) => {
    res.json(await registerWorker(req.body ?? {}));
  });

  app.post("/workers/:id/heartbeat", async (req, res) => {
    const hb = await heartbeatWithCancel(req.params.id, req.body?.metrics, req.body?.taskIds);
    if (!hb.alive) return res.status(410).json({ error: "WORKER_DEAD" });
    // claimMode rides along so running workers follow a coordinator restarted in the other mode.
    // cancel: speculated tasks this worker should drop (the other attempt won, or its copy ended).
    res.json({ ok: true, claimMode: config.claimMode, cancel: hb.cancel });
  });

  app.post("/workers/:id/deregister", async (req, res) => {
    await deregisterWorker(req.params.id);
    res.json({ ok: true });
  });

  app.post("/tasks/claim-confirm", async (req, res) => {
    const { workerId, taskIds } = req.body ?? {};
    if (typeof workerId !== "string" || !Array.isArray(taskIds)) {
      return res.status(400).json({ error: "body must be { workerId, taskIds: [] }" });
    }
    res.json({ leases: await claimConfirm(workerId, taskIds) });
  });

  // Long-poll claim (CLAIM_MODE=postgres only: in hybrid mode the ready queue is in Redis, and
  // claiming around it would leave its IDs behind as duplicates).
  app.post("/tasks/claim", async (req, res) => {
    const { workerId, stage, max, waitMs } = req.body ?? {};
    if (typeof workerId !== "string" || (stage !== "detect" && stage !== "classify")) {
      return res.status(400).json({ error: "body must be { workerId, stage: 'detect'|'classify', max?, waitMs? }" });
    }
    if (config.claimMode !== "postgres") return res.status(409).json({ error: "CLAIM_MODE_HYBRID" });
    let gone = false;
    res.on("close", () => {
      if (!res.writableEnded) gone = true;
    });
    const leases = await claimTasks(workerId, stage, Number(max ?? 1), Number(waitMs ?? 1000), () => gone);
    if (gone) {
      // Nobody will run these: hand them back at once (free) instead of letting the leases expire.
      await Promise.all(leases.map((l) => releaseTask(l.taskId, workerId, l.leaseEpoch, "claim abandoned")));
      return;
    }
    res.json({ leases });
  });

  app.post("/tasks/:id/complete", async (req, res) => {
    const { workerId, leaseEpoch, result, timings, next, traceparent } = req.body ?? {};
    sendOutcome(
      res,
      await withReportContext(traceparent, () =>
        completeTask(req.params.id, String(workerId ?? ""), leaseEpoch, result, timings, nextCount(next)),
      ),
    );
  });

  // Many completions in one statement; each item is fenced on its own epoch and one stale item
  // never fails the batch. `next: k` claims the worker's next k tasks in the same request.
  app.post("/tasks/complete-batch", async (req, res) => {
    const { workerId, items, next } = req.body ?? {};
    if (typeof workerId !== "string" || !Array.isArray(items) || items.length > 1000) {
      return res.status(400).json({ error: "body must be { workerId, items: [≤1000 items], next? }" });
    }
    const out = await completeTasks(workerId, items, nextCount(next));
    res.json({ results: out.results.map(({ taskId, status }) => ({ taskId, status: status === "not_found" ? "invalid" : status })), leases: out.leases });
  });

  app.post("/tasks/:id/fail", async (req, res) => {
    const { workerId, leaseEpoch, error, nonRetryable } = req.body ?? {};
    const outcome = await failTask(req.params.id, String(workerId ?? ""), leaseEpoch, error, nonRetryable === true);
    void settled(req.params.id, leaseEpoch, "fail", outcome.status, req.body?.traceparent, {
      "wildebeest.error": String(error ?? "").slice(0, 200),
    });
    sendOutcome(res, outcome);
  });

  app.post("/tasks/:id/release", async (req, res) => {
    const { workerId, leaseEpoch, reason } = req.body ?? {};
    const outcome = await releaseTask(req.params.id, String(workerId ?? ""), leaseEpoch, reason);
    void settled(req.params.id, leaseEpoch, "release", outcome.status, req.body?.traceparent, {
      "wildebeest.reason": String(reason ?? "").slice(0, 200),
    });
    sendOutcome(res, outcome);
  });

  // ---- errors ------------------------------------------------------------------------------

  app.use((_req, res) => {
    res.status(404).json({ error: "NOT_FOUND" });
  });

  app.use((err: any, req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof ValidationError) return res.status(400).json({ error: err.message });
    if (err instanceof WorkerNotFoundError) return res.status(404).json({ error: err.message });
    if (err instanceof ConflictError) return res.status(409).json({ error: err.code, message: err.message });
    if (err instanceof SampleUnavailableError) return res.status(400).json({ error: err.message });
    if (err instanceof multer.MulterError) return res.status(400).json({ error: err.message });
    if (err?.type === "entity.parse.failed") return res.status(400).json({ error: "invalid JSON body" });
    if (req.path.endsWith("/kill") || req.path.endsWith("/pause")) {
      const op = req.path.endsWith("/kill") ? "kill" : "pause";
      return res.status(502).json({ error: `docker ${op} failed: ${err?.message ?? err}` });
    }
    console.error(`[api] ${req.method} ${req.path} failed:`, err);
    res.status(500).json({ error: err?.message ?? "internal error" });
  });

  return app;
}
