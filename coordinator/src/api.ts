import express, { type NextFunction, type Request, type Response } from "express";
import multer from "multer";
import { getChaos, setChaos } from "./chaos.js";
import { config } from "./config.js";
import { getPool, query } from "./db.js";
import { killWorker, WorkerNotFoundError } from "./docker.js";
import { recentLogEvents } from "./events.js";
import { createJob, createSampleJob, jobImages, jobSummary, listJobs, SampleUnavailableError, sha256Of } from "./jobs.js";
import { computeMetrics } from "./metrics.js";
import { claimConfirm, completeTask, failTask, isUuid, ValidationError, type Outcome } from "./tasks.js";
import { deregisterWorker, heartbeat, listWorkers, registerWorker } from "./workers.js";

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024, files: 5000 } });

function sendOutcome(res: Response, outcome: Outcome) {
  if (outcome.status === "ok") return res.json({ ok: true });
  if (outcome.status === "stale") return res.status(409).json({ error: "STALE_LEASE" });
  return res.status(404).json({ error: "NOT_FOUND" });
}

export function createApp() {
  const app = express();
  app.disable("x-powered-by");
  app.use(express.json({ limit: "5mb" }));

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
    });
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
    const { jobId } = await createSampleJob(size, req.body?.countryCode);
    res.json({ jobId });
  });

  app.get("/jobs", async (_req, res) => {
    res.json({ jobs: await listJobs() });
  });

  app.get("/jobs/:id", async (req, res) => {
    const job = isUuid(req.params.id) ? await jobSummary(req.params.id) : null;
    if (!job) return res.status(404).json({ error: "NOT_FOUND" });
    res.json(job);
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

  app.get("/chaos", (_req, res) => {
    res.json(getChaos());
  });

  app.post("/chaos", (req, res) => {
    res.json(setChaos(req.body ?? {}));
  });

  app.get("/metrics", async (_req, res) => {
    res.json(await computeMetrics());
  });

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
    const ok = await heartbeat(req.params.id, req.body?.metrics, req.body?.taskIds);
    if (!ok) return res.status(410).json({ error: "WORKER_DEAD" });
    res.json({ ok: true });
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

  app.post("/tasks/:id/complete", async (req, res) => {
    const { workerId, leaseEpoch, result } = req.body ?? {};
    sendOutcome(res, await completeTask(req.params.id, String(workerId ?? ""), leaseEpoch, result));
  });

  app.post("/tasks/:id/fail", async (req, res) => {
    const { workerId, leaseEpoch, error } = req.body ?? {};
    sendOutcome(res, await failTask(req.params.id, String(workerId ?? ""), leaseEpoch, error));
  });

  // ---- errors ------------------------------------------------------------------------------

  app.use((_req, res) => {
    res.status(404).json({ error: "NOT_FOUND" });
  });

  app.use((err: any, req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof ValidationError) return res.status(400).json({ error: err.message });
    if (err instanceof WorkerNotFoundError) return res.status(404).json({ error: err.message });
    if (err instanceof SampleUnavailableError) return res.status(400).json({ error: err.message });
    if (err instanceof multer.MulterError) return res.status(400).json({ error: err.message });
    if (err?.type === "entity.parse.failed") return res.status(400).json({ error: "invalid JSON body" });
    if (req.path.endsWith("/kill")) {
      return res.status(502).json({ error: `docker kill failed: ${err?.message ?? err}` });
    }
    console.error(`[api] ${req.method} ${req.path} failed:`, err);
    res.status(500).json({ error: err?.message ?? "internal error" });
  });

  return app;
}
