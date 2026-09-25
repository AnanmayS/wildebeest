// Runs the real Express app (plus the WebSocket hub) in-process and plays fake workers over HTTP,
// exactly the way the Python workers talk to the coordinator: register, BLMOVE on their own Redis
// connection, claim-confirm, heartbeat, complete. The loops are driven by hand (dispatchOnce /
// reapOnce) so the scenario is deterministic.
import http from "node:http";
import type { AddressInfo } from "node:net";
import { Redis } from "ioredis";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { WebSocket } from "ws";
import { createApp } from "../src/api.js";
import { config } from "../src/config.js";
import { query } from "../src/db.js";
import { dispatchOnce } from "../src/dispatcher.js";
import { setKiller } from "../src/docker.js";
import { hub } from "../src/events.js";
import { jobSummary } from "../src/jobs.js";
import { reapOnce } from "../src/reaper.js";
import { listWorkers } from "../src/workers.js";
import { classifyResult, detectResult, events, resetState, silenceWorker, teardown } from "./helpers.js";

let server: http.Server;
let base: string;
const workerRedis = new Redis(config.redisUrl); // the fake workers' own connection (BLMOVE blocks it)

beforeAll(async () => {
  server = http.createServer(createApp());
  hub.setBuilders({ jobSummary, workerList: listWorkers });
  hub.attach(server);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterAll(async () => {
  hub.close();
  await new Promise((r) => server.close(r));
  workerRedis.disconnect();
  setKiller();
  await teardown();
});

beforeEach(resetState);

async function api(method: string, route: string, body?: unknown) {
  const res = await fetch(base + route, {
    method,
    headers: body ? { "content-type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : undefined };
}

/** A fake worker speaking the HTTP contract. */
class FakeWorker {
  id = "";
  constructor(
    readonly stage: "detect" | "classify",
    readonly hostname: string,
  ) {}

  async register() {
    const res = await api("POST", "/workers/register", {
      stage: this.stage,
      hostname: this.hostname,
      containerId: this.hostname,
    });
    expect(res.status).toBe(200);
    expect(res.body.config).toMatchObject({ leaseMs: config.leaseMs, heartbeatMs: config.heartbeatMs });
    this.id = res.body.workerId;
    return this;
  }

  async claim() {
    if (config.claimMode === "postgres") {
      const res = await api("POST", "/tasks/claim", { workerId: this.id, stage: this.stage, max: 1, waitMs: 0 });
      expect(res.status).toBe(200);
      return (res.body.leases[0] ?? null) as { taskId: string; leaseEpoch: number; sha256: string; stage: string } | null;
    }
    const id = await workerRedis.blmove(`queue:${this.stage}`, `processing:${this.id}`, "LEFT", "RIGHT", 1);
    if (!id) return null;
    const res = await api("POST", "/tasks/claim-confirm", { workerId: this.id, taskIds: [id] });
    expect(res.status).toBe(200);
    return (res.body.leases[0] ?? null) as { taskId: string; leaseEpoch: number; sha256: string; stage: string } | null;
  }

  heartbeat(taskIds: string[] = []) {
    return api("POST", `/workers/${this.id}/heartbeat`, {
      taskIds,
      metrics: { tasksDone: 0, avgLatencyMs: 100, rssMb: 200, currentImageKey: null },
    });
  }

  complete(taskId: string, leaseEpoch: number, result: unknown) {
    return api("POST", `/tasks/${taskId}/complete`, { workerId: this.id, leaseEpoch, result });
  }
}

async function uploadJob(names: string[]) {
  const form = new FormData();
  for (const n of names) form.append("files", new Blob([`bytes-of-${n}`], { type: "image/jpeg" }), n);
  form.append("countryCode", "KEN");
  const res = await fetch(`${base}/jobs`, { method: "POST", body: form });
  expect(res.status).toBe(200);
  return ((await res.json()) as { jobId: string }).jobId;
}

describe("worker protocol over HTTP", () => {
  it("fences off a stale worker: A stalls, the reaper hands its task to B, A's late result is rejected", async () => {
    const jobId = await uploadJob(["stale.jpg"]);
    await dispatchOnce();
    const a = await new FakeWorker("detect", "worker-a").register();
    const b = await new FakeWorker("detect", "worker-b").register();

    // A claims the task, then goes silent (GC pause / network partition).
    const leaseA = (await a.claim())!;
    expect(leaseA).toMatchObject({ leaseEpoch: 1, stage: "detect" });
    expect((await api("GET", "/workers")).body.workers.find((w: any) => w.id === a.id)).toMatchObject({
      state: "busy",
      currentTaskIds: [leaseA.taskId],
    });

    await silenceWorker(a.id);
    expect((await b.heartbeat()).status).toBe(200);
    const reaped = await reapOnce();
    expect(reaped.dead).toEqual([a.id]);

    // B gets the same task with a higher epoch and completes it.
    await dispatchOnce();
    const leaseB = (await b.claim())!;
    expect(leaseB.taskId).toBe(leaseA.taskId);
    expect(leaseB.leaseEpoch).toBe(2);
    const done = await b.complete(leaseB.taskId, leaseB.leaseEpoch, detectResult([{ label: "human", conf: 0.8 }]));
    expect(done).toEqual({ status: 200, body: { ok: true } });

    // A wakes up and reports a different answer with its old epoch.
    const late = await a.complete(leaseA.taskId, leaseA.leaseEpoch, detectResult([{ label: "vehicle", conf: 0.99 }]));
    expect(late).toEqual({ status: 409, body: { error: "STALE_LEASE" } });
    expect((await a.heartbeat()).status).toBe(410);

    // B's result is the one kept.
    const { rows } = await query(`select detections from detection_results`);
    expect(rows).toHaveLength(1);
    expect(rows[0].detections[0].label).toBe("human");
    const job = (await api("GET", `/jobs/${jobId}`)).body;
    expect(job).toMatchObject({ status: "done", total: 1, processed: 1, categories: { human: 1 }, countryCode: "KEN" });
    const stale = await events("stale_rejected", leaseA.taskId);
    expect(stale).toHaveLength(1);
    expect(stale[0].worker_id).toBe(a.id);
    const { rows: t } = await query(`select state, lease_epoch, attempts, worker_id from tasks where id = $1`, [
      leaseA.taskId,
    ]);
    expect(t[0]).toMatchObject({ state: "SUCCEEDED", lease_epoch: 2, attempts: 1 });

    // The event log endpoint shows the story, newest first.
    const log = (await api("GET", "/events?limit=10")).body.events.map((e: any) => e.type);
    expect(log).toEqual(["heartbeat_refused", "stale_rejected", "job_done", "reassigned", "worker_died"]);
  });

  it("runs the two-stage pipeline and serves the gallery with presigned URLs", async () => {
    const jobId = await uploadJob(["zebra.jpg", "empty.jpg"]);
    const det = await new FakeWorker("detect", "d1").register();
    const cls = await new FakeWorker("classify", "c1").register();
    await dispatchOnce();
    for (let i = 0; i < 2; i++) {
      const lease = (await det.claim())!;
      const { rows } = await query(`select original_name from images where sha256 = $1`, [lease.sha256]);
      const dets = rows[0].original_name === "zebra.jpg" ? [{ label: "animal", conf: 0.95 }] : [];
      expect((await det.complete(lease.taskId, lease.leaseEpoch, detectResult(dets))).status).toBe(200);
    }
    await dispatchOnce();
    const c = (await cls.claim())!;
    expect(c.stage).toBe("classify");
    expect((await cls.complete(c.taskId, c.leaseEpoch, classifyResult("zebra"))).status).toBe(200);

    const job = (await api("GET", `/jobs/${jobId}`)).body;
    expect(job).toMatchObject({
      status: "done",
      processed: 2,
      categories: { animal: 1, empty: 1 },
      species: [{ commonName: "zebra", count: 1 }],
      impact: { emptyPct: 50 },
      sampleSize: null,
      throttled: false,
    });

    const gallery = (await api("GET", `/jobs/${jobId}/images?category=animal`)).body;
    expect(gallery.total).toBe(1);
    expect(gallery.images[0]).toMatchObject({ category: "animal", commonName: "zebra", cacheHit: false });
    expect(gallery.images[0].url).toMatch(/X-Amz-Signature=/);
    expect(gallery.images[0].detections[0]).toMatchObject({ label: "animal", conf: 0.95 });
    // Presigned URLs are stable between calls.
    const again = (await api("GET", `/jobs/${jobId}/images?category=animal`)).body;
    expect(again.images[0].url).toBe(gallery.images[0].url);

    const jobs = (await api("GET", "/jobs")).body.jobs;
    expect(jobs.map((j: any) => j.id)).toEqual([jobId]);
  });

  it("rejects bad input with 4xx", async () => {
    expect((await api("POST", "/workers/register", { stage: "paint" })).status).toBe(400);
    expect((await api("POST", "/tasks/claim-confirm", {})).status).toBe(400);
    expect((await api("GET", "/jobs/not-a-uuid")).status).toBe(404);
    expect((await api("POST", "/tasks/00000000-0000-0000-0000-000000000000/complete", { leaseEpoch: 1 })).status).toBe(
      404,
    );
    expect((await api("POST", "/workers/ghost/heartbeat", {})).status).toBe(410);
  });
});

describe("dashboard endpoints", () => {
  it("kills a worker through the (mocked) Docker socket and logs it", async () => {
    const killed: string[] = [];
    setKiller(async (id) => {
      killed.push(id);
    });
    const w = await new FakeWorker("detect", "victim").register();
    expect(await api("POST", `/workers/${w.id}/kill`)).toEqual({ status: 200, body: { ok: true } });
    expect(killed).toEqual(["victim"]);
    const [ev] = await events("worker_killed");
    expect(ev.worker_id).toBe(w.id);
    expect((await api("POST", "/workers/nobody/kill")).status).toBe(404);

    setKiller(async () => {
      throw new Error("no such container");
    });
    expect((await api("POST", `/workers/${w.id}/kill`)).status).toBe(502);
  });

  it("serves config, chaos, metrics and clear-cache", async () => {
    expect((await api("GET", "/healthz")).body).toEqual({ ok: true });
    expect((await api("GET", "/config")).body).toEqual({
      animalConfThreshold: config.animalConfThreshold,
      heartbeatMs: config.heartbeatMs,
      workerTimeoutMs: config.workerTimeoutMs,
      leaseMs: config.leaseMs,
      humanReviewSecondsPerImage: config.humanReviewSecondsPerImage,
    });
    expect((await api("POST", "/chaos", { enabled: false, killEverySec: 7 })).body).toEqual({
      enabled: false,
      killEverySec: 7,
    });
    expect((await api("GET", "/chaos")).body).toEqual({ enabled: false, killEverySec: 7 });

    const metrics = (await api("GET", "/metrics")).body;
    expect(metrics).toMatchObject({
      throttled: false,
      queues: { detect: 0, classify: 0 },
      workers: { alive: 0, dead: 0 },
      recoveryMs: [],
      latency: { p50: 0, p95: 0 },
    });

    await query(`insert into detection_results (sha256, model_version, detections) values ('x', 'v', '[]')`);
    expect((await api("POST", "/admin/clear-cache")).body).toEqual({
      ok: true,
      deleted: { detection: 1, classification: 0 },
    });
  });

  it("streams task events over the /events websocket", async () => {
    const ws = new WebSocket(`${base.replace("http", "ws")}/events`);
    const messages: any[] = [];
    ws.on("message", (data) => messages.push(JSON.parse(String(data))));
    await new Promise((r) => ws.on("open", r));

    await new FakeWorker("detect", "ws-worker").register();
    await query(`update workers set last_heartbeat_at = now() - interval '1 hour'`);
    await reapOnce();

    const deadline = Date.now() + 3000;
    const seen = (type: string) => messages.some((m) => m.type === type);
    while (Date.now() < deadline && !(seen("task_events") && seen("worker_update"))) {
      await new Promise((r) => setTimeout(r, 50));
    }
    ws.close();
    const batch = messages.find((m) => m.type === "task_events");
    expect(batch.events[0]).toMatchObject({ type: "worker_died", workerId: "detect-ws-worker" });
    expect(batch.events[0].message).toMatch(/died/);
    expect(messages.some((m) => m.type === "worker_update")).toBe(true);
  });
});
