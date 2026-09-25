// P1 telemetry: the `system` snapshot (exact contract shape), per-task timings, the live invariant
// check, synthetic jobs, and the websocket `system` message.
import http from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { WebSocket } from "ws";
import { createApp } from "../src/api.js";
import { config } from "../src/config.js";
import { query } from "../src/db.js";
import { dispatchOnce } from "../src/dispatcher.js";
import { hub } from "../src/events.js";
import { checkInvariants } from "../src/invariants.js";
import { jobSummary } from "../src/jobs.js";
import { reapOnce } from "../src/reaper.js";
import { resetSystemSnapshot, systemSnapshot } from "../src/system.js";
import { completeTask, flushCompleteTimings } from "../src/tasks.js";
import { SecondBuckets, percentile, telemetry } from "../src/telemetry.js";
import { heartbeat, listWorkers } from "../src/workers.js";
import {
  detectResult,
  hybrid,
  postgres,
  makeJob,
  pullAndClaim,
  registerTestWorker,
  resetState,
  silenceWorker,
  task,
  tasksOfJob,
  teardown,
} from "./helpers.js";

let server: http.Server;
let base: string;

beforeAll(async () => {
  server = http.createServer(createApp());
  hub.setBuilders({ jobSummary, workerList: listWorkers, system: systemSnapshot });
  hub.attach(server);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
afterAll(async () => {
  hub.close();
  await new Promise((r) => server.close(r));
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

const keysOf = (o: object) => Object.keys(o).sort();
const TIMING_KEYS = ["dispatchWaitMs", "queueWaitMs", "claimMs", "fetchMs", "inferMs", "uploadMs", "completeMs", "totalMs"];

/** Asserts every key of the SystemSnapshot in docs/CONTRACTS.md, no more and no fewer. */
function expectContractShape(s: any) {
  expect(keysOf(s)).toEqual(
    [
      "at",
      "config",
      "dispatcher",
      "queues",
      "stages",
      "leases",
      "timings",
      "recovery",
      "fencing",
      "invariants",
      "throughput",
      "cache",
      "speculation",
      "leader",
    ].sort(),
  );
  expect(new Date(s.at).toISOString()).toBe(s.at);
  expect(keysOf(s.config)).toEqual(
    [
      "claimMode",
      "leaseMs",
      "heartbeatMs",
      "workerTimeoutMs",
      "detectQueueTarget",
      "classifyHighWater",
      "classifyLowWater",
      "modelBackend",
    ].sort(),
  );
  expect(keysOf(s.dispatcher)).toEqual(["lastSweepRepaired", "mode", "pushedLast10s", "repairSweepsLast10s"]);
  expect(["tick", "push"]).toContain(s.dispatcher.mode);
  expect(keysOf(s.queues)).toEqual(["classify", "detect", "throttled"]);
  expect(keysOf(s.stages)).toEqual(["classify", "detect"]);
  for (const st of Object.values(s.stages) as any[]) {
    expect(keysOf(st)).toEqual(["completedPerSec", "inFlight", "p50ServiceMs", "workersAlive"]);
  }
  for (const l of s.leases) expect(keysOf(l)).toEqual(["ageMs", "attempt", "epoch", "imageUrl", "stage", "taskId", "workerId"]);
  expect(keysOf(s.timings)).toEqual(["overheadPct", "p50", "p95", "samples", "windowSec"]);
  expect(keysOf(s.timings.p50)).toEqual([...TIMING_KEYS].sort());
  expect(keysOf(s.timings.p95)).toEqual([...TIMING_KEYS].sort());
  for (const r of s.recovery) {
    expect(keysOf(r)).toEqual(
      ["detectedAt", "killedAt", "reclaimedAt", "reclaimedBy", "requeuedAt", "tasks", "totalMs", "via", "workerId"].sort(),
    );
  }
  expect(keysOf(s.fencing)).toEqual(["last", "staleRejected"]);
  if (s.fencing.last) expect(keysOf(s.fencing.last)).toEqual(["at", "currentEpoch", "epoch", "taskId", "workerId"]);
  expect(keysOf(s.invariants)).toEqual(["checkedAt", "duplicateResults", "lostImages", "ok", "stuckLeases"]);
  expect(s.throughput).toHaveLength(120);
  for (const b of s.throughput) expect(keysOf(b)).toEqual(["classify", "detect", "images", "t"]);
  expect(keysOf(s.cache)).toEqual(["hitRatePct", "hitsLast10m"]);
  expect(s.speculation).toEqual({ launched: 0, won: 0, wasted: 0 });
  expect(s.leader).toBeNull();
}

describe("GET /system", () => {
  it("has exactly the contract shape on an idle system", async () => {
    const res = await api("GET", "/system");
    expect(res.status).toBe(200);
    expectContractShape(res.body);
    expect(res.body).toMatchObject({
      config: { claimMode: config.claimMode, leaseMs: config.leaseMs, detectQueueTarget: hybrid ? config.detectQueueTarget : 0 },
      dispatcher: { mode: "push" },
      queues: { detect: 0, classify: 0, throttled: false },
      leases: [],
      recovery: [],
      fencing: { staleRejected: 0, last: null },
      invariants: { ok: true },
      timings: { samples: 0, windowSec: 60 },
    });
  });

  it("reflects leases, timings, recovery, fencing and throughput from real traffic", async () => {
    const { jobId } = await makeJob(["a", "b", "c"]);
    await dispatchOnce();
    const w = await registerTestWorker("detect", "w1");
    const dies = await registerTestWorker("detect", "dies");

    // One task completes with worker timings.
    const l1 = (await pullAndClaim(w, "detect"))!;
    const timings = { claimMs: 4.2, fetchMs: 12, inferMs: 300, uploadMs: 0 };
    await completeTask(l1.taskId, w, l1.leaseEpoch, detectResult([]), timings);

    // One is held by a worker that dies, then re-claimed with a higher epoch.
    const l2 = (await pullAndClaim(dies, "detect"))!;
    await silenceWorker(dies);
    await reapOnce();
    const l2b = (await pullAndClaim(w, "detect"))!;
    expect(l2b.taskId).toBe(l2.taskId);
    // ...and the dead worker's late result is fenced off.
    expect(await completeTask(l2.taskId, dies, l2.leaseEpoch, detectResult([]))).toEqual({ status: "stale" });

    resetSystemSnapshot();
    const s = (await api("GET", "/system")).body;
    expectContractShape(s);

    expect(s.stages.detect).toMatchObject({ workersAlive: 1, inFlight: 1 });
    expect(s.leases).toEqual([
      expect.objectContaining({ taskId: l2.taskId, workerId: w, stage: "detect", epoch: 2, attempt: 2 }),
    ]);
    expect(s.leases[0].imageUrl).toMatch(/X-Amz-Signature=/);

    expect(s.timings.samples).toBe(1);
    expect(s.timings.p50).toMatchObject({ claimMs: 4.2, fetchMs: 12, inferMs: 300, uploadMs: 0 });
    expect(s.timings.p50.completeMs).toBeGreaterThan(0);
    expect(s.timings.p50.totalMs).toBeGreaterThanOrEqual(s.timings.p50.queueWaitMs);

    expect(s.recovery).toEqual([
      expect.objectContaining({ workerId: dies, via: "heartbeat", tasks: 1, killedAt: null, reclaimedBy: w }),
    ]);
    expect(s.recovery[0].reclaimedAt).not.toBeNull();

    expect(s.fencing).toMatchObject({
      staleRejected: 1,
      last: { taskId: l2.taskId, workerId: dies, epoch: 1, currentEpoch: 2 },
    });
    // 3 new + 1 recovered (postgres mode has no push step)
    expect(s.dispatcher.pushedLast10s).toBeGreaterThanOrEqual(hybrid ? 4 : 0);
    expect(s.dispatcher.repairSweepsLast10s).toBe(1);

    // The completion lands in the throughput series once its second is over.
    await new Promise((r) => setTimeout(r, 1000));
    resetSystemSnapshot();
    const later = await systemSnapshot();
    expect(later.throughput.reduce((n, b) => n + b.detect, 0)).toBe(1);
    expect(later.throughput.reduce((n, b) => n + b.images, 0)).toBe(1);
    expect(later.stages.detect.completedPerSec).toBe(0.1);
    expect(later.cache).toEqual({ hitsLast10m: 0, hitRatePct: 0 });
    expect(jobId).toBeTruthy();
  });

  it("stores pushed_at, the worker's timings and (written behind) complete_ms on the task", async () => {
    const { jobId } = await makeJob(["t"]);
    await dispatchOnce();
    const w = await registerTestWorker("detect", "w1");
    const l = (await pullAndClaim(w, "detect"))!;
    // pushed_at: when it entered the ready queue (postgres mode: when it was claimed).
    const [t0] = await tasksOfJob(jobId);
    expect(t0.pushed_at).not.toBeNull();
    await completeTask(l.taskId, w, l.leaseEpoch, detectResult([]), { claimMs: 1, fetchMs: 2, inferMs: 3, uploadMs: 4 });
    expect(await task(l.taskId)).toMatchObject({
      timings: { claimMs: 1, fetchMs: 2, inferMs: 3, uploadMs: 4 },
      complete_ms: null,
    });
    expect(await flushCompleteTimings()).toBe(1);
    expect((await task(l.taskId)).complete_ms).toBeGreaterThan(0);
  });

  it("ignores malformed timings instead of rejecting the result", async () => {
    const { jobId } = await makeJob(["t"]);
    await dispatchOnce();
    const w = await registerTestWorker("detect", "w1");
    const l = (await pullAndClaim(w, "detect"))!;
    expect(await completeTask(l.taskId, w, l.leaseEpoch, detectResult([]), { claimMs: "fast" })).toEqual({ status: "ok" });
    expect((await task(l.taskId)).timings).toBeNull();
    expect(jobId).toBeTruthy();
  });
});

describe("dispatch wait vs backlog", () => {
  // Postgres mode has no queue target to be held behind.
  it.skipIf(postgres)("counts a backlog held behind DETECT_QUEUE_TARGET as queue wait, not orchestration", async () => {
    // One more task than the queue target: the last one waits in Postgres until there is room.
    const { jobId } = await makeJob(Array.from({ length: config.detectQueueTarget + 1 }, (_, i) => `b${i}`));
    await dispatchOnce();
    const w = await registerTestWorker("detect", "w1");
    await new Promise((r) => setTimeout(r, 400)); // the backlog waits...
    const first = (await pullAndClaim(w, "detect"))!; // ...until a claim makes room (push mode)
    await dispatchOnce(); // (tick mode: this sweep would push it)
    const tasks = await tasksOfJob(jobId);
    const last = tasks.at(-1)!;
    expect(last.pushed_at).not.toBeNull();
    expect(new Date(last.eligible_at).getTime()).toBeGreaterThan(new Date(last.pending_at).getTime() + 300);

    // Drain the queue up to the backlogged task and complete only it.
    await completeTask(first.taskId, w, first.leaseEpoch, detectResult([]));
    telemetry.reset();
    let lease = (await pullAndClaim(w, "detect"))!;
    while (lease.taskId !== last.id) {
      await completeTask(lease.taskId, w, lease.leaseEpoch, detectResult([]));
      lease = (await pullAndClaim(w, "detect"))!;
    }
    telemetry.reset();
    await completeTask(lease.taskId, w, lease.leaseEpoch, detectResult([]));
    const t = telemetry.timings();
    expect(t.samples).toBe(1);
    expect(t.p50.dispatchWaitMs).toBeLessThan(100); // push latency, not the 400 ms backlog
    expect(t.p50.queueWaitMs).toBeGreaterThanOrEqual(350);
  });
});

describe("event log items", () => {
  it("carry detail, and a dead worker's refused heartbeat is recorded once", async () => {
    const w = await registerTestWorker("detect", "zombie");
    await silenceWorker(w);
    await reapOnce();
    expect(await heartbeat(w, {}, [])).toBe(false);
    expect(await heartbeat(w, {}, [])).toBe(false);
    const log = (await api("GET", "/events?limit=10")).body.events;
    expect(log.map((e: any) => e.type)).toEqual(["heartbeat_refused", "worker_died"]);
    expect(log[1].detail).toMatchObject({ via: "heartbeat", stage: "detect" });
    expect(log[0].detail.deadForMs).toBeGreaterThanOrEqual(0);
  });
});

describe("live invariants", () => {
  it("is ok on a healthy run and catches each kind of violation", async () => {
    const { jobId } = await makeJob(["a", "b", "c"]);
    await dispatchOnce();
    const w = await registerTestWorker("detect", "w1");
    const l = (await pullAndClaim(w, "detect"))!;
    await completeTask(l.taskId, w, l.leaseEpoch, detectResult([]));
    expect(await checkInvariants()).toMatchObject({ ok: true, duplicateResults: 0, stuckLeases: 0, lostImages: 0 });

    // A second accepted completion of the same task (what a fencing bug would look like).
    await query(`insert into task_events (task_id, worker_id, type) values ($1, 'ghost', 'succeeded')`, [l.taskId]);
    // A lease held by a worker that died long ago, and the reaper never ran.
    const l2 = (await pullAndClaim(w, "detect"))!;
    await query(`update workers set status = 'DEAD', last_heartbeat_at = now() - interval '1 minute' where id = $1`, [w]);
    // An unfinished image whose task vanished.
    const tasks = await tasksOfJob(jobId);
    const orphan = tasks.find((t) => t.id !== l.taskId && t.id !== l2.taskId)!;
    await query(`delete from tasks where id = $1`, [orphan.id]);

    expect(await checkInvariants()).toMatchObject({ ok: false, duplicateResults: 1, stuckLeases: 1, lostImages: 1 });
    // Duplicates are cumulative: they don't heal.
    expect((await checkInvariants()).duplicateResults).toBe(1);
  });
});

describe("synthetic jobs", () => {
  it("bulk-creates n images and tasks with synthetic keys and no objects", async () => {
    const started = Date.now();
    const res = await api("POST", "/jobs/synthetic", { count: 20_000 });
    const ms = Date.now() - started;
    expect(res.status).toBe(200);
    expect(ms).toBeLessThan(15_000); // ~0.3 s on an idle laptop; generous for a shared Docker VM
    const jobId = res.body.jobId;

    const { rows } = await query(
      `select count(*)::int as n, count(distinct i.sha256)::int as shas,
              bool_and(i.object_key = 'synthetic/' || i.sha256) as keys_ok,
              bool_and(length(i.sha256) = 64) as sha_ok,
              count(t.id) filter (where t.state = 'PENDING' and t.stage = 'detect' and not t.queued)::int as pending
         from images i join tasks t on t.image_id = i.id where i.job_id = $1`,
      [jobId],
    );
    // Pushed right after the commit, up to the detect queue target (postgres mode: no queue).
    const queued = hybrid ? config.detectQueueTarget : 0;
    expect(rows[0]).toEqual({ n: 20_000, shas: 20_000, keys_ok: true, sha_ok: true, pending: 20_000 - queued });
    expect((await api("GET", `/jobs/${jobId}`)).body).toMatchObject({
      name: "synthetic-20000",
      status: "running",
      total: 20_000,
      pending: { detect: 20_000, classify: 0 },
    });

    // Dispatch in creation order; lease URLs are null because there is no object to sign.
    await dispatchOnce();
    const w = await registerTestWorker("detect", "w1");
    const lease = (await pullAndClaim(w, "detect"))!;
    expect(lease.imageKey).toBe(`synthetic/${lease.sha256}`);
    const s = await systemSnapshot();
    expect(s.leases[0].imageUrl).toBeNull();
  }, 60_000);

  it("validates count and stage", async () => {
    expect((await api("POST", "/jobs/synthetic", { count: 0 })).status).toBe(400);
    expect((await api("POST", "/jobs/synthetic", { count: 1.5 })).status).toBe(400);
    expect((await api("POST", "/jobs/synthetic", { count: config.maxSyntheticTasks + 1 })).status).toBe(400);
    expect((await api("POST", "/jobs/synthetic", { count: 5, stage: "paint" })).status).toBe(400);
    const res = await api("POST", "/jobs/synthetic", { count: 5, stage: "classify" });
    expect(res.status).toBe(200);
    const tasks = await tasksOfJob(res.body.jobId);
    expect(tasks.map((t) => t.stage)).toEqual(Array(5).fill("classify"));
  });
});

describe("websocket system message", () => {
  it("pushes { type: 'system', system } to connected clients", async () => {
    const ws = new WebSocket(`${base.replace("http", "ws")}/events`);
    const messages: any[] = [];
    ws.on("message", (data) => messages.push(JSON.parse(String(data))));
    await new Promise((r) => ws.on("open", r));
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline && messages.filter((m) => m.type === "system").length < 2) {
      await new Promise((r) => setTimeout(r, 50));
    }
    ws.close();
    const system = messages.filter((m) => m.type === "system");
    expect(system.length).toBeGreaterThanOrEqual(2);
    expectContractShape(system[0].system);
  });
});

describe("telemetry primitives", () => {
  it("buckets counts per second and reports a gap-free series", () => {
    const b = new SecondBuckets(["x"] as const, 5);
    b.add("x", 2, 10_000);
    b.add("x", 1, 10_900);
    b.add("x", 4, 12_100);
    expect(b.series(5, 13_000)).toEqual([
      { t: 8000, x: 0 },
      { t: 9000, x: 0 },
      { t: 10_000, x: 3 },
      { t: 11_000, x: 0 },
      { t: 12_000, x: 4 },
    ]);
    expect(b.sum("x", 3, 12_500)).toBe(7);
  });

  it("computes nearest-rank percentiles", () => {
    expect(percentile([], 50)).toBe(0);
    expect(percentile([1, 2, 3, 4], 50)).toBe(2);
    expect(percentile([1, 2, 3, 4], 95)).toBe(4);
  });
});
