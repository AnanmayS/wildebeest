// P2 hot path: push-after-commit dispatch (and its repair sweep), the scaled detect queue, the
// lock-free job finalisation, single-statement and batched completes, complete-and-claim-next,
// the Postgres claim mode, and the heartbeat's lease renewal.
import http from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createApp } from "../src/api.js";
import { config } from "../src/config.js";
import { getPool, query } from "../src/db.js";
import {
  detectQueueTarget,
  dispatchIdle,
  dispatchOnce,
  isThrottled,
  refreshStats,
  suspendEventPushes,
} from "../src/dispatcher.js";
import { finishCompletedJobs, reapOnce } from "../src/reaper.js";
import { getRedis, keys } from "../src/redis.js";
import { categorize } from "../src/results.js";
import { claimNext, claimTasks, completeTask, completeTasks, failTask, setJitterSource } from "../src/tasks.js";
import { telemetry } from "../src/telemetry.js";
import { heartbeat } from "../src/workers.js";
import {
  classifyResult,
  detectResult,
  events,
  expireLease,
  hybrid,
  imagesOfJob,
  job,
  makeJob,
  pullAndClaim,
  registerTestWorker,
  resetState,
  task,
  tasksOfJob,
  teardown,
} from "./helpers.js";

let server: http.Server;
let base: string;

beforeAll(async () => {
  server = http.createServer(createApp());
  await new Promise<void>((resolve) => server.listen(0, resolve));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
afterAll(async () => {
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

const queue = (stage: string) => getRedis().lrange(keys.queue(stage), 0, -1);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const animal = detectResult([{ label: "animal", conf: 0.9 }]);

// ---------------------------------------------------------------------------------------------

describe.skipIf(!hybrid)("push-after-commit dispatch", () => {
  it("pushes a new job's tasks at commit time and refills queue:detect behind every claim", async () => {
    const { jobId } = await makeJob(Array.from({ length: 8 }, (_, i) => `p${i}`));
    const tasks = await tasksOfJob(jobId);
    // No dispatcher tick has run: the job's own request pushed up to the target, oldest first.
    expect(await queue("detect")).toEqual(tasks.slice(0, config.detectQueueTarget).map((t) => t.id));

    const w = await registerTestWorker("detect", "w1");
    await pullAndClaim(w, "detect"); // the claim refilled the queue in the background
    expect(await queue("detect")).toEqual(tasks.slice(1, config.detectQueueTarget + 1).map((t) => t.id));
    expect(await dispatchOnce()).toMatchObject({ detect: 0, classify: 0, retried: 0 });
    expect(telemetry.lastSweepRepaired).toBe(0);
  });

  it("pushes the classify task a detect completion creates, before the complete returns", async () => {
    await makeJob(["zebra"]);
    const w = await registerTestWorker("detect", "w1");
    const l = (await pullAndClaim(w, "detect"))!;
    await completeTask(l.taskId, w, l.leaseEpoch, animal);
    const [cls] = await queue("classify");
    expect(await task(cls)).toMatchObject({ stage: "classify", state: "PENDING", queued: true });
    // eligible from the moment it existed: dispatchWaitMs is only the push latency.
    const t = await task(cls);
    expect(new Date(t.eligible_at).getTime()).toBe(new Date(t.pending_at).getTime());
  });

  it("pushes a failed task when its backoff ends, without waiting for a tick", async () => {
    setJitterSource(() => 0.2); // 0.2 × 500 ms = 100 ms backoff
    await makeJob(["flaky"]);
    const w = await registerTestWorker("detect", "w1");
    const l = (await pullAndClaim(w, "detect"))!;
    await failTask(l.taskId, w, l.leaseEpoch, "boom");
    expect(await queue("detect")).toEqual([]);
    await sleep(250);
    expect(await queue("detect")).toEqual([l.taskId]);
  });

  it("repairs a crash between commit and push on the next tick", async () => {
    suspendEventPushes(); // as if the process died right after COMMIT
    const { jobId } = await makeJob(["c1", "c2", "c3"]);
    expect(await queue("detect")).toEqual([]);
    expect((await tasksOfJob(jobId)).every((t) => t.state === "PENDING" && !t.queued)).toBe(true);

    suspendEventPushes(false); // "restarted"
    const r = await dispatchOnce();
    expect(r.detect).toBe(3);
    expect(telemetry.lastSweepRepaired).toBe(3);
    expect(await queue("detect")).toEqual((await tasksOfJob(jobId)).map((t) => t.id));

    // The same for a classify task created by a completion whose push was lost.
    const w = await registerTestWorker("detect", "w1");
    const l = (await pullAndClaim(w, "detect"))!;
    suspendEventPushes();
    await completeTask(l.taskId, w, l.leaseEpoch, animal);
    expect(await queue("classify")).toEqual([]);
    suspendEventPushes(false);
    expect((await dispatchOnce()).classify).toBe(1);
    expect(await queue("classify")).toHaveLength(1);
  });

  it("keeps backpressure: nothing new is admitted to queue:detect while classify is throttled", async () => {
    await getRedis().rpush(keys.queue("classify"), ...Array.from({ length: config.classifyQueueHighWater + 1 }, (_, i) => `x${i}`));
    await dispatchOnce();
    expect(isThrottled()).toBe(true);
    await makeJob(["held1", "held2"]);
    expect(await queue("detect")).toEqual([]);

    await getRedis().del(keys.queue("classify"));
    expect((await dispatchOnce()).detect).toBe(2); // released below low water: admitted again
  });

  it("scales the detect queue with live detect workers × their claim batch unless pinned", async () => {
    const pinned = config.detectQueueTarget;
    try {
      config.detectQueueTarget = 0;
      await refreshStats();
      expect(detectQueueTarget()).toBe(config.detectQueueMin); // no workers: the floor
      const a = await registerTestWorker("detect", "a");
      const b = await registerTestWorker("detect", "b");
      await registerTestWorker("classify", "c"); // doesn't count
      await heartbeat(a, { claimBatch: 16 });
      await heartbeat(b, { claimBatch: 8 });
      await refreshStats();
      expect(detectQueueTarget()).toBe(2 * (16 + 8));
    } finally {
      config.detectQueueTarget = pinned;
    }
    expect(detectQueueTarget()).toBe(pinned);
  });

  it("tick mode (P1) still dispatches everything from the sweep", async () => {
    config.dispatchMode = "tick";
    await makeJob(["t1", "t2"]);
    expect(await queue("detect")).toEqual([]);
    expect((await dispatchOnce()).detect).toBe(2);
  });
});

// ---------------------------------------------------------------------------------------------

describe("job completion without a hot job-row lock", () => {
  it("finalising an image far from the end of a job never touches the job row", async () => {
    const { jobId } = await makeJob(Array.from({ length: 6 }, (_, i) => `far${i}`));
    const w = await registerTestWorker("detect", "w1");
    const l = (await pullAndClaim(w, "detect"))!;

    // Another transaction holds the job row. P1 would block on it; now the complete doesn't wait.
    const holder = await getPool().connect();
    try {
      await holder.query("begin");
      await holder.query("select 1 from jobs where id = $1 for update", [jobId]);
      await holder.query("set local statement_timeout = 0");
      const done = completeTask(l.taskId, w, l.leaseEpoch, detectResult([]));
      const winner = await Promise.race([done.then(() => "completed"), sleep(1500).then(() => "blocked")]);
      expect(winner).toBe("completed");
    } finally {
      await holder.query("rollback");
      holder.release();
    }
  });

  it("two last images finishing at once: the lock near the end serialises them, one finishes the job", async () => {
    const { jobId } = await makeJob(["last1", "last2"]);
    const imgs = await imagesOfJob(jobId);
    const [a, b] = [await getPool().connect(), await getPool().connect()];
    try {
      await a.query("begin");
      await b.query("begin");
      await a.query(`select wb_finalize_image($1, 'empty')`, [imgs[0].id]);
      await b.query(`select wb_finalize_image($1, 'empty')`, [imgs[1].id]);
      // Each sees the other's image as unfinalised (1 ≤ threshold 2), so both take the job lock.
      const ra = await a.query(`select * from wb_finish_job($1, 2)`, [jobId]);
      expect(ra.rows).toEqual([]); // b's image isn't committed yet
      const rb = b.query(`select * from wb_finish_job($1, 2)`, [jobId]); // waits for a's lock
      await sleep(100);
      await a.query("commit");
      expect((await rb).rows).toEqual([{ name: "test", total: 2 }]); // fresh snapshot after the wait
      await b.query("commit");
    } finally {
      a.release();
      b.release();
    }
    expect(await job(jobId)).toMatchObject({ status: "done" });
  });

  it("when both skip the lock (threshold too low), the reaper's sweep finishes the job", async () => {
    const { jobId } = await makeJob(["race1", "race2"]);
    const imgs = await imagesOfJob(jobId);
    const [a, b] = [await getPool().connect(), await getPool().connect()];
    try {
      await a.query("begin");
      await b.query("begin");
      await a.query(`select wb_finalize_image($1, 'empty')`, [imgs[0].id]);
      await b.query(`select wb_finalize_image($1, 'empty')`, [imgs[1].id]);
      expect((await a.query(`select * from wb_finish_job($1, 0)`, [jobId])).rows).toEqual([]);
      expect((await b.query(`select * from wb_finish_job($1, 0)`, [jobId])).rows).toEqual([]);
      await a.query("commit");
      await b.query("commit");
    } finally {
      a.release();
      b.release();
    }
    expect(await job(jobId)).toMatchObject({ status: "running" }); // the race was missed...
    expect(await finishCompletedJobs()).toBe(1); // ...and the 1 s sweep catches it
    expect(await job(jobId)).toMatchObject({ status: "done" });
    expect(await events("job_done")).toHaveLength(1);
  });

  it("the SQL categorisation agrees with results.ts", async () => {
    const t = config.animalConfThreshold;
    const cases = [
      [],
      [{ label: "animal", conf: t - 0.01, bbox: [] }],
      [{ label: "animal", conf: t, bbox: [] }],
      [{ label: "vehicle", conf: 0.9, bbox: [] }, { label: "human", conf: t, bbox: [] }],
      [{ label: "human", conf: 0.99, bbox: [] }, { label: "animal", conf: 0.5, bbox: [] }],
      [{ label: "vehicle", conf: 0.5, bbox: [] }],
    ];
    for (const dets of cases) {
      const { rows } = await query(`select wb_categorize($1::jsonb, $2) as c`, [JSON.stringify(dets), t]);
      expect(rows[0].c).toBe(categorize(dets));
    }
  });
});

// ---------------------------------------------------------------------------------------------

describe("single-statement completes", () => {
  it("fences in the statement: an old epoch changes nothing and is logged; the right one succeeds", async () => {
    await makeJob(["fence"]);
    const a = await registerTestWorker("detect", "a");
    const b = await registerTestWorker("detect", "b");
    const la = (await pullAndClaim(a, "detect"))!;
    await expireLease(la.taskId);
    await reapOnce();
    const lb = (await pullAndClaim(b, "detect"))!;
    expect(lb).toMatchObject({ taskId: la.taskId, leaseEpoch: 2 });

    expect(await completeTask(la.taskId, a, la.leaseEpoch, detectResult([{ label: "vehicle", conf: 0.9 }]))).toEqual({
      status: "stale",
    });
    expect((await query(`select count(*)::int as n from detection_results`)).rows[0].n).toBe(0);
    expect(await completeTask(lb.taskId, b, lb.leaseEpoch, detectResult([{ label: "human", conf: 0.9 }]))).toEqual({
      status: "ok",
    });
    // A duplicate of the accepted complete (a retried request whose answer was lost) is answered
    // ok again, idempotently (010): nothing written, no stale_rejected. The old epoch stays fenced.
    expect(await completeTask(lb.taskId, b, lb.leaseEpoch, detectResult([]))).toEqual({ status: "ok" });
    expect(await completeTask(la.taskId, a, la.leaseEpoch, detectResult([]))).toEqual({ status: "stale" });

    const stale = await events("stale_rejected", la.taskId);
    expect(stale.map((e) => [e.worker_id, e.detail.leaseEpoch, e.detail.currentEpoch])).toEqual([
      [a, 1, 2],
      [a, 1, 2],
    ]);
    expect(await events("succeeded", la.taskId)).toHaveLength(1);
    expect(telemetry.fencing.staleRejected).toBe(2);
    const { rows } = await query(`select tasks_completed from workers where id = any($1::text[]) order by id`, [[a, b]]);
    expect(rows.map((r) => r.tasks_completed)).toEqual([0, 1]);
  });
});

describe("complete-batch", () => {
  it("completes a batch in one statement; a stale or malformed row never fails the rest", async () => {
    const { jobId } = await makeJob(["b1", "b2", "b3", "b4"]);
    const w = await registerTestWorker("detect", "w1");
    const leases = [];
    for (let i = 0; i < 4; i++) leases.push((await pullAndClaim(w, "detect"))!);
    const [l1, l2, l3, l4] = leases;

    const res = await api("POST", "/tasks/complete-batch", {
      workerId: w,
      items: [
        { taskId: l1.taskId, leaseEpoch: l1.leaseEpoch, result: detectResult([]), timings: { claimMs: 1, inferMs: 2 } },
        { taskId: l2.taskId, leaseEpoch: l2.leaseEpoch + 5, result: detectResult([]) }, // stale
        { taskId: l3.taskId, leaseEpoch: l3.leaseEpoch, result: { modelVersion: "x" } }, // no detections
        { taskId: l4.taskId, leaseEpoch: l4.leaseEpoch, result: animal },
        { taskId: "not-a-task", leaseEpoch: 1, result: detectResult([]) },
      ],
    });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      results: [
        { taskId: l1.taskId, status: "ok" },
        { taskId: l2.taskId, status: "stale" },
        { taskId: l3.taskId, status: "invalid" },
        { taskId: l4.taskId, status: "ok" },
        { taskId: "not-a-task", status: "invalid" },
      ],
      leases: [],
    });
    expect((await task(l1.taskId)).timings).toEqual({ claimMs: 1, fetchMs: 0, inferMs: 2, uploadMs: 0 });
    expect(await task(l2.taskId)).toMatchObject({ state: "LEASED" }); // untouched, lease kept
    expect(await task(l3.taskId)).toMatchObject({ state: "LEASED" }); // invalid keeps its lease
    expect(await task(l4.taskId)).toMatchObject({ state: "SUCCEEDED" });
    const imgs = await imagesOfJob(jobId);
    expect(imgs.filter((i) => i.final_category === "empty")).toHaveLength(1);
    expect((await tasksOfJob(jobId)).filter((t) => t.stage === "classify")).toHaveLength(1);
    expect(await events("stale_rejected", l2.taskId)).toHaveLength(1);
    expect(telemetry.timings().samples).toBe(2);

    // The rest of the job finishes through the batch path too, with the job_done in the same call.
    const rest = await completeTasks(w, [
      { taskId: l2.taskId, leaseEpoch: l2.leaseEpoch, result: detectResult([]) },
      { taskId: l3.taskId, leaseEpoch: l3.leaseEpoch, result: detectResult([]) },
    ]);
    expect(rest.results.map((r) => r.status)).toEqual(["ok", "ok"]);
    const cls = (await pullAndClaim(await registerTestWorker("classify", "c1"), "classify"))!;
    expect(await completeTask(cls.taskId, "classify-c1", cls.leaseEpoch, classifyResult("zebra"))).toEqual({ status: "ok" });
    expect(await job(jobId)).toMatchObject({ status: "done" });
    expect(await events("job_done")).toHaveLength(1);
  });

  it("rejects a body that isn't a batch", async () => {
    expect((await api("POST", "/tasks/complete-batch", { workerId: "w" })).status).toBe(400);
    expect((await api("POST", "/tasks/complete-batch", { items: [] })).status).toBe(400);
  });
});

describe("complete-and-claim-next", () => {
  it("returns the worker's next leases in the complete response", async () => {
    await makeJob(["n1", "n2", "n3", "n4"]);
    const w = await registerTestWorker("detect", "w1");
    const first = (await pullAndClaim(w, "detect"))!;

    const res = await api("POST", `/tasks/${first.taskId}/complete`, {
      workerId: w,
      leaseEpoch: first.leaseEpoch,
      result: detectResult([]),
      next: 2,
    });
    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);
    expect(res.body.leases).toHaveLength(2);
    for (const l of res.body.leases) {
      expect(l).toMatchObject({ stage: "detect", leaseEpoch: 1 });
      expect(await task(l.taskId)).toMatchObject({ state: "LEASED", worker_id: w });
    }
    await dispatchIdle();
    if (hybrid) {
      // Taken from the head of the ready queue, which was then refilled; none of them is queued twice.
      const q = await queue("detect");
      expect(q).not.toContain(res.body.leases[0].taskId);
      expect(q).not.toContain(res.body.leases[1].taskId);
    }
    // Plain complete without next keeps the P1 response.
    const l = res.body.leases[0];
    expect((await api("POST", `/tasks/${l.taskId}/complete`, { workerId: w, leaseEpoch: 1, result: detectResult([]) })).body).toEqual({ ok: true });
  });

  it("a batch with next claims after the completes, even if every item was stale", async () => {
    await makeJob(["s1", "s2"]);
    const w = await registerTestWorker("detect", "w1");
    const l = (await pullAndClaim(w, "detect"))!;
    const out = await completeTasks(w, [{ taskId: l.taskId, leaseEpoch: 99, result: detectResult([]) }], 5);
    expect(out.results).toEqual([{ taskId: l.taskId, status: "stale" }]);
    expect(out.leases.map((x) => x.stage)).toEqual(["detect"]);
  });

  it("claims nothing for a worker that isn't ALIVE, and hybrid mode puts the IDs back", async () => {
    await makeJob(["d1", "d2"]);
    const w = await registerTestWorker("detect", "w1");
    await query(`update workers set status = 'DEAD' where id = $1`, [w]);
    const before = hybrid ? await queue("detect") : [];
    expect(await claimNext(w, "detect", 2)).toEqual([]);
    if (hybrid) expect(await queue("detect")).toEqual(before);
  });
});

// ---------------------------------------------------------------------------------------------

describe("postgres claim mode", () => {
  beforeEach(() => {
    config.claimMode = "postgres";
  });

  it("leases up to max tasks with one SKIP LOCKED update; concurrent claimers get disjoint sets", async () => {
    await makeJob(Array.from({ length: 10 }, (_, i) => `pg${i}`));
    expect(await getRedis().llen(keys.queue("detect"))).toBe(0); // no ready queue in this mode
    const a = await registerTestWorker("detect", "a");
    const b = await registerTestWorker("detect", "b");
    const [la, lb] = await Promise.all([claimTasks(a, "detect", 4, 0), claimTasks(b, "detect", 4, 0)]);
    expect(la).toHaveLength(4);
    expect(lb).toHaveLength(4);
    const ids = [...la, ...lb].map((l) => l.taskId);
    expect(new Set(ids).size).toBe(8);
    for (const l of la) expect(await task(l.taskId)).toMatchObject({ state: "LEASED", worker_id: a, lease_epoch: 1 });
    expect(await events("claimed")).toHaveLength(8);
    // Claim time is the push time: there is no dispatch step to wait for.
    const t = await task(la[0].taskId);
    expect(new Date(t.pushed_at).getTime()).toBe(new Date(t.started_at).getTime());
  });

  it("claims retries first, only for ALIVE workers of the task's stage", async () => {
    await makeJob(["r1", "r2", "r3"]);
    const w = await registerTestWorker("detect", "w1");
    const cls = await registerTestWorker("classify", "c1");
    expect(await claimTasks(cls, "detect", 1, 0)).toEqual([]); // wrong stage
    const [first] = await claimTasks(w, "detect", 1, 0);
    await expireLease(first.taskId);
    await reapOnce();
    const [again] = await claimTasks(w, "detect", 1, 0);
    expect(again).toMatchObject({ taskId: first.taskId, leaseEpoch: 2 });

    await query(`update workers set status = 'DEAD' where id = $1`, [w]);
    expect(await claimTasks(w, "detect", 5, 0)).toEqual([]);
  });

  it("long-polls: a waiting claim is woken by the commit that creates work", async () => {
    const w = await registerTestWorker("detect", "w1");
    const started = Date.now();
    const waiting = claimTasks(w, "detect", 2, 3000);
    await sleep(100);
    await makeJob(["late1", "late2"]);
    const leases = await waiting;
    expect(leases).toHaveLength(2);
    expect(Date.now() - started).toBeLessThan(1500);
    // With nothing to do it gives up at waitMs.
    const t0 = Date.now();
    expect(await claimTasks(w, "detect", 1, 300)).toEqual([]);
    expect(Date.now() - t0).toBeGreaterThanOrEqual(250);
  });

  it("applies backpressure to new detect work from the PENDING classify backlog", async () => {
    const { jobId } = await makeJob(Array.from({ length: config.classifyQueueHighWater + 1 }, (_, i) => `bp${i}`));
    const imgs = await imagesOfJob(jobId);
    // A classify backlog above the high-water mark (as if detect had outrun classify).
    await query(
      `insert into tasks (id, image_id, stage, state) select gen_random_uuid(), id, 'classify', 'PENDING' from images where id = any($1::uuid[])`,
      [imgs.map((i) => i.id)],
    );
    await dispatchOnce();
    expect(isThrottled()).toBe(true);
    const w = await registerTestWorker("detect", "w1");
    expect(await claimTasks(w, "detect", 1, 0)).toEqual([]); // new detect work held back
    const c = await registerTestWorker("classify", "c1");
    expect(await claimTasks(c, "classify", 5, 0)).toHaveLength(5); // stage 2 drains
    await query(`update tasks set state = 'CANCELLED' where stage = 'classify' and state = 'PENDING'`);
    await dispatchOnce();
    expect(isThrottled()).toBe(false);
    expect(await claimTasks(w, "detect", 1, 0)).toHaveLength(1);
  });

  it("serves POST /tasks/claim, and refuses it in hybrid mode", async () => {
    await makeJob(["h1"]);
    const w = await registerTestWorker("detect", "w1");
    const res = await api("POST", "/tasks/claim", { workerId: w, stage: "detect", max: 3, waitMs: 0 });
    expect(res.status).toBe(200);
    expect(res.body.leases).toHaveLength(1);
    expect((await api("POST", "/tasks/claim", { workerId: w, stage: "paint" })).status).toBe(400);
    config.claimMode = "hybrid";
    expect(await api("POST", "/tasks/claim", { workerId: w, stage: "detect" })).toEqual({
      status: 409,
      body: { error: "CLAIM_MODE_HYBRID" },
    });
  });

  it("register tells workers the claim mode and batch bounds", async () => {
    const res = await api("POST", "/workers/register", { stage: "detect", hostname: "cfg" });
    expect(res.body.config).toMatchObject({
      claimMode: "postgres",
      claimBatchSize: config.claimBatchSize,
      maxClaimBatch: config.maxClaimBatch,
    });
  });
});

// ---------------------------------------------------------------------------------------------

describe("heartbeat lease renewal", () => {
  it("renews held leases in one statement and never waits on a completing transaction", async () => {
    await makeJob(["hb1", "hb2"]);
    const w = await registerTestWorker("detect", "w1");
    const l1 = (await pullAndClaim(w, "detect"))!;
    const l2 = (await pullAndClaim(w, "detect"))!;
    await query(`update tasks set lease_expires_at = now() + interval '100 ms' where worker_id = $1`, [w]);

    const holder = await getPool().connect(); // a completion holding l1's row
    try {
      await holder.query("begin");
      await holder.query("select 1 from tasks where id = $1 for update", [l1.taskId]);
      const hb = heartbeat(w, { claimBatch: 2 }, [l1.taskId, l2.taskId]);
      expect(await Promise.race([hb, sleep(1500).then(() => "blocked")])).toBe(true);
    } finally {
      await holder.query("rollback");
      holder.release();
    }
    const soon = Date.now() + 1000;
    expect(new Date((await task(l2.taskId)).lease_expires_at).getTime()).toBeGreaterThan(soon); // renewed
    expect(new Date((await task(l1.taskId)).lease_expires_at).getTime()).toBeLessThan(soon); // skipped this time
    // An idle worker's heartbeat only touches its own row.
    expect(await heartbeat(w, {}, [])).toBe(true);
  });
});
