// Final fix-ups (docs/decisions/f-fixups.md):
//  - task IDs orphaned in a live worker's processing list (a BLMOVE / MULTI+LMOVE whose reply was
//    lost to a connection reset) are taken back by the leader's queued-row audit;
//  - a retried complete whose first send already committed is answered ok, idempotently.
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createApp } from "../src/api.js";
import { config } from "../src/config.js";
import { FencedError, getPool, query, withFence, type Fence } from "../src/db.js";
import { repairLostQueued } from "../src/dispatcher.js";
import { checkInvariants } from "../src/invariants.js";
import { startLeaderLoops } from "../src/loops.js";
import { reapOnce } from "../src/reaper.js";
import { getRedis, keys } from "../src/redis.js";
import { claimConfirm, completeTask, completeTasks, requeueLostLeases, type Lease } from "../src/tasks.js";
import { telemetry } from "../src/telemetry.js";
import { heartbeat } from "../src/workers.js";
import { probation, serviceTimes } from "../src/speculation.js";
import {
  detectResult,
  eventually,
  events,
  hybrid,
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

const queue = (stage = "detect") => getRedis().lrange(keys.queue(stage), 0, -1);
const processing = (workerId: string) => getRedis().lrange(keys.processing(workerId), 0, -1);

/** Makes pushed_at old enough for the audit (it only judges rows pushed more than 5 s ago). */
const agePushes = () => query(`update tasks set pushed_at = now() - interval '10 seconds' where queued`);

/** What a worker's BLMOVE does when the connection resets before its reply: the ID moves, nobody knows. */
async function orphan(workerId: string): Promise<string> {
  const id = await getRedis().lmove(keys.queue("detect"), keys.processing(workerId), "LEFT", "RIGHT");
  expect(id).toBeTruthy();
  return id!;
}

// ---------------------------------------------------------------------------------------------

describe.skipIf(!hybrid)("orphaned IDs in a live worker's processing list", () => {
  const saved = { audit: config.queuedAuditMs, grace: config.orphanGraceMs };
  afterEach(() => {
    config.queuedAuditMs = saved.audit;
    config.orphanGraceMs = saved.grace;
  });

  it("are re-dispatched by the leader's audit within a bounded time, and the job finishes", async () => {
    const { jobId } = await makeJob(["o1", "o2"]);
    const victim = await registerTestWorker("detect", "victim");
    const other = await registerTestWorker("detect", "other");
    await eventually(async () => (await queue()).length === 2);
    const lost = await orphan(victim);
    const normal = (await pullAndClaim(other, "detect"))!;
    await completeTask(normal.taskId, other, normal.leaseEpoch, detectResult([]));
    await agePushes();
    // Before the fix nothing ever looked at it again: PENDING, queued, in a live worker's list.
    expect(await task(lost)).toMatchObject({ state: "PENDING", queued: true });

    config.queuedAuditMs = 200;
    config.orphanGraceMs = 600; // production: 3 heartbeats (6 s) with a 5 s audit
    const stop = startLeaderLoops({ self: "fixups-test" });
    const started = Date.now();
    try {
      // The victim stays ALIVE the whole time (it heartbeats; it just never learned of the ID).
      const beat = setInterval(() => {
        for (const w of [victim, other]) void heartbeat(w, {}, []).catch(() => {});
      }, 300);
      try {
        await eventually(async () => (await queue()).includes(lost), 5000);
      } finally {
        clearInterval(beat);
      }
      const tookMs = Date.now() - started;
      // Bound: grace + two audit intervals (first sighting, then the pass after the grace) + slack.
      expect(tookMs).toBeLessThan(config.orphanGraceMs + 2 * config.queuedAuditMs + 1500);
      expect(await processing(victim)).toEqual([]);
      expect(await task(lost)).toMatchObject({ state: "PENDING", queued: true });

      const l = (await pullAndClaim(other, "detect"))!;
      expect(l.taskId).toBe(lost);
      expect(await completeTask(lost, other, l.leaseEpoch, detectResult([]))).toEqual({ status: "ok" });
      await eventually(async () => (await job(jobId)).status === "done");
      expect(await events("succeeded", lost)).toHaveLength(1);
    } finally {
      stop();
    }
  });

  it("the audit waits out the grace, and leaves alone an ID that was confirmed meanwhile", async () => {
    await makeJob(["g1", "g2"]);
    const w = await registerTestWorker("detect", "w");
    await eventually(async () => (await queue()).length === 2);
    const a = await orphan(w);
    const b = await orphan(w);
    await agePushes();

    expect(await repairLostQueued(5000, 60_000)).toBe(0); // first sighting
    expect(await repairLostQueued(5000, 60_000)).toBe(0); // still inside the grace
    expect(await processing(w)).toEqual([a, b]);

    // b's claim-confirm gets through after all (it was only slow); a is never confirmed.
    const [lease] = await claimConfirm(w, [b]);
    expect(lease.taskId).toBe(b);
    expect(await repairLostQueued(5000, 0)).toBe(1);
    expect(await processing(w)).toEqual([]);
    expect(await queue()).toEqual([a]); // head of the queue, b not pushed again
    expect(await task(b)).toMatchObject({ state: "LEASED", worker_id: w });
  });

  it("is safe against a claim-confirm still in flight: one lease, the extra queue entry is skipped", async () => {
    await makeJob(["f1"]);
    const slow = await registerTestWorker("detect", "slow");
    const other = await registerTestWorker("detect", "other");
    await eventually(async () => (await queue()).length === 1);
    const id = await orphan(slow);
    await agePushes();
    await repairLostQueued(5000, 0);
    expect(await repairLostQueued(5000, 0)).toBe(1);
    expect(await queue()).toEqual([id]);

    // The slow worker's confirm lands now: the task is still PENDING, so it is leased to it.
    const [mine] = await claimConfirm(slow, [id]);
    expect(mine).toMatchObject({ taskId: id, leaseEpoch: 1 });
    // Another worker pulls the duplicate entry: nothing to lease, and it isn't put back.
    expect(await pullAndClaim(other, "detect")).toBeNull();
    expect(await queue()).toEqual([]);
    expect(await completeTask(id, slow, 1, detectResult([]))).toEqual({ status: "ok" });
    expect(await events("claimed", id)).toHaveLength(1);
  });

  it("a deposed leader's audit is fenced off before it touches Redis", async () => {
    await makeJob(["d1"]);
    const w = await registerTestWorker("detect", "w");
    await eventually(async () => (await queue()).length === 1);
    const id = await orphan(w);
    await agePushes();
    await query(`update coordinator_leader set holder = 'coord-new', instance = 'i2', term = 7`);
    const stale: Fence = { holder: "coord-old", instance: "i1", term: 6 };
    const current: Fence = { holder: "coord-new", instance: "i2", term: 7 };

    await withFence(current, () => repairLostQueued(5000, 0)); // first sighting
    await expect(withFence(stale, () => repairLostQueued(5000, 0))).rejects.toBeInstanceOf(FencedError);
    expect(await processing(w)).toEqual([id]);
    expect(await queue()).toEqual([]);
    expect(await withFence(current, () => repairLostQueued(5000, 0))).toBe(1);
    expect(await queue()).toEqual([id]);
  });

  it("leaves alone an ID that is also still in a ready queue (that entry gets claimed)", async () => {
    await makeJob(["q1", "q2"]);
    const w = await registerTestWorker("detect", "w");
    await eventually(async () => (await queue()).length === 2);
    const dup = (await queue())[0];
    await getRedis().rpush(keys.processing(w), dup); // also still queued: the queue entry gets claimed
    await agePushes();
    await repairLostQueued(5000, 0);
    expect(await repairLostQueued(5000, 0)).toBe(0);
    expect(await processing(w)).toEqual([dup]);
  });
});

// ---------------------------------------------------------------------------------------------

describe("idempotent completion retry", () => {
  it("a retried single complete is answered 200 ok: no second result, no stale_rejected", async () => {
    const { jobId } = await makeJob(["r1", "r2"]);
    const w = await registerTestWorker("detect", "w");
    const l = (await pullAndClaim(w, "detect"))!;
    const body = { workerId: w, leaseEpoch: l.leaseEpoch, result: detectResult([{ label: "human", conf: 0.9 }]) };
    expect((await api("POST", `/tasks/${l.taskId}/complete`, body)).status).toBe(200);
    // The answer was lost (the replica died after COMMIT); the worker sends the same request again.
    const retry = await api("POST", `/tasks/${l.taskId}/complete`, body);
    expect(retry).toEqual({ status: 200, body: { ok: true } });

    expect(await events("succeeded", l.taskId)).toHaveLength(1);
    expect(await events("stale_rejected", l.taskId)).toHaveLength(0);
    expect(telemetry.fencing.staleRejected).toBe(0);
    expect(telemetry.timings().samples).toBe(1); // counted once
    const { rows } = await query(`select tasks_completed from workers where id = $1`, [w]);
    expect(rows[0].tasks_completed).toBe(1);
    expect((await job(jobId)).status).toBe("running");

    // With `next`, the retry carries leases like any accepted complete.
    const withNext = await api("POST", `/tasks/${l.taskId}/complete`, { ...body, next: 1 });
    expect(withNext.status).toBe(200);
    expect(withNext.body.leases.map((x: Lease) => x.taskId)).toHaveLength(1);
  });

  it("a retried complete-batch gets status ok for the rows that already committed", async () => {
    await makeJob(["b1", "b2"]);
    const w = await registerTestWorker("detect", "w");
    const l1 = (await pullAndClaim(w, "detect"))!;
    const l2 = (await pullAndClaim(w, "detect"))!;
    const items = [
      { taskId: l1.taskId, leaseEpoch: l1.leaseEpoch, result: detectResult([]) },
      { taskId: l2.taskId, leaseEpoch: l2.leaseEpoch, result: detectResult([]) },
    ];
    await completeTasks(w, [items[0]]); // only the first made it before the connection dropped
    const retry = await api("POST", "/tasks/complete-batch", { workerId: w, items });
    expect(retry.status).toBe(200);
    expect(retry.body.results).toEqual([
      { taskId: l1.taskId, status: "ok" },
      { taskId: l2.taskId, status: "ok" },
    ]);
    expect(await events("succeeded")).toHaveLength(2);
    expect(await events("stale_rejected")).toHaveLength(0);
    expect(telemetry.timings().samples).toBe(2);
  });

  it("only the attempt that committed gets ok: an old epoch, or another worker, is still fenced", async () => {
    await makeJob(["e1"]);
    const a = await registerTestWorker("detect", "a");
    const b = await registerTestWorker("detect", "b");
    const la = (await pullAndClaim(a, "detect"))!;
    await query(`update tasks set lease_expires_at = now() - interval '1 second' where id = $1`, [la.taskId]);
    await reapOnce();
    const lb = (await pullAndClaim(b, "detect"))!;
    expect(await completeTask(lb.taskId, b, lb.leaseEpoch, detectResult([]))).toEqual({ status: "ok" });
    expect(await completeTask(lb.taskId, b, lb.leaseEpoch, detectResult([]))).toEqual({ status: "ok" });
    expect(await completeTask(la.taskId, a, la.leaseEpoch, detectResult([]))).toEqual({ status: "stale" });
    expect(await completeTask(lb.taskId, a, lb.leaseEpoch, detectResult([]))).toEqual({ status: "stale" });
    expect((await events("stale_rejected", la.taskId)).map((e) => e.worker_id)).toEqual([a, a]);
    expect(await events("succeeded", la.taskId)).toHaveLength(1);
  });

  it("speculation: the winning copy's retry is ok, the losing original still gets already_done", async () => {
    config.speculation = "on";
    const { jobId } = await makeJob(["s1"]);
    const slow = await registerTestWorker("detect", "slow");
    const fast = await registerTestWorker("detect", "fast");
    const l = (await pullAndClaim(slow, "detect"))!;
    // A running copy for `fast` with epoch 2, shadowing the lease (as speculateOnce + claim would leave it).
    await query(`update tasks set spec_epoch = 2 where id = $1`, [l.taskId]);
    await query(
      `insert into task_attempts (task_id, epoch, worker_id, shadow_epoch, state, started_at, lease_expires_at)
       values ($1, 2, $2, 1, 'running', now(), now() + interval '10 seconds')`,
      [l.taskId, fast],
    );
    expect(await completeTask(l.taskId, fast, 2, detectResult([]))).toEqual({ status: "ok" });
    expect(await completeTask(l.taskId, fast, 2, detectResult([]))).toEqual({ status: "ok" });
    expect(await completeTask(l.taskId, slow, 1, detectResult([]))).toEqual({ status: "already_done" });
    expect(await events("succeeded", l.taskId)).toHaveLength(1);
    expect(await events("speculation_won", l.taskId)).toHaveLength(1);
    expect(await events("stale_rejected", l.taskId)).toHaveLength(0);
    expect((await tasksOfJob(jobId))[0]).toMatchObject({ state: "SUCCEEDED", worker_id: fast, lease_epoch: 2 });
  });
});

// ---------------------------------------------------------------------------------------------
// Gaps the dashboard's final pass found (docs/decisions/e-dashboard.md, "Coordinator gaps").

describe("dashboard gaps", () => {
  const saved = { dir: config.benchmarksDir, grafana: config.grafanaPublicUrl };
  afterEach(() => {
    config.benchmarksDir = saved.dir;
    config.grafanaPublicUrl = saved.grafana;
  });

  it("GET /benchmarks serves summary.json from the mounted directory, 404 without one", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "wb-bench-"));
    config.benchmarksDir = dir;
    expect((await api("GET", "/benchmarks")).status).toBe(404);
    const summary = { generatedAt: "2026-09-25T00:00:00Z", faults: { runs: 12, faultsInjected: 60, violations: 0 } };
    fs.writeFileSync(path.join(dir, "summary.json"), JSON.stringify(summary));
    expect(await api("GET", "/benchmarks")).toEqual({ status: 200, body: summary });
  });

  it("GET /config carries grafanaUrl from GRAFANA_PUBLIC_URL", async () => {
    config.grafanaPublicUrl = "http://localhost:3300/d/wildebeest-overview";
    expect((await api("GET", "/config")).body.grafanaUrl).toBe("http://localhost:3300/d/wildebeest-overview");
  });

  it("a recovery record still closes when its task was re-claimed before the record opened", () => {
    const detectedAt = new Date(Date.now() - 50);
    telemetry.recordClaimed(["t-old"], "dead-worker", detectedAt.getTime() - 10_000); // the dead worker's own claim
    telemetry.recordClaimed(["t1"], "dead-worker", detectedAt.getTime() - 9_000);
    telemetry.recordClaimed(["t1"], "rescuer", Date.now() - 5); // the push raced the record
    const early = telemetry.recordRecovery({
      workerId: "dead-worker",
      killedAt: null,
      startMs: detectedAt.getTime() - 100,
      detectedAt,
      via: "docker_event",
      requeuedAt: new Date(),
      taskIds: ["t1", "t-old"],
    });
    expect(early.map((c) => c.taskId)).toEqual(["t1"]);
    let [rec] = telemetry.recoveryRecords();
    expect(rec).toMatchObject({ tasks: 2, reclaimedAt: null }); // t-old's claim predates the death
    telemetry.recordClaimed(["t-old"], "rescuer-2");
    [rec] = telemetry.recoveryRecords();
    expect(rec).toMatchObject({ reclaimedBy: "rescuer-2" });
    expect(rec.reclaimedAt).not.toBeNull();
  });

  it("speculation baselines are time-windowed: an earlier job's fast tasks don't put a slower job on probation", () => {
    const now = Date.now();
    // A 5 ms synthetic job two minutes ago, 200 completions over four workers...
    for (let i = 0; i < 200; i++) serviceTimes.record(`w${i % 4}`, "detect", 5, now - 120_000 + i);
    // ...then a 1 s job: every worker is equally "slow", nobody is a straggler.
    for (let i = 0; i < 24; i++) serviceTimes.record(`w${i % 4}`, "detect", 1000 + (i % 3) * 10, now - 20_000 + i * 500);
    expect(probation(now)).toEqual([]);
    expect(serviceTimes.stage("detect", now).p50).toBeGreaterThanOrEqual(1000);
    // A real straggler among them is still caught.
    for (let i = 0; i < 6; i++) serviceTimes.record("slowpoke", "detect", 6000, now - 15_000 + i * 1000);
    expect(probation(now).map((p) => p.workerId)).toEqual(["slowpoke"]);
  });

  it("probation needs enough samples (a replica that just started or took over has few)", () => {
    const now = Date.now();
    for (let i = 0; i < 8; i++) serviceTimes.record(`f${i % 2}`, "classify", 300, now - 5000 + i);
    for (let i = 0; i < 3; i++) serviceTimes.record("c1", "classify", 2000, now - 4000 + i);
    expect(probation(now)).toEqual([]); // 3 worker samples, 11 stage samples: not enough to judge
  });
});

describe("live invariant checks are bounded", () => {
  const saved = config.invariantTimeoutMs;
  afterEach(() => {
    config.invariantTimeoutMs = saved;
  });

  it("a check that runs past INVARIANT_TIMEOUT_MS is cut off and keeps its last value", async () => {
    await makeJob(["i1"]);
    const first = await checkInvariants();
    expect(first).toMatchObject({ ok: true, lostImages: 0, stuckLeases: 0 });

    config.invariantTimeoutMs = 200;
    const blocker = await getPool().connect();
    try {
      await blocker.query("begin; lock table tasks in access exclusive mode");
      const started = Date.now();
      const r = await checkInvariants(); // every query waits on the lock and is cancelled
      expect(Date.now() - started).toBeLessThan(2000);
      expect(r).toMatchObject({ ok: true, lostImages: 0, stuckLeases: 0 });
      expect(await activeInvariantQueries()).toBe(0); // nothing left running
    } finally {
      await blocker.query("rollback");
      blocker.release();
    }
  });
});

async function activeInvariantQueries(): Promise<number> {
  const { rows } = await query(
    `select count(*)::int as n from pg_stat_activity
      where state = 'active' and pid <> pg_backend_pid() and query like '%final_category is null%'`,
  );
  return rows[0].n;
}

describe("leases lost with their response", () => {
  /** A claim that committed but whose answer never reached the worker, `ageMs` ago. */
  async function lostResponse(workerId: string, ageMs: number) {
    const l = (await pullAndClaim(workerId, "detect"))!;
    await query(
      `update tasks set started_at = now() - ($2::int * interval '1 millisecond'),
                        lease_expires_at = now() - ($2::int * interval '1 millisecond') + ($3::int * interval '1 millisecond')
        where id = $1`,
      [l.taskId, ageMs, config.leaseMs],
    );
    return l;
  }

  it("a lease the worker never acknowledged costs no attempt when it expires", async () => {
    await makeJob(["u1", "u2"]);
    const w = await registerTestWorker("detect", "w");
    const unacked = await lostResponse(w, config.leaseMs + 5000);
    // The worker keeps heartbeating (it's fine), but never lists the task: it doesn't know it has it.
    await heartbeat(w, {}, []);
    // Control: a lease the worker did hold (renewed by a heartbeat) and then lost is charged as before.
    const held = (await pullAndClaim(w, "detect"))!;
    await heartbeat(w, {}, [held.taskId]);
    await query(`update tasks set lease_expires_at = now() - interval '1 second' where id = $1`, [held.taskId]);

    await reapOnce();
    const [free] = await events("lease_expired", unacked.taskId);
    expect(free.detail).toMatchObject({ charged: false, unacknowledged: true });
    expect(await task(unacked.taskId)).toMatchObject({ state: "PENDING", attempts: 0, lease_losses: 0, releases: 1 });
    const [charged] = await events("lease_expired", held.taskId);
    expect(charged.detail).toMatchObject({ charged: true });
    expect(charged.detail.unacknowledged).toBeUndefined();
    expect(await task(held.taskId)).toMatchObject({ state: "PENDING", attempts: 1 });
  });

  it("a worker that went silent right after the claim is still charged (it may have run the task)", async () => {
    await makeJob(["s1"]);
    const w = await registerTestWorker("detect", "w");
    const l = await lostResponse(w, config.leaseMs + 5000);
    await query(`update workers set last_heartbeat_at = now() - ($1::int * interval '1 millisecond') where id = $2`, [
      config.leaseMs + 4900,
      w,
    ]); // no heartbeat since the claim (the reaper would declare it dead soon)
    await requeueLostLeases();
    expect(await task(l.taskId)).toMatchObject({ state: "PENDING", attempts: 1 });
  });
});
