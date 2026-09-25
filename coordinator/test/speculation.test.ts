// P3 straggler speculation: multi-attempt fencing (first commit wins, the loser gets
// already_done and a cancel, a lost lease still gets STALE_LEASE), promotion, the policy
// (thresholds, idle workers, fastest first, never the holder, at most once) and probation.
// Runs in both claim modes like the rest of the suite.
import http from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createApp } from "../src/api.js";
import { config } from "../src/config.js";
import { query } from "../src/db.js";
import { dispatchOnce } from "../src/dispatcher.js";
import { reapOnce } from "../src/reaper.js";
import { getRedis, keys } from "../src/redis.js";
import { probation, serviceTimes, speculateOnce, speculationSummary } from "../src/speculation.js";
import { resetSystemSnapshot, systemSnapshot } from "../src/system.js";
import {
  claimConfirm,
  claimTasks,
  completeTask,
  completeTasks,
  failTask,
  leaseCopies,
  releaseTask,
  type Lease,
} from "../src/tasks.js";
import { deregisterWorker, heartbeat, heartbeatWithCancel, listWorkers } from "../src/workers.js";
import {
  detectResult,
  events,
  expireLease,
  imagesOfJob,
  job,
  makeJob,
  postgres,
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
  await new Promise<void>((resolve) => server.listen(0, resolve));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
afterAll(async () => {
  await new Promise((r) => server.close(r));
  await teardown();
});
beforeEach(async () => {
  await resetState();
  config.speculateMinSamples = 5;
  config.speculateMultiplier = 3;
  config.speculateMinMs = 1000;
  config.speculateProbationMultiplier = 3;
  config.speculateOfferTtlMs = 3000;
});

async function api(method: string, route: string, body?: unknown) {
  const res = await fetch(base + route, {
    method,
    headers: body ? { "content-type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : undefined };
}

/** Recent completions of the stage: `ms` for each named worker (5 samples each). */
function seedServiceTimes(perWorker: Record<string, number>, stage: "detect" | "classify" = "detect") {
  for (const [w, ms] of Object.entries(perWorker)) for (let i = 0; i < 5; i++) serviceTimes.record(w, stage, ms);
}

async function age(taskId: string, ms: number) {
  await query(`update tasks set started_at = now() - ($2::int * interval '1 millisecond') where id = $1`, [taskId, ms]);
}

/** What the offered worker does: hybrid LMOVEs from spec:{id} and confirms; postgres just claims. */
async function takeCopy(workerId: string): Promise<Lease | null> {
  if (postgres) return (await claimTasks(workerId, "detect", 1, 0))[0] ?? null;
  const id = await getRedis().lmove(keys.spec(workerId), keys.processing(workerId), "LEFT", "RIGHT");
  if (!id) return null;
  return (await claimConfirm(workerId, [id]))[0] ?? null;
}

/**
 * One detect task leased to `slow`, running for 10 s, with a fast idle worker and a stage p50 of
 * 100 ms: the textbook straggler. Returns the task, the original lease and the copy's lease.
 */
async function straggler() {
  const { jobId } = await makeJob(["a"]);
  await dispatchOnce();
  const slow = await registerTestWorker("detect", "slow");
  const original = (await pullAndClaim(slow, "detect"))!;
  const fast = await registerTestWorker("detect", "fast");
  seedServiceTimes({ [fast]: 100 });
  await age(original.taskId, 10_000);
  const offers = await speculateOnce();
  expect(offers).toHaveLength(1);
  const copy = (await takeCopy(fast))!;
  expect(copy).not.toBeNull();
  return { jobId, taskId: original.taskId, slow, fast, original, copy };
}

const count = async (sql: string, params: unknown[] = []) => (await query(sql, params)).rows[0].n as number;

describe("multi-attempt fencing", () => {
  it("leases a copy with its own epoch next to the untouched lease", async () => {
    const { taskId, slow, fast, original, copy } = await straggler();
    expect(original.leaseEpoch).toBe(1);
    expect(copy).toMatchObject({ taskId, leaseEpoch: 2, speculative: true, stage: "detect" });
    expect(await task(taskId)).toMatchObject({ state: "LEASED", worker_id: slow, lease_epoch: 1, spec_epoch: 2 });
    const { rows } = await query(`select * from task_attempts where task_id = $1`, [taskId]);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ epoch: 2, worker_id: fast, shadow_epoch: 1, state: "running" });
    const [ev] = await events("speculated", taskId);
    expect(ev.worker_id).toBe(fast);
    expect(ev.detail).toMatchObject({ originalWorker: slow, speculativeWorker: fast, epoch: 2, originalEpoch: 1 });

    // Both attempts are valid: neither is told to cancel, both leases are renewed.
    await query(`update task_attempts set lease_expires_at = now() where task_id = $1`, [taskId]);
    expect(await heartbeatWithCancel(slow, {}, [taskId])).toEqual({ alive: true, cancel: [] });
    expect(await heartbeatWithCancel(fast, {}, [taskId])).toEqual({ alive: true, cancel: [] });
    const { rows: renewed } = await query(
      `select lease_expires_at > now() + interval '1 second' as fresh from task_attempts where task_id = $1`,
      [taskId],
    );
    expect(renewed[0].fresh).toBe(true);
  });

  it("copy first: one result, the original gets already_done (not stale) and a cancel", async () => {
    const { jobId, taskId, slow, fast } = await straggler();
    expect(await completeTask(taskId, fast, 2, detectResult([]))).toEqual({ status: "ok" });
    expect(await completeTask(taskId, slow, 1, detectResult([{ label: "human", conf: 0.9 }]))).toEqual({
      status: "already_done",
    });
    // Its fail/release are answered the same way.
    expect(await failTask(taskId, slow, 1, "boom")).toEqual({ status: "already_done" });
    expect(await releaseTask(taskId, slow, 1, "s3 down")).toEqual({ status: "already_done" });

    expect(await task(taskId)).toMatchObject({ state: "SUCCEEDED", worker_id: fast, lease_epoch: 2, attempts: 0 });
    expect(await events("succeeded", taskId)).toHaveLength(1);
    expect(await events("stale_rejected", taskId)).toHaveLength(0);
    const [won] = await events("speculation_won", taskId);
    expect(won.worker_id).toBe(fast);
    expect(won.detail).toMatchObject({ winner: fast, originalWorker: slow, promoted: false });
    expect(await count(`select count(*)::int as n from detection_results`)).toBe(1);
    const [img] = await imagesOfJob(jobId);
    expect(img.final_category).toBe("empty"); // the copy's result, not the late "human"
    expect((await job(jobId)).status).toBe("done");

    expect(await heartbeatWithCancel(slow, {}, [taskId])).toEqual({ alive: true, cancel: [taskId] });
    expect(await speculationSummary()).toMatchObject({ launched: 1, won: 1, wasted: 0, running: 0 });
  });

  it("original first: one result, the copy gets already_done and is wasted", async () => {
    const { taskId, slow, fast } = await straggler();
    expect(await completeTask(taskId, slow, 1, detectResult([]))).toEqual({ status: "ok" });
    const batch = await completeTasks(fast, [{ taskId, leaseEpoch: 2, result: detectResult([]) }]);
    expect(batch.results).toEqual([{ taskId, status: "already_done" }]);
    expect(await task(taskId)).toMatchObject({ state: "SUCCEEDED", worker_id: slow, lease_epoch: 1 });
    expect(await events("succeeded", taskId)).toHaveLength(1);
    expect(await events("stale_rejected", taskId)).toHaveLength(0);
    const [wasted] = await events("speculation_wasted", taskId);
    expect(wasted.detail).toMatchObject({ winner: slow, speculativeWorker: fast });
    expect(await heartbeatWithCancel(fast, {}, [taskId])).toEqual({ alive: true, cancel: [taskId] });
    const { rows } = await query(`select state from task_attempts where task_id = $1`, [taskId]);
    expect(rows[0].state).toBe("lost");
    expect(await speculationSummary()).toMatchObject({ launched: 1, won: 0, wasted: 1 });
  });

  it("both attempts completing at once still produce exactly one result", async () => {
    const { taskId, slow, fast } = await straggler();
    const [a, b] = await Promise.all([
      completeTask(taskId, slow, 1, detectResult([])),
      completeTask(taskId, fast, 2, detectResult([])),
    ]);
    expect([a.status, b.status].sort()).toEqual(["already_done", "ok"]);
    expect(await events("succeeded", taskId)).toHaveLength(1);
    const spec = [...(await events("speculation_won", taskId)), ...(await events("speculation_wasted", taskId))];
    expect(spec).toHaveLength(1);
  });

  it("an original paused past its lease: the copy is promoted and the original's report is STALE_LEASE", async () => {
    const { taskId, slow, fast } = await straggler();
    await expireLease(taskId); // the original froze; its lease ran out (the copy keeps heartbeating)
    await reapOnce();

    expect(await task(taskId)).toMatchObject({ state: "LEASED", worker_id: fast, lease_epoch: 2, attempts: 0 });
    const [ev] = await events("lease_expired", taskId);
    expect(ev.detail).toMatchObject({ promoted: true, to: fast, epoch: 2, charged: false });

    // The original wakes up: fenced exactly as before speculation existed.
    expect(await completeTask(taskId, slow, 1, detectResult([]))).toEqual({ status: "stale" });
    expect(await events("stale_rejected", taskId)).toHaveLength(1);
    expect(await heartbeatWithCancel(slow, {}, [taskId])).toEqual({ alive: true, cancel: [taskId] });

    expect(await completeTask(taskId, fast, 2, detectResult([]))).toEqual({ status: "ok" });
    const [won] = await events("speculation_won", taskId);
    expect(won.detail).toMatchObject({ promoted: true, winner: fast });
    // A late report after the win is still stale: that lease was lost before the copy won.
    expect(await completeTask(taskId, slow, 1, detectResult([]))).toEqual({ status: "stale" });
    expect(await events("succeeded", taskId)).toHaveLength(1);
  });

  it("an original whose worker was declared dead: promoted, and the zombie is fenced (410, STALE_LEASE)", async () => {
    const { taskId, slow, fast } = await straggler();
    await silenceWorker(slow);
    await reapOnce();
    expect(await task(taskId)).toMatchObject({ state: "LEASED", worker_id: fast, lease_epoch: 2 });
    expect((await events("reassigned", taskId))[0].detail).toMatchObject({ promoted: true, to: fast });
    expect(await heartbeat(slow, {}, [taskId])).toBe(false);
    expect(await completeTask(taskId, slow, 1, detectResult([]))).toEqual({ status: "stale" });
    expect(await completeTask(taskId, fast, 2, detectResult([]))).toEqual({ status: "ok" });
  });

  it("a copy whose worker died is dropped: its report is STALE_LEASE and the original still wins", async () => {
    const { taskId, slow, fast } = await straggler();
    await silenceWorker(fast);
    await reapOnce();
    const { rows } = await query(`select state, end_reason from task_attempts where task_id = $1`, [taskId]);
    expect(rows[0]).toMatchObject({ state: "dropped", end_reason: "worker not alive" });
    expect(await completeTask(taskId, fast, 2, detectResult([]))).toEqual({ status: "stale" });
    expect(await completeTask(taskId, slow, 1, detectResult([]))).toEqual({ status: "ok" });
    expect((await events("speculation_wasted", taskId))).toHaveLength(1);
  });

  it("a copy whose own lease expired is dropped and fenced", async () => {
    const { taskId, fast } = await straggler();
    await query(`update task_attempts set lease_expires_at = now() - interval '1 second' where task_id = $1`, [taskId]);
    await reapOnce();
    expect(await completeTask(taskId, fast, 2, detectResult([]))).toEqual({ status: "stale" });
  });

  it("a copy that fails or releases ends only itself: nothing charged, the original carries on", async () => {
    const { taskId, slow, fast } = await straggler();
    expect(await failTask(taskId, fast, 2, "ValueError: boom")).toEqual({ status: "ok" });
    expect(await task(taskId)).toMatchObject({ state: "LEASED", worker_id: slow, lease_epoch: 1, attempts: 0 });
    expect((await events("failed", taskId))[0].detail).toMatchObject({ speculative: true });
    expect(await completeTask(taskId, fast, 2, detectResult([]))).toEqual({ status: "stale" });
    expect(await completeTask(taskId, slow, 1, detectResult([]))).toEqual({ status: "ok" });
  });

  it("when the original fails, the task is retried and the copy is cancelled; epochs never repeat", async () => {
    const { taskId, slow, fast } = await straggler();
    expect(await failTask(taskId, slow, 1, "boom")).toEqual({ status: "ok" });
    expect(await task(taskId)).toMatchObject({ state: "PENDING", attempts: 1 });
    expect(await heartbeatWithCancel(fast, {}, [taskId])).toEqual({ alive: true, cancel: [taskId] });
    expect(await completeTask(taskId, fast, 2, detectResult([]))).toEqual({ status: "stale" });

    // The retry's lease skips the copy's epoch, so the dead copy can never match it.
    await query(`update tasks set not_before = null where id = $1`, [taskId]);
    await dispatchOnce();
    const again = (await pullAndClaim(slow, "detect"))!;
    expect(again.leaseEpoch).toBe(3);
    expect(await completeTask(taskId, fast, 2, detectResult([]))).toEqual({ status: "stale" });
    expect(await completeTask(taskId, slow, 3, detectResult([]))).toEqual({ status: "ok" });
  });

  it("a deregistering original hands its lease to the copy", async () => {
    const { taskId, slow, fast } = await straggler();
    await deregisterWorker(slow);
    expect(await task(taskId)).toMatchObject({ state: "LEASED", worker_id: fast, lease_epoch: 2, releases: 0 });
    expect((await events("released", taskId))[0].detail).toMatchObject({ promoted: true, to: fast });
    expect(await completeTask(taskId, fast, 2, detectResult([]))).toEqual({ status: "ok" });
  });

  it("the API answers a lost race with 409 ALREADY_DONE and puts cancels in the heartbeat", async () => {
    const { taskId, slow, fast } = await straggler();
    expect((await api("POST", `/tasks/${taskId}/complete`, { workerId: fast, leaseEpoch: 2, result: detectResult([]) })).status).toBe(200);
    const late = await api("POST", `/tasks/${taskId}/complete`, {
      workerId: slow,
      leaseEpoch: 1,
      result: detectResult([]),
      next: 4,
    });
    expect(late).toEqual({ status: 409, body: { error: "ALREADY_DONE" } });
    const hb = await api("POST", `/workers/${slow}/heartbeat`, { taskIds: [taskId], metrics: {} });
    expect(hb.body).toMatchObject({ ok: true, cancel: [taskId] });
    const batch = await api("POST", "/tasks/complete-batch", {
      workerId: slow,
      items: [{ taskId, leaseEpoch: 1, result: detectResult([]) }],
    });
    expect(batch.body.results).toEqual([{ taskId, status: "already_done" }]);
  });

  it("a rejected single complete with `next` claims nothing (409 carries no leases)", async () => {
    await makeJob(["a", "b"]);
    await dispatchOnce();
    const w = await registerTestWorker("detect", "w1");
    const l = (await pullAndClaim(w, "detect"))!;
    expect(await completeTask(l.taskId, w, 99, detectResult([]), undefined, 2)).toEqual({ status: "stale" });
    expect(await count(`select count(*)::int as n from tasks where state = 'LEASED'`)).toBe(1);
  });
});

describe("policy", () => {
  async function oneLeased(ageMs: number) {
    const { jobId } = await makeJob(["a"]);
    await dispatchOnce();
    const slow = await registerTestWorker("detect", "slow");
    const l = (await pullAndClaim(slow, "detect"))!;
    await age(l.taskId, ageMs);
    return { jobId, slow, taskId: l.taskId };
  }

  it("waits for a baseline, then for the task to pass max(floor, multiplier × stage p50)", async () => {
    const { taskId } = await oneLeased(1500);
    const fast = await registerTestWorker("detect", "fast");
    serviceTimes.record(fast, "detect", 100);
    expect(await speculateOnce()).toEqual([]); // 1 sample < SPECULATE_MIN_SAMPLES

    seedServiceTimes({ [fast]: 400 }); // p50 400 → threshold 1200 ms < 1.5 s
    config.speculateMultiplier = 5; // → 2000 ms > 1.5 s
    expect(await speculateOnce()).toEqual([]);
    config.speculateMultiplier = 3;
    const [offer] = await speculateOnce();
    expect(offer).toMatchObject({ taskId, workerId: fast });
    const { rows } = await query(`select detail from task_attempts where task_id = $1`, [taskId]);
    expect(rows[0].detail).toMatchObject({ thresholdMs: 1200, stageP50Ms: 400, multiplier: 3 });
  });

  it("never speculates a task younger than the floor, however fast the stage", async () => {
    await oneLeased(800);
    const fast = await registerTestWorker("detect", "fast");
    seedServiceTimes({ [fast]: 10 }); // 3 × 10 ms = 30 ms, but the floor is 1 s
    expect(await speculateOnce()).toEqual([]);
  });

  it("only speculates when the stage has nothing left to hand out", async () => {
    const { jobId } = await makeJob(["a", "b"]);
    await dispatchOnce();
    const slow = await registerTestWorker("detect", "slow");
    const l = (await pullAndClaim(slow, "detect"))!;
    await age(l.taskId, 10_000);
    const fast = await registerTestWorker("detect", "fast");
    seedServiceTimes({ [fast]: 100 });
    expect(await speculateOnce()).toEqual([]); // "b" is still PENDING: fast should just take it
    const b = (await pullAndClaim(fast, "detect"))!;
    await completeTask(b.taskId, fast, b.leaseEpoch, detectResult([]));
    expect(await speculateOnce()).toHaveLength(1);
    expect(jobId).toBeTruthy();
  });

  it("needs an idle worker, and is off with SPECULATION=off", async () => {
    await oneLeased(10_000);
    seedServiceTimes({ "detect-other": 100 });
    expect(await speculateOnce()).toEqual([]); // nobody idle but the holder
    await registerTestWorker("detect", "fast");
    config.speculation = "off";
    expect(await speculateOnce()).toEqual([]);
    config.speculation = "on";
    expect(await speculateOnce()).toHaveLength(1);
  });

  it("at most once per task, and never onto the worker holding the lease", async () => {
    const { taskId, slow } = await oneLeased(10_000);
    const fast = await registerTestWorker("detect", "fast");
    seedServiceTimes({ [fast]: 100, [slow]: 100 });
    expect(await speculateOnce()).toHaveLength(1);
    expect(await speculateOnce()).toEqual([]); // offered already
    const copy = (await takeCopy(fast))!;
    await failTask(taskId, fast, copy.leaseEpoch, "boom"); // the copy is gone...
    const other = await registerTestWorker("detect", "other");
    expect(await speculateOnce()).toEqual([]); // ...but a task is only ever speculated once
    expect(other).toBeTruthy();

    // An offer addressed to the holder itself (only possible by hand) is never leased.
    await query(`delete from task_attempts`);
    await query(`update tasks set spec_epoch = 5 where id = $1`, [taskId]);
    await query(
      `insert into task_attempts (task_id, epoch, worker_id, shadow_epoch, state) values ($1, 5, $2, 1, 'offered')`,
      [taskId, slow],
    );
    expect(await leaseCopies(slow, null)).toEqual([]);
  });

  it("prefers the fastest idle worker (the pool is heterogeneous)", async () => {
    const { slow } = await oneLeased(10_000);
    const cpu = await registerTestWorker("detect", "cpu");
    const mps = await registerTestWorker("detect", "mps");
    const fresh = await registerTestWorker("detect", "fresh"); // no samples yet: ranked last
    seedServiceTimes({ [slow]: 900, [cpu]: 700, [mps]: 250 });
    const [offer] = await speculateOnce();
    expect(offer.workerId).toBe(mps);
    expect(fresh).toBeTruthy();
    if (!postgres) expect(await getRedis().lrange(keys.spec(mps), 0, -1)).toEqual([offer.taskId]);
  });

  it("puts a worker whose p50 is > 3x the stage p50 on probation: no copies, flagged in /system and /workers", async () => {
    const { slow } = await oneLeased(10_000);
    const a = await registerTestWorker("detect", "a");
    const b = await registerTestWorker("detect", "b");
    const laggard = await registerTestWorker("detect", "laggard");
    seedServiceTimes({ [a]: 100, [b]: 100, [slow]: 110, [laggard]: 2000 });
    // Only the laggard is idle.
    await makeJob(["x", "y"]);
    await dispatchOnce();
    await pullAndClaim(a, "detect");
    await pullAndClaim(b, "detect");
    expect(await speculateOnce()).toEqual([]);

    expect(probation()).toEqual([{ workerId: laggard, stage: "detect", p50ServiceMs: 2000, stageP50ServiceMs: 100 }]);
    resetSystemSnapshot();
    expect((await systemSnapshot()).speculation.probation.map((p) => p.workerId)).toEqual([laggard]);
    const workers = await listWorkers();
    expect(workers.find((w) => w.id === laggard)).toMatchObject({ probation: true, p50ServiceMs: 2000 });
    expect(workers.find((w) => w.id === a)).toMatchObject({ probation: false });
  });

  it("drops an offer nobody took, and gives that worker a rest before offering again", async () => {
    const { taskId } = await oneLeased(10_000);
    const fast = await registerTestWorker("detect", "fast");
    seedServiceTimes({ [fast]: 100 });
    expect(await speculateOnce()).toHaveLength(1);
    config.speculateOfferTtlMs = 0;
    await new Promise((r) => setTimeout(r, 5));
    await reapOnce();
    const { rows } = await query(`select state, end_reason from task_attempts where task_id = $1`, [taskId]);
    expect(rows[0]).toMatchObject({ state: "dropped", end_reason: "offer not taken" });
    expect(await speculateOnce()).toEqual([]); // fast is cooling down
    const other = await registerTestWorker("detect", "other");
    const [again] = await speculateOnce(); // an untaken offer doesn't use up the task's one copy
    expect(again).toMatchObject({ taskId, workerId: other, epoch: 3 });
  });

  it("shows the running copy on its lease in /system and on the worker in /workers", async () => {
    const { taskId, fast } = await straggler();
    resetSystemSnapshot();
    const s = await systemSnapshot();
    expect(s.leases.find((l) => l.taskId === taskId)?.copy).toMatchObject({ workerId: fast, epoch: 2 });
    expect(s.speculation).toMatchObject({ launched: 1, running: 1 });
    const w = (await listWorkers()).find((x) => x.id === fast)!;
    expect(w).toMatchObject({ state: "busy", speculativeTaskIds: [taskId], currentTaskIds: [] });
  });
});

describe("invariants under speculation", () => {
  it("a job where every task is speculated ends with one result per image and no violations", async () => {
    const seeds = Array.from({ length: 6 }, (_, i) => `s${i}`);
    const { jobId } = await makeJob(seeds);
    await dispatchOnce();
    const slow = await registerTestWorker("detect", "slow");
    const leases: Lease[] = [];
    for (let i = 0; i < seeds.length; i++) leases.push((await pullAndClaim(slow, "detect"))!);
    const fast = await registerTestWorker("detect", "fast");
    seedServiceTimes({ [fast]: 100 });
    for (const l of leases) await age(l.taskId, 10_000);

    // One copy at a time (one idle worker); alternate winners.
    for (let i = 0; i < leases.length; i++) {
      const [offer] = await speculateOnce();
      expect(offer).toBeTruthy();
      const copy = (await takeCopy(fast))!;
      const first = i % 2 === 0 ? [fast, copy] : [slow, leases.find((l) => l.taskId === copy.taskId)!];
      const second = i % 2 === 0 ? [slow, leases.find((l) => l.taskId === copy.taskId)!] : [fast, copy];
      expect(await completeTask(copy.taskId, first[0] as string, (first[1] as Lease).leaseEpoch, detectResult([]))).toEqual({ status: "ok" });
      expect(await completeTask(copy.taskId, second[0] as string, (second[1] as Lease).leaseEpoch, detectResult([]))).toEqual({ status: "already_done" });
    }
    expect((await job(jobId)).status).toBe("done");
    const perTask = await query(
      `select task_id, count(*)::int as n from task_events where type = 'succeeded' group by task_id`,
    );
    expect(perTask.rows).toHaveLength(seeds.length);
    expect(perTask.rows.every((r) => r.n === 1)).toBe(true);
    expect(await speculationSummary()).toMatchObject({ launched: 6, won: 3, wasted: 3 });
    expect((await tasksOfJob(jobId)).every((t) => t.state === "SUCCEEDED")).toBe(true);
  });
});
