import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { config } from "../src/config.js";
import { query } from "../src/db.js";
import { dispatchOnce } from "../src/dispatcher.js";
import { recoveryStats } from "../src/metrics.js";
import { reapOnce } from "../src/reaper.js";
import { getRedis, keys } from "../src/redis.js";
import { claimConfirm, completeTask, failTask } from "../src/tasks.js";
import { deregisterWorker, heartbeat } from "../src/workers.js";
import {
  classifyResult,
  detectResult,
  events,
  expireLease,
  imagesOfJob,
  job,
  makeJob,
  pullAndClaim,
  registerTestWorker,
  resetState,
  silenceWorker,
  task,
  tasksOfJob,
  teardown,
} from "./helpers.js";

beforeEach(resetState);
afterAll(teardown);

/** One image, dispatched to queue:detect; returns its detect task ID. */
async function oneDetectTask(seed = "img") {
  const { jobId } = await makeJob([seed]);
  await dispatchOnce();
  const [t] = await tasksOfJob(jobId);
  return { jobId, taskId: t.id as string };
}

describe("claim", () => {
  it("leases only PENDING tasks and bumps the epoch", async () => {
    const { taskId } = await oneDetectTask();
    const w = await registerTestWorker("detect", "w1");

    const [lease] = await claimConfirm(w, [taskId]);
    expect(lease).toMatchObject({ taskId, leaseEpoch: 1, stage: "detect", countryCode: "TZA", detections: null });
    expect(lease.imageKey).toMatch(/^images\/[0-9a-f]{64}\.jpg$/);

    const t = await task(taskId);
    expect(t).toMatchObject({ state: "LEASED", worker_id: w, lease_epoch: 1, queued: false });
    expect(t.started_at).not.toBeNull();
    expect(await events("claimed", taskId)).toHaveLength(1);

    // Already LEASED: a second claim (a duplicate queue entry) gets nothing.
    expect(await claimConfirm(w, [taskId])).toEqual([]);

    // SUCCEEDED: still nothing.
    await completeTask(taskId, w, 1, detectResult([]));
    expect(await claimConfirm(w, [taskId])).toEqual([]);
    expect((await task(taskId)).lease_epoch).toBe(1);
  });

  it("lets exactly one of two concurrent claims win", async () => {
    const { taskId } = await oneDetectTask();
    const a = await registerTestWorker("detect", "a");
    const b = await registerTestWorker("detect", "b");
    const [la, lb] = await Promise.all([claimConfirm(a, [taskId]), claimConfirm(b, [taskId])]);
    expect(la.length + lb.length).toBe(1);
    const winner = la.length ? a : b;
    expect(await task(taskId)).toMatchObject({ state: "LEASED", worker_id: winner, lease_epoch: 1 });
  });

  it("refuses tasks of the other stage and claims from non-ALIVE workers, putting the ID back", async () => {
    const { taskId } = await oneDetectTask();
    const cls = await registerTestWorker("classify", "c1");
    expect(await claimConfirm(cls, [taskId])).toEqual([]);

    const w = await registerTestWorker("detect", "zombie");
    const redis = getRedis();
    await redis.del(keys.queue("detect"));
    await redis.rpush(keys.processing(w), taskId); // as if BLMOVEd just before being declared dead
    await query(`update workers set status = 'DEAD' where id = $1`, [w]);

    expect(await claimConfirm(w, [taskId])).toEqual([]);
    expect(await redis.lrange(keys.processing(w), 0, -1)).toEqual([]);
    expect(await redis.lrange(keys.queue("detect"), 0, -1)).toEqual([taskId]);
    expect(await task(taskId)).toMatchObject({ state: "PENDING", queued: true });
  });

  it("removes confirmed IDs from the worker's processing list", async () => {
    await oneDetectTask();
    const w = await registerTestWorker("detect", "w1");
    const lease = await pullAndClaim(w, "detect");
    expect(lease).not.toBeNull();
    expect(await getRedis().llen(keys.processing(w))).toBe(0);
  });
});

describe("complete and fencing", () => {
  it("rejects a complete with the wrong epoch as STALE_LEASE and records it", async () => {
    const { taskId } = await oneDetectTask();
    const w = await registerTestWorker("detect", "w1");
    await claimConfirm(w, [taskId]);

    expect(await completeTask(taskId, w, 7, detectResult([]))).toEqual({ status: "stale" });
    const [ev] = await events("stale_rejected", taskId);
    expect(ev.worker_id).toBe(w);
    expect(ev.detail).toMatchObject({ leaseEpoch: 7, currentEpoch: 1, state: "LEASED" });
    expect(await task(taskId)).toMatchObject({ state: "LEASED" });
    const { rows } = await query(`select count(*)::int as n from detection_results`);
    expect(rows[0].n).toBe(0);

    expect(await completeTask(taskId, w, 1, detectResult([]))).toEqual({ status: "ok" });
  });

  it("returns not_found for unknown tasks", async () => {
    expect(await completeTask("00000000-0000-0000-0000-000000000000", "w", 1, detectResult([]))).toEqual({
      status: "not_found",
    });
  });

  it("finalises non-animal images after stage 1 and finishes the job", async () => {
    const { jobId } = await makeJob(["e", "h", "v", "lowconf"]);
    await dispatchOnce();
    const w = await registerTestWorker("detect", "w1");
    const results: Record<string, any> = {
      "e.jpg": detectResult([]),
      "h.jpg": detectResult([{ label: "human", conf: 0.9 }, { label: "vehicle", conf: 0.95 }]),
      "v.jpg": detectResult([{ label: "vehicle", conf: 0.5 }]),
      "lowconf.jpg": detectResult([{ label: "animal", conf: config.animalConfThreshold - 0.01 }]),
    };
    for (let i = 0; i < 4; i++) {
      const lease = (await pullAndClaim(w, "detect"))!;
      const { rows } = await query(`select original_name from images where sha256 = $1`, [lease.sha256]);
      expect(await completeTask(lease.taskId, w, lease.leaseEpoch, results[rows[0].original_name])).toEqual({
        status: "ok",
      });
    }
    const imgs = await imagesOfJob(jobId);
    expect(imgs.map((i) => [i.original_name, i.final_category])).toEqual([
      ["e.jpg", "empty"],
      ["h.jpg", "human"],
      ["lowconf.jpg", "empty"],
      ["v.jpg", "vehicle"],
    ]);
    expect(imgs.every((i) => i.finalized_at)).toBe(true);
    expect(await job(jobId)).toMatchObject({ status: "done" });
    expect(await events("job_done")).toHaveLength(1);
    const { rows } = await query(`select tasks_completed from workers where id = $1`, [w]);
    expect(rows[0].tasks_completed).toBe(4);
  });

  it("runs an animal through both stages; classify gets the stage 1 detections", async () => {
    const { jobId } = await makeJob(["zebra"]);
    await dispatchOnce();
    const det = await registerTestWorker("detect", "d1");
    const cls = await registerTestWorker("classify", "c1");
    const d = (await pullAndClaim(det, "detect"))!;
    await completeTask(d.taskId, det, d.leaseEpoch, detectResult([{ label: "animal", conf: 0.93 }]));

    expect((await imagesOfJob(jobId))[0].final_category).toBeNull();
    expect(await job(jobId)).toMatchObject({ status: "running" });

    await dispatchOnce();
    const c = (await pullAndClaim(cls, "classify"))!;
    expect(c.stage).toBe("classify");
    expect(c.detections).toEqual([{ label: "animal", conf: 0.93, bbox: [0.1, 0.1, 0.5, 0.5] }]);
    expect(await completeTask(c.taskId, cls, c.leaseEpoch, classifyResult("plains zebra", 0.87))).toEqual({
      status: "ok",
    });

    const [img] = await imagesOfJob(jobId);
    expect(img).toMatchObject({ final_category: "animal", species_common_name: "plains zebra" });
    expect(img.species_conf).toBeCloseTo(0.87);
    expect(await job(jobId)).toMatchObject({ status: "done" });
  });

  it("rejects malformed results without consuming the lease", async () => {
    const { taskId } = await oneDetectTask();
    const w = await registerTestWorker("detect", "w1");
    await claimConfirm(w, [taskId]);
    await expect(completeTask(taskId, w, 1, { detections: "nope" })).rejects.toThrow(/detections/);
    expect(await task(taskId)).toMatchObject({ state: "LEASED" });
  });
});

describe("leases, retries and the reaper", () => {
  it("requeues an expired lease with attempts+1 and pushes it straight back to the queue", async () => {
    const { taskId } = await oneDetectTask();
    const w = await registerTestWorker("detect", "w1");
    await pullAndClaim(w, "detect");
    await expireLease(taskId);

    const r = await reapOnce();
    expect(r).toMatchObject({ requeued: 1, pushed: 1 });
    expect(await task(taskId)).toMatchObject({
      state: "PENDING",
      attempts: 1,
      lease_losses: 1,
      releases: 0,
      queued: true,
      worker_id: null,
    });
    const [ev] = await events("lease_expired", taskId);
    expect(ev.worker_id).toBe(w);

    // Pushed right after the requeue committed, not on the next dispatcher tick.
    expect(await getRedis().lrange(keys.queue("detect"), 0, -1)).toEqual([taskId]);
    expect((await dispatchOnce()).retried).toBe(0);
    const again = await pullAndClaim(w, "detect");
    expect(again?.leaseEpoch).toBe(2);
  });

  it("puts recovered work at the head of the queue, ahead of new tasks", async () => {
    const { jobId } = await makeJob(Array.from({ length: config.detectQueueTarget + 3 }, (_, i) => `img${i}`));
    await dispatchOnce();
    const w = await registerTestWorker("detect", "w1");
    const lease = await pullAndClaim(w, "detect");
    await expireLease(lease!.taskId);

    // The queue is still full of new work, but the retry is pushed anyway, to the front, by the
    // reaper itself.
    expect((await reapOnce()).pushed).toBe(1);
    expect((await dispatchOnce()).retried).toBe(0);
    const queue = await getRedis().lrange(keys.queue("detect"), 0, -1);
    expect(queue[0]).toBe(lease!.taskId);
    expect(queue).toHaveLength(config.detectQueueTarget);
    expect((await tasksOfJob(jobId)).filter((t) => t.queued)).toHaveLength(config.detectQueueTarget);
  });

  it("moves a task to FAILED after MAX_ATTEMPTS and finalises the image as failed", async () => {
    const { jobId, taskId } = await oneDetectTask();
    const w = await registerTestWorker("detect", "w1");
    for (let attempt = 1; attempt <= config.maxAttempts; attempt++) {
      await dispatchOnce();
      const lease = await pullAndClaim(w, "detect");
      expect(lease?.leaseEpoch).toBe(attempt);
      await expireLease(taskId);
      await reapOnce();
    }
    expect(await task(taskId)).toMatchObject({ state: "FAILED", attempts: config.maxAttempts });
    expect((await imagesOfJob(jobId))[0].final_category).toBe("failed");
    expect(await job(jobId)).toMatchObject({ status: "done" });
    const failed = await events("failed", taskId);
    expect(failed.at(-1).detail).toMatchObject({ final: true });
  });

  it("counts worker-reported failures as attempts", async () => {
    const { taskId } = await oneDetectTask();
    const w = await registerTestWorker("detect", "w1");
    await pullAndClaim(w, "detect");
    expect(await failTask(taskId, w, 1, "boom")).toEqual({ status: "ok" });
    expect(await task(taskId)).toMatchObject({ state: "PENDING", attempts: 1, error: "boom" });
    // The old epoch is fenced off for fail too.
    expect(await failTask(taskId, w, 1, "again")).toEqual({ status: "stale" });
  });

  it("extends leases on heartbeat", async () => {
    const { taskId } = await oneDetectTask();
    const w = await registerTestWorker("detect", "w1");
    await pullAndClaim(w, "detect");
    await query(`update tasks set lease_expires_at = now() + interval '50 milliseconds' where id = $1`, [taskId]);
    const before = new Date((await task(taskId)).lease_expires_at).getTime();

    expect(await heartbeat(w, { tasksDone: 0, rssMb: 100 }, [taskId])).toBe(true);
    const after = new Date((await task(taskId)).lease_expires_at).getTime();
    expect(after).toBeGreaterThan(before + config.leaseMs / 2);
    expect(await getRedis().pttl(keys.alive(w))).toBeGreaterThan(0);

    await new Promise((r) => setTimeout(r, 100));
    expect((await reapOnce()).requeued).toBe(0);
    expect(await task(taskId)).toMatchObject({ state: "LEASED" });
  });

  it("does not renew a lease the worker no longer reports, so an abandoned task gets reassigned", async () => {
    const { taskId } = await oneDetectTask();
    const w = await registerTestWorker("detect", "w1");
    await pullAndClaim(w, "detect");
    await query(`update tasks set lease_expires_at = now() + interval '50 milliseconds' where id = $1`, [taskId]);

    // The worker is alive and heartbeating, but it has dropped this task.
    expect(await heartbeat(w, {}, [])).toBe(true);
    await new Promise((r) => setTimeout(r, 100));
    expect((await reapOnce()).requeued).toBe(1);
    expect(await task(taskId)).toMatchObject({ state: "PENDING", attempts: 1 });
  });

  it("marks silent workers DEAD, reassigns their tasks, and drains their processing list", async () => {
    await makeJob(["a", "b"]);
    await dispatchOnce();
    const dead = await registerTestWorker("detect", "dies");
    const live = await registerTestWorker("detect", "lives");
    const leased = (await pullAndClaim(dead, "detect"))!;
    // A second ID was BLMOVEd but never confirmed when the worker died.
    const unconfirmed = (await getRedis().lmove(keys.queue("detect"), keys.processing(dead), "LEFT", "RIGHT"))!;
    expect(await getRedis().llen(keys.queue("detect"))).toBe(0);

    await silenceWorker(dead);
    const r = await reapOnce();
    expect(r.dead).toEqual([dead]);
    expect(r.requeued).toBe(1);
    expect(r.drained).toBe(1);

    const { rows } = await query(`select status, dead_at, reassigned_count from workers where id = $1`, [dead]);
    expect(rows[0]).toMatchObject({ status: "DEAD", reassigned_count: 1 });
    expect(await task(leased.taskId)).toMatchObject({ state: "PENDING", attempts: 1 });
    expect((await events("reassigned", leased.taskId))[0].worker_id).toBe(dead);
    expect(await events("worker_died")).toHaveLength(1);
    expect(await getRedis().exists(keys.processing(dead))).toBe(0);
    // Both recovered IDs are back at the head of the queue at once: the leased one first.
    expect(await getRedis().lrange(keys.queue("detect"), 0, -1)).toEqual([leased.taskId, unconfirmed]);

    // A dead worker's heartbeat is refused (→ 410 WORKER_DEAD).
    expect(await heartbeat(dead, {})).toBe(false);

    // The live worker picks both up; recovery is then complete.
    const got = [(await pullAndClaim(live, "detect"))!, (await pullAndClaim(live, "detect"))!];
    expect(got.map((l) => l.taskId).sort()).toEqual([leased.taskId, unconfirmed].sort());
    expect(got.find((l) => l.taskId === leased.taskId)!.leaseEpoch).toBe(2);
    const [rec] = await recoveryStats();
    expect(rec).toMatchObject({ workerId: dead, reassignedTasks: 1, recovered: true });
    expect(rec.fromDeadMs).toBeGreaterThanOrEqual(0);
  });

  it("releases a deregistering worker's leases without costing an attempt", async () => {
    const { taskId } = await oneDetectTask();
    const w = await registerTestWorker("detect", "w1");
    await pullAndClaim(w, "detect");
    await deregisterWorker(w);
    expect(await task(taskId)).toMatchObject({ state: "PENDING", attempts: 0, worker_id: null });
    const { rows } = await query(`select status from workers where id = $1`, [w]);
    expect(rows[0].status).toBe("STOPPED");
    expect(await events("reassigned")).toHaveLength(0);
    // Its late result is fenced off.
    expect(await completeTask(taskId, w, 1, detectResult([]))).toEqual({ status: "stale" });
  });
});
