// P1 error classification and retry hygiene: /release, the attempt budget, non-retryable
// failures, full-jitter backoff, and the DLQ with redrive (over HTTP where the worker/dashboard
// would use it).
import http from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createApp } from "../src/api.js";
import { config } from "../src/config.js";
import { query } from "../src/db.js";
import { dispatchOnce } from "../src/dispatcher.js";
import { setDockerOps, killWorker } from "../src/docker.js";
import { reapOnce } from "../src/reaper.js";
import { getRedis, keys } from "../src/redis.js";
import { claimTasks, completeTask, failTask, releaseTask, setJitterSource } from "../src/tasks.js";
import {
  detectResult,
  events,
  expireLease,
  imagesOfJob,
  job,
  makeJob,
  pullAndClaim,
  postgres,
  hybrid,
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

async function leasedTask(seed = "img", hostname = "w1") {
  const { jobId } = await makeJob([seed]);
  await dispatchOnce();
  const workerId = await registerTestWorker("detect", hostname, { containerId: hostname.padEnd(12, "0") });
  const lease = (await pullAndClaim(workerId, "detect"))!;
  return { jobId, workerId, taskId: lease.taskId, epoch: lease.leaseEpoch };
}

/** Lets a task in backoff be dispatched now, as if the delay had passed. */
const endBackoff = (taskId: string) => query(`update tasks set not_before = now() - interval '1 ms' where id = $1`, [taskId]);

describe("release (infrastructure errors)", () => {
  it("returns the task without spending an attempt, fenced on the epoch", async () => {
    const { workerId, taskId, epoch } = await leasedTask();
    const res = await api("POST", `/tasks/${taskId}/release`, { workerId, leaseEpoch: epoch, reason: "S3 unavailable" });
    expect(res).toEqual({ status: 200, body: { ok: true } });
    expect(await task(taskId)).toMatchObject({ state: "PENDING", attempts: 0, releases: 1, worker_id: null });
    expect((await events("released", taskId))[0].detail).toMatchObject({ reason: "S3 unavailable" });
    expect(await getRedis().llen(keys.processing(workerId))).toBe(0);

    // Same epoch again: the lease is gone.
    expect((await api("POST", `/tasks/${taskId}/release`, { workerId, leaseEpoch: epoch })).status).toBe(409);

    // Released work is dispatched again at once (no backoff), by the release itself: it is already
    // at the head of the queue before any dispatcher tick (P2: push after commit).
    if (hybrid) expect(await getRedis().lrange(keys.queue("detect"), 0, -1)).toEqual([taskId]);
    expect((await dispatchOnce()).retried).toBe(0);
    const again = (await pullAndClaim(workerId, "detect"))!;
    expect(again.leaseEpoch).toBe(epoch + 1);
  });

  it("any number of releases never fails a task", async () => {
    const { workerId, taskId } = await leasedTask();
    for (let i = 1; i <= config.maxAttempts + 2; i++) {
      const t = await task(taskId);
      expect(await releaseTask(taskId, workerId, t.lease_epoch, "minio down")).toEqual({ status: "ok" });
      await dispatchOnce();
      await pullAndClaim(workerId, "detect");
    }
    expect(await task(taskId)).toMatchObject({ state: "LEASED", attempts: 0, releases: config.maxAttempts + 2 });
  });
});

describe("attempt budget", () => {
  it("counts task errors and lease losses together, but not releases or our own kills", async () => {
    setJitterSource(() => 0); // no backoff, to keep the test short
    const killed: string[] = [];
    setDockerOps({ kill: async (id) => void killed.push(id) });
    const { jobId, workerId, taskId, epoch } = await leasedTask();

    // 1. task error → attempts 1
    await failTask(taskId, workerId, epoch, "model blew up");
    // 2. release → free
    await dispatchOnce();
    let l = (await pullAndClaim(workerId, "detect"))!;
    await releaseTask(taskId, workerId, l.leaseEpoch, "timeout");
    // 3. our own chaos kill → free
    await dispatchOnce();
    l = (await pullAndClaim(workerId, "detect"))!;
    await killWorker(workerId, "chaos");
    await silenceWorker(workerId);
    await reapOnce();
    expect(await task(taskId)).toMatchObject({ state: "PENDING", attempts: 1, task_errors: 1, lease_losses: 0, releases: 2 });

    // 4. a genuine lease loss → attempts 2
    const w2 = await registerTestWorker("detect", "w2", { containerId: "w2".padEnd(12, "0") });
    l = (await pullAndClaim(w2, "detect"))!;
    await expireLease(taskId);
    await reapOnce();
    // 5. another task error → attempts 3 = MAX_ATTEMPTS → FAILED
    l = (await pullAndClaim(w2, "detect"))!;
    await failTask(taskId, w2, l.leaseEpoch, "model blew up again");

    expect(await task(taskId)).toMatchObject({
      state: "FAILED",
      attempts: 3,
      task_errors: 2,
      lease_losses: 1,
      releases: 2,
    });
    expect((await imagesOfJob(jobId))[0].final_category).toBe("failed");
    expect(await job(jobId)).toMatchObject({ status: "done" });
  });

  it("a non-retryable error fails the task at once", async () => {
    const { jobId, workerId, taskId, epoch } = await leasedTask();
    const res = await api("POST", `/tasks/${taskId}/fail`, {
      workerId,
      leaseEpoch: epoch,
      error: "UnidentifiedImageError: cannot identify image file",
      nonRetryable: true,
    });
    expect(res.status).toBe(200);
    expect(await task(taskId)).toMatchObject({ state: "FAILED", attempts: 1, task_errors: 1 });
    expect((await events("failed", taskId))[0].detail).toMatchObject({ final: true, nonRetryable: true });
    expect(await job(jobId)).toMatchObject({ status: "done" });
  });
});

describe("full-jitter backoff", () => {
  it("holds a failed task back for random(0, min(cap, base·2^n)) before re-dispatch", async () => {
    setJitterSource(() => 1); // the top of the jitter range
    const { workerId, taskId, epoch } = await leasedTask();

    await failTask(taskId, workerId, epoch, "flaky");
    const t1 = await task(taskId);
    const delay1 = new Date(t1.not_before).getTime() - Date.now();
    expect(delay1).toBeGreaterThan(config.retryBaseMs - 200);
    expect(delay1).toBeLessThanOrEqual(config.retryBaseMs + 50);
    expect((await events("failed", taskId))[0].detail.retryInMs).toBeGreaterThan(0);

    // Still backing off: the dispatcher leaves it alone, and it can't be claimed.
    expect((await dispatchOnce()).retried).toBe(0);
    expect(await getRedis().llen(keys.queue("detect"))).toBe(0);
    if (postgres) expect(await claimTasks(workerId, "detect", 1, 0)).toEqual([]);

    await endBackoff(taskId);
    expect((await dispatchOnce()).retried).toBe(hybrid ? 1 : 0);
    const l2 = (await pullAndClaim(workerId, "detect"))!;
    await failTask(taskId, workerId, l2.leaseEpoch, "flaky");
    const delay2 = new Date((await task(taskId)).not_before).getTime() - Date.now();
    expect(delay2).toBeGreaterThan(2 * config.retryBaseMs - 200); // doubled
  });

  it("a zero jitter draw retries immediately", async () => {
    setJitterSource(() => 0);
    const { workerId, taskId, epoch } = await leasedTask();
    await failTask(taskId, workerId, epoch, "flaky");
    // Pushed by the fail path itself (no backoff to wait for), not by the next tick.
    if (hybrid) expect(await getRedis().lrange(keys.queue("detect"), 0, -1)).toEqual([taskId]);
    expect((await dispatchOnce()).retried).toBe(0);
    expect((await pullAndClaim(workerId, "detect"))?.taskId).toBe(taskId);
  });
});

describe("dead-letter queue", () => {
  it("lists FAILED tasks and redrives one back through the pipeline", async () => {
    const { jobId, workerId, taskId, epoch } = await leasedTask("corrupt");
    await failTask(taskId, workerId, epoch, "UnidentifiedImageError", true);

    const dlq = (await api("GET", "/dlq")).body;
    expect(dlq.total).toBe(1);
    expect(dlq.tasks[0]).toMatchObject({
      taskId,
      jobId,
      stage: "detect",
      error: "UnidentifiedImageError",
      attempts: 1,
      taskErrors: 1,
      leaseLosses: 0,
      releases: 0,
      originalName: "corrupt.jpg",
    });
    expect(dlq.tasks[0].imageUrl).toMatch(/X-Amz-Signature=/);
    expect(dlq.tasks[0].failedAt).not.toBeNull();
    expect(await job(jobId)).toMatchObject({ status: "done" });

    expect(await api("POST", `/dlq/${taskId}/redrive`)).toEqual({ status: 200, body: { ok: true } });
    expect(await task(taskId)).toMatchObject({ state: "PENDING", attempts: 0, task_errors: 0, error: null });
    expect((await imagesOfJob(jobId))[0]).toMatchObject({ final_category: null, finalized_at: null });
    expect(await job(jobId)).toMatchObject({ status: "running", finished_at: null });
    expect((await api("GET", "/dlq")).body.total).toBe(0);
    expect((await events("redriven", taskId))[0].detail).toMatchObject({ previousError: "UnidentifiedImageError" });

    // It runs again and the job finishes normally.
    await dispatchOnce();
    const l = (await pullAndClaim(workerId, "detect"))!;
    expect(await completeTask(taskId, workerId, l.leaseEpoch, detectResult([]))).toEqual({ status: "ok" });
    expect(await job(jobId)).toMatchObject({ status: "done" });
    expect((await imagesOfJob(jobId))[0].final_category).toBe("empty");
  });

  it("refuses to redrive a task that isn't FAILED, and 404s unknown ones", async () => {
    const { jobId } = await makeJob(["pending"]);
    const [t] = await tasksOfJob(jobId);
    expect(await api("POST", `/dlq/${t.id}/redrive`)).toMatchObject({ status: 409, body: { error: "NOT_FAILED" } });
    expect((await api("POST", `/dlq/00000000-0000-0000-0000-000000000000/redrive`)).status).toBe(404);
    expect((await api("POST", `/dlq/nope/redrive`)).status).toBe(404);
  });
});
