// Phase 4 acceptance.
//  1. Resubmitting the same 300 images completes with 100% cache hits in under 2 seconds.
//  2. Backpressure engages with 1 classifier and 6 detectors, visible in logs and metrics.
//     With the default watermarks (500/200) a 300-image job never builds a 500-deep classify
//     queue, so this test restarts the coordinator with low watermarks (20/5).
import { afterAll, describe, expect, it } from "vitest";
import { api, compose, db, sleep, startStack, waitForJobDone } from "./helpers";

const IMAGES = Number(process.env.CACHE_IMAGES ?? 300);
const pool = db();

afterAll(async () => {
  await pool.end();
  // Put the coordinator back on its default watermarks.
  compose("up -d --no-deps --force-recreate coordinator");
});

describe("content-hash cache", () => {
  it("reruns an already-processed batch with 100% cache hits in under 2s", async () => {
    await startStack(4, 2);
    // Make sure every image has results (a no-op if an earlier test already processed them).
    const first = await api("POST", "/jobs/sample", { size: IMAGES, countryCode: "TZA" });
    await waitForJobDone(first.body.jobId);

    const started = Date.now();
    const rerun = await api("POST", "/jobs/sample", { size: IMAGES, countryCode: "TZA" });
    const job = await waitForJobDone(rerun.body.jobId, 10_000);
    const elapsed = Date.now() - started;
    console.log(`cache rerun: ${job.cacheHits}/${job.total} hits in ${elapsed} ms`);

    expect(job.total).toBe(IMAGES);
    expect(job.cacheHits).toBe(IMAGES);
    expect(elapsed).toBeLessThan(2000);
  });
});

describe("backpressure", () => {
  it("throttles stage 1 when the classify queue passes the high-water mark", async () => {
    const env = { CLASSIFY_QUEUE_HIGH_WATER: "20", CLASSIFY_QUEUE_LOW_WATER: "5" };
    compose("up -d --no-deps --force-recreate coordinator", env);
    await startStack(6, 1, env);
    await api("POST", "/admin/clear-cache");

    const res = await api("POST", "/jobs/sample", { size: IMAGES, countryCode: "TZA" });
    let sawThrottle = false;
    let maxClassifyQueue = 0;
    const deadline = Date.now() + 15 * 60_000;
    while (Date.now() < deadline) {
      const metrics = (await api("GET", "/metrics")).body;
      sawThrottle ||= Boolean(metrics.throttled);
      maxClassifyQueue = Math.max(maxClassifyQueue, metrics.queues?.classify ?? 0);
      if ((await api("GET", `/jobs/${res.body.jobId}`)).body.status === "done") break;
      await sleep(250);
    }
    console.log(`backpressure: throttled=${sawThrottle}, max classify queue=${maxClassifyQueue}`);
    expect(sawThrottle).toBe(true);

    const { rows } = await pool.query(`select count(*)::int as n from task_events where type = 'throttled'`);
    // Throttle changes are also logged as events (the dashboard's event log reads them).
    expect(rows[0].n).toBeGreaterThan(0);
  });
});
