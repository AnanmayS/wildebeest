// Phase 3 acceptance: the system survives workers being SIGKILLed mid-job, and a stale
// worker that comes back after losing its lease cannot overwrite the real result.
//
//   4 detectors + 2 classifiers, 300 images, 2 random workers killed mid-job.
//   Expect: job completes, every image has exactly one final result, no task left LEASED.
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { aliveWorkers, api, db, getJob, REDIS_URL, sleep, startStack, waitFor, waitForJobDone } from "./helpers";

const IMAGES = Number(process.env.CHAOS_IMAGES ?? 300);
const pool = db();

beforeAll(async () => {
  await startStack(4, 2);
  // Start from a cold cache so every image really goes through the workers.
  const cleared = await api("POST", "/admin/clear-cache");
  expect(cleared.status).toBe(200);
});

afterAll(async () => {
  await pool.end();
});

describe("chaos: SIGKILL workers mid-job", () => {
  let jobId: string;
  let staleTaskId: string;
  let staleEpoch: number;
  let staleWorkerId: string;
  const killed: string[] = [];

  it("submits a job and plays a stale worker that stops heartbeating", async () => {
    const res = await api("POST", "/jobs/sample", { size: IMAGES, countryCode: "TZA" });
    expect(res.status).toBe(200);
    jobId = res.body.jobId;

    // A fake worker grabs one detect task the same way a real worker does, then goes silent
    // (like a process frozen by a GC pause or a network partition).
    const reg = await api("POST", "/workers/register", { stage: "detect", hostname: "stale-test", containerId: "stale-test" });
    staleWorkerId = reg.body.workerId;
    const redis = new Redis(REDIS_URL);
    await waitFor(async () => {
      const id = await redis.blmove("queue:detect", `processing:${staleWorkerId}`, "LEFT", "RIGHT", 1);
      if (!id) return false;
      const claim = await api("POST", "/tasks/claim-confirm", { workerId: staleWorkerId, taskIds: [id] });
      const lease = claim.body.leases?.[0];
      if (!lease) return false;
      staleTaskId = lease.taskId;
      staleEpoch = lease.leaseEpoch;
      return true;
    }, 60_000, "stale worker to lease a task", 0);
    await redis.quit();
  });

  it("kills 2 busy workers once the job is underway", async () => {
    await waitFor(async () => (await getJob(jobId)).processed >= IMAGES * 0.2, 10 * 60_000, "20% progress");
    const workers = (await aliveWorkers()).filter((w) => w.containerId !== "stale-test");
    const busyFirst = [...workers].sort((a, b) => b.currentTaskIds.length - a.currentTaskIds.length);
    // Kill one detector and one random other worker, so both stages see a failure.
    const detector = busyFirst.find((w) => w.stage === "detect")!;
    const others = busyFirst.filter((w) => w.id !== detector.id);
    const victims = [detector, others[Math.floor(Math.random() * others.length)]];
    for (const w of victims) {
      const res = await api("POST", `/workers/${w.id}/kill`);
      expect(res.status).toBe(200);
      killed.push(w.id);
    }
  });

  it("finishes the job with exactly one final result per image and nothing left leased", async () => {
    const job = await waitForJobDone(jobId);
    expect(job.processed).toBe(IMAGES);
    expect(job.categories.failed ?? 0).toBe(0);

    const { rows: unfinished } = await pool.query(
      `select count(*)::int as n from images where job_id = $1 and final_category is null`, [jobId]);
    expect(unfinished[0].n).toBe(0);

    const { rows: open } = await pool.query(
      `select count(*)::int as n from tasks t join images i on i.id = t.image_id
        where i.job_id = $1 and t.state in ('LEASED', 'PENDING')`, [jobId]);
    expect(open[0].n).toBe(0);

    // Each image has exactly one detection result, and each animal exactly one classification.
    const { rows: detections } = await pool.query(
      `select i.id, count(d.sha256)::int as n from images i
         left join detection_results d on d.sha256 = i.sha256
        where i.job_id = $1 group by i.id having count(d.sha256) <> 1`, [jobId]);
    expect(detections).toEqual([]);
    const { rows: classifications } = await pool.query(
      `select i.id, count(c.sha256)::int as n from images i
         left join classification_results c on c.sha256 = i.sha256
        where i.job_id = $1 and i.final_category = 'animal'
        group by i.id having count(c.sha256) <> 1`, [jobId]);
    expect(classifications).toEqual([]);

    // Killed workers were detected as dead and their work was reassigned.
    const { rows: dead } = await pool.query(`select id, status from workers where id = any($1)`, [killed]);
    expect(dead.map((w) => w.status)).toEqual(["DEAD", "DEAD"]);
    const { rows: reassigned } = await pool.query(
      `select count(*)::int as n from task_events e join tasks t on t.id = e.task_id join images i on i.id = t.image_id
        where i.job_id = $1 and e.type in ('reassigned', 'lease_expired')`, [jobId]);
    expect(reassigned[0].n).toBeGreaterThan(0);
  });

  it("rejects the stale worker's late result with 409 STALE_LEASE and keeps the real one", async () => {
    const { rows: [task] } = await pool.query(
      `select t.state, t.lease_epoch, i.sha256, i.final_category from tasks t join images i on i.id = t.image_id where t.id = $1`,
      [staleTaskId]);
    expect(task.state).toBe("SUCCEEDED");
    expect(task.lease_epoch).toBeGreaterThan(staleEpoch);
    const { rows: [before] } = await pool.query(`select detections from detection_results where sha256 = $1`, [task.sha256]);

    const bogus = { modelVersion: "stale", latencyMs: 1, detections: [{ label: "vehicle", conf: 0.99, bbox: [0, 0, 1, 1] }] };
    const late = await api("POST", `/tasks/${staleTaskId}/complete`, { workerId: staleWorkerId, leaseEpoch: staleEpoch, result: bogus });
    expect(late.status).toBe(409);
    expect(late.body.error).toBe("STALE_LEASE");

    await sleep(200);
    const { rows: [after] } = await pool.query(
      `select d.detections, i.final_category from detection_results d join images i on i.sha256 = d.sha256
        where d.sha256 = $1 and i.job_id = $2`, [task.sha256, jobId]);
    expect(after.detections).toEqual(before.detections);
    expect(after.final_category).toBe(task.final_category);

    const { rows: events } = await pool.query(
      `select count(*)::int as n from task_events where task_id = $1 and type = 'stale_rejected'`, [staleTaskId]);
    expect(events[0].n).toBe(1);
  });
});
