import { closePool, migrate, query } from "../src/db.js";
import { resetDispatcherState } from "../src/dispatcher.js";
import { createJob, sha256Of, type ImageInput } from "../src/jobs.js";
import { closeRedis, getRedis, keys } from "../src/redis.js";
import { ensureBucket } from "../src/storage.js";
import { claimConfirm, type Lease } from "../src/tasks.js";
import { registerWorker } from "../src/workers.js";

let ready = false;

/** Migrates the test DB (first call only), then wipes all rows and the test Redis DB. */
export async function resetState() {
  if (!ready) {
    await migrate();
    await ensureBucket(3);
    ready = true;
  }
  await query(
    `truncate jobs, images, tasks, detection_results, classification_results, workers, task_events restart identity cascade`,
  );
  await getRedis().flushdb();
  resetDispatcherState();
}

export async function teardown() {
  await closePool();
  await closeRedis();
}

/** A fake "photo": the coordinator never decodes images, so any bytes will do. */
export function fakeImage(seed: string): ImageInput {
  const buf = Buffer.from(`fake-jpeg-bytes:${seed}`);
  return { originalName: `${seed}.jpg`, sha256: sha256Of(buf), read: async () => buf };
}

export async function makeJob(seeds: string[], name = "test") {
  return createJob({ name, images: seeds.map(fakeImage) });
}

export async function registerTestWorker(stage: "detect" | "classify", hostname: string) {
  const { workerId } = await registerWorker({ stage, hostname, containerId: hostname });
  return workerId;
}

/** What a real worker does: move one ID from the ready queue to its processing list, then confirm. */
export async function pullAndClaim(workerId: string, stage: "detect" | "classify"): Promise<Lease | null> {
  const id = await getRedis().lmove(keys.queue(stage), keys.processing(workerId), "LEFT", "RIGHT");
  if (!id) return null;
  const leases = await claimConfirm(workerId, [id]);
  return leases[0] ?? null;
}

/** Pretends the worker has been silent for `ms` (instead of sleeping in tests). */
export async function silenceWorker(workerId: string, ms = 60_000) {
  await query(`update workers set last_heartbeat_at = now() - ($2::int * interval '1 millisecond') where id = $1`, [
    workerId,
    ms,
  ]);
}

export async function expireLease(taskId: string) {
  await query(`update tasks set lease_expires_at = now() - interval '1 second' where id = $1`, [taskId]);
}

export async function task(id: string) {
  const { rows } = await query(`select * from tasks where id = $1`, [id]);
  return rows[0];
}

export async function tasksOfJob(jobId: string) {
  const { rows } = await query(
    `select t.* from tasks t join images i on i.id = t.image_id where i.job_id = $1 order by t.enqueued_at`,
    [jobId],
  );
  return rows;
}

export async function imagesOfJob(jobId: string) {
  const { rows } = await query(`select * from images where job_id = $1 order by original_name`, [jobId]);
  return rows;
}

export async function job(jobId: string) {
  const { rows } = await query(`select * from jobs where id = $1`, [jobId]);
  return rows[0];
}

export async function events(type: string, taskId?: string) {
  const { rows } = taskId
    ? await query(`select * from task_events where type = $1 and task_id = $2 order by id`, [type, taskId])
    : await query(`select * from task_events where type = $1 order by id`, [type]);
  return rows;
}

export const detectResult = (detections: Array<{ label: string; conf: number }>) => ({
  modelVersion: "test-detector-v1",
  latencyMs: 10,
  detections: detections.map((d) => ({ ...d, bbox: [0.1, 0.1, 0.5, 0.5] })),
});

export const classifyResult = (commonName: string, confidence = 0.9) => ({
  modelVersion: "test-classifier-v1",
  latencyMs: 10,
  label: `uuid;mammalia;perissodactyla;equidae;equus;quagga;${commonName}`,
  commonName,
  confidence,
  cropKey: `crops/x_test-classifier-v1.jpg`,
  raw: { top: commonName },
});
