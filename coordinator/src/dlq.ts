import { query, tx } from "./db.js";
import { ConflictError } from "./docker.js";
import { pushNow } from "./dispatcher.js";
import { hub, recordEvents } from "./events.js";
import { presign } from "./storage.js";
import { isUuid } from "./tasks.js";

// Dead-letter queue: FAILED tasks are the DLQ. A task lands here when it runs out of attempts
// (task errors + lease losses not caused by us) or fails with a non-retryable error such as an
// undecodable image. Redrive puts it back to PENDING with fresh counters (SQS-style redrive).

export async function listDlq(limit = 100) {
  const n = Math.min(500, Math.max(1, Math.floor(limit) || 100));
  const [{ rows }, total] = await Promise.all([
    query(
      `select t.id, t.stage, t.error, t.attempts, t.task_errors, t.lease_losses, t.releases, t.lease_epoch,
              t.finished_at, i.id as image_id, i.sha256, i.object_key, i.original_name, j.id as job_id, j.name as job_name
         from tasks t join images i on i.id = t.image_id join jobs j on j.id = i.job_id
        where t.state = 'FAILED'
        order by t.finished_at desc nulls last, t.id
        limit $1`,
      [n],
    ),
    query(`select count(*)::int as n from tasks where state = 'FAILED'`),
  ]);
  const tasks = await Promise.all(
    rows.map(async (r) => ({
      taskId: r.id as string,
      jobId: r.job_id as string,
      jobName: r.job_name as string,
      imageId: r.image_id as string,
      originalName: r.original_name as string | null,
      imageUrl: await presign(r.object_key),
      stage: r.stage as string,
      error: r.error as string | null,
      attempts: r.attempts as number,
      taskErrors: r.task_errors as number,
      leaseLosses: r.lease_losses as number,
      releases: r.releases as number,
      leaseEpoch: r.lease_epoch as number,
      failedAt: r.finished_at ? new Date(r.finished_at).toISOString() : null,
    })),
  );
  return { tasks, total: total.rows[0].n as number };
}

/**
 * FAILED → PENDING with the attempt budget reset, and the image and job reopened so the job's
 * progress and completion stay truthful. Returns false if there is no such task.
 */
export async function redrive(taskId: string): Promise<boolean> {
  if (!isUuid(taskId)) return false;
  const result = await tx(async (c) => {
    const { rows: found } = await c.query(
      `select t.state, t.error, j.id as job_id, j.status as job_status from tasks t
         join images i on i.id = t.image_id join jobs j on j.id = i.job_id
        where t.id = $1 for update of t, j`,
      [taskId],
    );
    if (found.length === 0) return null;
    if (found[0].state !== "FAILED") throw new ConflictError("NOT_FAILED", `task is ${found[0].state}, not FAILED`);
    if (found[0].job_status === "cancelled") throw new ConflictError("JOB_CANCELLED", "the task's job was cancelled");

    const { rows } = await c.query(
      `update tasks set state = 'PENDING', attempts = 0, task_errors = 0, lease_losses = 0, not_before = null,
                        finished_at = null, pending_at = now(), queued = false, error = null
        where id = $1 and state = 'FAILED'
        returning image_id, stage`,
      [taskId],
    );
    await c.query(`update images set final_category = null, finalized_at = null where id = $1 and final_category = 'failed'`, [
      rows[0].image_id,
    ]);
    const jobId = found[0].job_id as string;
    await c.query(`update jobs set status = 'running', finished_at = null where id = $1 and status = 'done'`, [jobId]);
    const events = await recordEvents(c, [
      { type: "redriven", taskId, detail: { stage: rows[0].stage, jobId, previousError: found[0].error } },
    ]);
    return { events, jobId };
  });
  if (!result) return false;
  hub.publishEvents(result.events);
  hub.jobChanged(result.jobId);
  await pushNow([taskId]);
  return true;
}
