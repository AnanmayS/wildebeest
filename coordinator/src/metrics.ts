import { query } from "./db.js";
import { isThrottled } from "./dispatcher.js";
import { getRedis, keys } from "./redis.js";

/**
 * Recovery time per dead worker: from its DEAD mark (and from its kill, when killed via the API)
 * until every task reassigned away from it had been claimed again by a live worker. Workers whose
 * reassigned tasks are not all re-claimed yet are reported with recovered=false.
 */
export async function recoveryStats() {
  const { rows } = await query(
    `select w.id, w.stage, w.dead_at, w.killed_at,
            count(*)::int as reassigned,
            count(c.claimed_at)::int as reclaimed,
            max(c.claimed_at) as last_reclaim
       from workers w
       join task_events r on r.worker_id = w.id and r.type = 'reassigned'
       left join lateral (
         select min(e.at) as claimed_at from task_events e
          where e.task_id = r.task_id and e.type = 'claimed' and e.at >= r.at
       ) c on true
      where w.dead_at is not null
      group by w.id
      order by w.dead_at`,
  );
  return rows.map((r) => {
    const recovered = r.reclaimed === r.reassigned && r.last_reclaim !== null;
    const end = recovered ? new Date(r.last_reclaim).getTime() : null;
    return {
      workerId: r.id as string,
      stage: r.stage as string,
      deadAt: new Date(r.dead_at).toISOString(),
      killedAt: r.killed_at ? new Date(r.killed_at).toISOString() : null,
      reassignedTasks: r.reassigned as number,
      recovered,
      fromDeadMs: end !== null ? end - new Date(r.dead_at).getTime() : null,
      fromKillMs: end !== null && r.killed_at ? end - new Date(r.killed_at).getTime() : null,
    };
  });
}

/** p50/p95 of image created → finalised, in ms, for one job. */
async function latencyFor(jobId: string) {
  const { rows } = await query(
    `select percentile_cont(0.5) within group (order by ms) as p50,
            percentile_cont(0.95) within group (order by ms) as p95,
            count(*)::int as n
       from (select extract(epoch from (finalized_at - created_at)) * 1000 as ms
               from images where job_id = $1 and finalized_at is not null) x`,
    [jobId],
  );
  return { p50: Math.round(rows[0].p50 ?? 0), p95: Math.round(rows[0].p95 ?? 0), samples: rows[0].n as number };
}

export async function computeMetrics() {
  const redis = getRedis();
  const [detectQ, classifyQ, workerRows, jobRows, recovery] = await Promise.all([
    redis.llen(keys.queue("detect")),
    redis.llen(keys.queue("classify")),
    query(`select stage, status, count(*)::int as n from workers group by stage, status`),
    query(
      `select j.id, j.name, j.status, j.total_images, j.created_at, j.finished_at,
              count(i.final_category)::int as processed,
              count(*) filter (where i.cache_hit)::int as cache_hits,
              count(*) filter (where i.final_category = 'failed')::int as failed
         from jobs j left join images i on i.job_id = j.id
        group by j.id order by j.created_at desc limit 20`,
    ),
    recoveryStats(),
  ]);

  const workers = { alive: 0, dead: 0, stopped: 0, byStage: {} as Record<string, Record<string, number>> };
  for (const r of workerRows.rows) {
    if (r.status === "ALIVE") workers.alive += r.n;
    if (r.status === "DEAD") workers.dead += r.n;
    if (r.status === "STOPPED") workers.stopped += r.n;
    (workers.byStage[r.stage] ??= {})[r.status] = r.n;
  }

  const jobs = await Promise.all(
    jobRows.rows.map(async (j) => {
      const end = j.finished_at ? new Date(j.finished_at).getTime() : Date.now();
      const elapsedMs = end - new Date(j.created_at).getTime();
      return {
        id: j.id,
        name: j.name,
        status: j.status,
        total: j.total_images,
        processed: j.processed,
        failed: j.failed,
        cacheHits: j.cache_hits,
        elapsedMs,
        imagesPerSec: elapsedMs > 0 ? Math.round((j.processed / elapsedMs) * 1000 * 100) / 100 : 0,
        latency: await latencyFor(j.id),
      };
    }),
  );

  const recovered = recovery.filter((r) => r.recovered);
  return {
    throttled: isThrottled(),
    queues: { detect: detectQ, classify: classifyQ },
    workers,
    recoveryMs: recovered.map((r) => r.fromKillMs ?? r.fromDeadMs!),
    recovery,
    // Latency of the most recent job that actually ran through workers (not a pure cache rerun).
    latency: jobs.find((j) => j.cacheHits < j.total)?.latency ?? { p50: 0, p95: 0, samples: 0 },
    jobs,
  };
}
