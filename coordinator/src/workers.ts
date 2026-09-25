import { config } from "./config.js";
import { query } from "./db.js";
import { hub } from "./events.js";
import { getRedis, keys } from "./redis.js";
import { presign } from "./storage.js";
import { releaseWorkerTasks, requeueWaiting, ValidationError } from "./tasks.js";

export type Stage = "detect" | "classify";
export const STAGES: Stage[] = ["detect", "classify"];

function workerIdFor(stage: Stage, hostname: string) {
  const safe = hostname.replace(/[^A-Za-z0-9_.-]/g, "").slice(0, 64) || "anon";
  return `${stage}-${safe}`;
}

async function setAlive(workerId: string) {
  await getRedis().set(keys.alive(workerId), "1", "PX", 3 * config.heartbeatMs);
}

/**
 * Moves every ID in a worker's processing list (BLMOVEd but never claim-confirmed) back to its
 * ready queue. LRANGE + DEL run in one MULTI so an ID a zombie worker moves in concurrently is
 * either drained now or left for the next pass, never lost. Only IDs whose task is still
 * PENDING-and-queued are pushed back; anything else is already leased, done, or will be
 * re-dispatched from Postgres.
 */
export async function drainProcessingList(workerId: string): Promise<number> {
  const key = keys.processing(workerId);
  const res = await getRedis().multi().lrange(key, 0, -1).del(key).exec();
  const ids = (res?.[0]?.[1] as string[] | undefined) ?? [];
  if (ids.length === 0) return 0;
  return requeueWaiting(ids);
}

export async function registerWorker(body: { stage?: unknown; hostname?: unknown; containerId?: unknown }) {
  const stage = body.stage;
  if (stage !== "detect" && stage !== "classify") throw new ValidationError("stage must be 'detect' or 'classify'");
  const hostname = typeof body.hostname === "string" && body.hostname ? body.hostname : "anon";
  const containerId = typeof body.containerId === "string" && body.containerId ? body.containerId : hostname;
  const id = workerIdFor(stage, hostname);

  // Same ID registering again means the worker process restarted: whatever the previous
  // incarnation held is orphaned, so release it before the new one starts claiming.
  await releaseWorkerTasks(id, "re-registered");
  await drainProcessingList(id);

  await query(
    `insert into workers (id, stage, hostname, container_id, status, registered_at, last_heartbeat_at)
     values ($1, $2, $3, $4, 'ALIVE', now(), now())
     on conflict (id) do update
        set stage = excluded.stage, hostname = excluded.hostname, container_id = excluded.container_id,
            status = 'ALIVE', registered_at = now(), last_heartbeat_at = now(), metrics = '{}'::jsonb`,
    [id, stage, hostname, containerId],
  );
  await setAlive(id);
  hub.workersChanged();
  console.log(`[workers] registered ${id} (container ${containerId})`);
  return {
    workerId: id,
    config: {
      heartbeatMs: config.heartbeatMs,
      leaseMs: config.leaseMs,
      claimBatchSize: config.claimBatchSize,
      animalConfThreshold: config.animalConfThreshold,
    },
  };
}

/**
 * Liveness + lease renewal. Returns false if the worker is not ALIVE (declared dead or stopped):
 * the API turns that into 410 WORKER_DEAD and the worker must drop its work and re-register.
 */
export async function heartbeat(workerId: string, metrics: unknown): Promise<boolean> {
  const m = metrics && typeof metrics === "object" ? metrics : {};
  const { rowCount } = await query(
    `update workers set last_heartbeat_at = now(), metrics = $2 where id = $1 and status = 'ALIVE'`,
    [workerId, JSON.stringify(m)],
  );
  if (!rowCount) return false;
  await query(
    `update tasks set lease_expires_at = now() + ($2::int * interval '1 millisecond')
      where worker_id = $1 and state = 'LEASED'`,
    [workerId, config.leaseMs],
  );
  await setAlive(workerId);
  hub.workersChanged();
  return true;
}

/** Graceful exit: STOPPED (not DEAD), leases released without a reassignment storm. */
export async function deregisterWorker(workerId: string): Promise<boolean> {
  const { rowCount } = await query(`update workers set status = 'STOPPED' where id = $1 and status <> 'STOPPED'`, [
    workerId,
  ]);
  await releaseWorkerTasks(workerId, "deregistered");
  await drainProcessingList(workerId);
  await getRedis().del(keys.alive(workerId));
  hub.workersChanged();
  return Boolean(rowCount);
}

export interface WorkerView {
  id: string;
  stage: Stage;
  status: "ALIVE" | "DEAD" | "STOPPED";
  state: "idle" | "busy" | "dead";
  containerId: string | null;
  hostname: string | null;
  tasksCompleted: number;
  currentTaskIds: string[];
  currentImageUrl: string | null;
  avgLatencyMs: number | null;
  rssMb: number | null;
  lastHeartbeatAt: string;
  registeredAt: string;
  diedAt: string | null;
  killedAt: string | null;
  reassignedCount: number;
}

/** ALIVE workers plus DEAD/STOPPED ones seen in the last 10 minutes. */
export async function listWorkers(): Promise<WorkerView[]> {
  const { rows } = await query(
    `select w.*,
            coalesce(array_agg(t.id order by t.started_at) filter (where t.id is not null), '{}') as task_ids,
            (array_agg(i.object_key order by t.started_at) filter (where t.id is not null))[1] as image_key
       from workers w
       left join tasks t on t.worker_id = w.id and t.state = 'LEASED'
       left join images i on i.id = t.image_id
      where w.status = 'ALIVE' or greatest(w.last_heartbeat_at, w.dead_at, w.killed_at) > now() - interval '10 minutes'
      group by w.id
      order by w.stage, w.registered_at, w.id`,
  );
  return Promise.all(
    rows.map(async (r): Promise<WorkerView> => {
      const m = r.metrics ?? {};
      const taskIds: string[] = r.task_ids ?? [];
      const alive = r.status === "ALIVE";
      return {
        id: r.id,
        stage: r.stage,
        status: r.status,
        state: !alive ? "dead" : taskIds.length > 0 ? "busy" : "idle",
        containerId: r.container_id,
        hostname: r.hostname,
        tasksCompleted: r.tasks_completed,
        currentTaskIds: alive ? taskIds : [],
        currentImageUrl: alive ? await presign(r.image_key ?? m.currentImageKey ?? null) : null,
        avgLatencyMs: typeof m.avgLatencyMs === "number" ? m.avgLatencyMs : null,
        rssMb: typeof m.rssMb === "number" ? m.rssMb : null,
        lastHeartbeatAt: new Date(r.last_heartbeat_at).toISOString(),
        registeredAt: new Date(r.registered_at).toISOString(),
        diedAt: r.status === "DEAD" && r.dead_at ? new Date(r.dead_at).toISOString() : null,
        killedAt: r.killed_at ? new Date(r.killed_at).toISOString() : null,
        reassignedCount: r.reassigned_count,
      };
    }),
  );
}
