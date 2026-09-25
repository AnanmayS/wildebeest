import { config } from "./config.js";
import { getPool, query } from "./db.js";
import { hub, recordEvents } from "./events.js";
import { getRedis, keys } from "./redis.js";
import { refreshStats } from "./dispatcher.js";
import { presign } from "./storage.js";
import { isUuid, releaseWorkerTasks, requeueWaiting, ValidationError } from "./tasks.js";

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
export async function drainProcessingList(workerId: string): Promise<string[]> {
  const key = keys.processing(workerId);
  const res = await getRedis().multi().lrange(key, 0, -1).del(key).exec();
  const ids = (res?.[0]?.[1] as string[] | undefined) ?? [];
  if (ids.length === 0) return [];
  return requeueWaiting(ids);
}

const RUNTIMES = ["container", "native"] as const;
const DEVICES = ["cpu", "mps", "cuda"] as const;

function oneOf<T extends string>(value: unknown, allowed: readonly T[], what: string): T {
  if (value === undefined || value === null || value === "") return allowed[0];
  if (typeof value === "string" && (allowed as readonly string[]).includes(value)) return value as T;
  throw new ValidationError(`${what} must be one of ${allowed.join(", ")}`);
}

export async function registerWorker(body: {
  stage?: unknown;
  hostname?: unknown;
  containerId?: unknown;
  runtime?: unknown;
  device?: unknown;
}) {
  const stage = body.stage;
  if (stage !== "detect" && stage !== "classify") throw new ValidationError("stage must be 'detect' or 'classify'");
  const hostname = typeof body.hostname === "string" && body.hostname ? body.hostname : "anon";
  const containerId = typeof body.containerId === "string" && body.containerId ? body.containerId : hostname;
  const runtime = oneOf(body.runtime, RUNTIMES, "runtime");
  const device = oneOf(body.device, DEVICES, "device");
  const id = workerIdFor(stage, hostname);

  // Same ID registering again means the worker process restarted: whatever the previous
  // incarnation held is orphaned, so release it before the new one starts claiming.
  await releaseWorkerTasks(id, "re-registered");
  await drainProcessingList(id);

  await query(
    `insert into workers (id, stage, hostname, container_id, runtime, device, status, registered_at, last_heartbeat_at)
     values ($1, $2, $3, $4, $5, $6, 'ALIVE', now(), now())
     on conflict (id) do update
        set stage = excluded.stage, hostname = excluded.hostname, container_id = excluded.container_id,
            runtime = excluded.runtime, device = excluded.device,
            status = 'ALIVE', registered_at = now(), last_heartbeat_at = now(), metrics = '{}'::jsonb`,
    [id, stage, hostname, containerId, runtime, device],
  );
  await setAlive(id);
  hub.workersChanged();
  void refreshStats().catch(() => {}); // the detect queue target scales with live workers
  console.log(`[workers] registered ${id} (${runtime}/${device}, container ${containerId})`);
  return {
    workerId: id,
    config: {
      heartbeatMs: config.heartbeatMs,
      leaseMs: config.leaseMs,
      // The worker sizes its claim batch between these two (≈ round trip ÷ service time).
      claimBatchSize: config.claimBatchSize,
      maxClaimBatch: Math.max(config.claimBatchSize, config.maxClaimBatch),
      // hybrid: BLMOVE + claim-confirm; postgres: long-poll POST /tasks/claim.
      claimMode: config.claimMode,
      animalConfThreshold: config.animalConfThreshold,
    },
  };
}

/**
 * Liveness + lease renewal. Returns false if the worker is not ALIVE (declared dead or stopped):
 * the API turns that into 410 WORKER_DEAD and the worker must drop its work and re-register.
 * Only the leases the worker says it still holds are renewed: a task it
 * gave up on (e.g. its /complete retries ran out during a coordinator outage) is not in `taskIds`,
 * so its lease runs out and the reaper hands it to someone else instead of it staying LEASED forever.
 */
export async function heartbeat(workerId: string, metrics: unknown, taskIds: unknown = []): Promise<boolean> {
  const m = metrics && typeof metrics === "object" ? metrics : {};
  const held = Array.isArray(taskIds) ? taskIds.filter(isUuid) : [];
  // One statement. An idle worker (nothing held) never touches tasks. Renewal skips rows a
  // completion is holding right now (SKIP LOCKED): those leases are ending anyway, and waiting on
  // them could deadlock against a batch that locks the same rows in a different order.
  const { rows } = await query<{ alive: boolean }>(
    held.length === 0
      ? `update workers set last_heartbeat_at = now(), metrics = $2 where id = $1 and status = 'ALIVE'
         returning true as alive`
      : `with w as (
           update workers set last_heartbeat_at = now(), metrics = $2 where id = $1 and status = 'ALIVE'
           returning id
         ),
         renewed as (
           update tasks set lease_expires_at = now() + ($3::int * interval '1 millisecond')
            where id in (select id from tasks
                          where id = any($4::uuid[]) and worker_id = $1 and state = 'LEASED'
                            and exists (select 1 from w)
                          for update skip locked)
         )
         select true as alive from w`,
    held.length === 0 ? [workerId, JSON.stringify(m)] : [workerId, JSON.stringify(m), config.leaseMs, held],
  );
  if (rows.length === 0) {
    await recordRefusedHeartbeat(workerId, held);
    return false;
  }
  await setAlive(workerId);
  hub.workersChanged();
  return true;
}

/**
 * A worker we declared DEAD is still alive (it was paused, partitioned, or just slow). Record it
 * once per death, so the dashboard can show the zombie waking up; its late results are then fenced.
 */
async function recordRefusedHeartbeat(workerId: string, heldTaskIds: string[]) {
  const { rows } = await query(
    `select dead_at, (extract(epoch from (now() - dead_at)) * 1000)::int as dead_for_ms from workers
      where id = $1 and status = 'DEAD'
        and not exists (select 1 from task_events e where e.worker_id = $1 and e.type = 'heartbeat_refused'
                                                      and e.at >= workers.dead_at)`,
    [workerId],
  );
  if (rows.length === 0) return;
  const events = await recordEvents(getPool(), [
    { type: "heartbeat_refused", workerId, detail: { deadForMs: rows[0].dead_for_ms, heldTaskIds } },
  ]);
  hub.publishEvents(events);
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
  void refreshStats().catch(() => {});
  return Boolean(rowCount);
}

export interface WorkerView {
  id: string;
  stage: Stage;
  status: "ALIVE" | "DEAD" | "STOPPED";
  state: "idle" | "busy" | "dead";
  containerId: string | null;
  hostname: string | null;
  runtime: "container" | "native";
  device: "cpu" | "mps" | "cuda";
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
        runtime: r.runtime,
        device: r.device,
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
