import Docker from "dockerode";
import { config } from "./config.js";
import { getPool, query } from "./db.js";
import { hub, recordEvents } from "./events.js";
import { ValidationError } from "./tasks.js";

// Fault injection through the mounted Docker socket: SIGKILL (the dashboard's Kill button and
// chaos mode) and a timed `docker pause` (a SIGSTOP-style freeze that outlives the lease, to show
// fencing). Nothing graceful happens to the worker; recovery is the death-watch/heartbeat path.
//
// Attribution: workers.killed_at / paused_at (+ paused_until) are stamped *before* the signal is
// sent, so when the `die` event (or the heartbeat timeout) arrives, requeueLostLeases can tell that
// the coordinator caused the lost lease and must not charge the task an attempt. If Docker refuses,
// the stamp is rolled back.

export interface DockerOps {
  kill(containerId: string): Promise<void>;
  pause(containerId: string): Promise<void>;
  unpause(containerId: string): Promise<void>;
}

let docker: Docker | null = null;

/** The shared dockerode client (also used by the death watch). */
export function getDocker(): Docker {
  return (docker ??= new Docker({ socketPath: config.dockerSocket }));
}

const realOps: DockerOps = {
  kill: async (id) => {
    await getDocker().getContainer(id).kill({ signal: "SIGKILL" });
  },
  pause: async (id) => {
    await getDocker().getContainer(id).pause();
  },
  unpause: async (id) => {
    await getDocker().getContainer(id).unpause();
  },
};

let ops: DockerOps = realOps;

/** Test hook: replace some or all Docker calls. Pass nothing to restore the real ones. */
export function setDockerOps(fake?: Partial<DockerOps>) {
  ops = fake ? { ...realOps, ...fake } : realOps;
}

/** Test hook kept for the original tests: replace only the kill call. */
export function setKiller(fn?: DockerOps["kill"]) {
  setDockerOps(fn ? { kill: fn } : undefined);
}

export class WorkerNotFoundError extends Error {}

/** A request that conflicts with the worker's state; the API answers 409 { error: code }. */
export class ConflictError extends Error {
  constructor(readonly code: string, message = code) {
    super(message);
  }
}

async function containerOf(workerId: string): Promise<string> {
  const { rows } = await query(`select container_id, runtime from workers where id = $1`, [workerId]);
  if (rows.length === 0) throw new WorkerNotFoundError(`unknown worker ${workerId}`);
  if (rows[0].runtime !== "container") {
    throw new ConflictError("NOT_A_CONTAINER", `${workerId} is a native worker; it has no container to signal`);
  }
  return rows[0].container_id;
}

/**
 * Stamps the fault on the worker row (`set`), runs `signal`, and restores the previous values if the
 * signal fails, so a refused kill/pause never excuses a later, unrelated lease loss.
 */
async function stampAround(
  workerId: string,
  columns: string[],
  set: string,
  params: unknown[],
  signal: () => Promise<void>,
) {
  const { rows } = await query(
    `update workers w set ${set}
       from (select id, ${columns.join(", ")} from workers where id = $1) old
      where w.id = old.id
      returning ${columns.map((c) => `old.${c}`).join(", ")}`,
    [workerId, ...params],
  );
  try {
    await signal();
  } catch (err) {
    const previous = columns.map((c) => rows[0]?.[c] ?? null);
    await query(`update workers set ${columns.map((c, i) => `${c} = $${i + 2}`).join(", ")} where id = $1`, [
      workerId,
      ...previous,
    ]);
    throw err;
  }
}

export async function killWorker(workerId: string, source: "api" | "chaos") {
  const containerId = await containerOf(workerId);
  await stampAround(workerId, ["killed_at"], "killed_at = now()", [], () => ops.kill(containerId));
  const events = await recordEvents(getPool(), [
    { type: "worker_killed", workerId, detail: { containerId, source } },
  ]);
  hub.publishEvents(events);
  hub.workersChanged();
  console.log(`[docker] SIGKILL sent to ${workerId} (container ${containerId}, ${source})`);
}

// ---------------------------------------------------------------------------------------------
// pause / resume
// ---------------------------------------------------------------------------------------------

const paused = new Map<string, { containerId: string; timer: NodeJS.Timeout; since: number }>();

export async function pauseWorker(workerId: string, ms: unknown, source: "api" | "chaos" = "api") {
  const duration = Number(ms);
  if (!Number.isInteger(duration) || duration < 1 || duration > config.maxPauseMs) {
    throw new ValidationError(`ms must be an integer between 1 and ${config.maxPauseMs}`);
  }
  const containerId = await containerOf(workerId);
  if (paused.has(workerId)) throw new ConflictError("ALREADY_PAUSED", `${workerId} is already paused`);

  await stampAround(
    workerId,
    ["paused_at", "paused_until"],
    "paused_at = now(), paused_until = now() + ($2::int * interval '1 millisecond')",
    [duration],
    () => ops.pause(containerId),
  );
  const timer = setTimeout(() => void resumeWorker(workerId), duration);
  timer.unref();
  paused.set(workerId, { containerId, timer, since: Date.now() });

  const events = await recordEvents(getPool(), [
    { type: "worker_paused", workerId, detail: { containerId, ms: duration, source } },
  ]);
  hub.publishEvents(events);
  hub.workersChanged();
  console.log(`[docker] paused ${workerId} for ${duration} ms (container ${containerId}, ${source})`);
  return { ok: true, resumesAt: new Date(Date.now() + duration).toISOString() };
}

/** Unpauses a worker we paused (normally from its timer). Returns false if it wasn't paused. */
export async function resumeWorker(workerId: string): Promise<boolean> {
  const p = paused.get(workerId);
  if (!p) return false;
  paused.delete(workerId);
  clearTimeout(p.timer);
  try {
    await ops.unpause(p.containerId);
  } catch (err) {
    // Typically the container was killed while paused; there is nothing left to resume.
    console.warn(`[docker] unpause ${workerId} failed: ${(err as Error).message}`);
    return false;
  }
  const events = await recordEvents(getPool(), [
    { type: "worker_resumed", workerId, detail: { containerId: p.containerId, pausedMs: Date.now() - p.since } },
  ]);
  hub.publishEvents(events);
  hub.workersChanged();
  console.log(`[docker] resumed ${workerId}`);
  return true;
}

/** Shutdown: never leave a container frozen behind us. */
export async function resumeAll() {
  await Promise.allSettled([...paused.keys()].map((id) => resumeWorker(id)));
}
