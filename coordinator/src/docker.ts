import Docker from "dockerode";
import { config } from "./config.js";
import { getPool, query } from "./db.js";
import { hub, recordEvents } from "./events.js";

// SIGKILL through the mounted Docker socket (the dashboard's Kill button and chaos mode).
// Nothing graceful happens to the worker: recovery is entirely the heartbeat/lease path.

type Killer = (containerId: string) => Promise<void>;

let docker: Docker | null = null;

const dockerKill: Killer = async (containerId) => {
  docker ??= new Docker({ socketPath: config.dockerSocket });
  await docker.getContainer(containerId).kill({ signal: "SIGKILL" });
};

let killer: Killer = dockerKill;

/** Test hook: replace the Docker call. Pass nothing to restore the real one. */
export function setKiller(fn?: Killer) {
  killer = fn ?? dockerKill;
}

export class WorkerNotFoundError extends Error {}

export async function killWorker(workerId: string, source: "api" | "chaos") {
  const { rows } = await query(`select id, container_id from workers where id = $1`, [workerId]);
  if (rows.length === 0) throw new WorkerNotFoundError(`unknown worker ${workerId}`);
  const containerId: string = rows[0].container_id;
  await killer(containerId);
  await query(`update workers set killed_at = now() where id = $1`, [workerId]);
  const events = await recordEvents(getPool(), [
    { type: "worker_killed", workerId, detail: { containerId, source } },
  ]);
  hub.publishEvents(events);
  hub.workersChanged();
  console.log(`[docker] SIGKILL sent to ${workerId} (container ${containerId}, ${source})`);
}
