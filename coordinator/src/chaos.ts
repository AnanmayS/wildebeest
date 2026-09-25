import { query } from "./db.js";
import { killWorker } from "./docker.js";

// Chaos mode: every killEverySec, SIGKILL one random ALIVE worker. It never kills the last live
// worker of a stage, since workers don't restart ("restart: no") and the job would stall forever.
//
// With several coordinator replicas the switch lives in Postgres (coordinator_leader.chaos_*), so
// POST /chaos can land on any replica; only the leader's loop kills (the kill's attribution stamp is
// a fenced write, so a deposed leader can't fire one).

export interface ChaosState {
  enabled: boolean;
  killEverySec: number;
}

let timer: NodeJS.Timeout | null = null;
let generation = 0;
let lastKillAt = 0;

export async function getChaos(): Promise<ChaosState> {
  const { rows } = await query(`select chaos_enabled, chaos_every_sec from coordinator_leader where id = 1`);
  return { enabled: Boolean(rows[0]?.chaos_enabled), killEverySec: Number(rows[0]?.chaos_every_sec ?? 20) };
}

export async function chaosKillOnce(): Promise<string | null> {
  const { rows } = await query(
    `select id from (
       select id, count(*) over (partition by stage) as stage_alive from workers where status = 'ALIVE'
     ) w where stage_alive >= 2`,
  );
  if (rows.length === 0) return null;
  const victim = rows[Math.floor(Math.random() * rows.length)].id as string;
  await killWorker(victim, "chaos");
  return victim;
}

export async function setChaos(next: { enabled?: unknown; killEverySec?: unknown }): Promise<ChaosState> {
  const current = await getChaos();
  const every = Number(next.killEverySec ?? current.killEverySec);
  const state: ChaosState = {
    enabled: Boolean(next.enabled ?? current.enabled),
    killEverySec: Number.isFinite(every) && every >= 1 ? Math.round(every) : current.killEverySec,
  };
  await query(`update coordinator_leader set chaos_enabled = $1, chaos_every_sec = $2 where id = 1`, [
    state.enabled,
    state.killEverySec,
  ]);
  if (!current.enabled && state.enabled) lastKillAt = Date.now(); // first kill one interval from now
  console.log(`[chaos] ${state.enabled ? `on, killing every ${state.killEverySec}s` : "off"}`);
  return state;
}

/** One tick of the leader's chaos loop: kill if chaos is on and an interval has passed. */
async function chaosTick() {
  const state = await getChaos();
  if (!state.enabled) {
    lastKillAt = 0;
    return;
  }
  if (lastKillAt === 0) lastKillAt = Date.now();
  if (Date.now() - lastKillAt < state.killEverySec * 1000) return;
  lastKillAt = Date.now();
  await chaosKillOnce();
}

/** Leader only: checks the shared switch every second and kills on schedule. Returns its stop. */
export function startChaos(): () => void {
  stopChaos();
  lastKillAt = 0;
  const gen = generation;
  const run = () => {
    chaosTick()
      .catch((err) => console.error("[chaos] kill failed:", (err as Error).message))
      .finally(() => {
        if (gen === generation) timer = setTimeout(run, 1000);
      });
  };
  timer = setTimeout(run, 1000);
  return () => {
    if (gen === generation) stopChaos();
  };
}

/** Stops this replica's chaos loop (on shutdown or when it stops being the leader). */
export function stopChaos() {
  generation++;
  if (timer) clearTimeout(timer);
  timer = null;
}
