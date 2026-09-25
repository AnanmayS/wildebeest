import { query } from "./db.js";
import { killWorker } from "./docker.js";

// Chaos mode: every killEverySec, SIGKILL one random ALIVE worker. It never kills the last live
// worker of a stage, since workers don't restart ("restart: no") and the job would stall forever.

let state = { enabled: false, killEverySec: 20 };
let timer: NodeJS.Timeout | null = null;

export const getChaos = () => ({ ...state });

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

export function setChaos(next: { enabled?: unknown; killEverySec?: unknown }) {
  const every = Number(next.killEverySec ?? state.killEverySec);
  state = {
    enabled: Boolean(next.enabled ?? state.enabled),
    killEverySec: Number.isFinite(every) && every >= 1 ? every : state.killEverySec,
  };
  if (timer) clearInterval(timer);
  timer = null;
  if (state.enabled) {
    timer = setInterval(() => {
      chaosKillOnce().catch((err) => console.error("[chaos] kill failed:", (err as Error).message));
    }, state.killEverySec * 1000);
  }
  console.log(`[chaos] ${state.enabled ? `on, killing every ${state.killEverySec}s` : "off"}`);
  return getChaos();
}

export function stopChaos() {
  if (timer) clearInterval(timer);
  timer = null;
  state = { ...state, enabled: false };
}
