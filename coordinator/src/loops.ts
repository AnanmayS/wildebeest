import { config } from "./config.js";
import { query } from "./db.js";
import { dispatchOnce, rebuildQueues } from "./dispatcher.js";
import { reapOnce } from "./reaper.js";

/** Runs fn every intervalMs, never overlapping itself; returns a stop function. */
export function every(name: string, intervalMs: number, fn: () => Promise<unknown>) {
  let stopped = false;
  let timer: NodeJS.Timeout | null = null;
  const run = async () => {
    try {
      await fn();
    } catch (err) {
      console.error(`[${name}] ${(err as Error).message}`);
    }
    if (!stopped) timer = setTimeout(run, intervalMs);
  };
  timer = setTimeout(run, intervalMs);
  return () => {
    stopped = true;
    if (timer) clearTimeout(timer);
  };
}

export function startLoops() {
  const stops = [
    every("dispatcher", config.dispatchIntervalMs, dispatchOnce),
    every("reaper", config.reapIntervalMs, reapOnce),
  ];
  return () => stops.forEach((s) => s());
}

/**
 * Startup reconciliation after a coordinator restart:
 *  - The ready queues are rebuilt from Postgres (see rebuildQueues).
 *  - Workers and leases get a grace period, so the coordinator's own downtime isn't mistaken for
 *    every worker dying at once.
 */
export async function reconcileOnStartup() {
  await rebuildQueues("coordinator startup");
  await query(`update workers set last_heartbeat_at = now() where status = 'ALIVE'`);
  await query(
    `update tasks set lease_expires_at = greatest(lease_expires_at, now() + ($1::int * interval '1 millisecond'))
      where state = 'LEASED'`,
    [config.leaseMs],
  );
}
