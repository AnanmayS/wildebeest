import { useEffect, useRef } from 'react';
import type { LeaseSightings } from '../lib/recovery';
import type { SystemSnapshot } from '../lib/types';

const MAX_TASKS = 4000;

/**
 * Remembers who leased each task and when, from the claim time the coordinator reports for live
 * leases (`system.at − lease.ageMs`). Leases of fast tasks live under a second, so they have to be
 * caught as the snapshots arrive; the recovery view uses this when a record never closes.
 */
export function useLeaseSightings(system: SystemSnapshot | null): LeaseSightings {
  const sightings = useRef<LeaseSightings>(new Map());

  useEffect(() => {
    if (!system?.leases) return;
    const snapshotAt = Date.parse(system.at);
    const map = sightings.current;
    for (const l of system.leases) {
      const seen = map.get(l.taskId) ?? [];
      if (seen.some((s) => s.workerId === l.workerId && s.epoch === l.epoch)) continue;
      seen.push({ workerId: l.workerId, epoch: l.epoch, at: snapshotAt - l.ageMs });
      map.delete(l.taskId); // re-insert: Map order = least recently seen first
      map.set(l.taskId, seen);
    }
    while (map.size > MAX_TASKS) map.delete(map.keys().next().value!);
  }, [system]);

  return sightings.current;
}
