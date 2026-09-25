// What a worker lane should say right now. The coordinator reports status (ALIVE/DEAD) and
// busy/idle; the interesting states (SIGKILL in flight, frozen, fenced after waking up) are
// reconstructed from the event stream plus the actions this browser started.

import type { PendingAction } from '../hooks/useWildebeest';
import type { RecoveryRecord, TaskEvent, Worker } from './types';

export type Phase = 'busy' | 'idle' | 'killing' | 'paused' | 'dead' | 'stopped';

export interface WorkerStory {
  phase: Phase;
  native: boolean;
  /** Frozen with `docker pause`: when it started and when it thaws. */
  pause?: { from: number; until: number; declaredDeadAt?: number };
  /** How the coordinator learned this worker died. */
  death?: { at: number; via?: string; detectMs?: number };
  /** Its late result was fenced off: shown on the lane for a while after waking. */
  fenced?: { at: number; epoch?: number; currentEpoch?: number };
}

/** How long the fencing verdict stays on a lane after the stale result is rejected. */
const VERDICT_MS = 15_000;
const DEFAULT_PAUSE_MS = 20_000;

export function workerStory(
  w: Worker,
  events: TaskEvent[], // newest first
  now: number,
  kill?: PendingAction,
  pauseReq?: PendingAction,
  recovery?: RecoveryRecord,
): WorkerStory {
  const native = w.runtime === 'native';
  const mine = events.filter((e) => e.workerId === w.id);
  const latest = (type: string) => mine.find((e) => e.type === type);
  const num = (e: TaskEvent | undefined, key: string) => {
    const v = e?.detail?.[key];
    return typeof v === 'number' ? v : undefined;
  };

  // Pause: from the coordinator's worker_paused event, or our own request if the event hasn't arrived.
  const paused = latest('worker_paused');
  const resumed = latest('worker_resumed');
  const pausedAt = paused ? Date.parse(paused.at) : pauseReq?.at;
  const pauseMs = num(paused, 'ms') ?? pauseReq?.ms ?? DEFAULT_PAUSE_MS;
  const resumedAfterPause = resumed && pausedAt && Date.parse(resumed.at) >= pausedAt;
  const died = latest('worker_died');
  const diedAt = died ? Date.parse(died.at) : w.diedAt ? Date.parse(w.diedAt) : undefined;

  const story: WorkerStory = { phase: 'idle', native };

  if (w.status === 'DEAD' || w.state === 'dead') {
    story.death = diedAt
      ? { at: diedAt, via: (died?.detail?.via as string | undefined) ?? recovery?.via, detectMs: num(died, 'detectMs') }
      : undefined;
  }

  if (pausedAt && !resumedAfterPause && now < pausedAt + pauseMs + 3000) {
    story.phase = 'paused';
    story.pause = { from: pausedAt, until: pausedAt + pauseMs, declaredDeadAt: diedAt && diedAt >= pausedAt ? diedAt : undefined };
    return story;
  }

  const stale = latest('stale_rejected');
  if (stale && now - Date.parse(stale.at) < VERDICT_MS) {
    const parsed = epochsFromMessage(stale.message);
    story.fenced = {
      at: Date.parse(stale.at),
      epoch: num(stale, 'leaseEpoch') ?? parsed.epoch,
      currentEpoch: num(stale, 'currentEpoch') ?? parsed.currentEpoch,
    };
  }

  if (w.status === 'STOPPED') story.phase = 'stopped';
  else if (w.status === 'DEAD' || w.state === 'dead') story.phase = 'dead';
  else if (kill) story.phase = 'killing';
  else story.phase = w.state === 'busy' ? 'busy' : 'idle';
  return story;
}

/** Parses "(epoch 3 ≠ 4)" out of a v1 stale_rejected message when the event has no detail. */
export function epochsFromMessage(message: string): { epoch?: number; currentEpoch?: number } {
  const m = message.match(/epoch (\d+)\D+(\d+)/);
  return m ? { epoch: Number(m[1]), currentEpoch: Number(m[2]) } : {};
}
