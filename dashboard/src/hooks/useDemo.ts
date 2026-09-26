import { useCallback, useRef, useState } from 'react';
import type { Episode } from '../lib/narration';
import type { Worker } from '../lib/types';
import { PAUSE_MS, type Live, type LiveJob } from './useWildebeest';

export const DEMO_SIZE = 100;

/**
 * The Story view's three buttons. Each one records an episode (what the visitor did and when) that the
 * narration then tells from real events. Crash and Freeze always pick the busiest *container* detector:
 * Docker can't kill a native worker, and the only classifier is left alone so the job can finish.
 */
export function useDemo(live: Live) {
  const [episodes, setEpisodes] = useState<Episode[]>([]);
  const [starting, setStarting] = useState(false);
  const push = (e: Episode) => setEpisodes((cur) => [e, ...cur].slice(0, 4));
  // Remember each job's latest state: the narration of an older run outlives it being the current job.
  const jobs = useRef(new Map<string, LiveJob>());
  if (live.job) jobs.current.set(live.job.id, live.job);

  const running = live.job?.status === 'running';
  const acting = Object.keys(live.killRequested).length > 0 || Object.keys(live.pauseRequested).length > 0;
  const target = busiestContainerDetector(live.workers, live);

  const run = useCallback(async () => {
    setStarting(true);
    try {
      const jobId = await live.startSample(DEMO_SIZE);
      push({ kind: 'run', at: Date.now(), jobId, size: DEMO_SIZE });
    } catch (err) {
      live.logLocal('failed', `Could not start the demo: ${(err as Error).message}`);
    } finally {
      setStarting(false);
    }
  }, [live.startSample, live.logLocal]);

  const crash = useCallback(() => {
    if (!target) return;
    push({ kind: 'crash', at: Date.now(), workerId: target.id });
    live.killWorker(target.id);
  }, [target?.id, live.killWorker]);

  const freeze = useCallback(() => {
    if (!target) return;
    push({ kind: 'freeze', at: Date.now(), workerId: target.id, ms: PAUSE_MS });
    live.pauseWorker(target.id);
  }, [target?.id, live.pauseWorker]);

  return {
    episodes,
    jobs: jobs.current,
    run, crash, freeze,
    canRun: !running && !starting,
    starting,
    running,
    /** Why Crash / Freeze are disabled, or null when they can be pressed. */
    blocked: !running ? 'Start a demo first' : acting ? 'Wait for the last one to play out' : !target ? 'No container worker left to crash' : null,
  };
}

export type Demo = ReturnType<typeof useDemo>;

function busiestContainerDetector(workers: Worker[], live: Live): Worker | null {
  const candidates = workers.filter((w) =>
    w.stage === 'detect' && w.status === 'ALIVE' && w.runtime !== 'native'
    && !live.killRequested[w.id] && !live.pauseRequested[w.id]);
  candidates.sort((a, b) =>
    b.currentTaskIds.length - a.currentTaskIds.length
    || Number(b.state === 'busy') - Number(a.state === 'busy')
    || b.tasksCompleted - a.tasksCompleted);
  return candidates[0] ?? null;
}
