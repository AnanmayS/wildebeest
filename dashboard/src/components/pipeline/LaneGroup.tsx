import type { PendingAction } from '../../hooks/useWildebeest';
import type { Lease, Stage, StageStats, TaskEvent, Worker } from '../../lib/types';
import { workerStory } from '../../lib/workerStory';
import type { PipelineModel } from '../../lib/pipeline';
import { fmtMs, fmtRate, shortId } from '../../lib/format';
import { WorkerLane } from './WorkerLane';

/** A dead lane stays full-size this long (so the recovery is visible), then folds into the "dead" line. */
const DEAD_LANE_MS = 20_000;

interface Props {
  stage: Stage;
  title: string;
  model: string;
  workers: Worker[];
  events: TaskEvent[];
  pipeline: PipelineModel;
  killRequested: Record<string, PendingAction>;
  pauseRequested: Record<string, PendingAction>;
  now: number;
  onKill: (id: string) => void;
  onPause: (id: string) => void;
}

export function LaneGroup({ stage, title, model, workers, events, pipeline, killRequested, pauseRequested, now, onKill, onPause }: Props) {
  const mine = workers.filter((w) => w.stage === stage && w.status !== 'STOPPED').sort(byRegistration);
  const longDead = (w: Worker) => w.status === 'DEAD' && now - Date.parse(w.diedAt ?? w.lastHeartbeatAt) > DEAD_LANE_MS && !pauseRequested[w.id];
  const lanes = mine.filter((w) => !longDead(w));
  const graveyard = mine.filter(longDead);
  const leaseOf = (id: string): Lease | undefined => pipeline.leasesByTask.get(id);

  return (
    <div className="flex min-w-0 flex-col">
      <StageHeader title={title} model={model} stats={pipeline.stages[stage]} />
      <div className="flex flex-1 flex-col justify-center gap-1 py-1">
        {lanes.length === 0 && (
          <p className="ml-5 rounded-md border border-dashed border-ink-700 px-3 py-3 text-[12.5px] text-ink-500">
            No {stage === 'detect' ? 'detectors' : 'classifiers'}. <code className="font-mono text-ink-300">docker compose up --scale {stage === 'detect' ? 'detector' : 'classifier'}=N</code>
          </p>
        )}
        {lanes.map((w) => (
          <WorkerLane
            key={w.id}
            worker={w}
            story={workerStory(w, events, now, killRequested[w.id], pauseRequested[w.id], pipeline.recoveryByWorker.get(w.id))}
            leaseOf={leaseOf}
            limits={pipeline.limits}
            now={now}
            canPause={pipeline.v2}
            onKill={onKill}
            onPause={onPause}
          />
        ))}
        {graveyard.length > 0 && (
          <p className="ml-5 truncate font-mono text-[11px] text-ember-300/70" title={graveyard.map((w) => w.id).join(', ')}>
            + {graveyard.length} dead: {graveyard.map((w) => shortId(w.id)).join(' · ')}
          </p>
        )}
      </div>
    </div>
  );
}

function StageHeader({ title, model, stats }: { title: string; model: string; stats: Partial<StageStats> }) {
  return (
    <div className="ml-5 flex items-baseline justify-between gap-2 pr-4">
      <div className="min-w-0 truncate">
        <span className="text-[14px] font-semibold">{title}</span>
        <span className="ml-1.5 text-[12px] text-ink-500">{model}</span>
      </div>
      <div className="shrink-0 text-[12px] tabular text-ink-400" title="tasks completed per second (last 10 s) · p50 service time (claim to complete)">
        <b className="text-[15px] font-semibold text-ink-100">{stats.completedPerSec != null ? fmtRate(stats.completedPerSec) : '—'}</b>
        <span className="text-ink-500">/s</span>
        {stats.p50ServiceMs != null && <> · p50 {fmtMs(stats.p50ServiceMs)}</>}
      </div>
    </div>
  );
}

/** Oldest first so lanes keep their place; id order if registeredAt is missing. */
function byRegistration(a: Worker, b: Worker): number {
  if (a.registeredAt && b.registeredAt && a.registeredAt !== b.registeredAt) return a.registeredAt < b.registeredAt ? -1 : 1;
  return a.id.localeCompare(b.id);
}
