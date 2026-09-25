import { useEffect, useRef, useState, type ReactNode } from 'react';
import type { Lease, Worker } from '../../lib/types';
import type { WorkerStory } from '../../lib/workerStory';
import { fmtInt, fmtMs, shortId } from '../../lib/format';
import { Badge } from '../ui/Badge';
import { Epoch, LeaseChip } from './LeaseChip';
import { usePackets, Wire } from './Wire';

interface Props {
  worker: Worker;
  story: WorkerStory;
  leaseOf: (taskId: string) => Lease | undefined;
  limits: { leaseMs: number; workerTimeoutMs: number };
  now: number;
  /** v1 coordinators have no POST /workers/:id/pause. */
  canPause: boolean;
  onKill: (id: string) => void;
  onPause: (id: string) => void;
}

/** Left edge of the lane: its state as a stripe (solid, dashed, hatched), readable from across a room. */
const STRIPE: Record<WorkerStory['phase'], string> = {
  busy: 'bg-leaf-400',
  idle: 'bg-ink-600',
  killing: 'bg-ember-400',
  dead: 'bg-ember-600',
  paused: 'hatch text-sun-400',
  stopped: 'bg-ink-700',
};

/**
 * One worker, drawn as a lane: queue → [worker] → next stage. A packet runs in when the
 * coordinator leases it a task, out when the task completes, and back (amber) when the
 * worker dies and its task is requeued.
 */
export function WorkerLane({ worker: w, story, leaseOf, limits, now, canPause, onKill, onPause }: Props) {
  const inbound = usePackets();
  const outbound = usePackets();
  const frozenTask = useFrozenTask(w, story, leaseOf);
  const dead = story.phase === 'dead';

  // Diff the worker's held task IDs between updates: every change is a real claim, completion or requeue.
  const held = w.currentTaskIds;
  const prev = useRef(held);
  const heldKey = held.join(',');
  useEffect(() => {
    const before = prev.current;
    prev.current = held;
    for (const id of held) if (!before.includes(id)) inbound.spawn('fwd');
    for (const id of before) {
      if (held.includes(id)) continue;
      if (dead || story.phase === 'paused') inbound.spawn('back');
      else outbound.spawn('fwd');
    }
  }, [heldKey]);

  const actionable = (story.phase === 'busy' || story.phase === 'idle') && !story.native;
  const why = story.native ? 'Native process, not a container: Docker cannot kill or pause it' : undefined;

  const detail = <LaneDetail worker={w} story={story} leaseOf={leaseOf} limits={limits} now={now} frozenTask={frozenTask} />;

  return (
    // A dead lane shrinks to one line: it has nothing left to do but say how it died.
    <div className={`flex items-stretch transition-[height] duration-500 ${dead ? 'h-[30px]' : 'h-[46px]'}`}>
      <Wire className="w-5" packets={inbound.packets} onDone={inbound.remove} />
      <article
        className={`relative flex min-w-0 flex-1 flex-col justify-center gap-[3px] overflow-hidden rounded-md border py-1 pl-3.5 pr-2 transition-colors duration-500 ${
          dead ? 'animate-death border-ember-600/60 bg-ember-600/[0.06]' : story.phase === 'paused' ? 'border-sun-400/40 bg-sun-400/[0.04]' : 'border-ink-700 bg-ink-850'
        } ${story.phase === 'stopped' ? 'opacity-50' : ''}`}
      >
        <span className={`absolute inset-y-0 left-0 w-[4px] ${STRIPE[story.phase]}`} aria-hidden />
        <div className="flex min-w-0 items-center gap-1.5">
          <span className={`shrink-0 truncate font-mono text-[13px] ${dead ? 'text-ember-300' : 'text-ink-100'}`} title={w.id}>
            {shortId(w.id)}
          </span>
          {dead ? (
            <span className="flex min-w-0 items-center gap-2 pl-1 text-[12px]">{detail}</span>
          ) : (
            <>
              <Badge tone={w.device && w.device !== 'cpu' ? 'sky' : 'ink'}>{w.device ?? 'cpu'}</Badge>
              {story.native && <Badge tone="sky" title="Runs outside Docker, registered with the same coordinator">native</Badge>}
              <span className="flex-1" />
              <LaneButton label={`Pause ${w.id}`} tone="sun" disabled={!actionable || !canPause} title={why ?? (!canPause ? 'Needs a v2 coordinator (POST /workers/:id/pause)' : undefined) ?? 'docker pause for 20 s: longer than the heartbeat timeout, so its task is reclaimed and its late result gets fenced'} onClick={() => onPause(w.id)}>
                Pause
              </LaneButton>
              <LaneButton label={`Kill ${w.id}`} tone="ember" disabled={!actionable} title={why ?? `SIGKILL container ${w.containerId}`} onClick={() => onKill(w.id)}>
                Kill
              </LaneButton>
            </>
          )}
        </div>
        {!dead && <div className="flex min-w-0 items-center gap-2 text-[12px]">{detail}</div>}
      </article>
      <Wire className="w-4" packets={outbound.packets} onDone={outbound.remove} />
    </div>
  );
}

function LaneDetail({ worker: w, story, leaseOf, limits, now, frozenTask }: Omit<Props, 'onKill' | 'onPause' | 'canPause'> & { frozenTask: FrozenTask | null }) {
  if (story.phase === 'paused' && story.pause) {
    return <PauseTrack pause={story.pause} now={now} limits={limits} frozenTask={frozenTask} />;
  }
  if (story.fenced) {
    const { epoch, currentEpoch } = story.fenced;
    return (
      <span className="flex min-w-0 items-center gap-1.5 text-violet-300">
        woke with {epoch != null ? <Epoch n={epoch} stale /> : 'an old epoch'}
        {currentEpoch != null && <span className="flex items-center gap-1.5">· task at <Epoch n={currentEpoch} highlight /></span>}
        <span className="truncate font-semibold">→ result rejected</span>
      </span>
    );
  }
  if (story.phase === 'killing') return <span className="truncate text-ember-300">SIGKILL sent · waiting for Docker's die event…</span>;
  if (story.phase === 'dead') {
    const d = story.death;
    const how = d?.via === 'docker_event'
      ? <>Docker event in <b className="font-semibold">{fmtMs(d.detectMs)}</b></>
      : d?.via === 'heartbeat' ? <>heartbeat timeout</> : <>declared dead</>;
    return (
      <span className="truncate text-ember-300">
        died · {how}
        {w.reassignedCount > 0 && <span className="text-sun-300"> · {w.reassignedCount} requeued</span>}
      </span>
    );
  }
  return (
    <>
      {w.currentTaskIds.length ? (
        w.currentTaskIds.slice(0, 2).map((id) => <LeaseChip key={id} taskId={id} lease={leaseOf(id)} leaseMs={limits.leaseMs} />)
      ) : (
        <span className="text-ink-500">idle</span>
      )}
      <span className="ml-auto shrink-0 tabular text-ink-400">
        {w.avgLatencyMs != null && <>{fmtMs(w.avgLatencyMs)}<span className="text-ink-500">/task · </span></>}
        {fmtInt(w.tasksCompleted)}
        <span className="text-ink-500"> done</span>
      </span>
    </>
  );
}

/** Countdown of a `docker pause`, with the heartbeat timeout and lease expiry marked to scale. */
function PauseTrack({ pause, now, limits, frozenTask }: { pause: NonNullable<WorkerStory['pause']>; now: number; limits: Props['limits']; frozenTask: FrozenTask | null }) {
  const total = pause.until - pause.from;
  const elapsed = Math.min(total, now - pause.from);
  const mark = (ms: number) => `${Math.min(100, (ms / total) * 100)}%`;
  const declared = pause.declaredDeadAt != null;
  return (
    <>
      {frozenTask && <LeaseChip taskId={frozenTask.taskId} lease={frozenTask.lease} leaseMs={limits.leaseMs} stale={declared} />}
      <span className={`shrink-0 font-semibold ${declared ? 'text-ember-300' : 'text-sun-300'}`}>
        {declared ? 'declared dead' : 'frozen'}
      </span>
      <span className="relative h-2 min-w-10 flex-1 rounded-sm bg-ink-800" title={`heartbeat timeout at ${limits.workerTimeoutMs / 1000}s, lease expiry at ${limits.leaseMs / 1000}s`}>
        <span className="hatch absolute inset-y-0 left-0 rounded-sm text-sun-400/80" style={{ width: `${(elapsed / total) * 100}%` }} />
        {/* Declared dead when heartbeats had been silent for the timeout: at most workerTimeoutMs after freezing. */}
        <span className="absolute -inset-y-1 w-px bg-ember-300" style={{ left: mark(declared ? pause.declaredDeadAt! - pause.from : limits.workerTimeoutMs) }} />
        <span className="absolute -inset-y-1 w-px bg-ink-300" style={{ left: mark(limits.leaseMs) }} />
      </span>
      <span className="w-8 shrink-0 text-right tabular text-sun-300">{Math.max(0, Math.ceil((total - elapsed) / 1000))}s</span>
    </>
  );
}

interface FrozenTask { taskId: string; lease?: Lease }

/** Remember what a worker was holding when it froze: the coordinator forgets it once it is declared dead. */
function useFrozenTask(w: Worker, story: WorkerStory, leaseOf: Props['leaseOf']): FrozenTask | null {
  const [task, setTask] = useState<FrozenTask | null>(null);
  const paused = story.phase === 'paused';
  const current = w.currentTaskIds[0];
  useEffect(() => {
    if (!paused) setTask(null);
    else if (current && !task) setTask({ taskId: current, lease: leaseOf(current) });
    else if (current && task?.taskId === current) setTask({ taskId: current, lease: leaseOf(current) ?? task.lease });
  }, [paused, current, leaseOf]);
  return task;
}

function LaneButton({ label, tone, disabled, title, onClick, children }: { label: string; tone: 'sun' | 'ember'; disabled: boolean; title: string; onClick: () => void; children: ReactNode }) {
  const cls = tone === 'ember'
    ? 'border-ember-400/40 text-ember-300 hover:border-ember-400 hover:bg-ember-400 hover:text-ink-950'
    : 'border-sun-400/35 text-sun-300 hover:border-sun-400 hover:bg-sun-400 hover:text-ink-950';
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      title={title}
      aria-label={label}
      className={`h-[22px] shrink-0 rounded border px-2 text-[11px] font-semibold uppercase tracking-wider transition disabled:cursor-not-allowed disabled:border-ink-700 disabled:bg-transparent disabled:text-ink-600 ${cls}`}
    >
      {children}
    </button>
  );
}
