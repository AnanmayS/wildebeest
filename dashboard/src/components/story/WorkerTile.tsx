import type { Worker } from '../../lib/types';
import type { Phase } from '../../lib/workerStory';
import { fmtInt } from '../../lib/format';
import { workerName } from '../../lib/narration';

const LOOK: Record<Phase, { label: string; dot: string; bar: string; text: string }> = {
  busy: { label: 'Busy', dot: 'bg-leaf-400 animate-pulse-dot', bar: 'bg-leaf-400', text: 'text-leaf-300' },
  idle: { label: 'Idle', dot: 'bg-ink-500', bar: 'bg-ink-600', text: 'text-ink-400' },
  killing: { label: 'Crashing…', dot: 'bg-ember-400', bar: 'bg-ember-400', text: 'text-ember-300' },
  dead: { label: 'Crashed', dot: 'bg-ember-400', bar: 'bg-ember-600', text: 'text-ember-300' },
  paused: { label: 'Frozen', dot: 'bg-sun-400', bar: 'hatch text-sun-400', text: 'text-sun-300' },
  stopped: { label: 'Stopped', dot: 'bg-ink-600', bar: 'bg-ink-700', text: 'text-ink-500' },
};

/** "Mac GPU" for the native Apple-silicon worker; nothing for the ordinary CPU containers. */
function hardware(w: Worker): string | null {
  if (w.device === 'mps') return 'Mac GPU';
  if (w.device === 'cuda') return 'GPU';
  return w.runtime === 'native' ? 'This Mac' : null;
}

/**
 * One computer doing the work: its name, what it's doing right now, and how many photos it has done.
 * `compact` (a larger pool) drops the count so the stage keeps its height.
 */
export function WorkerTile({ worker: w, phase, compact = false }: { worker: Worker; phase: Phase; compact?: boolean }) {
  const look = LOOK[phase];
  const hw = hardware(w);
  const gone = phase === 'dead' || phase === 'killing';
  return (
    <div
      className={`relative flex items-center gap-3 overflow-hidden rounded-lg border pl-4 pr-3 ${compact ? 'py-1' : 'py-2'} transition-colors duration-500 ${
        gone ? 'animate-death border-ember-400/50' : phase === 'paused' ? 'border-sun-400/50 bg-ink-850' : phase === 'busy' ? 'border-leaf-400/30 bg-ink-850' : 'border-ink-700 bg-ink-850'
      }`}
    >
      <span className={`absolute inset-y-0 left-0 w-1.5 ${look.bar}`} aria-hidden />
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className={`truncate font-mono text-[15px] ${gone ? 'text-ink-400 line-through decoration-ember-400/70' : 'text-ink-100'}`}>{workerName(w.id)}</span>
          {hw && <span className="shrink-0 rounded-md bg-sky-400/15 px-1.5 py-0.5 text-[11px] font-semibold text-sky-400">{hw}</span>}
        </div>
        {!compact && <div className="mt-0.5 text-[12.5px] tabular text-ink-500">{fmtInt(w.tasksCompleted)} photos done</div>}
      </div>
      <span className={`flex shrink-0 items-center gap-2 text-[16px] font-semibold ${look.text}`}>
        <span className={`h-2.5 w-2.5 rounded-full ${look.dot}`} />
        {look.label}
      </span>
    </div>
  );
}
