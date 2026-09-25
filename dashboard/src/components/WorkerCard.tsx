import type { ReactNode } from 'react';
import type { Worker } from '../lib/types';
import { shortId } from '../lib/format';
import { Num } from './Num';

interface Props {
  worker: Worker;
  killSentAt?: number;
  workerTimeoutMs: number;
  now: number;
  onKill: (id: string) => void;
}

/** One worker container. Dead cards flash and turn red; kill-in-flight cards count missed heartbeats. */
export function WorkerCard({ worker: w, killSentAt, workerTimeoutMs, now, onKill }: Props) {
  const dead = w.status === 'DEAD';
  const stopped = w.status === 'STOPPED';
  const killing = !!killSentAt && w.status === 'ALIVE';
  const silentFor = Math.max(0, (now - Date.parse(w.lastHeartbeatAt)) / 1000);

  const frame = dead
    ? 'border-ember-400/70 bg-ember-600/15 animate-death'
    : killing
      ? 'border-ember-400/50 border-dashed bg-ink-850'
      : w.state === 'busy'
        ? 'border-leaf-400/30 bg-ink-850'
        : 'border-ink-800 bg-ink-850/60';

  return (
    <article className={`relative flex gap-3 rounded-lg border p-2.5 transition-colors duration-500 ${frame} ${stopped ? 'opacity-45' : ''}`}>
      <Thumb worker={w} dead={dead} />

      <div className="flex min-w-0 flex-1 flex-col">
        <div className="flex items-center justify-between gap-2">
          <span className="truncate font-mono text-[13px] text-ink-300" title={w.id}>
            {shortId(w.id)}
          </span>
          <StatePill worker={w} killing={killing} />
        </div>

        {dead ? (
          <div className="mt-auto">
            <div className="text-[15px] font-semibold leading-tight text-ember-400">DEAD —</div>
            <div className="text-xs leading-tight text-ember-300/90">
              {w.reassignedCount} task{w.reassignedCount === 1 ? '' : 's'} reassigned
            </div>
          </div>
        ) : (
          <div className="mt-auto flex items-end justify-between gap-2">
            <div className="leading-none">
              <Num value={w.tasksCompleted} className="text-[28px] font-semibold" />
              <span className="ml-1.5 text-xs text-ink-500">done</span>
              <div className="mt-1 text-[11px] text-ink-500 tabular">
                {killing
                  ? `no heartbeat ${silentFor.toFixed(0)}s / ${Math.round(workerTimeoutMs / 1000)}s`
                  : `${w.avgLatencyMs != null ? `${Math.round(w.avgLatencyMs)} ms` : '— ms'}${w.rssMb ? ` · ${Math.round(w.rssMb)} MB` : ''}`}
              </div>
            </div>
            {!stopped && (
              <button
                onClick={() => onKill(w.id)}
                disabled={killing}
                className="rounded-md border border-ember-400/40 px-2 py-1 text-[11px] font-semibold uppercase tracking-wider text-ember-400 transition hover:border-ember-400 hover:bg-ember-400 hover:text-ink-950 disabled:pointer-events-none disabled:opacity-40"
                title={`SIGKILL container ${w.containerId}`}
                aria-label={`Kill worker ${w.id}`}
              >
                Kill
              </button>
            )}
          </div>
        )}
      </div>
    </article>
  );
}

function Thumb({ worker: w, dead }: { worker: Worker; dead: boolean }) {
  return (
    <div className="relative h-[72px] w-24 shrink-0 overflow-hidden rounded-md bg-ink-800">
      {w.currentImageUrl && !dead ? (
        <>
          <img src={w.currentImageUrl} alt="" className="h-full w-full object-cover" />
          <div className="absolute inset-x-0 h-full animate-scan bg-[linear-gradient(180deg,transparent_70%,rgb(125_220_147/0.35)_96%,transparent)]" />
        </>
      ) : (
        <div className={`grid h-full place-items-center text-[10px] uppercase tracking-widest ${dead ? 'text-ember-400/80' : 'text-ink-600'}`}>
          {dead ? <Cross /> : 'idle'}
        </div>
      )}
    </div>
  );
}

function StatePill({ worker: w, killing }: { worker: Worker; killing: boolean }) {
  if (killing) return <Pill cls="text-ember-300 bg-ember-400/10">SIGKILL sent</Pill>;
  if (w.status === 'STOPPED') return <Pill cls="text-ink-400 bg-ink-800">stopped</Pill>;
  if (w.state === 'dead') return <Pill cls="text-ember-400 bg-ember-400/15">dead</Pill>;
  if (w.state === 'busy')
    return (
      <Pill cls="text-leaf-400 bg-leaf-400/10">
        <span className="h-1.5 w-1.5 rounded-full bg-leaf-400 animate-pulse-dot" />
        busy
      </Pill>
    );
  return <Pill cls="text-ink-400 bg-ink-800">idle</Pill>;
}

const Pill = ({ cls, children }: { cls: string; children: ReactNode }) => (
  <span className={`inline-flex shrink-0 items-center gap-1.5 rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] ${cls}`}>
    {children}
  </span>
);

const Cross = () => (
  <svg viewBox="0 0 24 24" className="h-6 w-6" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden>
    <path d="M6 6l12 12M18 6 6 18" strokeLinecap="round" />
  </svg>
);
