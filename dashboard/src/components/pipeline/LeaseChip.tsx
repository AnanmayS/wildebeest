import type { Lease } from '../../lib/types';
import { shortTask } from '../../lib/format';

interface Props {
  taskId: string;
  lease?: Lease;
  leaseMs: number;
  /** The frozen worker's chip: still "held" in its memory, but the coordinator has moved on. */
  stale?: boolean;
  /** This chip is a speculative copy the lane's worker runs next to another worker's lease. */
  speculative?: boolean;
}

/**
 * One in-flight task: its id, the fencing epoch it was claimed with, and how much of the
 * lease is used up (underline). Epochs above 1 mean the task was claimed before: violet.
 */
export function LeaseChip({ taskId, lease, leaseMs, stale, speculative }: Props) {
  if (speculative) return <CopyChip taskId={taskId} lease={lease} leaseMs={leaseMs} />;
  const used = lease ? Math.min(1, lease.ageMs / leaseMs) : 0;
  const retried = (lease?.epoch ?? 1) > 1;
  const copy = lease?.copy;
  return (
    <span
      className={`relative inline-flex h-[22px] shrink-0 items-center gap-1.5 overflow-hidden rounded border pl-1.5 pr-1 font-mono text-[11.5px] ${
        stale ? 'border-dashed border-ember-400/60 text-ink-400' : 'border-ink-700 bg-ink-850 text-ink-300'
      }`}
      title={lease ? `task ${taskId}\nepoch ${lease.epoch} · attempt ${lease.attempt} · lease ${Math.round(lease.ageMs / 100) / 10}s of ${leaseMs / 1000}s` : `task ${taskId}`}
    >
      {shortTask(taskId)}
      {lease && <Epoch n={lease.epoch} highlight={retried && !stale} stale={stale} />}
      {copy && !stale && (
        <span className="font-sans text-[10.5px] font-semibold text-sky-400" title={`straggler: a speculative copy (epoch ${copy.epoch}) runs on ${copy.workerId}; first result wins`}>
          +copy
        </span>
      )}
      {lease && <span className="absolute bottom-0 left-0 h-[2px] bg-leaf-400/60" style={{ width: `${used * 100}%` }} />}
    </span>
  );
}

/** A speculative copy: dashed and sky-blue, a ghost of the straggling lease it races. */
function CopyChip({ taskId, lease, leaseMs }: { taskId: string; lease?: Lease; leaseMs: number }) {
  const copy = lease?.copy;
  const used = copy ? Math.min(1, copy.ageMs / leaseMs) : 0;
  return (
    <span
      className="relative inline-flex h-[22px] shrink-0 items-center gap-1.5 overflow-hidden rounded border border-dashed border-sky-400/70 bg-sky-400/[0.06] pl-1.5 pr-1 font-mono text-[11.5px] text-sky-400"
      title={`speculative copy of task ${taskId}${lease ? `\noriginal: ${lease.workerId}, epoch ${lease.epoch}, running ${Math.round(lease.ageMs / 100) / 10}s` : ''}${copy ? `\ncopy: epoch ${copy.epoch}, running ${Math.round(copy.ageMs / 100) / 10}s` : ''}\nfirst result to commit wins`}
    >
      <span className="font-sans text-[10.5px] font-semibold uppercase tracking-[0.06em]">copy</span>
      {shortTask(taskId)}
      {copy && <Epoch n={copy.epoch} highlight />}
      {copy && <span className="absolute bottom-0 left-0 h-[2px] bg-sky-400/60" style={{ width: `${used * 100}%` }} />}
    </span>
  );
}

/** A fencing epoch. `stale` marks the one that lost: outlined in ember (no strike-through, which makes "e1" read as "e4"). */
export function Epoch({ n, highlight = false, stale = false }: { n: number; highlight?: boolean; stale?: boolean }) {
  const tone = stale
    ? 'border-ember-400/70 text-ember-300'
    : highlight ? 'border-transparent bg-violet-400/20 text-violet-300' : 'border-transparent bg-ink-700 text-ink-300';
  return (
    <span className={`rounded-sm border px-1 font-mono text-[10.5px] font-semibold leading-[14px] ${tone}`} title={stale ? `stale epoch ${n}` : `epoch ${n}`}>
      e{n}
    </span>
  );
}
