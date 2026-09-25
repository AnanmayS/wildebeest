import type { Lease, SystemSnapshot, TaskEvent } from '../../lib/types';
import { epochsFromMessage } from '../../lib/workerStory';
import { fmtAgo, fmtInt, shortId, shortTask } from '../../lib/format';
import { Epoch } from '../pipeline/LeaseChip';
import { Card } from '../ui/Card';

interface Props {
  fencing: SystemSnapshot['fencing'] | undefined;
  leases: Lease[] | undefined;
  events: TaskEvent[];
  leaseMs: number;
  now: number;
}

const ROWS = 3;

/** Fencing tokens at work: live leases with their epochs, and the last late write that was refused. */
export function FencingPanel({ fencing, leases, events, leaseMs, now }: Props) {
  // A same-epoch rejection is a worker re-sending a result that had already committed (its first
  // response was lost, e.g. with a coordinator replica): refused, but not the zombie story. Prefer a real one.
  let last: FencedWrite | null = fencing?.last ?? lastFromEvents(events, false);
  if (last && isRepeat(last)) last = lastFromEvents(events, true) ?? last;
  const repeat = !!last && isRepeat(last);
  // fencing.staleRejected counts since the answering replica started; the log may remember older ones.
  const count = Math.max(fencing?.staleRejected ?? 0, events.filter((e) => e.type === 'stale_rejected').length);
  // Retried tasks first (they are the interesting ones), then the oldest.
  const shown = [...(leases ?? [])].sort((a, b) => b.epoch - a.epoch || b.ageMs - a.ageMs).slice(0, ROWS);

  return (
    <Card
      title="Fencing"
      caption="Every claim bumps the task's epoch; a result carrying an old epoch is rejected, so a paused worker can't overwrite its replacement."
      aside={`${fmtInt(count)} stale rejected`}
    >
      {last ? (
        <div className="rounded-md border border-violet-400/30 bg-violet-400/[0.06] px-3 py-2">
          {repeat ? (
            <div className="flex items-center gap-2 text-[13px] leading-snug">
              <span className="font-mono text-ink-100">{shortId(last.workerId)}</span>
              <span className="text-ink-300">re-sent its</span>
              <Epoch n={last.epoch!} />
              <span className="text-ink-300">result after it had already committed</span>
            </div>
          ) : (
            <div className="flex items-center gap-2 text-[13px] leading-snug">
              <span className="font-mono text-ink-100">{shortId(last.workerId)}</span>
              <span className="text-ink-300">woke up holding</span>
              {last.epoch != null ? <Epoch n={last.epoch} stale /> : '?'}
              <span className="text-ink-300">· task now at</span>
              {last.currentEpoch != null ? <Epoch n={last.currentEpoch} highlight /> : '?'}
            </div>
          )}
          <div className="mt-0.5 text-[12px] text-ink-400">
            <b className="font-semibold text-violet-300">409 STALE_LEASE: {repeat ? 'no duplicate written' : 'result rejected'}</b>
            {last.taskId && <> · task <span className="font-mono">{shortTask(last.taskId)}</span></>}
            {last.at && <> · {fmtAgo(last.at, now)}</>}
          </div>
        </div>
      ) : (
        <p className="rounded-md border border-dashed border-ink-700 px-3 py-2.5 text-[12.5px] text-ink-500">
          No stale results yet. <b className="text-sun-300">Pause</b> a busy worker for 20 s to see one fenced off.
        </p>
      )}

      {leases && (
        <table className="mt-2 w-full table-fixed text-[12px]">
          <thead>
            <tr className="text-left text-[10.5px] uppercase tracking-[0.1em] text-ink-500">
              <th className="w-[68px] pb-1 font-medium">task</th>
              <th className="pb-1 font-medium">holder</th>
              <th className="w-[44px] pb-1 font-medium">epoch</th>
              <th className="w-[38%] pb-1 font-medium">lease used</th>
            </tr>
          </thead>
          <tbody>
            {shown.map((l) => (
              <tr key={l.taskId} className="h-[20px]">
                <td className="font-mono text-ink-300">{shortTask(l.taskId)}</td>
                <td className="truncate font-mono text-ink-400">{shortId(l.workerId)}</td>
                <td><Epoch n={l.epoch} highlight={l.epoch > 1} /></td>
                <td>
                  <span className="flex items-center gap-1.5">
                    <span className="h-1.5 flex-1 rounded-sm bg-ink-800">
                      <span className={`block h-full rounded-sm ${l.ageMs > leaseMs * 0.66 ? 'bg-sun-400' : 'bg-leaf-400/60'}`} style={{ width: `${Math.min(100, (l.ageMs / leaseMs) * 100)}%` }} />
                    </span>
                    <span className="w-9 text-right tabular text-ink-500">{(l.ageMs / 1000).toFixed(1)}s</span>
                  </span>
                </td>
              </tr>
            ))}
            {shown.length === 0 && (
              <tr><td colSpan={4} className="py-1 text-ink-500">No leases right now.</td></tr>
            )}
          </tbody>
        </table>
      )}
    </Card>
  );
}

interface FencedWrite { taskId: string; workerId: string; at: string; epoch?: number; currentEpoch?: number }

const isRepeat = (l: FencedWrite) => l.epoch != null && l.epoch === l.currentEpoch;

/** The newest stale_rejected event (epochs parsed from its message if it has no detail); optionally skipping repeats. */
function lastFromEvents(events: TaskEvent[], skipRepeats: boolean): FencedWrite | null {
  for (const e of events) {
    if (e.type !== 'stale_rejected') continue;
    const d = e.detail ?? {};
    const parsed = epochsFromMessage(e.message);
    const w: FencedWrite = {
      taskId: e.taskId ?? '', workerId: e.workerId ?? '?', at: e.at,
      epoch: typeof d.leaseEpoch === 'number' ? d.leaseEpoch : parsed.epoch,
      currentEpoch: typeof d.currentEpoch === 'number' ? d.currentEpoch : parsed.currentEpoch,
    };
    if (!skipRepeats || !isRepeat(w)) return w;
  }
  return null;
}
