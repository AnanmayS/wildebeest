import type { ReactNode } from 'react';
import type { RecoveryView } from '../../lib/recovery';
import { fmtAgo, fmtMs, fmtSec, shortId } from '../../lib/format';
import { Card } from '../ui/Card';

interface Props {
  recovery: RecoveryView | null;
  workerTimeoutMs: number;
  /** v1 coordinators only detect death by heartbeat timeout. */
  dockerEvents: boolean;
  now: number;
}

/**
 * The latest failure, kill → detected → requeued → reclaimed, drawn to scale against the
 * 6 s heartbeat timeout that used to be the only way to notice a dead worker.
 */
export function RecoveryTimeline({ recovery: r, workerTimeoutMs, dockerEvents, now }: Props) {
  return (
    <Card
      title="Latest failure → recovery"
      caption={dockerEvents
        ? `Docker reports a dead container instantly, so its task is requeued at once, not after a ${fmtSec(workerTimeoutMs)} heartbeat timeout.`
        : `A worker is declared dead after ${fmtSec(workerTimeoutMs)} without a heartbeat; then its task goes back to the head of the queue.`}
      aside={r ? fmtAgo(r.killedAt, now) : undefined}
    >
      {r ? <Timeline r={r} workerTimeoutMs={workerTimeoutMs} /> : (
        <p className="rounded-md border border-dashed border-ink-700 px-3 py-5 text-center text-[13px] text-ink-500">
          No failures yet. Press <b className="text-ember-300">Kill</b> on a busy worker and the recovery is timed here.
        </p>
      )}
    </Card>
  );
}

function Timeline({ r, workerTimeoutMs }: { r: RecoveryView; workerTimeoutMs: number }) {
  const detect = r.detectedAt != null ? r.detectedAt - r.killedAt : null;
  const requeue = r.requeuedAt != null && r.detectedAt != null ? r.requeuedAt - r.detectedAt : null;
  const reclaim = r.reclaimedAt != null && r.requeuedAt != null ? r.reclaimedAt - r.requeuedAt : null;
  const total = r.totalMs ?? (r.reclaimedAt ?? r.requeuedAt ?? r.detectedAt ?? r.killedAt) - r.killedAt;
  const scale = Math.max(workerTimeoutMs, total) * 1.04;
  const pct = (ms: number) => `${(ms / scale) * 100}%`;
  const viaDocker = r.via === 'docker_event';

  return (
    <div>
      <div className="flex items-baseline gap-2">
        <span className="text-[30px] font-semibold leading-none">{fmtMs(total)}</span>
        <span className="text-[13px] text-ink-300">
          from <span className="font-mono">{shortId(r.workerId)}</span> failing to {r.reclaimedAt ? 'its task running again' : r.requeuedAt ? 'its task being requeued' : 'being declared dead'}
        </span>
      </div>

      {/* To scale: this recovery vs the heartbeat timeout alone. */}
      <div key={r.killedAt} className="mt-2.5 grid grid-cols-[112px_1fr] items-center gap-x-3 gap-y-1.5 text-[12px]">
        <span className="text-ink-300">This failure</span>
        <span className="relative h-3">
          <span className="absolute inset-y-0 left-0 flex origin-left animate-grow-x overflow-hidden rounded-r-[2px]" style={{ width: `max(3px, ${pct(total)})` }}>
            <span className="h-full bg-ember-400" style={{ width: `${((detect ?? total) / (total || 1)) * 100}%` }} />
            <span className="h-full flex-1 bg-sun-400" />
          </span>
        </span>
        <span className="text-ink-500">Heartbeat timeout</span>
        <span className="relative h-3">
          <span className="hatch absolute inset-y-0 left-0 rounded-r-[2px] text-ink-600" style={{ width: pct(workerTimeoutMs) }} />
          <span className="absolute top-1/2 -translate-x-full -translate-y-1/2 whitespace-nowrap bg-ink-900 px-1.5 text-[11.5px] text-ink-300" style={{ left: pct(workerTimeoutMs) }}>
            {fmtSec(workerTimeoutMs)} just to notice
          </span>
        </span>
      </div>

      {/* Zoomed in: the steps, with the time each one took. */}
      <ol className="mt-3 flex items-start text-[12px]">
        <Step dot="bg-ember-400" title="Failure" sub={viaDocker ? 'SIGKILL, exit 137' : 'killed or frozen'} />
        <Gap ms={detect} tone="text-ember-300" />
        <Step dot="bg-ember-400" title="Detected" sub={viaDocker ? 'Docker die event' : 'heartbeat timeout'} />
        <Gap ms={requeue} tone="text-sun-300" />
        <Step dot="bg-sun-400" title="Requeued" sub={r.tasks ? `${r.tasks} task${r.tasks > 1 ? 's' : ''} → queue head` : 'nothing in flight'} />
        <Gap ms={reclaim} tone="text-sun-300" />
        <Step
          dot={r.reclaimedAt ? 'bg-leaf-400' : 'bg-ink-600'}
          title="Reclaimed"
          sub={r.reclaimedBy ? <>by <span className="font-mono">{shortId(r.reclaimedBy)}</span></> : r.reclaimedAt ? 'by a live worker' : r.source === 'events' ? 'not reported (v1)' : r.tasks ? 'waiting for a worker' : '—'}
        />
      </ol>
    </div>
  );
}

function Step({ dot, title, sub }: { dot: string; title: string; sub: ReactNode }) {
  return (
    <li className="w-[92px] shrink-0">
      <div className="flex items-center gap-1.5">
        <span className={`h-2.5 w-2.5 rounded-full ${dot}`} />
        <span className="font-semibold text-ink-100">{title}</span>
      </div>
      <div className="mt-0.5 pl-4 leading-snug text-ink-400">{sub}</div>
    </li>
  );
}

function Gap({ ms, tone }: { ms: number | null; tone: string }) {
  return (
    <li className="mt-[5px] flex min-w-0 flex-1 flex-col items-center px-1" aria-hidden={ms == null}>
      <span className="h-px w-full bg-ink-600" />
      <span className={`mt-0.5 text-[11.5px] font-semibold tabular ${ms == null ? 'text-ink-600' : tone}`}>{ms == null ? '?' : `+${fmtMs(ms)}`}</span>
    </li>
  );
}
