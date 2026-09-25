import type { ReactNode } from 'react';
import type { LiveJob } from '../hooks/useWildebeest';
import { useNow } from '../hooks/useNow';
import { useTween } from '../hooks/useTween';
import { fmtDuration, fmtInt, fmtPct, fmtReviewTime } from '../lib/format';
import { Num } from './Num';

interface Props {
  job: LiveJob | null;
}

/** Progress bar + stats on top, the empty/animal/people funnel underneath. */
export function JobPanel({ job }: Props) {
  const now = useNow(250);
  const running = job?.status === 'running';
  const elapsed = job ? job.elapsedMs + (running ? now - job.receivedAt : 0) : 0;
  const total = job?.total ?? 0;
  const c = job?.categories ?? { empty: 0, animal: 0, human: 0, vehicle: 0, failed: 0 };
  const people = c.human + c.vehicle;
  const pct = total ? (job!.processed / total) * 100 : 0;

  return (
    <section className="card px-6 pb-5 pt-4">
      <div className="flex items-end justify-between gap-6">
        <div>
          <div className="eyebrow flex items-center gap-2">
            {job ? (
              <>
                <span>Job</span>
                <span className="font-mono normal-case tracking-normal text-ink-300">{job.name}</span>
                <StatusTag status={job.status} />
              </>
            ) : (
              'No job yet'
            )}
          </div>
          <div className="mt-1 flex items-baseline gap-3">
            <Num value={job?.processed ?? 0} className="text-5xl font-semibold leading-none tracking-tight" />
            <span className="text-2xl font-medium text-ink-500 tabular">/ {fmtInt(total)}</span>
            <span className="text-2xl font-medium text-ink-400 tabular">{job ? `${Math.floor(pct)}%` : ''}</span>
          </div>
        </div>

        <dl className="flex items-end gap-6 pb-1">
          <Stat label="Elapsed" value={fmtDuration(elapsed)} />
          <Stat label="Throughput" value={<Throughput value={running ? job!.throughput : 0} />} unit="img/s" />
          <Stat label="Cache hits" value={<Num value={job?.cacheHits ?? 0} />} />
        </dl>
      </div>

      <ProgressBar job={job} />

      {job?.status === 'done' && <Impact job={job} />}

      <div className="mt-4 grid grid-cols-[1.1fr_auto_1fr_1fr_1fr] items-end gap-6">
        <FunnelStat label="Photos" value={total} tone="text-ink-100" />
        <Arrow />
        <FunnelStat label="Empty" value={c.empty} of={job?.processed} tone="text-dust-400" dot="bg-dust-400" />
        <FunnelStat label="Animals" value={c.animal} of={job?.processed} tone="text-leaf-400" dot="bg-leaf-400" />
        <FunnelStat label="People · vehicles" value={people} of={job?.processed} tone="text-sky-400" dot="bg-sky-400" />
      </div>
    </section>
  );
}

function Stat({ label, value, unit }: { label: string; value: ReactNode; unit?: string }) {
  return (
    <div>
      <dt className="eyebrow">{label}</dt>
      <dd className="mt-1 flex items-baseline gap-1.5">
        <span className="text-2xl font-semibold tabular leading-none">{value}</span>
        {unit && <span className="text-xs text-ink-500">{unit}</span>}
      </dd>
    </div>
  );
}

function Throughput({ value }: { value: number }) {
  const v = useTween(value, 500);
  return <>{v.toFixed(1)}</>;
}

function StatusTag({ status }: { status: 'running' | 'done' | 'cancelled' }) {
  if (status === 'running')
    return (
      <span className="inline-flex items-center gap-1.5 text-leaf-400">
        <span className="h-1.5 w-1.5 rounded-full bg-leaf-400" /> Running
      </span>
    );
  return <span className="text-ink-300">{status === 'done' ? 'Done' : 'Cancelled'}</span>;
}

/** Segmented bar: each finished photo is coloured by its category, so the bar is also the funnel. */
function ProgressBar({ job }: { job: LiveJob | null }) {
  const total = job?.total || 1;
  const c = job?.categories;
  const segments = c
    ? [
        { key: 'empty', n: c.empty, cls: 'bg-dust-400/80' },
        { key: 'animal', n: c.animal, cls: 'bg-leaf-400' },
        { key: 'people', n: c.human + c.vehicle, cls: 'bg-sky-400' },
        { key: 'failed', n: c.failed, cls: 'bg-ember-400' },
      ]
    : [];
  return (
    <div className="relative mt-4 flex h-3 overflow-hidden rounded-full bg-ink-800">
      {segments.map((s) => (
        <div
          key={s.key}
          className={`h-full ${s.cls} transition-[width] duration-700 ease-out`}
          style={{ width: `${(s.n / total) * 100}%` }}
        />
      ))}
    </div>
  );
}

function FunnelStat({ label, value, of, tone, dot }: { label: string; value: number; of?: number; tone: string; dot?: string }) {
  return (
    <div className="min-w-0">
      <div className="eyebrow flex items-center gap-2">
        {dot && <span className={`h-2 w-2 rounded-sm ${dot}`} />}
        {label}
      </div>
      <div className="mt-1 flex items-baseline gap-2">
        <Num value={value} className={`text-4xl font-semibold leading-none tracking-tight ${tone}`} />
        {of !== undefined && of > 0 && <span className="text-sm tabular text-ink-500">{fmtPct((value / of) * 100)}</span>}
      </div>
    </div>
  );
}

const Arrow = () => (
  <svg viewBox="0 0 40 16" className="mb-2 h-4 w-10 text-ink-600" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden>
    <path d="M1 8h36M31 2l6 6-6 6" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);

/** Shown once the job finishes: the whole point of the system, in one sentence. */
function Impact({ job }: { job: LiveJob }) {
  const top = job.species.slice(0, 3).map((s) => `${fmtInt(s.count)} ${s.commonName}`).join(' · ');
  return (
    <div className="mt-4 flex animate-rise items-center justify-between gap-6 rounded-lg border border-leaf-400/25 bg-leaf-400/[0.07] px-5 py-3">
      <p className="text-lg font-medium leading-snug tracking-tight">
        <span className="text-leaf-400">{fmtPct(job.impact.emptyPct)}</span> of photos were empty, saving{' '}
        <span className="text-leaf-400">{fmtReviewTime(job.impact.hoursSaved)}</span> of volunteer review.
      </p>
      <p className="shrink-0 text-right text-xs leading-relaxed text-ink-400">
        Done in {fmtDuration(job.elapsedMs)}
        {job.cacheHits > 0 && <> · {fmtInt(job.cacheHits)} cache hits</>}
        {top && <><br /><span className="capitalize">{top}</span></>}
      </p>
    </div>
  );
}
