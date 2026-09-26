import type { ReactNode } from 'react';
import type { Live, LiveJob } from '../../hooks/useWildebeest';
import type { Stage, Worker } from '../../lib/types';
import { workerStory } from '../../lib/workerStory';
import { fmtInt, fmtRate } from '../../lib/format';
import { fmtSeconds } from '../../lib/proof';
import { Num } from '../Num';
import { FlowArrow, useNewIdCount } from './FlowArrow';
import { WorkerTile } from './WorkerTile';

/** A crashed tile stays up this long so the crash is visible, then leaves the picture. */
const SHOW_CRASHED_MS = 60_000;

/**
 * Photos in → Finding animals → Naming species → Sorted. The workers are simple tiles
 * (busy / idle / crashed / frozen); photos move along the arrows only on real claims and results.
 */
export function StoryPipeline({ live, now }: { live: Live; now: number }) {
  const { job, workers } = live;
  const tiles = (stage: Stage) => workers
    .filter((w) => w.stage === stage && w.status !== 'STOPPED')
    .map((w) => ({ w, phase: workerStory(w, live.events, now, live.killRequested[w.id], live.pauseRequested[w.id]).phase }))
    .filter(({ w, phase }) => phase !== 'dead' || now - Date.parse(w.diedAt ?? w.lastHeartbeatAt) < SHOW_CRASHED_MS)
    .sort((a, b) => a.w.id.localeCompare(b.w.id));
  const detect = tiles('detect');
  const classify = tiles('classify');

  const intoDetect = useNewIdCount(heldBy(workers, 'detect'));
  const intoClassify = useNewIdCount(heldBy(workers, 'classify'));

  return (
    <section className="card px-6 pb-5 pt-4">
      <Status job={job} now={now} />
      <div className="mt-4 grid grid-cols-[124px_48px_minmax(0,1fr)_48px_minmax(0,1fr)_48px_168px] items-stretch">
        <Column title="Photos in" sub="from camera traps">
          <PhotosIn job={job} />
        </Column>
        <FlowArrow count={intoDetect} />
        <Column title="Finding animals" sub="AI spots animals, people, or nothing">
          <Tiles items={detect} empty="No workers running" />
        </Column>
        <FlowArrow count={intoClassify} />
        <Column title="Naming species" sub="A second AI names the animal">
          <Tiles items={classify} empty="No workers running" />
          <p className="mt-2 text-[12.5px] text-ink-500">Empty photos skip this step.</p>
        </Column>
        <FlowArrow count={job?.processed ?? 0} />
        <Column title="Sorted" sub="ready for researchers">
          <Sorted job={job} />
        </Column>
      </div>
    </section>
  );
}

/** The one line a visitor reads first: what is happening right now. */
function Status({ job, now }: { job: LiveJob | null; now: number }) {
  if (job?.status === 'running') {
    const pct = job.total ? (job.processed / job.total) * 100 : 0;
    const elapsed = job.elapsedMs + (now - job.receivedAt);
    return (
      <div>
        <div className="flex items-baseline gap-3">
          <span className="h-3 w-3 shrink-0 self-center rounded-full bg-leaf-400 animate-pulse-dot" />
          <h2 className="text-[24px] font-semibold tracking-tight">
            Sorting {fmtInt(job.total)} photos
            <span className="font-normal text-ink-300"> · <b className="font-semibold text-ink-100">{fmtInt(job.processed)}</b> done</span>
            {job.throughput > 0 && <span className="font-normal text-ink-300"> · {fmtRate(job.throughput)} a second</span>}
          </h2>
          <span className="ml-auto text-[15px] tabular text-ink-400">{fmtSeconds(elapsed)}</span>
        </div>
        <div className="mt-2.5 h-1.5 overflow-hidden rounded-full bg-ink-800">
          <div className="h-full rounded-full bg-leaf-400 transition-[width] duration-700" style={{ width: `${pct}%` }} />
        </div>
      </div>
    );
  }
  return (
    <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1">
      <h2 className="text-[24px] font-semibold tracking-tight text-ink-100">
        Idle <span className="font-normal text-ink-400">— press</span> Run a live demo
      </h2>
      {job && <LastRun job={job} />}
    </div>
  );
}

function LastRun({ job }: { job: LiveJob }) {
  const cached = job.total > 0 && job.cacheHits === job.total;
  const time = job.elapsedMs < 100 ? 'under 0.1 s' : fmtSeconds(job.elapsedMs);
  return (
    <p className="text-[15px] text-ink-400">
      Last run: <b className="font-semibold text-ink-100">{fmtInt(job.processed)} photos</b> sorted in {time}
      {cached ? ' (all remembered from before)' : ''} · {fmtInt(job.categories.animal)} with animals
    </p>
  );
}

function Column({ title, sub, children }: { title: string; sub: string; children: ReactNode }) {
  return (
    <div className="flex min-w-0 flex-col">
      <h3 className="text-[19px] font-semibold leading-tight tracking-tight">{title}</h3>
      <p className="mt-0.5 truncate text-[13px] leading-snug text-ink-400" title={sub}>{sub}</p>
      <div className="mt-2 flex flex-1 flex-col justify-center">{children}</div>
    </div>
  );
}

function Tiles({ items, empty }: { items: { w: Worker; phase: ReturnType<typeof workerStory>['phase'] }[]; empty: string }) {
  if (!items.length) return <p className="rounded-lg border border-dashed border-ink-700 px-3 py-4 text-center text-[14px] text-ink-500">{empty}</p>;
  return (
    <div className={`flex flex-col ${items.length > 3 ? 'gap-1.5' : 'gap-2'}`}>
      {items.map(({ w, phase }) => <WorkerTile key={w.id} worker={w} phase={phase} compact={items.length > 3} />)}
    </div>
  );
}

function PhotosIn({ job }: { job: LiveJob | null }) {
  if (!job) return <p className="text-[15px] text-ink-500">None yet</p>;
  const waiting = job.total - job.processed;
  return (
    <div>
      <div className="text-[44px] font-semibold leading-none tabular"><Num value={job.total} /></div>
      <p className="mt-1.5 text-[14px] text-ink-400">
        {job.status === 'running' ? <>{fmtInt(waiting)} still to sort</> : 'photos in the last run'}
      </p>
    </div>
  );
}

function Sorted({ job }: { job: LiveJob | null }) {
  const c = job?.categories;
  const rows = [
    { label: 'Empty', n: c?.empty ?? 0, dot: 'bg-dust-400' },
    { label: 'Animals', n: c?.animal ?? 0, dot: 'bg-leaf-400' },
    { label: 'People', n: (c?.human ?? 0) + (c?.vehicle ?? 0), dot: 'bg-sky-400', title: 'People and vehicles' },
  ];
  return (
    <ul className="flex flex-col gap-2.5">
      {rows.map((r) => (
        <li key={r.label} className="flex items-baseline gap-2.5" title={r.title}>
          <span className={`h-2.5 w-2.5 shrink-0 self-center rounded-full ${r.dot}`} />
          <span className="flex-1 text-[16px] text-ink-300">{r.label}</span>
          <span className="text-[30px] font-semibold leading-none tabular"><Num value={r.n} /></span>
        </li>
      ))}
    </ul>
  );
}

function heldBy(workers: Worker[], stage: Stage): string[] {
  return workers.filter((w) => w.stage === stage).flatMap((w) => w.currentTaskIds);
}
