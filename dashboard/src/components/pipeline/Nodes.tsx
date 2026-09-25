import type { ReactNode } from 'react';
import type { LiveJob } from '../../hooks/useWildebeest';
import type { SystemSnapshot } from '../../lib/types';
import type { PipelineModel } from '../../lib/pipeline';
import { fmtInt, fmtRate } from '../../lib/format';
import { Num } from '../Num';
import { MiniSpark } from '../fast/ThroughputSpark';
import type { ThroughputPoint } from '../../hooks/useThroughputSeries';

/** A labelled box on the pipeline: eyebrow, one big number, and a line or two of context. */
function Node({ label, children, className = '' }: { label: string; children: ReactNode; className?: string }) {
  return (
    <div className={`self-center rounded-lg border border-ink-700 bg-ink-850 px-3 py-2.5 ${className}`}>
      <div className="eyebrow">{label}</div>
      {children}
    </div>
  );
}

const Big = ({ children }: { children: ReactNode }) => <div className="mt-1 text-[24px] font-semibold leading-none tabular">{children}</div>;
const Sub = ({ children }: { children: ReactNode }) => <div className="mt-1.5 text-[12px] leading-snug text-ink-400">{children}</div>;

export function SourceNode({ job }: { job: LiveJob | null }) {
  return (
    <Node label="Job">
      {job ? (
        <>
          <div className="mt-0.5 truncate font-mono text-[12px] text-ink-300" title={job.name}>{job.name}</div>
          <Big><Num value={job.total} /></Big>
          <Sub>
            photos, hashed
            {job.cacheHits > 0 && <><br /><span className="text-sky-400">{fmtInt(job.cacheHits)} from cache</span></>}
          </Sub>
        </>
      ) : (
        <Sub>No job yet. Load a sample to start.</Sub>
      )}
    </Node>
  );
}

export function DispatcherNode({ pipeline }: { pipeline: PipelineModel }) {
  const d = pipeline.dispatcher;
  const push = d.mode === 'push';
  return (
    <Node label="Dispatcher">
      <div className="mt-0.5 text-[12px] text-ink-300">{d.mode == null ? '200 ms tick' : push ? 'push after commit' : `${d.mode} mode`}</div>
      <Big>
        {d.pushedPerSec != null ? fmtRate(d.pushedPerSec) : '—'}
        <span className="ml-1 text-[13px] font-normal text-ink-500">/s</span>
      </Big>
      <Sub>
        pushed to Redis
        {d.sweepsPerSec != null && <><br />repair sweep {fmtRate(d.sweepsPerSec)}/s{d.lastSweepRepaired ? ` · fixed ${d.lastSweepRepaired}` : ''}</>}
      </Sub>
    </Node>
  );
}

/** The backpressure valve on the detect admission wire: open, or closed while classify is backed up. */
export function Valve({ closed }: { closed: boolean }) {
  return (
    <div className="relative flex flex-col items-center justify-center self-center" title={closed ? 'Detect dispatch paused: classify queue above its high-water mark' : 'Detect dispatch open'}>
      <svg viewBox="0 0 48 28" className="h-7 w-12" aria-hidden>
        <line x1="0" y1="14" x2="48" y2="14" className={closed ? 'stroke-ink-700' : 'stroke-ink-600'} strokeWidth="1" />
        <rect x="19" y="3" width="10" height="22" rx="2" className={closed ? 'fill-sun-400' : 'fill-none stroke-ink-500'} strokeWidth="1.2" />
        {!closed && <line x1="19" y1="14" x2="29" y2="14" className="stroke-leaf-400" strokeWidth="2" />}
      </svg>
      <span className={`mt-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] ${closed ? 'text-sun-300' : 'text-ink-500'}`}>
        {closed ? 'closed' : 'open'}
      </span>
    </div>
  );
}

interface TankProps {
  name: string;
  depth: number | null;
  /** Depth that fills the tank. */
  capacity: number;
  marks: { at: number; label: string; tone: string }[];
  alert?: boolean;
}

/** A Redis ready list drawn as a tank, with its target or watermarks at their real heights. */
export function QueueTank({ name, depth, capacity, marks, alert = false }: TankProps) {
  const fill = depth == null ? 0 : Math.min(1, depth / capacity);
  return (
    <div className="flex min-h-0 flex-col items-center py-1" title={`Redis list queue:${name}`}>
      <div className="text-[10px] uppercase leading-none tracking-[0.12em] text-ink-500">queue</div>
      <div className="font-mono text-[11.5px] leading-tight text-ink-300">{name}</div>
      <div className={`text-[22px] font-semibold leading-tight tabular ${alert ? 'text-sun-300' : ''}`}>{depth ?? '—'}</div>
      <div className="relative mt-1 w-11 flex-1 overflow-hidden rounded-md border border-ink-600 bg-ink-900">
        <div
          className={`absolute inset-x-0 bottom-0 transition-[height] duration-500 ease-out ${alert ? 'bg-sun-400/60' : 'bg-leaf-400/30'}`}
          style={{ height: `${fill * 100}%` }}
        />
        {marks.map((m) => (
          <div key={m.label} className="absolute inset-x-0" style={{ bottom: `${Math.min(1, m.at / capacity) * 100}%` }}>
            <span className={`block h-px w-full ${m.tone}`} />
            <span className="absolute bottom-px right-1 text-[9.5px] leading-none text-ink-100">{m.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

interface StoreProps {
  job: LiveJob | null;
  system: SystemSnapshot | null;
  imagesPerSec: number | null;
  series: ThroughputPoint[];
}

export function StoreNode({ job, system, imagesPerSec, series }: StoreProps) {
  const inv = system?.invariants;
  return (
    <Node label="Result store" className="w-full">
      <Big>
        <Num value={job?.processed ?? 0} />
        {job && <span className="ml-1 text-[13px] font-normal text-ink-500">/ {fmtInt(job.total)}</span>}
      </Big>
      <Sub>
        finalised{imagesPerSec != null && <> · <b className="font-semibold text-ink-100">{fmtRate(imagesPerSec)}</b>/s</>}
      </Sub>
      <MiniSpark series={series} />
      {inv ? (
        <div
          className={`mt-2 flex items-center gap-1.5 rounded border px-1.5 py-1 text-[11px] font-semibold uppercase tracking-[0.08em] ${
            inv.duplicateResults === 0 ? 'border-leaf-400/40 text-leaf-300' : 'border-ember-400/60 text-ember-300'
          }`}
          title="Live check: result rows per image > 1"
        >
          <Check ok={inv.duplicateResults === 0} /> exactly-once · {inv.duplicateResults} dup
        </div>
      ) : (
        <div className="mt-2 font-mono text-[10.5px] leading-snug text-ink-500">ON CONFLICT (sha256, model) DO NOTHING</div>
      )}
    </Node>
  );
}

export const Check = ({ ok }: { ok: boolean }) => (
  <svg viewBox="0 0 12 12" className="h-3 w-3 shrink-0" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden>
    {ok ? <path d="M2 6.5 5 9l5-6" strokeLinecap="round" strokeLinejoin="round" /> : <path d="M3 3l6 6M9 3 3 9" strokeLinecap="round" />}
  </svg>
);
