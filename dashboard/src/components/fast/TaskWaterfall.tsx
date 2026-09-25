import { useState } from 'react';
import type { SystemSnapshot, TimingStep } from '../../lib/types';
import { fmtInt, fmtMs } from '../../lib/format';
import { Card } from '../ui/Card';

type Kind = 'orchestration' | 'wait' | 'work';

const STEPS: { key: TimingStep; label: string; kind: Kind; what: string }[] = [
  { key: 'dispatchWaitMs', label: 'Dispatch', kind: 'orchestration', what: 'Postgres commit → task ID pushed onto the Redis ready list (push after commit, no 200 ms tick).' },
  { key: 'queueWaitMs', label: 'Queue wait', kind: 'wait', what: 'Sitting in the ready list until a worker is free. This is load, not overhead: more workers shrink it.' },
  { key: 'claimMs', label: 'Claim', kind: 'orchestration', what: 'BLMOVE into the worker’s processing list, then claim-confirm: lease + fencing epoch in one guarded UPDATE.' },
  { key: 'fetchMs', label: 'Fetch photo', kind: 'work', what: 'Worker downloads the photo from MinIO by its content hash.' },
  { key: 'inferMs', label: 'Inference', kind: 'work', what: 'The model itself: MegaDetector or SpeciesNet.' },
  { key: 'uploadMs', label: 'Upload crop', kind: 'work', what: 'Classifier writes the animal crop back to MinIO (detect tasks skip this).' },
  { key: 'completeMs', label: 'Complete', kind: 'orchestration', what: 'Fenced UPDATE (epoch must match) + INSERT … ON CONFLICT DO NOTHING, one round trip.' },
];

const BAR: Record<Kind, string> = {
  orchestration: 'bg-leaf-400',
  work: 'bg-ink-400',
  wait: 'hatch text-ink-600',
};

/** Share of the bar given to queue wait when it would otherwise squash every other step. */
const BROKEN_QUEUE_FRACTION = 0.22;

/** Where a task's time goes, from the coordinator's per-hop timings (p50 and p95 over the last minute). */
export function TaskWaterfall({ timings }: { timings: NonNullable<SystemSnapshot['timings']> }) {
  const [hover, setHover] = useState<TimingStep | null>(null);
  const p50 = (k: TimingStep) => timings.p50[k] ?? 0;
  const queue = p50('queueWaitMs');
  const service = STEPS.reduce((s, st) => s + (st.key === 'queueWaitMs' ? 0 : p50(st.key)), 0) || 1;
  const orchestration = p50('dispatchWaitMs') + p50('claimMs') + p50('completeMs');
  const overhead = Number.isFinite(timings.overheadPct) ? timings.overheadPct : (orchestration / service) * 100;
  // Queue wait is load, not cost; when it dwarfs the rest, give it a fixed, visibly broken bar
  // so the service steps stay to scale with each other.
  const broken = queue > service * 0.35;
  const queueFrac = broken ? BROKEN_QUEUE_FRACTION : queue / (service + queue);
  const perMs = (1 - queueFrac) / service;
  const active = STEPS.find((s) => s.key === hover);

  let offset = 0;
  return (
    <Card
      title="Where a task's time goes"
      caption="The coordinator's own work (green) is a sliver next to the model; the rest is waiting for a free worker."
      aside={`p50 · p95 of ${fmtInt(timings.samples)} tasks, last ${timings.windowSec} s`}
    >
      <div className="flex gap-5">
        <div className="flex w-[132px] shrink-0 flex-col">
          <div className="text-[44px] font-semibold leading-none tracking-tight text-leaf-400">
            {overhead < 10 ? overhead.toFixed(1) : Math.round(overhead)}%
          </div>
          <div className="mt-1.5 text-[13px] leading-snug text-ink-100">of service time is orchestration</div>
          <div className="mt-1 text-[12px] leading-snug text-ink-400">
            {fmtMs(orchestration)} of {fmtMs(service)} per task; queue wait excluded
          </div>
        </div>

        <div className="min-w-0 flex-1" onMouseLeave={() => setHover(null)}>
          {STEPS.map((s) => {
            const v = p50(s.key);
            const width = s.key === 'queueWaitMs' ? queueFrac : v * perMs;
            const left = offset;
            offset += width;
            const dim = hover && hover !== s.key;
            return (
              <div
                key={s.key}
                onMouseEnter={() => setHover(s.key)}
                onFocus={() => setHover(s.key)}
                onBlur={() => setHover(null)}
                tabIndex={0}
                className={`grid h-[17px] cursor-default outline-none focus-visible:bg-ink-800 grid-cols-[84px_1fr_50px_50px] items-center text-[12.5px] transition-opacity ${dim ? 'opacity-40' : ''}`}
              >
                <span className={s.kind === 'orchestration' ? 'text-ink-100' : 'text-ink-400'}>{s.label}</span>
                <span className="relative h-full border-l border-ink-700">
                  <span
                    className={`absolute top-1/2 h-[10px] -translate-y-1/2 rounded-[2px] ${BAR[s.kind]}`}
                    style={{ left: `${left * 100}%`, width: `max(3px, ${width * 100}%)` }}
                  />
                  {s.key === 'queueWaitMs' && broken && (
                    <span className="absolute top-1/2 h-[16px] w-[5px] -translate-y-1/2 skew-x-[-20deg] border-x border-ink-400 bg-ink-900" style={{ left: `${(left + width / 2) * 100}%` }} />
                  )}
                </span>
                <span className="text-right tabular text-ink-100">{fmtMs(v)}</span>
                <span className="text-right tabular text-ink-500">{fmtMs(timings.p95[s.key])}</span>
              </div>
            );
          })}
          <p className="mt-1 flex h-[32px] flex-wrap content-start items-center gap-x-3 text-[12px] leading-snug text-ink-400">
            {active ? (
              <span><b className="font-semibold text-ink-100">{active.label}.</b> {active.what}</span>
            ) : (
              <>
                <Key cls={BAR.orchestration} label="orchestration" />
                <Key cls={BAR.work} label="work" />
                <Key cls={BAR.wait} label={broken ? 'waiting (bar not to scale)' : 'waiting'} />
                <span className="text-ink-500">· hover a step</span>
              </>
            )}
          </p>
        </div>
      </div>
    </Card>
  );
}

const Key = ({ cls, label }: { cls: string; label: string }) => (
  <span className="inline-flex items-center gap-1.5">
    <span className={`h-2.5 w-3.5 shrink-0 rounded-[2px] ${cls}`} />
    {label}
  </span>
);
