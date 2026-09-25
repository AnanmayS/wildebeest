import { useState } from 'react';
import type { ThroughputPoint } from '../../hooks/useThroughputSeries';
import { fmtRate } from '../../lib/format';
import { Card } from '../ui/Card';

const W = 300;
const H = 64;
const SMOOTH = 5; // seconds: 1 s buckets of a few images each are too spiky to read

/** 5 s moving average of images/s: 1 s buckets of a few images each are too spiky to read. */
export function smoothed(series: ThroughputPoint[]): number[] {
  return series.map((_, i) => {
    const win = series.slice(Math.max(0, i - SMOOTH + 1), i + 1);
    return win.reduce((s, p) => s + p.images, 0) / win.length;
  });
}

/** Inline two-minute trend for the result store: no axes, just the shape and the current rate. */
export function MiniSpark({ series }: { series: ThroughputPoint[] }) {
  const values = smoothed(series);
  if (values.length < 2) return null;
  const max = Math.max(1, ...values) * 1.1;
  const d = values.map((v, i) => `${i ? 'L' : 'M'}${((i / (values.length - 1)) * 120).toFixed(1)},${(28 - (v / max) * 28).toFixed(1)}`).join('');
  return (
    <svg viewBox="0 0 120 28" preserveAspectRatio="none" className="mt-1.5 block h-7 w-full" role="img" aria-label="Images finalised per second, last 2 minutes">
      <path d={`${d}L120,28L0,28Z`} className="fill-leaf-400/10" />
      <path d={d} fill="none" className="stroke-leaf-400" strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
    </svg>
  );
}

/** Images finalised per second over the last two minutes, 5 s moving average, with a hover readout. */
export function ThroughputSpark({ series }: { series: ThroughputPoint[] }) {
  const [hover, setHover] = useState<number | null>(null);
  const smooth = smoothed(series);
  const max = Math.max(1, ...smooth) * 1.15;
  const x = (i: number) => (series.length > 1 ? (i / (series.length - 1)) * W : W);
  const y = (v: number) => H - (v / max) * H;
  const line = smooth.map((v, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join('');
  const area = smooth.length ? `${line}L${W},${H}L0,${H}Z` : '';
  const current = smooth.at(-1) ?? 0;
  const peak = Math.max(0, ...smooth);
  const shown = hover != null ? series[hover] : null;

  return (
    <Card
      title="Throughput"
      caption="Photos finalised per second, last 2 minutes."
      aside={shown ? `${Math.round((Date.now() - shown.t) / 1000)} s ago` : `peak ${fmtRate(peak)}/s`}
    >
      <div className="flex items-baseline gap-1.5">
        <span className="text-[30px] font-semibold leading-none">{fmtRate(shown ? smooth[hover!] : current)}</span>
        <span className="text-[13px] text-ink-400">images/s</span>
        {shown?.detect != null && (
          <span className="ml-auto text-[12px] tabular text-ink-400">
            detect {shown.detect} · classify {shown.classify}
          </span>
        )}
      </div>
      <div
        className="relative mt-2"
        onMouseMove={(e) => {
          const r = e.currentTarget.getBoundingClientRect();
          setHover(Math.round(((e.clientX - r.left) / r.width) * (series.length - 1)));
        }}
        onMouseLeave={() => setHover(null)}
      >
        <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="block h-16 w-full overflow-visible" role="img" aria-label={`Throughput, currently ${fmtRate(current)} images per second`}>
          <line x1="0" x2={W} y1={H} y2={H} className="stroke-ink-700" vectorEffect="non-scaling-stroke" />
          <path d={area} className="fill-leaf-400/10" />
          <path d={line} fill="none" className="stroke-leaf-400" strokeWidth="2" strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
          {hover != null && <line x1={x(hover)} x2={x(hover)} y1="0" y2={H} className="stroke-ink-300" vectorEffect="non-scaling-stroke" />}
        </svg>
        <div className="mt-1 flex justify-between text-[10.5px] tabular text-ink-500">
          <span>−2 min</span>
          <span>now</span>
        </div>
      </div>
    </Card>
  );
}
