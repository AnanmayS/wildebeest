import { useState } from 'react';
import type { Benchmarks as BenchmarksData, ScalingPoint } from '../../lib/types';
import { fmtInt, fmtMs, fmtRate } from '../../lib/format';
import { Card } from '../ui/Card';

/** Measured results from `make benchmark` (GET /benchmarks). Each part is hidden if the harness didn't produce it. */
export function Benchmarks({ data }: { data: BenchmarksData }) {
  const series = data.ceiling?.series ?? [];
  const befores = [
    data.recovery?.before && data.recovery.after && {
      label: 'Recovery after kill', unit: 'p50', gain: 'faster',
      before: data.recovery.before.p50Ms, after: data.recovery.after.p50Ms,
    },
    data.overhead?.before && data.overhead.after && {
      label: 'Overhead per task', unit: 'ms', gain: 'cheaper',
      before: data.overhead.before.perTaskMs, after: data.overhead.after.perTaskMs,
    },
  ].filter((r): r is { label: string; unit: string; gain: string; before: number; after: number } => !!r);
  if (!series.length && !befores.length) return null;

  return (
    <Card
      title="Measured"
      caption="From the benchmark harness: where throughput bends as workers are added, and what v2 bought."
      aside={<span title={data.machine}>{data.machine && data.machine.length > 34 ? `${data.machine.slice(0, 32)}…` : data.machine}</span>}
    >
      <div className="flex gap-5">
        {series.length > 0 && <ScalingChart series={series} usl={data.ceiling?.usl} />}
        {befores.length > 0 && (
          <div className="flex w-[220px] shrink-0 flex-col gap-3">
            {befores.map((r) => <BeforeAfter key={r.label} {...r} />)}
          </div>
        )}
      </div>
    </Card>
  );
}

const TONE = ['stroke-leaf-400', 'stroke-ink-300', 'stroke-ink-500'];
const DOT = ['fill-leaf-400', 'fill-ink-300', 'fill-ink-500'];
const VW = 400;
const VH = 80;
const PAD = { l: 30, r: 40, t: 6, b: 16 };

/** Throughput vs workers on a log₂ x-axis: linear scaling is a straight climb, the bend is the ceiling. */
function ScalingChart({ series, usl }: { series: { taskMs: number; points: ScalingPoint[] }[]; usl?: { alpha: number; beta: number } }) {
  const [hover, setHover] = useState<number | null>(null);
  const ns = [...new Set(series.flatMap((s) => s.points.map((p) => p.workers)))].sort((a, b) => a - b);
  const maxN = ns.at(-1) ?? 1;
  const maxY = niceCeil(Math.max(...series.flatMap((s) => s.points.map((p) => p.throughput))));
  const x = (n: number) => PAD.l + (Math.log2(n) / Math.log2(maxN || 2)) * (VW - PAD.l - PAD.r);
  const y = (v: number) => PAD.t + (1 - v / maxY) * (VH - PAD.t - PAD.b);
  const headline = series[0];
  const peak = headline.points.reduce((a, b) => (b.throughput > a.throughput ? b : a));
  const ideal = headline.points[0];
  const peakN = usl && usl.beta > 0 ? Math.sqrt((1 - usl.alpha) / usl.beta) : null;
  // Direct labels at the line ends, pushed apart when two lines finish close together.
  const endLabelY: number[] = [];
  series
    .map((s, i) => ({ i, y: y(s.points.at(-1)!.throughput) }))
    .sort((a, b) => a.y - b.y)
    .forEach((l, k, arr) => { endLabelY[l.i] = k ? Math.max(l.y, endLabelY[arr[k - 1].i] + 11) : l.y; });

  return (
    <div className="min-w-0 flex-1">
      <div className="flex items-baseline gap-2">
        <span className="text-[26px] font-semibold leading-none">{fmtInt(peak.throughput)}</span>
        <span className="text-[12.5px] leading-tight text-ink-400">tasks/s ceiling ({headline.taskMs} ms tasks, {peak.workers} workers)</span>
      </div>
      <svg
        viewBox={`0 0 ${VW} ${VH}`}
        className="mt-1 block w-full"
        role="img"
        aria-label="Throughput versus number of workers"
        onMouseLeave={() => setHover(null)}
        onMouseMove={(e) => {
          const r = e.currentTarget.getBoundingClientRect();
          const px = ((e.clientX - r.left) / r.width) * VW;
          setHover(ns.reduce((best, n) => (Math.abs(x(n) - px) < Math.abs(x(best) - px) ? n : best), ns[0]));
        }}
      >
        {[0, maxY / 2, maxY].map((v) => (
          <g key={v}>
            <line x1={PAD.l} x2={VW - PAD.r} y1={y(v)} y2={y(v)} className="stroke-ink-800" />
            <text x={PAD.l - 5} y={y(v) + 3.5} textAnchor="end" className="fill-ink-500 text-[9.5px] tabular">{fmtK(v)}</text>
          </g>
        ))}
        {ns.map((n) => (
          <text key={n} x={x(n)} y={VH - 4} textAnchor="middle" className="fill-ink-500 text-[9.5px] tabular">{n}</text>
        ))}
        {/* Perfectly linear scaling from the 1-worker point, for reference. */}
        <line
          x1={x(ideal.workers)} y1={y(ideal.throughput)}
          x2={x(Math.min(maxN, (maxY / ideal.throughput) * ideal.workers))} y2={y(Math.min(maxY, (ideal.throughput * maxN) / ideal.workers))}
          className="stroke-ink-400" strokeDasharray="3 3"
        />
        {hover != null && <line x1={x(hover)} x2={x(hover)} y1={PAD.t} y2={VH - PAD.b} className="stroke-ink-600" />}
        {series.map((s, i) => {
          const d = s.points.map((p, j) => `${j ? 'L' : 'M'}${x(p.workers)},${y(p.throughput)}`).join('');
          const last = s.points.at(-1)!;
          const labelY = endLabelY[i];
          return (
            <g key={s.taskMs}>
              <path d={d} fill="none" className={TONE[i] ?? TONE[2]} strokeWidth="2" strokeLinejoin="round" />
              {s.points.map((p) => (
                <circle key={p.workers} cx={x(p.workers)} cy={y(p.throughput)} r={hover === p.workers ? 4 : 2.5} className={`${DOT[i] ?? DOT[2]} stroke-ink-900`} strokeWidth="1.5" />
              ))}
              <text x={x(last.workers) + 6} y={labelY + 3.5} className="fill-ink-300 text-[9.5px]">{s.taskMs} ms</text>
            </g>
          );
        })}
      </svg>
      <div className="mt-0.5 h-4 text-[11.5px] tabular text-ink-400">
        {hover != null ? (
          <>
            {hover} workers: {series.map((s) => `${fmtRate(s.points.find((p) => p.workers === hover)?.throughput ?? 0)}/s @ ${s.taskMs} ms`).join(' · ')}
          </>
        ) : (
          <>
            workers, log scale · dashed = linear
            {peakN && usl && <> · USL α {usl.alpha} β {usl.beta}, peak ≈ {Math.round(peakN)}</>}
          </>
        )}
      </div>
    </div>
  );
}

/** Two bars on one scale: the old number in ink, the new one in the accent. */
function BeforeAfter({ label, unit, gain, before, after }: { label: string; unit: string; gain: string; before: number; after: number }) {
  const max = Math.max(before, after) || 1;
  const factor = after > 0 ? before / after : null;
  const fmt = unit === 'ms' ? (v: number) => `${fmtRate(v)} ms` : fmtMs;
  return (
    <div>
      <div className="flex items-baseline justify-between gap-2 whitespace-nowrap text-[12.5px]">
        <span className="text-ink-300">{label}</span>
        {factor && factor > 1 && <span className="font-semibold text-leaf-400">{factor >= 10 ? Math.round(factor) : factor.toFixed(1)}× {gain}</span>}
      </div>
      {[
        { v: before, cls: 'bg-ink-600', tag: 'v1' },
        { v: after, cls: 'bg-leaf-400', tag: 'now' },
      ].map((b) => (
        <div key={b.tag} className="mt-1 flex items-center gap-2 text-[11.5px] tabular">
          <span className="w-7 text-ink-500">{b.tag}</span>
          <span className="h-2 flex-1">
            <span className={`block h-full origin-left animate-grow-x rounded-r-[2px] ${b.cls}`} style={{ width: `${Math.max(2, (b.v / max) * 100)}%` }} />
          </span>
          <span className="w-12 text-right text-ink-100">{fmt(b.v)}</span>
        </div>
      ))}
    </div>
  );
}

function niceCeil(v: number) {
  const mag = 10 ** Math.floor(Math.log10(v || 1));
  return Math.ceil(v / mag) * mag;
}

const fmtK = (v: number) => (v >= 1000 ? `${(v / 1000).toFixed(v % 1000 ? 1 : 0)}k` : String(Math.round(v)));
