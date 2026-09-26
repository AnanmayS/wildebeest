// The three numbers the Story view leads with, from GET /benchmarks (benchmarks/summary.json).
// Each falls back on its own to the README's headline table, so the page never shows a blank or a
// wrong number when the endpoint is missing (older coordinator, harness not run yet).

import type { Benchmarks } from './types';

export interface Proof {
  recovery: { afterMs: number; beforeMs: number; samples: number | null };
  speedup: { factor: number; afterPerSec: number; beforePerSec: number };
  faults: { faults: number; runs: number; violations: number };
  /** False when any number came from the fallback. */
  measured: boolean;
}

/** README "Headline numbers" (benchmarks/ceiling/results.md, benchmarks/faults/results.md). */
const README = {
  recovery: { afterMs: 163, beforeMs: 5562, samples: 20 },
  ceilingAfter: 4480.7,
  faults: { faults: 30, runs: 6, violations: 0 },
};
/**
 * The first version's ceiling: its 200 ms dispatcher tick refilled 50 tasks at a time, so it was flat at
 * ~240 tasks/s from 2 worker loops on (benchmarks/ceiling/results.md). summary.json only carries the
 * current series; a `ceiling.before` series is used if the harness ever writes one.
 */
const CEILING_BEFORE = 240;
/** The README's comparison point: 0 ms tasks at 16 worker loops (the widest point with a tight CI). */
const CEILING_AT_WORKERS = 16;

type Series = { taskMs: number; points: { workers: number; throughput: number }[] }[];

function ceilingOf(series: Series | undefined): number | null {
  const zero = series?.find((s) => s.taskMs === 0)?.points ?? [];
  if (!zero.length) return null;
  return zero.find((p) => p.workers === CEILING_AT_WORKERS)?.throughput ?? Math.max(...zero.map((p) => p.throughput));
}

export function proofFrom(b: Benchmarks | null): Proof {
  const rec = b?.recovery?.before && b.recovery.after
    ? { afterMs: b.recovery.after.p50Ms, beforeMs: b.recovery.before.p50Ms, samples: b.recovery.after.samples }
    : null;
  const after = ceilingOf(b?.ceiling?.series);
  const before = ceilingOf((b?.ceiling as { before?: { series: Series } } | undefined)?.before?.series) ?? CEILING_BEFORE;
  const faults = b?.faults ? { faults: b.faults.faultsInjected, runs: b.faults.runs, violations: b.faults.violations } : null;
  const afterPerSec = after ?? README.ceilingAfter;
  return {
    recovery: rec ?? README.recovery,
    speedup: { factor: afterPerSec / before, afterPerSec, beforePerSec: before },
    faults: faults ?? README.faults,
    measured: !!(rec && after && faults),
  };
}

/** "0.16 s", "5.6 s", "26 ms": seconds with two figures below 1 s, which reads better than "163 ms" to most people. */
export function fmtSeconds(ms: number): string {
  if (ms < 100) return `${Math.round(ms)} ms`;
  if (ms < 1000) return `${(ms / 1000).toFixed(2)} s`;
  if (ms < 10_000) return `${(ms / 1000).toFixed(1)} s`;
  return `${Math.round(ms / 1000)} s`;
}

/** "~19×" above 10, "~2.7×" below: no false precision. */
export const fmtFactor = (f: number) => `~${f >= 10 ? Math.round(f) : f.toFixed(1)}×`;
