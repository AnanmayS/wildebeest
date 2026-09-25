import { useEffect, useRef, useState } from 'react';
import type { LiveJob } from './useWildebeest';
import type { SystemSnapshot } from '../lib/types';

export interface ThroughputPoint { t: number; images: number; detect?: number; classify?: number }

/**
 * Images finalised per second over the last two minutes. v2 coordinators send the series in
 * `system.throughput`; for older ones we record the job's own 5-second throughput client-side.
 */
export function useThroughputSeries(system: SystemSnapshot | null, job: LiveJob | null): ThroughputPoint[] {
  const [local, setLocal] = useState<ThroughputPoint[]>([]);
  const latest = useRef(job);
  latest.current = job;

  const hasServerSeries = !!system?.throughput?.length;
  useEffect(() => {
    if (hasServerSeries) return;
    const id = setInterval(() => {
      const j = latest.current;
      const images = j?.status === 'running' ? j.throughput : 0;
      setLocal((cur) => [...cur, { t: Date.now(), images }].slice(-120));
    }, 1000);
    return () => clearInterval(id);
  }, [hasServerSeries]);

  if (hasServerSeries) {
    return system!.throughput!.map((p) => ({ t: Date.parse(p.t), images: p.images, detect: p.detect, classify: p.classify }));
  }
  return local;
}
