import { useEffect, useState } from 'react';
import { api } from '../lib/api';
import type { Benchmarks } from '../lib/types';

/** GET /benchmarks once (and every few minutes): null until the harness has written summary.json. */
export function useBenchmarks(): Benchmarks | null {
  const [data, setData] = useState<Benchmarks | null>(null);
  useEffect(() => {
    const load = () => api.getBenchmarks().then(setData).catch(() => setData(null));
    load();
    const id = setInterval(load, 5 * 60_000);
    return () => clearInterval(id);
  }, []);
  return data;
}
