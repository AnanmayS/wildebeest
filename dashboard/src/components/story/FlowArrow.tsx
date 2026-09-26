import { useEffect, useRef, useState } from 'react';
import { usePackets, Wire } from '../pipeline/Wire';

/**
 * An arrow between two stages. A photo runs along it each time `count` goes up, and only then:
 * the counts are real claims and completions, so a still arrow means nothing is moving.
 */
export function FlowArrow({ count }: { count: number }) {
  const { packets, spawn, remove } = usePackets();
  const prev = useRef(count);
  useEffect(() => {
    const added = count - prev.current;
    prev.current = count;
    const timers: number[] = [];
    // A burst (e.g. 100 cached photos at once) shows as a short train of three, not a hundred.
    for (let i = 0; i < Math.min(added, 3); i++) timers.push(window.setTimeout(() => spawn('fwd'), i * 140));
    return () => timers.forEach(clearTimeout);
  }, [count, spawn]);
  return (
    <div className="flex items-center self-center px-2">
      <Wire className="w-full" arrow packets={packets} onDone={remove} tone="bg-ink-600" />
    </div>
  );
}

/** How many task IDs have ever appeared in `ids` (ignoring the ones present on first render). */
export function useNewIdCount(ids: string[]): number {
  const seen = useRef<Set<string> | null>(null);
  const [n, setN] = useState(0);
  const key = ids.join(',');
  useEffect(() => {
    if (!seen.current) {
      seen.current = new Set(ids);
      return;
    }
    let added = 0;
    for (const id of ids) {
      if (seen.current.has(id)) continue;
      seen.current.add(id);
      added++;
    }
    if (added) setN((x) => x + added);
  }, [key]);
  return n;
}
