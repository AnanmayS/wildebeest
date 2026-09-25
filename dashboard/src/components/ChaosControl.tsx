import { useEffect, useState } from 'react';
import type { Chaos } from '../lib/types';

/** Chaos mode: the coordinator SIGKILLs a random worker every N seconds. */
export function ChaosControl({ chaos, onChange }: { chaos: Chaos; onChange: (next: Chaos) => void }) {
  const [every, setEvery] = useState(String(chaos.killEverySec));
  useEffect(() => setEvery(String(chaos.killEverySec)), [chaos.killEverySec]);

  const seconds = Math.max(3, Number(every) || 20);
  const on = chaos.enabled;

  return (
    <div
      className={`flex items-center gap-3 rounded-lg border py-1 pl-3 pr-1 transition-colors ${
        on ? 'border-ember-400/60 bg-ember-400/10' : 'border-ink-800 bg-ink-850'
      }`}
    >
      <span className={`flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.14em] ${on ? 'text-ember-400' : 'text-ink-400'}`}>
        {on && <span className="h-2 w-2 rounded-full bg-ember-400 animate-pulse-dot" />}
        Chaos mode
      </span>
      <label className="flex items-center gap-1 text-xs text-ink-400">
        kill every
        <input
          type="number"
          min={3}
          value={every}
          onChange={(e) => setEvery(e.target.value)}
          onBlur={() => on && seconds !== chaos.killEverySec && onChange({ enabled: true, killEverySec: seconds })}
          className="h-7 w-12 rounded-md border border-ink-700 bg-ink-900 px-1.5 text-center text-sm tabular text-ink-100 outline-none focus:border-ink-500"
        />
        s
      </label>
      <button
        role="switch"
        aria-checked={on}
        aria-label="Chaos mode"
        onClick={() => onChange({ enabled: !on, killEverySec: seconds })}
        className={`relative h-7 w-12 rounded-md transition-colors ${on ? 'bg-ember-400' : 'bg-ink-700 hover:bg-ink-600'}`}
      >
        <span className={`absolute top-1 h-5 w-5 rounded-[5px] bg-ink-950 transition-all ${on ? 'left-6' : 'left-1'}`} />
      </button>
    </div>
  );
}
