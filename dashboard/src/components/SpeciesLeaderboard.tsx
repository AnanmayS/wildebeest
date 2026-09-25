import type { JobSummary } from '../lib/types';
import { Num } from './Num';

const ROWS = 8;
const ROW_H = 30;

/** Live bar chart. Rows are absolutely positioned so re-ranking slides instead of jumping. */
export function SpeciesLeaderboard({ species }: { species: JobSummary['species'] }) {
  const top = species.slice(0, ROWS);
  const max = Math.max(1, ...top.map((s) => s.count));
  const hidden = species.length - top.length;

  return (
    <section className="panel px-5 pb-4 pt-4">
      <div className="flex items-baseline justify-between">
        <h2 className="text-lg font-semibold tracking-tight">Species</h2>
        <span className="text-xs text-ink-500">{species.length ? `${species.length} species${hidden > 0 ? ` · top ${ROWS} shown` : ''}` : ''}</span>
      </div>
      {top.length === 0 ? (
        <p className="mt-2 text-sm text-ink-500" style={{ height: ROWS * ROW_H - 8 }}>
          Species counts appear as classifiers label animals.
        </p>
      ) : (
        <div className="relative mt-3" style={{ height: ROWS * ROW_H }}>
          {top.map((s, i) => (
            <div
              key={s.commonName}
              className="absolute inset-x-0 flex items-center gap-3 transition-transform duration-500 ease-out"
              style={{ transform: `translateY(${i * ROW_H}px)`, height: ROW_H - 6 }}
            >
              <span className="w-[136px] shrink-0 truncate text-sm capitalize text-ink-300" title={s.commonName}>
                {s.commonName}
              </span>
              <div className="relative h-full flex-1 overflow-hidden rounded bg-ink-800/70">
                <div
                  className={`h-full rounded transition-[width] duration-700 ease-out ${i === 0 ? 'bg-leaf-400' : 'bg-leaf-600'}`}
                  style={{ width: `${(s.count / max) * 100}%` }}
                />
              </div>
              <Num value={s.count} className="w-10 shrink-0 text-right text-sm font-semibold" />
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
