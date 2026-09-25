import type { TaskEvent } from '../lib/types';
import { timeOfDay } from '../lib/format';

// Colour = meaning: red is loss, amber is recovery/pressure, violet is fencing, blue is cache, green is success.
const STYLE: Record<string, { label: string; cls: string }> = {
  worker_died: { label: 'Worker died', cls: 'text-ember-400 bg-ember-400/12' },
  worker_killed: { label: 'Killed', cls: 'text-ember-300 bg-ember-400/8' },
  failed: { label: 'Failed', cls: 'text-ember-400 bg-ember-400/12' },
  reassigned: { label: 'Reassigned', cls: 'text-sun-400 bg-sun-400/12' },
  lease_expired: { label: 'Lease expired', cls: 'text-sun-400 bg-sun-400/12' },
  throttled: { label: 'Throttled', cls: 'text-sun-300 bg-sun-400/12' },
  unthrottled: { label: 'Unthrottled', cls: 'text-ink-300 bg-ink-800' },
  stale_rejected: { label: 'Stale rejected', cls: 'text-violet-400 bg-violet-400/12' },
  cache_hit: { label: 'Cache hit', cls: 'text-sky-400 bg-sky-400/12' },
  job_done: { label: 'Job done', cls: 'text-leaf-400 bg-leaf-400/12' },
  job_cancelled: { label: 'Cancelled', cls: 'text-ink-300 bg-ink-300/12' },
  chaos_on: { label: 'Chaos', cls: 'text-ember-300 bg-ember-400/8' },
  chaos_off: { label: 'Chaos', cls: 'text-ink-300 bg-ink-800' },
};

export function EventLog({ events }: { events: TaskEvent[] }) {
  return (
    <section className="panel flex min-h-0 flex-1 flex-col">
      <div className="flex items-baseline justify-between px-5 pb-2 pt-4">
        <h2 className="text-lg font-semibold tracking-tight">Event log</h2>
        <span className="text-xs tabular text-ink-500">{events.length ? `${events.length} events` : ''}</span>
      </div>
      {events.length === 0 ? (
        <p className="px-5 pb-5 text-sm text-ink-500">
          Worker deaths, reassignments, stale results, cache hits and backpressure show up here. Try killing a busy worker.
        </p>
      ) : (
        <ol className="scroll-thin min-h-0 flex-1 overflow-y-auto px-3 pb-3">
          {events.map((e) => (
            <EventRow key={e.id} event={e} />
          ))}
        </ol>
      )}
    </section>
  );
}

function EventRow({ event: e }: { event: TaskEvent }) {
  const style = STYLE[e.type] ?? { label: e.type.replace(/_/g, ' '), cls: 'text-ink-300 bg-ink-800' };
  return (
    <li className="animate-slide-in rounded-md px-2 py-1.5">
      <div className="flex items-center gap-2">
        <span className="font-mono text-[11px] tabular text-ink-500">{timeOfDay(e.at)}</span>
        <span className={`rounded px-1.5 py-px text-[10px] font-semibold uppercase tracking-[0.1em] ${style.cls}`}>{style.label}</span>
      </div>
      <p className="mt-0.5 text-[13px] leading-snug text-ink-300">{e.message}</p>
    </li>
  );
}
