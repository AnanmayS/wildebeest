import { useNow } from '../hooks/useNow';
import type { Chaos, Worker } from '../lib/types';
import { ChaosControl } from './ChaosControl';
import { WorkerCard } from './WorkerCard';

/** Dead workers stay as full cards for a minute, then shrink to chips so chaos mode can't flood the grid. */
const FULL_CARD_AFTER_DEATH_MS = 60_000;

interface Props {
  workers: Worker[];
  killRequested: Record<string, number>;
  workerTimeoutMs: number;
  chaos: Chaos;
  onKill: (id: string) => void;
  onChaos: (next: Chaos) => void;
}

export function WorkerGrid({ workers, killRequested, workerTimeoutMs, chaos, onKill, onChaos }: Props) {
  const now = useNow(1000);
  const alive = workers.filter((w) => w.status === 'ALIVE').length;
  const dead = workers.filter((w) => w.status === 'DEAD').length;

  return (
    <section className="panel flex min-h-0 flex-1 flex-col px-5 pb-4 pt-4">
      <div className="flex items-center justify-between gap-4">
        <div className="flex items-baseline gap-3">
          <h2 className="text-lg font-semibold tracking-tight">Workers</h2>
          <span className="text-sm tabular text-ink-400">
            <span className="text-leaf-400">{alive} alive</span>
            {dead > 0 && <span className="text-ember-400"> · {dead} dead</span>}
          </span>
        </div>
        <ChaosControl chaos={chaos} onChange={onChaos} />
      </div>

      <div className="scroll-thin -mr-2 mt-3 min-h-0 flex-1 space-y-4 overflow-y-auto pr-2">
        <Group title="Detectors" subtitle="MegaDetector · stage 1" stage="detect" {...{ workers, killRequested, workerTimeoutMs, now, onKill }} />
        <Group title="Classifiers" subtitle="SpeciesNet · stage 2" stage="classify" {...{ workers, killRequested, workerTimeoutMs, now, onKill }} />
      </div>
    </section>
  );
}

interface GroupProps {
  title: string;
  subtitle: string;
  stage: Worker['stage'];
  workers: Worker[];
  killRequested: Record<string, number>;
  workerTimeoutMs: number;
  now: number;
  onKill: (id: string) => void;
}

function Group({ title, subtitle, stage, workers, killRequested, workerTimeoutMs, now, onKill }: GroupProps) {
  // Workers that shut down cleanly (SIGTERM, scale-down) aren't news; only live and dead ones are shown.
  const mine = workers.filter((w) => w.stage === stage && w.status !== 'STOPPED').sort(byRegistration);
  // diedAt when the coordinator sends it; otherwise the last heartbeat is a close enough proxy.
  const longDead = (w: Worker) =>
    w.status !== 'ALIVE' && now - Date.parse(w.diedAt ?? w.lastHeartbeatAt) > FULL_CARD_AFTER_DEATH_MS;
  const cards = mine.filter((w) => !longDead(w));
  const chips = mine.filter(longDead);

  return (
    <div>
      <div className="mb-2 flex items-baseline gap-2">
        <h3 className="eyebrow text-ink-300">{title}</h3>
        <span className="text-[11px] text-ink-600">{subtitle}</span>
      </div>
      {mine.length === 0 ? (
        <p className="rounded-lg border border-dashed border-ink-800 px-3 py-4 text-sm text-ink-500">
          No {title.toLowerCase()} registered. Start some with <code className="font-mono text-ink-300">docker compose up --scale {stage === 'detect' ? 'detector' : 'classifier'}=N</code>
        </p>
      ) : (
        <div className="grid grid-cols-[repeat(auto-fill,minmax(264px,1fr))] gap-2">
          {cards.map((w) => (
            <WorkerCard key={w.id} worker={w} killSentAt={killRequested[w.id]} workerTimeoutMs={workerTimeoutMs} now={now} onKill={onKill} />
          ))}
        </div>
      )}
      {chips.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1.5">
          {chips.map((w) => (
            <span key={w.id} className="rounded-md border border-ember-400/25 bg-ember-600/10 px-2 py-0.5 font-mono text-[11px] text-ember-300/80">
              {w.id.replace(/^(detect|classify)-/, '')} · dead · {w.reassignedCount} reassigned
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

/** Oldest first so cards keep their place; id order if registeredAt is missing. */
function byRegistration(a: Worker, b: Worker): number {
  if (a.registeredAt && b.registeredAt && a.registeredAt !== b.registeredAt) return a.registeredAt < b.registeredAt ? -1 : 1;
  return a.id.localeCompare(b.id);
}
