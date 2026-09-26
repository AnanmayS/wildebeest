import type { Live } from '../../hooks/useWildebeest';
import type { Benchmarks } from '../../lib/types';
import type { LeaseSightings } from '../../lib/recovery';
import type { Demo } from '../../hooks/useDemo';
import { proofFrom } from '../../lib/proof';
import { AnimalStrip } from './AnimalStrip';
import { ProofNumbers } from './ProofNumbers';
import { StoryPipeline } from './StoryPipeline';
import { TryIt } from './TryIt';

interface Props {
  live: Live;
  /** Owned by App so the narration survives a trip to the Engineer view and back. */
  demo: Demo;
  benchmarks: Benchmarks | null;
  sightings: LeaseSightings;
  workerTimeoutMs: number;
  now: number;
}

/**
 * The default view, for a first look: what it is, the pipeline at work, three measured numbers,
 * and buttons to start a job and break it. Plain words only; the Engineer view has the machinery.
 */
export function StoryView({ live, demo, benchmarks, sightings, workerTimeoutMs, now }: Props) {
  const ctx = { job: live.job, jobs: demo.jobs, workers: live.workers, events: live.events, system: live.system, sightings, workerTimeoutMs, now };
  return (
    <main className="mt-2 grid grid-cols-1 gap-3.5 xl:grid-cols-[minmax(0,1fr)_360px]">
      <div className="flex min-w-0 flex-col gap-3.5">
        <header className="max-w-[980px]">
          <h2 className="text-[34px] font-semibold leading-[1.15] tracking-tight">
            Sorts wildlife camera photos across many computers <span className="text-leaf-300">— and keeps going when one crashes.</span>
          </h2>
          <p className="mt-1.5 text-[16px] leading-snug text-ink-400">
            Two AI models find and name the animals. The scheduler that shares out the work and survives failures is built from scratch.
          </p>
        </header>
        <StoryPipeline live={live} now={now} />
        <ProofNumbers proof={proofFrom(benchmarks)} workerTimeoutMs={workerTimeoutMs} />
      </div>
      <TryIt demo={demo} ctx={ctx} />
      <div className="xl:col-span-2">
        <AnimalStrip job={live.job} />
      </div>
    </main>
  );
}
