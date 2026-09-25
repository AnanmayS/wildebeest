import type { ReactNode } from 'react';
import { EventLog } from './components/EventLog';
import { Gallery } from './components/Gallery';
import { Header } from './components/Header';
import { JobPanel } from './components/JobPanel';
import { SpeciesLeaderboard } from './components/SpeciesLeaderboard';
import { Pipeline } from './components/pipeline/Pipeline';
import { Benchmarks } from './components/fast/Benchmarks';
import { TaskWaterfall } from './components/fast/TaskWaterfall';
import { ThroughputSpark } from './components/fast/ThroughputSpark';
import { StragglerStrip } from './components/fast/StragglerStrip';
import { FailoverStrip } from './components/robust/FailoverStrip';
import { FencingPanel } from './components/robust/FencingPanel';
import { Invariants } from './components/robust/Invariants';
import { RecoveryTimeline } from './components/robust/RecoveryTimeline';
import { useBenchmarks } from './hooks/useBenchmarks';
import { useNow } from './hooks/useNow';
import { useThroughputSeries } from './hooks/useThroughputSeries';
import { useLeaseSightings } from './hooks/useLeaseSightings';
import { useWildebeest } from './hooks/useWildebeest';
import { pipelineModel } from './lib/pipeline';
import { latestRecovery } from './lib/recovery';
import { latestFailover, useHaView } from './lib/ha';

/**
 * Above the fold at 1440×900: how work flows (hero), why it's fast, why it's robust.
 * Below: what the job actually found (funnel, species, events, photos).
 */
export default function App() {
  const live = useWildebeest();
  const benchmarks = useBenchmarks();
  const now = useNow(250);
  const { job, system } = live;

  const pipeline = pipelineModel({
    system, workers: live.workers, queues: live.queues, throttled: live.throttle.throttled, config: live.config,
  });
  const series = useThroughputSeries(system, job);
  const imagesPerSec = series.length ? series.slice(-5).reduce((s, p) => s + p.images, 0) / Math.min(5, series.length) : null;
  const sightings = useLeaseSightings(system);
  const recovery = latestRecovery(system, live.events, sightings);
  const hasBenchParts = !!(benchmarks?.ceiling?.series.length || benchmarks?.recovery || benchmarks?.overhead);
  const hasTimings = !!system?.timings && system.timings.samples > 0;
  const ha = useHaView(live.cluster, system, now);
  const failover = latestFailover(live.events);
  const speculation = system?.speculation?.enabled !== false ? system?.speculation : undefined;
  // Build-time VITE_GRAFANA_URL, or a `grafanaUrl` in GET /config should the coordinator ever serve one.
  const grafanaUrl = live.config.grafanaUrl || import.meta.env.VITE_GRAFANA_URL || null;

  return (
    <div className="mx-auto max-w-[1680px] px-5 pb-10 pt-2">
      <Header
        connection={live.connection}
        job={job}
        chaos={live.chaos}
        onChaos={live.setChaos}
        onStartSample={live.startSample}
        onUploaded={live.focusJob}
        onError={(msg) => live.logLocal('failed', msg)}
        grafanaUrl={grafanaUrl}
      />

      <main className="mt-1.5 flex flex-col gap-2.5">
        <Pipeline live={live} pipeline={pipeline} ha={ha} imagesPerSec={imagesPerSec} series={series} now={now} />

        <div className="grid grid-cols-1 gap-x-3 gap-y-2.5 xl:grid-cols-2">
          <Group title="Why it's fast" tone="text-leaf-400">
            {hasTimings && <TaskWaterfall timings={system!.timings!} />}
            {speculation && <StragglerStrip speculation={speculation} events={live.events} />}
            {benchmarks && hasBenchParts && <Benchmarks data={benchmarks} />}
            {/* The live trend also sits in the result store; it gets its own panel only when there's room. */}
            {!(hasTimings && hasBenchParts) && <ThroughputSpark series={series} />}
          </Group>

          <Group title="Why it's robust" tone="text-violet-300">
            <RecoveryTimeline recovery={recovery} workerTimeoutMs={pipeline.limits.workerTimeoutMs} dockerEvents={pipeline.v2} now={now} />
            <FailoverStrip failover={failover} silent={ha?.leaderSilent ?? null} now={now} />
            <div className={`grid gap-2.5 ${system?.invariants || benchmarks?.faults ? 'grid-cols-[1fr_236px]' : 'grid-cols-1'}`}>
              <FencingPanel fencing={system?.fencing} leases={system?.leases} events={live.events} leaseMs={pipeline.limits.leaseMs} now={now} />
              <Invariants invariants={system?.invariants} faults={benchmarks?.faults} now={now} />
            </div>
          </Group>
        </div>

        <section id="results" className="scroll-mt-4 pt-4">
          <h2 className="mb-2 text-[17px] font-semibold tracking-tight">
            What the job found <span className="ml-2 text-[13px] font-normal text-ink-400">photos sorted, species counted, every event</span>
          </h2>
          <div className="grid grid-cols-1 gap-3 xl:h-[420px] xl:grid-cols-[minmax(0,1fr)_340px_400px]">
            <div className="flex flex-col gap-3">
              <JobPanel job={job} />
            </div>
            <SpeciesLeaderboard species={job?.species ?? []} />
            <EventLog events={live.events} />
          </div>
          <div className="mt-3">
            <Gallery job={job} minBoxConf={live.config.animalConfThreshold} />
          </div>
        </section>
      </main>
    </div>
  );
}

function Group({ title, tone, children }: { title: string; tone: string; children: ReactNode }) {
  return (
    <div className="flex min-w-0 flex-col gap-1.5">
      <h2 className={`text-[12px] font-semibold uppercase leading-4 tracking-[0.16em] ${tone}`}>{title}</h2>
      {children}
    </div>
  );
}
