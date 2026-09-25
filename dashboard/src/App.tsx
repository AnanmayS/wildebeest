import { EventLog } from './components/EventLog';
import { Gallery } from './components/Gallery';
import { Header } from './components/Header';
import { JobPanel } from './components/JobPanel';
import { SpeciesLeaderboard } from './components/SpeciesLeaderboard';
import { WorkerGrid } from './components/WorkerGrid';
import { useWildebeest } from './hooks/useWildebeest';

export default function App() {
  const live = useWildebeest();
  const { job } = live;
  // Prefer the explicit field; older coordinators only encode it in the name ("sample-1000").
  const lastSampleSize = job ? (job.sampleSize ?? (Number(job.name.match(/^sample-(\d+)$/)?.[1]) || null)) : null;

  return (
    <div className="mx-auto max-w-[1680px] px-6 py-4">
      <Header
        connection={live.connection}
        lastSampleSize={lastSampleSize}
        onStartSample={live.startSample}
        onUploaded={live.focusJob}
        onError={(msg) => live.logLocal('failed', msg)}
      />

      {/* Above the fold at 1440×900: progress, funnel, workers, species and events. */}
      <main className="mt-4 grid grid-cols-1 gap-4 xl:h-[calc(100vh-110px)] xl:min-h-[700px] xl:grid-cols-[minmax(0,1fr)_400px]">
        <div className="flex min-h-0 flex-col gap-4">
          <JobPanel job={job} throttled={live.throttle.throttled} classifyQueue={live.throttle.classifyQueue} />
          <WorkerGrid
            workers={live.workers}
            killRequested={live.killRequested}
            workerTimeoutMs={live.config.workerTimeoutMs}
            chaos={live.chaos}
            onKill={live.killWorker}
            onChaos={live.setChaos}
          />
        </div>
        <div className="flex min-h-0 flex-col gap-4">
          <SpeciesLeaderboard species={job?.species ?? []} />
          <EventLog events={live.events} />
        </div>
      </main>

      <div className="mt-4">
        <Gallery job={job} minBoxConf={live.config.animalConfThreshold} />
      </div>
    </div>
  );
}
