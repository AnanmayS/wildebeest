import type { Live } from '../../hooks/useWildebeest';
import type { PipelineModel } from '../../lib/pipeline';
import type { HaView } from '../../lib/ha';
import { Coordinators } from './Coordinators';
import type { ThroughputPoint } from '../../hooks/useThroughputSeries';
import { fmtPct } from '../../lib/format';
import { Badge } from '../ui/Badge';
import { LaneGroup } from './LaneGroup';
import { DispatcherNode, QueueTank, SourceNode, StoreNode, Valve } from './Nodes';
import { Wire } from './Wire';

interface Props {
  live: Live;
  pipeline: PipelineModel;
  ha: HaView | null;
  imagesPerSec: number | null;
  series: ThroughputPoint[];
  now: number;
}

// job · wire · dispatcher · valve · queue:detect · detectors · queue:classify · classifiers · store
const COLUMNS = '112px 16px 140px 44px 64px minmax(0,1.3fr) 64px minmax(0,1fr) 150px';

/**
 * The hero: the actual topology, left to right, with live state in every box. Packets on the
 * worker lanes are real lease changes (claims, completions, requeues), not decoration.
 */
export function Pipeline({ live, pipeline, ha, imagesPerSec, series, now }: Props) {
  const { job, system } = live;
  const { queues, limits } = pipeline;
  const c = job?.categories;
  const skipPct = c && job!.processed ? ((c.empty + c.human + c.vehicle) / job!.processed) * 100 : null;
  const lanes = {
    workers: live.workers, events: live.events, pipeline, killRequested: live.killRequested,
    pauseRequested: live.pauseRequested, now, onKill: live.killWorker, onPause: live.pauseWorker,
  };

  return (
    <section className="card px-5 pb-2 pt-2.5">
      <header className="flex items-baseline justify-between gap-4">
        <div className="flex min-w-0 items-baseline gap-3">
          <h2 className="shrink-0 whitespace-nowrap text-[17px] font-semibold tracking-tight">How work flows</h2>
          <p className="caption truncate">
            Each photo is a task: the dispatcher pushes it to Redis, a worker leases it with a fencing epoch, and the result is written once. Dots are real claims and completions.
          </p>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          {ha && ha.nodes.length > 0 && <Coordinators ha={ha} />}
          {pipeline.claimMode && <Badge title="CLAIM_MODE: hybrid = Redis ready queue + Postgres lease; postgres = SKIP LOCKED claim">claim: {pipeline.claimMode}</Badge>}
          {!pipeline.v2 && <Badge tone="sun" title="The coordinator has no GET /system: showing what v1 endpoints provide">v1 coordinator</Badge>}
        </div>
      </header>

      {/* Below ~1200 px the topology scrolls sideways rather than squashing the lanes. */}
      <div className="scroll-thin overflow-x-auto">
        <div className="mt-1 grid min-w-[1180px] gap-x-0 [&>*]:row-start-2" style={{ gridTemplateColumns: COLUMNS, gridTemplateRows: '14px minmax(196px, auto) 18px' }}>
          <FeedbackRail throttled={queues.throttled} high={limits.highWater} />

          <SourceNode job={job} />
          <Wire className="w-full" arrow />
          <DispatcherNode pipeline={pipeline} />
          <div className="flex flex-col items-center">
            <span className={`w-0 flex-1 border-l border-dashed ${queues.throttled ? 'border-sun-400' : 'border-ink-600'}`} />
            <div className="flex w-full items-center">
              <Wire className="flex-1" />
              <Valve closed={queues.throttled} />
              <Wire className="flex-1" arrow />
            </div>
            <span className="flex-1" />
          </div>
          <QueueTank
            name="detect"
            depth={queues.detect}
            capacity={Math.max(limits.detectTarget * 1.15, queues.detect ?? 0)}
            marks={[{ at: limits.detectTarget, label: `max ${limits.detectTarget}`, tone: 'bg-ink-300' }]}
          />
          <LaneGroup stage="detect" title="Detect" model="MegaDetector" {...lanes} />
          <QueueTank
            name="classify"
            depth={queues.classify}
            capacity={limits.highWater * 1.25}
            alert={queues.throttled}
            marks={[
              { at: limits.highWater, label: `high ${limits.highWater}`, tone: 'bg-sun-400' },
              { at: limits.lowWater, label: `low ${limits.lowWater}`, tone: 'bg-ink-300' },
            ]}
          />
          <LaneGroup stage="classify" title="Classify" model="SpeciesNet" {...lanes} />
          <div className="flex items-stretch">
            <span className="my-6 w-px bg-ink-600" />
            <div className="flex flex-1 items-center pl-3">
              <StoreNode job={job} system={system} imagesPerSec={imagesPerSec} series={series} />
            </div>
          </div>

          <BypassRail skipPct={skipPct} />
        </div>
      </div>
    </section>
  );
}

/** Backpressure is a feedback loop: classify depth (right) controls detect admission (left). */
function FeedbackRail({ throttled, high }: { throttled: boolean; high: number }) {
  return (
    <div
      className={`relative rounded-t-md border-x border-t border-dashed ${throttled ? 'border-sun-400' : 'border-ink-600'}`}
      style={{ gridColumn: '4 / 8', gridRow: 1, marginLeft: 22, marginRight: 32, marginTop: 7 }}
    >
      <span className={`absolute left-1/2 top-0 -translate-x-1/2 -translate-y-1/2 whitespace-nowrap bg-ink-900 px-2 text-[11.5px] ${throttled ? 'font-semibold text-sun-300' : 'text-ink-400'}`}>
        {throttled ? `Backpressure engaged: classify queue passed ${high}, detect admission is closed` : `Backpressure: if the classify queue passes ${high}, stop admitting detect work`}
      </span>
    </div>
  );
}

/** Photos with no animal are final after stage 1 and skip the classifier entirely. */
function BypassRail({ skipPct }: { skipPct: number | null }) {
  return (
    <div className="relative rounded-b-md border-x border-b border-ink-600" style={{ gridColumn: '7 / 10', gridRow: 3, marginRight: 70, marginBottom: 8 }}>
      <span className="absolute -right-[4px] -top-[5px] h-0 w-0 border-x-[3.5px] border-b-[5px] border-x-transparent border-b-ink-500" />
      <span className="absolute bottom-0 left-1/2 -translate-x-1/2 translate-y-1/2 whitespace-nowrap bg-ink-900 px-2 text-[11.5px] text-ink-400">
        no animal found → final after detect{skipPct != null && <> · <b className="font-semibold text-ink-300">{fmtPct(skipPct)}</b> skip stage 2</>}
      </span>
    </div>
  );
}
