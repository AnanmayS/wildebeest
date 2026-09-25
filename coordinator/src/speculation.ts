import { config } from "./config.js";
import { query } from "./db.js";
import { wake, type Stage } from "./dispatcher.js";
import { hub } from "./events.js";
import { getRedis, keys } from "./redis.js";
import { percentile } from "./telemetry.js";

// Straggler speculation (report item #11, docs/decisions/p3-speculation.md).
//
// At the tail of a job the ready queue is empty and some workers sit idle while a slow one (a
// CPU container next to a fast MPS worker, a throttled or thrashing container) still holds the
// last tasks. This loop offers such a task to the fastest idle worker as a speculative copy; the
// first result to commit wins and the loser is told to cancel (migration 007 has the attempt
// model and the fencing rules).
//
// Policy, per stage, every SPECULATE_INTERVAL_MS:
//   - only when the stage has no PENDING task (nothing left to hand out) and a worker is idle;
//   - only for LEASED tasks older than max(SPECULATE_MIN_MS, SPECULATE_MULTIPLIER × stage p50);
//   - at most one copy per task, never on the worker holding the lease;
//   - fastest idle worker first (per-worker p50 service time: the pool is heterogeneous);
//   - workers on probation (p50 > SPECULATE_PROBATION_MULTIPLIER × stage p50) get no copies.
//
// Service times are kept here, in memory (like the rest of the telemetry): completions of the
// last 10 minutes, the last 200 per stage and the last 20 per worker. "Service time" is the
// coordinator's view, claimed → complete handled, the same thing a task's age is measured in.

const STAGES: Stage[] = ["detect", "classify"];
const HORIZON_MS = 10 * 60_000;
const STAGE_SAMPLES = 200;
const WORKER_SAMPLES = 20;
const WORKER_MIN_SAMPLES = 3;
/** A worker that didn't pick up its offer isn't offered another for this long. */
const OFFER_COOLDOWN_MS = 30_000;

interface Sample {
  at: number;
  ms: number;
}

function p50(samples: Sample[], at: number) {
  const recent = samples.filter((s) => s.at >= at - HORIZON_MS).map((s) => s.ms).sort((a, b) => a - b);
  return { p50: Math.round(percentile(recent, 50)), samples: recent.length };
}

class ServiceTimes {
  private stages: Record<Stage, Sample[]> = { detect: [], classify: [] };
  private workers = new Map<string, { stage: Stage; samples: Sample[] }>();

  record(workerId: string, stage: Stage, ms: number, at = Date.now()) {
    const s = { at, ms: Math.max(0, ms) };
    const st = this.stages[stage];
    st.push(s);
    if (st.length > STAGE_SAMPLES) st.splice(0, st.length - STAGE_SAMPLES);
    let w = this.workers.get(workerId);
    if (!w) this.workers.set(workerId, (w = { stage, samples: [] }));
    w.stage = stage;
    w.samples.push(s);
    if (w.samples.length > WORKER_SAMPLES) w.samples.splice(0, w.samples.length - WORKER_SAMPLES);
  }

  stage(stage: Stage, at = Date.now()) {
    return p50(this.stages[stage], at);
  }

  worker(workerId: string, at = Date.now()) {
    const w = this.workers.get(workerId);
    return w ? { stage: w.stage, ...p50(w.samples, at) } : null;
  }

  /** Workers with a completion in the horizon (older entries are forgotten: workers come and go). */
  workerIds(at = Date.now()) {
    for (const [id, w] of this.workers) {
      if (w.samples[w.samples.length - 1].at < at - HORIZON_MS) this.workers.delete(id);
    }
    return [...this.workers.keys()];
  }

  reset() {
    this.stages = { detect: [], classify: [] };
    this.workers.clear();
  }
}

export const serviceTimes = new ServiceTimes();
const cooldown = new Map<string, number>();

/** Test hook. */
export function resetSpeculation() {
  serviceTimes.reset();
  cooldown.clear();
}

/** Called by the reaper when an offer expired unclaimed: that worker isn't taking offers. */
export function offerIgnored(workerId: string, at = Date.now()) {
  cooldown.set(workerId, at + OFFER_COOLDOWN_MS);
}

export interface Probation {
  workerId: string;
  stage: Stage;
  p50ServiceMs: number;
  stageP50ServiceMs: number;
}

/**
 * Workers whose recent p50 service time is more than SPECULATE_PROBATION_MULTIPLIER × their stage's
 * p50. They keep their normal work (probation isn't a punishment, it's a routing hint) but never
 * receive speculative copies: a copy on a slow worker would just be a second straggler.
 */
export function probation(at = Date.now()): Probation[] {
  const out: Probation[] = [];
  for (const id of serviceTimes.workerIds(at)) {
    const w = serviceTimes.worker(id, at)!;
    const st = serviceTimes.stage(w.stage, at);
    if (w.samples < WORKER_MIN_SAMPLES || st.samples < config.speculateMinSamples || st.p50 <= 0) continue;
    if (w.p50 > config.speculateProbationMultiplier * st.p50) {
      out.push({ workerId: id, stage: w.stage, p50ServiceMs: w.p50, stageP50ServiceMs: st.p50 });
    }
  }
  return out;
}

/** The age a LEASED task must reach before it may be speculated, or null when there is no baseline yet. */
export function thresholdMs(stage: Stage, at = Date.now()): { thresholdMs: number; stageP50Ms: number } | null {
  const st = serviceTimes.stage(stage, at);
  if (st.samples < config.speculateMinSamples) return null;
  return { thresholdMs: Math.max(config.speculateMinMs, config.speculateMultiplier * st.p50), stageP50Ms: st.p50 };
}

export interface Offer {
  taskId: string;
  stage: Stage;
  workerId: string;
  originalWorker: string;
  epoch: number;
  ageMs: number;
}

/**
 * One pass of the policy. Returns the offers made. An offer reserves the copy's epoch
 * (tasks.spec_epoch) and a task_attempts row in state 'offered' for one worker; the worker takes
 * it with its next claim (hybrid: the ID is RPUSHed to spec:{workerId}, which workers LMOVE from
 * before the shared queue; postgres: its long-poll is woken). Unclaimed offers are dropped by the
 * reaper after SPECULATE_OFFER_TTL_MS.
 */
export async function speculateOnce(at = Date.now()): Promise<Offer[]> {
  if (config.speculation !== "on") return [];
  const offers: Offer[] = [];
  const onProbation = new Set(probation(at).map((p) => p.workerId));
  for (const stage of STAGES) {
    const th = thresholdMs(stage, at);
    if (!th) continue;

    // Nothing left to hand out? (PENDING includes tasks waiting in Redis and retries in backoff.)
    const { rows: pending } = await query(
      `select exists (select 1 from tasks where state = 'PENDING' and stage = $1) as any`,
      [stage],
    );
    if (pending[0].any) continue;

    // Idle: alive, heartbeating, holding no lease and no copy/offer.
    const { rows: idle } = await query<{ id: string }>(
      `select w.id from workers w
        where w.status = 'ALIVE' and w.stage = $1
          and w.last_heartbeat_at > now() - ($2::int * interval '1 millisecond')
          and not exists (select 1 from tasks t where t.worker_id = w.id and t.state = 'LEASED')
          and not exists (select 1 from task_attempts a where a.worker_id = w.id and a.state in ('offered', 'running'))`,
      [stage, 2 * config.heartbeatMs],
    );
    const speed = (id: string) => {
      const w = serviceTimes.worker(id, at);
      return w && w.samples >= WORKER_MIN_SAMPLES ? w.p50 : Number.POSITIVE_INFINITY;
    };
    const targets = idle
      .map((r) => r.id)
      .filter((id) => !onProbation.has(id) && (cooldown.get(id) ?? 0) <= at)
      .sort((a, b) => speed(a) - speed(b) || a.localeCompare(b));
    if (targets.length === 0) continue;

    // Stragglers, oldest first; never speculated before (an offer nobody took doesn't count).
    const { rows: stragglers } = await query<{ id: string; worker_id: string; lease_epoch: number; age_ms: number }>(
      `select t.id, t.worker_id, t.lease_epoch, (extract(epoch from (now() - t.started_at)) * 1000)::int as age_ms
         from tasks t
        where t.state = 'LEASED' and t.stage = $1
          and t.started_at < now() - ($2::int * interval '1 millisecond')
          and not exists (select 1 from task_attempts a
                           where a.task_id = t.id and (a.state <> 'dropped' or a.started_at is not null))
        order by t.started_at, t.id
        limit $3`,
      [stage, Math.round(th.thresholdMs), targets.length],
    );

    const used = new Set<string>();
    for (const s of stragglers) {
      const target = targets.find((id) => !used.has(id) && id !== s.worker_id);
      if (!target) break;
      const offer = await makeOffer(stage, s, target, th);
      if (!offer) continue;
      used.add(target);
      offers.push(offer);
    }
  }
  if (offers.length > 0) await deliver(offers);
  return offers;
}

async function makeOffer(
  stage: Stage,
  s: { id: string; worker_id: string; lease_epoch: number; age_ms: number },
  target: string,
  th: { thresholdMs: number; stageP50Ms: number },
): Promise<Offer | null> {
  const detail = {
    ageMs: s.age_ms,
    thresholdMs: Math.round(th.thresholdMs),
    stageP50Ms: th.stageP50Ms,
    multiplier: config.speculateMultiplier,
    originalWorker: s.worker_id,
    originalEpoch: s.lease_epoch,
    targetP50Ms: serviceTimes.worker(target)?.p50 ?? null,
  };
  // Guarded: still the same lease, not on the target, and no copy launched or offered meanwhile.
  const { rows } = await query<{ epoch: number }>(
    `with t as (
       update tasks t set spec_epoch = greatest(t.lease_epoch, coalesce(t.spec_epoch, 0)) + 1
        where t.id = $1 and t.state = 'LEASED' and t.lease_epoch = $2 and t.worker_id <> $3
          and not exists (select 1 from task_attempts a
                           where a.task_id = t.id and (a.state <> 'dropped' or a.started_at is not null))
       returning t.id, t.spec_epoch, t.lease_epoch
     )
     insert into task_attempts (task_id, epoch, worker_id, shadow_epoch, state, detail)
     select t.id, t.spec_epoch, $3, t.lease_epoch, 'offered', $4::jsonb from t
     returning epoch`,
    [s.id, s.lease_epoch, target, JSON.stringify(detail)],
  );
  if (rows.length === 0) return null;
  return { taskId: s.id, stage, workerId: target, originalWorker: s.worker_id, epoch: rows[0].epoch, ageMs: s.age_ms };
}

async function deliver(offers: Offer[]) {
  if (config.claimMode === "hybrid") {
    const pipe = getRedis().pipeline();
    for (const o of offers) pipe.rpush(keys.spec(o.workerId), o.taskId);
    await pipe.exec();
  } else {
    wake([...new Set(offers.map((o) => o.stage))]);
  }
  hub.workersChanged();
}

/** `system.speculation`: durable counts (from task_events) plus the probation list. */
export async function speculationSummary() {
  const { rows } = await query<{ type: string; n: number }>(
    `select type, count(*)::int as n from task_events
      where type in ('speculated', 'speculation_won', 'speculation_wasted') group by type`,
  );
  const n = (type: string) => rows.find((r) => r.type === type)?.n ?? 0;
  const flagged = probation();
  const { rows: live } = await query<{ running: number; alive: string[] }>(
    `select (select count(*)::int from task_attempts where state = 'running') as running,
            array(select id from workers where status = 'ALIVE' and id = any($1::text[])) as alive`,
    [flagged.map((p) => p.workerId)],
  );
  const alive = new Set(live[0].alive);
  return {
    launched: n("speculated"),
    won: n("speculation_won"),
    wasted: n("speculation_wasted"),
    running: live[0].running,
    enabled: config.speculation === "on",
    // Live workers only: a dead one's samples linger for the horizon.
    probation: flagged.filter((p) => alive.has(p.workerId)),
  };
}
