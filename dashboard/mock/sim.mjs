// The mock coordinator's simulation: jobs, a push dispatcher with a repair sweep, bounded
// ready queues, leases with fencing epochs, backpressure with hysteresis, heterogeneous
// workers (CPU containers + one native MPS process), and the three failure paths the
// dashboard demonstrates:
//
//   kill  -> Docker `die` event ~40 ms later -> task LPUSHed to the queue head -> re-claimed
//   pause -> heartbeats stop -> declared dead after 6 s -> task reclaimed with epoch n+1 ->
//            the frozen worker wakes, posts its result with epoch n -> 409 STALE_LEASE
//   chaos -> a random busy container is killed every N seconds
//   straggler -> one detector is ~8x slower (on probation); at a stage's tail its task gets a
//            speculative copy on the fastest idle worker, and the first result wins
//   failover -> POST /mock/kill-leader: the leader replica stops renewing its 5 s lease, the
//            standby takes over with term + 1 (?mode=pause: the old leader wakes and is fenced)
//
// MOCK_LEGACY=1 turns it back into a v1 coordinator (no /system, no /benchmarks, no pause,
// heartbeat-only death detection) so the dashboard's fallbacks can be checked.

import { randomUUID, randomBytes } from 'node:crypto';
import { imageInfo, round } from './scene.mjs';

export const LEGACY = process.env.MOCK_LEGACY === '1';
const SPEED = Number(process.env.MOCK_SPEED ?? 1);
const REPLACE_KILLED = process.env.MOCK_REPLACE_KILLED !== '0'; // re-scale so a long demo doesn't run dry
const REVIEW_SECONDS_PER_IMAGE = 3;
const STRAGGLER = process.env.MOCK_STRAGGLER !== '0';
const SPECULATION = !LEGACY && process.env.MOCK_SPECULATION !== '0';
const SPEC = { multiplier: 3, minMs: 1000, minSamples: 5, probationMultiplier: 3, intervalMs: 250 };
const LEADER_TTL_MS = 5000;

export const CONFIG = {
  claimMode: 'hybrid', leaseMs: 15000, heartbeatMs: 2000, workerTimeoutMs: 6000,
  // Scaled down from 50 / 500 / 200 so queues and backpressure are visible in a short demo.
  detectQueueTarget: 16, classifyHighWater: 24, classifyLowWater: 8, modelBackend: 'mock',
};

// ---------------------------------------------------------------------------
// State

export const jobs = []; // newest first
export const workers = new Map();
export const chaos = { enabled: false, killEverySec: 20, lastKillAt: 0 };

const backlog = []; // detect tasks PENDING in "Postgres" and not yet pushed (queued = false)
const queues = { detect: [], classify: [] }; // "Redis" ready lists
const seenSha = new Set();
let throttled = false;

const events = { seq: 1, outbox: [], log: [] };
const recovery = []; // oldest first
const fencing = { staleRejected: 0, last: null };
const samples = []; // timings of completed tasks, last 60 s
const done = []; // { at, stage, finals } for throughput, last 120 s
const pushes = []; // dispatcher push timestamps, last 10 s
const sweeps = []; // repair sweep timestamps, last 10 s
const cacheLog = []; // { at, hits, misses }, last 10 min
let lastSweepRepaired = 0;
let invariants = { checkedAt: new Date().toISOString(), duplicateResults: 0, stuckLeases: 0, lostImages: 0, ok: true };
let finalisedTwice = 0;
const speculation = { launched: 0, won: 0, wasted: 0 };

const hex = (n) => randomBytes(n).toString('hex').slice(0, n);
const iso = (ms) => (ms == null ? null : new Date(ms).toISOString());
const between = (a, b) => a + Math.random() * (b - a);
const imgUrl = (idx) => `/api/mock/img/${idx}.svg`;

// ---------------------------------------------------------------------------
// Events

export function emit(type, message, extra = {}) {
  const event = { id: events.seq++, at: new Date().toISOString(), type, message, taskId: null, workerId: null, ...extra };
  if (LEGACY) delete event.detail;
  events.outbox.push(event);
  events.log.push(event);
  if (events.log.length > 500) events.log.shift();
}

export function takeEvents() {
  const out = events.outbox;
  events.outbox = [];
  return out;
}

export const recentEvents = (limit) => events.log.slice(-limit).reverse();

// ---------------------------------------------------------------------------
// Workers

export function addWorker(stage, { runtime = 'container', device = 'cpu', speed = 1, name } = {}) {
  const cid = hex(12);
  const id = name ?? `${stage}-${cid.slice(0, 6)}`;
  workers.set(id, {
    id, stage, runtime, device, speed, containerId: runtime === 'container' ? cid : null,
    status: 'ALIVE', registeredAt: Date.now(), lastHeartbeatAt: Date.now(),
    tasksCompleted: 0, latencies: [], reassignedCount: 0, job: null,
    killedAt: null, deadAt: null, pausedAt: null, pausedUntil: null, zombie: false,
    rssMb: stage === 'detect' ? 780 + Math.round(Math.random() * 80) : 1050 + Math.round(Math.random() * 120),
  });
  return workers.get(id);
}

// Three containers (one of them a throttled straggler, ~6 s per task, unless MOCK_STRAGGLER=0) + the native MPS worker.
for (let i = 0; i < Number(process.env.MOCK_DETECTORS ?? 3) - (STRAGGLER ? 1 : 0); i++) addWorker('detect');
if (STRAGGLER) addWorker('detect', { speed: 0.12 });
if (!LEGACY) addWorker('detect', { runtime: 'native', device: 'mps', speed: 2.6, name: 'detect-mbp-m2' });
for (let i = 0; i < Number(process.env.MOCK_CLASSIFIERS ?? 1); i++) addWorker('classify');

const holdsLease = (w) => !!w.job && !w.job.copy && !w.job.cancelled && w.job.task.holder === w.id && !w.job.task.done;
const runsCopy = (w) => !!w.job && w.job.copy && !w.job.cancelled && w.job.task.copy?.workerId === w.id;

export function workerView(w) {
  const dead = w.status === 'DEAD';
  const holds = holdsLease(w);
  const copying = !dead && runsCopy(w);
  const view = {
    id: w.id, stage: w.stage, status: w.status, state: dead ? 'dead' : holds || copying ? 'busy' : 'idle',
    containerId: w.containerId ?? '', tasksCompleted: w.tasksCompleted,
    currentTaskIds: holds && !dead ? [w.job.task.id] : [],
    currentImageUrl: holds && !dead ? imgUrl(w.job.task.item.info.idx) : null,
    avgLatencyMs: w.latencies.length ? Math.round(w.latencies.reduce((a, b) => a + b, 0) / w.latencies.length) : null,
    rssMb: dead ? null : w.rssMb + Math.round(Math.random() * 20),
    lastHeartbeatAt: iso(w.lastHeartbeatAt), reassignedCount: w.reassignedCount,
    registeredAt: iso(w.registeredAt), diedAt: iso(w.deadAt),
  };
  if (!LEGACY) {
    Object.assign(view, {
      runtime: w.runtime, device: w.device, hostname: w.id.replace(/^(detect|classify)-/, ''),
      killedAt: iso(w.killedAt),
      speculativeTaskIds: copying ? [w.job.task.id] : [],
      probation: !dead && onProbation(w),
      p50ServiceMs: workerP50(w),
    });
  }
  return view;
}

export function visibleWorkers() {
  return [...workers.values()]
    .filter((w) => !w.deadAt || Date.now() - w.deadAt < 10 * 60_000)
    .sort((a, b) => a.stage.localeCompare(b.stage) || a.registeredAt - b.registeredAt)
    .map(workerView);
}

/** POST /workers/:id/kill. Returns an HTTP status + body. */
export function kill(w, source = 'api') {
  if (!LEGACY && w.runtime === 'native') return [409, { error: 'NOT_A_CONTAINER' }];
  if (w.status !== 'ALIVE' || w.killedAt) return [409, { error: 'NOT_ALIVE' }];
  const now = Date.now();
  w.killedAt = now;
  w.pausedAt = w.pausedUntil = null; // SIGKILL on a frozen container still kills it
  emit('worker_killed', `SIGKILL sent to ${w.id}${source === 'chaos' ? ' (chaos)' : ''}`, {
    workerId: w.id, detail: { containerId: w.containerId, source },
  });
  if (!LEGACY) {
    // Docker's `die` event (exit 137) reaches the coordinator's event stream a few ms later.
    const lag = between(18, 60);
    setTimeout(() => w.status === 'ALIVE' && markDead(w, 'docker_event', Date.now() - now), lag);
  }
  if (REPLACE_KILLED) setTimeout(() => addWorker(w.stage), 10_000);
  return [200, { ok: true }];
}

/** POST /workers/:id/pause: freeze the container (docker pause) for `ms`. */
export function pause(w, ms) {
  if (LEGACY) return [404, { error: 'NOT_FOUND' }];
  if (w.runtime === 'native') return [409, { error: 'NOT_A_CONTAINER' }];
  if (w.status !== 'ALIVE' || w.killedAt || w.pausedUntil) return [409, { error: 'NOT_ALIVE' }];
  const now = Date.now();
  w.pausedAt = now;
  w.pausedUntil = now + ms;
  emit('worker_paused', `${w.id} frozen (docker pause) for ${Math.round(ms / 1000)}s`, {
    workerId: w.id, detail: { ms, taskId: w.job?.task.id ?? null, leaseEpoch: w.job?.epoch ?? null },
  });
  return [200, { ok: true, pausedUntil: iso(now + ms) }];
}

function markDead(w, via, detectMs) {
  const now = Date.now();
  w.status = 'DEAD';
  w.deadAt = now;
  const how = via === 'docker_event'
    ? `Docker die event, exit 137; detected in ${Math.round(detectMs)} ms`
    : `no heartbeat for ${((now - w.lastHeartbeatAt) / 1000).toFixed(1)}s`;
  emit('worker_died', `${w.id} died (${how})`, {
    workerId: w.id, detail: { via, detectMs: Math.round(detectMs), stage: w.stage },
  });

  const rec = {
    workerId: w.id, killedAt: iso(w.killedAt ?? w.pausedAt ?? now - detectMs), detectedAt: iso(now), via,
    requeuedAt: null, reclaimedAt: null, reclaimedBy: null, tasks: 0, totalMs: null, detectMs: Math.round(detectMs),
  };
  const t = w.job?.task;
  if (w.job?.copy) {
    // The copy's worker died: the copy is dropped, the original carries on.
    if (t.copy?.workerId === w.id) t.copy = null;
    w.job = null;
    rec.totalMs = now - Date.parse(rec.killedAt);
    recovery.push(rec);
    if (recovery.length > 20) recovery.shift();
    return;
  }
  const copyWorker = t?.copy && workers.get(t.copy.workerId);
  if (t && t.holder === w.id && !t.done && copyWorker && copyWorker.status === 'ALIVE' && !copyWorker.pausedUntil && copyWorker.job?.task === t) {
    // Promotion: the healthy speculative copy becomes the lease. Nothing is requeued or charged.
    t.holder = copyWorker.id;
    t.epoch = t.copy.epoch;
    t.claimedAt = t.copy.startedAt;
    t.copy = null;
    copyWorker.job.copy = false;
    copyWorker.job.promoted = true;
    emit('reassigned', `${w.id} died; its speculative copy on ${copyWorker.id} took over task ${t.id.slice(0, 8)}… (epoch ${t.epoch})`, {
      workerId: w.id, taskId: t.id, detail: { stage: t.stage, promoted: true, to: copyWorker.id, epoch: t.epoch, charged: false },
    });
    rec.totalMs = now - Date.parse(rec.killedAt);
  } else if (t && t.holder === w.id) {
    // Requeue straight to the head of the ready queue (LPUSH after commit), not on the next tick.
    t.holder = null;
    t.attempts++;
    t.pendingAt = now;
    t.dispatchMs = between(1, 3);
    t.recovery = rec;
    queues[t.stage].unshift(t);
    w.reassignedCount++;
    rec.tasks = 1;
    rec.requeuedAt = iso(now + t.dispatchMs);
    emit('reassigned', `${w.id} died; task ${t.id.slice(0, 8)}… requeued at the head of queue:${t.stage} (attempt ${t.attempts + 1})`, {
      workerId: w.id, taskId: t.id, detail: { attempts: t.attempts + 1, stage: t.stage, leaseEpoch: w.job.epoch },
    });
  } else {
    rec.totalMs = now - Date.parse(rec.killedAt);
  }
  if (!w.pausedUntil) w.job = null; // a SIGKILLed container loses its in-memory work; a frozen one keeps it
  recovery.push(rec);
  if (recovery.length > 20) recovery.shift();
}

// ---------------------------------------------------------------------------
// Jobs

export function createJob(name, indices, sampleSize = null) {
  const now = Date.now();
  const job = {
    id: randomUUID(), name, sampleSize, status: 'running', createdAt: iso(now), createdMs: now,
    finishedAt: null, items: [], cacheHits: 0, finalTimes: [],
  };
  for (const idx of indices) {
    const info = imageInfo(idx);
    const item = { id: randomUUID(), info, final: null, finalAt: 0, cacheHit: false };
    job.items.push(item);
    if (seenSha.has(info.sha256)) finalize(job, item, true);
    else backlog.push(newTask(job, item, 'detect', now));
  }
  cacheLog.push({ at: now, hits: job.cacheHits, misses: job.items.length - job.cacheHits });
  if (job.cacheHits > 0) {
    emit('cache_hit', `${job.cacheHits.toLocaleString()} cache hits: ${name} finalised already-seen photos at submit`, {
      detail: { jobId: job.id, count: job.cacheHits, total: job.items.length },
    });
  }
  jobs.unshift(job);
  topUpDetect(now); // push-after-commit
  checkDone(job);
  return job;
}

function newTask(job, item, stage, now) {
  return { id: randomUUID(), job, item, stage, epoch: 0, maxEpoch: 0, copy: null, speculated: false, attempts: 0, holder: null, pendingAt: now, dispatchMs: between(1, 4), recovery: null, done: false };
}

function finalize(job, item, cacheHit = false) {
  if (item.final) finalisedTwice++; // would be an invariant violation; never happens
  item.final = item.info.category;
  item.finalAt = Date.now();
  item.cacheHit = cacheHit;
  if (cacheHit) job.cacheHits++;
  job.finalTimes.push(item.finalAt);
  seenSha.add(item.info.sha256);
}

function checkDone(job) {
  if (job.status === 'done' || job.items.some((it) => !it.final)) return;
  job.status = 'done';
  job.finishedAt = iso(Date.now());
  const s = summary(job);
  emit('job_done', `job ${job.name} done: ${s.total.toLocaleString()} images in ${(s.elapsedMs / 1000).toFixed(1)}s`, {
    detail: { jobId: job.id, name: job.name, total: s.total },
  });
}

export function summary(job) {
  const categories = { empty: 0, animal: 0, human: 0, vehicle: 0, failed: 0 };
  const species = new Map();
  let processed = 0, pendingDetect = 0, pendingClassify = 0;
  for (const it of job.items) {
    if (it.final) {
      processed++;
      categories[it.final]++;
      if (it.final === 'animal') species.set(it.info.commonName, (species.get(it.info.commonName) ?? 0) + 1);
    } else if (it.detected) pendingClassify++;
    else pendingDetect++;
  }
  const now = Date.now();
  const recent = job.finalTimes.filter((t) => t > now - 5000).length;
  const window = Math.min(5, Math.max(1, (now - job.createdMs) / 1000));
  return {
    id: job.id, name: job.name, sampleSize: job.sampleSize, status: job.status, createdAt: job.createdAt, finishedAt: job.finishedAt,
    total: job.items.length, processed, failed: 0, cacheHits: job.cacheHits, categories,
    species: [...species].map(([commonName, count]) => ({ commonName, count })).sort((a, b) => b.count - a.count),
    elapsedMs: (job.finishedAt ? Date.parse(job.finishedAt) : now) - job.createdMs,
    throughput: job.status === 'done' ? 0 : round(recent / window, 1),
    pending: { detect: pendingDetect, classify: pendingClassify },
    throttled, classifyQueue: queues.classify.length,
    impact: {
      emptyPct: processed ? round((categories.empty / processed) * 100, 1) : 0,
      hoursSaved: round((categories.empty * REVIEW_SECONDS_PER_IMAGE) / 3600, 2),
    },
  };
}

export function jobImages(job, { category, species, page, pageSize }) {
  const rows = job.items
    .filter((it) => it.final && (!category || it.final === category) && (!species || (it.final === 'animal' && it.info.commonName === species)))
    .sort((a, b) => b.finalAt - a.finalAt);
  const images = rows.slice((page - 1) * pageSize, page * pageSize).map((it) => ({
    id: it.id, url: imgUrl(it.info.idx), cropUrl: imgUrl(it.info.idx), category: it.final,
    commonName: it.final === 'animal' ? it.info.commonName : null,
    speciesLabel: it.final === 'animal' ? it.info.label : null,
    confidence: it.final === 'animal' ? it.info.confidence : null,
    detections: it.info.detections, cacheHit: it.cacheHit,
  }));
  return { images, total: rows.length, page };
}

// ---------------------------------------------------------------------------
// Dispatcher: push after commit, plus a 200 ms repair sweep that tops the queue up.

function topUpDetect(now) {
  let n = 0;
  while (!throttled && queues.detect.length < CONFIG.detectQueueTarget && backlog.length) {
    queues.detect.push(backlog.shift());
    pushes.push(now);
    n++;
  }
  return n;
}

function checkBackpressure() {
  const depth = queues.classify.length;
  if (!throttled && depth > CONFIG.classifyHighWater) {
    throttled = true;
    emit('throttled', `backpressure on: classify queue ${depth} > ${CONFIG.classifyHighWater}; detect paused`, {
      detail: { classifyQueue: depth, highWater: CONFIG.classifyHighWater, lowWater: CONFIG.classifyLowWater },
    });
    return true;
  }
  if (throttled && depth < CONFIG.classifyLowWater) {
    throttled = false;
    emit('unthrottled', `backpressure off: classify queue ${depth} < ${CONFIG.classifyLowWater}`, {
      detail: { classifyQueue: depth, highWater: CONFIG.classifyHighWater, lowWater: CONFIG.classifyLowWater },
    });
    return true;
  }
  return false;
}

export const throttleState = () => ({ throttled, classifyQueue: queues.classify.length });

// ---------------------------------------------------------------------------
// Worker lifecycle

function serviceTimings(w) {
  const base = w.stage === 'detect' ? between(620, 900) : between(700, 980);
  return {
    claimMs: between(4, 9), fetchMs: between(10, 28), inferMs: base / w.speed / SPEED,
    uploadMs: w.stage === 'classify' ? between(8, 18) : 0, completeMs: between(5, 12),
  };
}

function claim(w, now) {
  let t;
  do t = queues[w.stage].shift();
  while (t && (t.holder || t.done)); // duplicate IDs in a queue are harmless: skip non-PENDING tasks
  if (!t) return;
  t.maxEpoch = (t.maxEpoch ?? 0) + 1; // epochs stay unique per task, copies included
  t.epoch = t.maxEpoch;
  t.copy = null; // a copy shadows one lease; a new claim invalidates it
  t.holder = w.id;
  t.claimedAt = now;
  const timings = serviceTimings(w);
  const total = timings.claimMs + timings.fetchMs + timings.inferMs + timings.uploadMs + timings.completeMs;
  w.job = { task: t, epoch: t.epoch, startedAt: now, endsAt: now + total, timings };
  if (t.recovery) {
    const rec = t.recovery;
    rec.reclaimedAt = iso(now + timings.claimMs);
    rec.reclaimedBy = w.id;
    rec.totalMs = Math.round(now + timings.claimMs - Date.parse(rec.killedAt));
    t.recovery = null;
  }
  if (w.stage === 'detect') topUpDetect(now);
}

function complete(w, now) {
  const { task: t, epoch, timings, startedAt, copy, cancelled, promoted } = w.job;
  w.job = null;
  // The other attempt won while this one was still valid: 409 ALREADY_DONE, result discarded, no event.
  if (cancelled) return;
  if (copy) {
    if (t.done || t.copy?.workerId !== w.id || !t.holder) return; // dropped copy: fenced, nothing to record
    // The copy's result commits first: it wins, and the original is told to cancel.
    const original = workers.get(t.holder);
    if (original?.job?.task === t) original.job.cancelled = true;
    speculation.won++;
    emit('speculation_won', `speculative copy on ${w.id} won task ${t.id.slice(0, 8)}…; ${t.holder} told to cancel`, {
      workerId: w.id, taskId: t.id,
      detail: { stage: t.stage, winner: w.id, epoch, originalWorker: t.holder, originalEpoch: t.epoch, originalAgeMs: now - t.claimedAt, promoted: false },
    });
    t.holder = w.id;
    t.epoch = epoch;
    t.copy = null;
  } else if (t.holder === w.id && t.epoch === epoch && t.copy) {
    // The original finished first: its copy was wasted.
    const cw = workers.get(t.copy.workerId);
    if (cw?.job?.task === t) cw.job.cancelled = true;
    speculation.wasted++;
    emit('speculation_wasted', `${w.id} finished task ${t.id.slice(0, 8)}… first; speculative copy on ${t.copy.workerId} wasted`, {
      workerId: w.id, taskId: t.id, detail: { stage: t.stage, winner: w.id, epoch, speculativeWorker: t.copy.workerId, speculativeEpoch: t.copy.epoch },
    });
    t.copy = null;
  }
  if (t.holder !== w.id || t.epoch !== epoch) {
    // Fenced: someone else holds a newer epoch (or already finished). The guarded UPDATE matches nothing.
    fencing.staleRejected++;
    fencing.last = { taskId: t.id, workerId: w.id, epoch, currentEpoch: t.epoch, at: iso(now) };
    emit('stale_rejected', `stale result from ${w.id} for task ${t.id.slice(0, 8)}… rejected (epoch ${epoch} ≠ ${t.epoch})`, {
      workerId: w.id, taskId: t.id, detail: { action: 'complete', leaseEpoch: epoch, currentEpoch: t.epoch },
    });
    return;
  }
  // ~0.2% of tasks hit a simulated MinIO timeout: released without spending an attempt.
  if (Math.random() < 0.002 && !t.speculated) {
    t.holder = null;
    t.pendingAt = now;
    queues[t.stage].unshift(t);
    emit('released', `${w.id} released task ${t.id.slice(0, 8)}… (S3 timeout); no attempt spent`, {
      workerId: w.id, taskId: t.id, detail: { reason: 'infra: S3 timeout', stage: t.stage },
    });
    return;
  }
  if (promoted) {
    speculation.won++;
    emit('speculation_won', `speculative copy on ${w.id} finished task ${t.id.slice(0, 8)}… after the original lost its lease`, {
      workerId: w.id, taskId: t.id, detail: { stage: t.stage, winner: w.id, epoch, promoted: true },
    });
  }
  t.holder = null;
  t.done = true;
  const serviceMs = now - startedAt;
  w.latencies.push(serviceMs);
  if (w.latencies.length > 20) w.latencies.shift();
  w.tasksCompleted++;
  samples.push({
    at: now, stage: t.stage, dispatchWaitMs: t.dispatchMs,
    queueWaitMs: Math.max(0, t.claimedAt - t.pendingAt - t.dispatchMs), ...timings,
    totalMs: t.claimedAt - t.pendingAt + serviceMs,
  });

  let finals = 0;
  if (t.stage === 'detect') {
    t.item.detected = true;
    if (t.item.info.category === 'animal') {
      queues.classify.push(newTask(t.job, t.item, 'classify', now)); // pushed straight after commit
      pushes.push(now);
    } else {
      finalize(t.job, t.item);
      finals = 1;
    }
  } else {
    finalize(t.job, t.item);
    finals = 1;
  }
  done.push({ at: now, stage: t.stage, finals });
}

function resume(w, now) {
  emit('worker_resumed', `${w.id} unfrozen after ${((now - w.pausedAt) / 1000).toFixed(0)}s`, {
    workerId: w.id, detail: { ms: now - w.pausedAt },
  });
  // The frozen inference picks up where it stopped; it finishes shortly and posts its (old-epoch) result.
  if (w.job) w.job.endsAt = now + Math.min(Math.max(0, w.job.endsAt - w.pausedAt), 500);
  w.zombie = w.status === 'DEAD';
  w.pausedAt = null;
  w.pausedUntil = null;
}

function reRegister(w, now) {
  // Same hostname -> same worker id; the coordinator releases whatever the old incarnation held.
  w.status = 'ALIVE';
  w.deadAt = null;
  w.zombie = false;
  w.lastHeartbeatAt = now;
}

// ---------------------------------------------------------------------------
// Straggler speculation (docs/decisions/p3-speculation.md), every 250 ms per stage.

const median = (arr) => (arr.length ? [...arr].sort((a, b) => a - b)[Math.floor(arr.length / 2)] : null);
const stageP50 = (stage) => {
  const svc = samples.filter((x) => x.stage === stage).map((x) => x.totalMs - x.queueWaitMs);
  return svc.length >= SPEC.minSamples ? median(svc) : null;
};
function workerP50(w) {
  return w.latencies.length >= 3 ? Math.round(median(w.latencies)) : null;
}
function onProbation(w) {
  const mine = workerP50(w);
  const stage = stageP50(w.stage);
  return mine != null && stage != null && mine > SPEC.probationMultiplier * stage;
}

let lastSpeculation = 0;
function speculate(now) {
  if (!SPECULATION || now - lastSpeculation < SPEC.intervalMs) return;
  lastSpeculation = now;
  for (const stage of ['detect', 'classify']) {
    if (queues[stage].length || (stage === 'detect' && backlog.length)) continue; // still work to hand out
    const p50 = stageP50(stage);
    if (p50 == null) continue;
    const threshold = Math.max(SPEC.minMs, SPEC.multiplier * p50);
    const idle = [...workers.values()]
      .filter((w) => w.stage === stage && w.status === 'ALIVE' && !w.killedAt && !w.pausedUntil && !w.zombie && !w.job && !onProbation(w))
      .sort((a, b) => (workerP50(a) ?? Infinity) - (workerP50(b) ?? Infinity));
    const candidates = [...workers.values()]
      .filter((w) => holdsLease(w) && w.stage === stage && !w.job.task.speculated && now - w.job.task.claimedAt > threshold)
      .map((w) => w.job.task)
      .sort((a, b) => a.claimedAt - b.claimedAt);
    for (const t of candidates) {
      const target = idle.find((w) => w.id !== t.holder);
      if (!target) break;
      idle.splice(idle.indexOf(target), 1);
      t.maxEpoch++;
      t.copy = { workerId: target.id, epoch: t.maxEpoch, startedAt: now };
      t.speculated = true;
      const timings = serviceTimings(target);
      const total = timings.claimMs + timings.fetchMs + timings.inferMs + timings.uploadMs + timings.completeMs;
      target.job = { task: t, epoch: t.maxEpoch, startedAt: now, endsAt: now + total, timings, copy: true };
      speculation.launched++;
      const ageMs = now - t.claimedAt;
      emit('speculated', `task ${t.id.slice(0, 8)}… running ${(ageMs / 1000).toFixed(1)}s on ${t.holder} (threshold ${(threshold / 1000).toFixed(1)}s); speculative copy on ${target.id}`, {
        workerId: target.id, taskId: t.id,
        detail: {
          stage, originalWorker: t.holder, originalEpoch: t.epoch, speculativeWorker: target.id, epoch: t.maxEpoch,
          ageMs, thresholdMs: Math.round(threshold), stageP50Ms: Math.round(p50), multiplier: SPEC.multiplier, targetP50Ms: workerP50(target),
        },
      });
    }
  }
}

// ---------------------------------------------------------------------------
// Coordinator HA: two replicas, a 5 s leader lease renewed every second, terms.

const coords = [
  { id: 'coord-1', instance: randomUUID(), startedAt: iso(Date.now()), lastSeen: Date.now(), alive: true, wakeAt: null },
  { id: 'coord-2', instance: randomUUID(), startedAt: iso(Date.now()), lastSeen: Date.now(), alive: true, wakeAt: null },
];
const lease = { holder: 'coord-1', term: 7, since: Date.now() - 12 * 60_000, renewedAt: Date.now(), electingAt: null };
let lastRenewal = 0;

/** POST /mock/kill-leader: the leader stops (kill) or freezes (pause, wakes after 9 s and is fenced). */
export function killLeader(mode = 'kill') {
  const c = coords.find((x) => x.id === lease.holder);
  if (!c?.alive) return [409, { error: 'NO_LIVE_LEADER' }];
  c.alive = false;
  c.wakeAt = Date.now() + (mode === 'pause' ? 9000 : 60_000); // a killed replica is "restarted" a minute later
  c.paused = mode === 'pause';
  return [200, { ok: true, leader: c.id, term: lease.term, mode }];
}

function tickHa(now) {
  if (now - lastRenewal < 1000) return;
  lastRenewal = now;
  for (const c of coords) {
    if (c.alive) c.lastSeen = now;
    else if (c.wakeAt && now >= c.wakeAt) {
      c.alive = true;
      c.wakeAt = null;
      if (c.paused) {
        // It still thinks it leads: its next guarded statement is refused by the term guard.
        emit('leader_fenced', `stale leader ${c.id} (term ${lease.term - 1}) tried to write; rejected (term ${lease.term} is current)`, {
          detail: { holder: c.id, term: lease.term - 1, currentTerm: lease.term, currentHolder: lease.holder },
        });
      } else {
        c.instance = randomUUID();
        c.startedAt = iso(now);
      }
      c.paused = false;
    }
  }
  const holder = coords.find((c) => c.id === lease.holder);
  if (holder?.alive && !lease.electingAt) {
    lease.renewedAt = now;
    return;
  }
  if (now < lease.renewedAt + LEADER_TTL_MS) return; // the old lease hasn't expired yet
  const next = coords.find((c) => c.alive && c.id !== lease.holder) ?? coords.find((c) => c.alive);
  if (!next) return;
  if (!lease.electingAt) {
    lease.electingAt = now; // acquired; reconcile + first sweep, then the events
    return;
  }
  if (now - lease.electingAt < 150) return;
  const prev = { holder: lease.holder, term: lease.term, renewedAt: lease.renewedAt };
  lease.holder = next.id;
  lease.term++;
  lease.since = lease.electingAt;
  lease.renewedAt = now;
  lease.electingAt = null;
  emitElection(prev, now);
}

function emitElection(prev, now, at) {
  emit('leader_lost', `${prev.holder} lost the lead (term ${prev.term}, lease expired)`, {
    detail: { holder: prev.holder, term: prev.term, reason: 'lease expired', renewedAt: iso(prev.renewedAt), expiresAt: iso(prev.renewedAt + LEADER_TTL_MS) },
  });
  const leaderlessMs = lease.since - prev.renewedAt;
  emit('leader_elected', `${lease.holder} is the leader (term ${lease.term}), took over from ${prev.holder} after ${(leaderlessMs / 1000).toFixed(1)}s`, {
    detail: {
      holder: lease.holder, instance: coords.find((c) => c.id === lease.holder).instance, term: lease.term, since: iso(lease.since),
      previousHolder: prev.holder, previousTerm: prev.term, previousRenewedAt: iso(prev.renewedAt), leaderlessMs,
      reconcileMs: now - lease.since, rebuilt: 2, forgottenReplicas: [], coldStart: false,
      firstSweep: { pushed: queues.detect.length + queues.classify.length, dead: 0, requeued: 0 },
    },
  });
  if (at) for (const e of events.log.slice(-2)) e.at = iso(at);
}

export function clusterView() {
  const now = Date.now();
  const answering = coords.find((c) => c.alive) ?? coords[0];
  const expiresAt = lease.renewedAt + LEADER_TTL_MS;
  return {
    self: { id: answering.id, instance: answering.instance, role: answering.id === lease.holder ? 'leader' : 'follower', term: answering.id === lease.holder ? lease.term : null },
    leader: {
      id: lease.holder, term: lease.term, since: iso(lease.since), renewedAt: iso(lease.renewedAt), expiresAt: iso(expiresAt),
      valid: now < expiresAt, // the row can't know its holder died: it's valid until it expires
    },
    // The leader forgets a replica silent for longer than the lease TTL.
    nodes: coords.filter((c) => now - c.lastSeen < LEADER_TTL_MS + 1000).map((c) => ({
      id: c.id, instance: c.instance, role: c.id === lease.holder ? 'leader' : 'follower', term: c.id === lease.holder ? lease.term : null,
      startedAt: c.startedAt, lastSeenAt: iso(c.lastSeen),
    })),
  };
}

// ---------------------------------------------------------------------------
// Main loop

let lastSweep = 0;
let lastInvariantCheck = 0;

function tick() {
  const now = Date.now();

  if (checkBackpressure() && !throttled) topUpDetect(now);

  if (now - lastSweep >= 200) {
    lastSweep = now;
    sweeps.push(now);
    lastSweepRepaired = topUpDetect(now);
  }

  for (const w of workers.values()) {
    if (w.pausedUntil) {
      if (now < w.pausedUntil) continue; // frozen: no heartbeats, no progress
      resume(w, now);
    }
    if (w.zombie) {
      if (w.job && now < w.job.endsAt) continue;
      if (w.job) complete(w, now); // -> 409 STALE_LEASE
      reRegister(w, now);
    }
    if (w.status !== 'ALIVE' || w.killedAt) continue;
    if (now - w.lastHeartbeatAt >= CONFIG.heartbeatMs) w.lastHeartbeatAt = now;
    if (w.job && now >= w.job.endsAt) complete(w, now);
    if (!w.job) claim(w, now);
  }

  // Reaper backstop: silent for workerTimeoutMs -> DEAD (the only path for a paused container,
  // and for everything in legacy mode).
  for (const w of workers.values()) {
    if (w.status === 'ALIVE' && now - w.lastHeartbeatAt > CONFIG.workerTimeoutMs) {
      markDead(w, 'heartbeat', now - (w.killedAt ?? w.pausedAt ?? w.lastHeartbeatAt));
    }
  }

  // Chaos monkey: kill a random busy container, never the last live one of its stage.
  if (chaos.enabled && now - chaos.lastKillAt > chaos.killEverySec * 1000) {
    const live = [...workers.values()].filter((w) => w.status === 'ALIVE' && !w.killedAt && !w.pausedUntil);
    const victims = live.filter((w) => w.job && w.runtime === 'container' && live.filter((o) => o.stage === w.stage).length > 1);
    if (victims.length) {
      chaos.lastKillAt = now;
      kill(victims[Math.floor(Math.random() * victims.length)], 'chaos');
    }
  }

  if (now - lastInvariantCheck >= 5000) {
    lastInvariantCheck = now;
    const stuck = [...workers.values()].filter((w) => w.status !== 'ALIVE' && w.job?.task.holder === w.id && now - w.deadAt > CONFIG.workerTimeoutMs + 2000).length;
    invariants = { checkedAt: iso(now), duplicateResults: finalisedTwice, stuckLeases: stuck, lostImages: 0, ok: finalisedTwice === 0 && stuck === 0 };
  }

  speculate(now);
  if (!LEGACY) tickHa(now);
  prune(now);
  for (const job of jobs) if (job.status === 'running') checkDone(job);
}

function prune(now) {
  const drop = (arr, keep) => { while (arr.length && !keep(arr[0])) arr.shift(); };
  drop(samples, (s) => s.at > now - 60_000);
  drop(done, (d) => d.at > now - 121_000);
  drop(pushes, (t) => t > now - 10_000);
  drop(sweeps, (t) => t > now - 10_000);
  drop(cacheLog, (c) => c.at > now - 600_000);
}

setInterval(tick, 25);

// ---------------------------------------------------------------------------
// GET /system

const pct = (arr, p) => {
  if (!arr.length) return 0;
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
};
const TIMING_KEYS = ['dispatchWaitMs', 'queueWaitMs', 'claimMs', 'fetchMs', 'inferMs', 'uploadMs', 'completeMs', 'totalMs'];

function timingsView() {
  const p = (q) => Object.fromEntries(TIMING_KEYS.map((k) => [k, round(pct(samples.map((s) => s[k]), q), 1)]));
  let orchestration = 0, service = 0;
  for (const s of samples) {
    orchestration += s.dispatchWaitMs + s.claimMs + s.completeMs;
    service += s.totalMs - s.queueWaitMs;
  }
  return { windowSec: 60, samples: samples.length, p50: p(50), p95: p(95), overheadPct: service ? round((orchestration / service) * 100, 1) : 0 };
}

function stageView(stage, now) {
  const mine = [...workers.values()].filter((w) => w.stage === stage);
  const recent = samples.filter((s) => s.stage === stage);
  return {
    workersAlive: mine.filter((w) => w.status === 'ALIVE' && !w.killedAt).length,
    inFlight: mine.filter((w) => w.job && w.job.task.holder === w.id).length,
    completedPerSec: round(done.filter((d) => d.stage === stage && d.at > now - 10_000).length / 10, 2),
    p50ServiceMs: Math.round(pct(recent.map((s) => s.totalMs - s.queueWaitMs), 50)),
  };
}

function throughputSeries(now) {
  const end = Math.floor(now / 1000);
  const buckets = new Map();
  for (const d of done) {
    const sec = Math.floor(d.at / 1000);
    const b = buckets.get(sec) ?? { detect: 0, classify: 0, images: 0 };
    b[d.stage]++;
    b.images += d.finals;
    buckets.set(sec, b);
  }
  const out = [];
  for (let s = end - 120; s < end; s++) out.push({ t: iso(s * 1000), ...(buckets.get(s) ?? { detect: 0, classify: 0, images: 0 }) });
  return out;
}

export function systemSnapshot() {
  const now = Date.now();
  const leases = [...workers.values()]
    .filter(holdsLease)
    .sort((a, b) => a.job.task.claimedAt - b.job.task.claimedAt)
    .slice(0, 40)
    .map((w) => {
      const t = w.job.task;
      return {
        taskId: t.id, workerId: w.id, stage: w.stage, epoch: t.epoch, ageMs: now - t.claimedAt,
        attempt: t.attempts + 1, imageUrl: imgUrl(t.item.info.idx),
        copy: t.copy ? { workerId: t.copy.workerId, epoch: t.copy.epoch, ageMs: now - t.copy.startedAt } : null,
      };
    });
  const alive = [...workers.values()].filter((w) => w.status === 'ALIVE');
  const hits = cacheLog.reduce((s, c) => s + c.hits, 0);
  const lookups = cacheLog.reduce((s, c) => s + c.hits + c.misses, 0);
  return {
    at: iso(now),
    config: CONFIG,
    dispatcher: { mode: 'push', pushedLast10s: pushes.length, repairSweepsLast10s: sweeps.length, lastSweepRepaired },
    queues: { detect: queues.detect.length, classify: queues.classify.length, throttled },
    stages: { detect: stageView('detect', now), classify: stageView('classify', now) },
    leases,
    timings: timingsView(),
    recovery: recovery.slice(-10),
    fencing,
    invariants,
    throughput: throughputSeries(now),
    cache: { hitsLast10m: hits, hitRatePct: lookups ? round((hits / lookups) * 100, 1) : 0 },
    speculation: {
      ...speculation, enabled: SPECULATION,
      running: [...workers.values()].filter((w) => w.status === 'ALIVE' && runsCopy(w)).length,
      probation: alive.filter(onProbation).map((w) => ({
        workerId: w.id, stage: w.stage, p50ServiceMs: workerP50(w), stageP50ServiceMs: Math.round(stageP50(w.stage)),
      })),
    },
    // As the answering replica last saw it: during a failover it still names the old leader.
    leader: { id: lease.holder, term: lease.term, since: iso(lease.since) },
  };
}

export function metrics() {
  const all = [...workers.values()];
  return {
    throttled, queues: { detect: queues.detect.length, classify: queues.classify.length },
    workers: { alive: all.filter((w) => w.status === 'ALIVE').length, dead: all.filter((w) => w.status === 'DEAD').length },
    recoveryMs: recovery.filter((r) => r.totalMs != null).map((r) => r.totalMs),
    latency: { p50: null, p95: null },
  };
}

// A past pause scenario, so the fencing panel has something to show before the first live demo.
if (!LEGACY && process.env.MOCK_SEED_HISTORY !== '0') {
  const at = Date.now() - 95_000;
  fencing.staleRejected = 1;
  fencing.last = { taskId: randomUUID(), workerId: 'detect-ab12cd', epoch: 3, currentEpoch: 4, at: iso(at) };
  emit('stale_rejected', `stale result from detect-ab12cd for task ${fencing.last.taskId.slice(0, 8)}… rejected (epoch 3 ≠ 4)`, {
    workerId: 'detect-ab12cd', taskId: fencing.last.taskId, detail: { action: 'complete', leaseEpoch: 3, currentEpoch: 4 },
  });
  events.log[events.log.length - 1].at = iso(at);
  // ...and a past coordinator failover (coord-2 -> coord-1, term 6 -> 7), for the robust panel.
  const failAt = Date.now() - 4 * 60_000;
  lease.since = failAt - 160;
  emitElection({ holder: 'coord-2', term: 6, renewedAt: failAt - 5310 }, failAt, failAt);
  events.outbox = [];
}
