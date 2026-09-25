// Dev-only mock of the Wildebeest coordinator's dashboard API (docs/CONTRACTS.md).
// Simulates a job moving through detect -> classify, workers that can be killed,
// reassignment, stale-lease rejections, cache hits, backpressure and chaos mode.
//
//   node mock/server.mjs                 # listens on :3000 (same as the coordinator)
//   PORT=3999 node mock/server.mjs       # then: API_TARGET=http://localhost:3999 npm run dev
//
// Knobs: MOCK_DETECTORS (4), MOCK_CLASSIFIERS (2), MOCK_SPEED (1 = realistic-ish),
// MOCK_AUTOSTART (e.g. 1000 to start a sample job on boot).

import http from 'node:http';
import { randomUUID, randomBytes } from 'node:crypto';
import { WebSocketServer } from 'ws';

const PORT = Number(process.env.PORT ?? 3000);
const SPEED = Number(process.env.MOCK_SPEED ?? 1);
const HEARTBEAT_MS = 2000;
const WORKER_TIMEOUT_MS = 6000;
const HIGH_WATER = 30; // scaled down from 500 so throttling shows up in a short demo
const LOW_WATER = 10;
const REVIEW_SECONDS_PER_IMAGE = 3;

// ---------------------------------------------------------------------------
// Deterministic "sample dataset": image i always has the same content.

const SPECIES = [
  ['zebra', 'plains zebra', 18], ['wildebeest', 'blue wildebeest', 16], ['gazelle', "thomson's gazelle", 12],
  ['buffalo', 'african buffalo', 8], ['impala', 'impala', 7], ['giraffe', 'giraffe', 6],
  ['elephant', 'african elephant', 6], ['warthog', 'common warthog', 5], ['hyena', 'spotted hyena', 4],
  ['lion', 'lion', 3], ['baboon', 'olive baboon', 3], ['ostrich', 'common ostrich', 2],
];
const SPECIES_TOTAL = SPECIES.reduce((s, x) => s + x[2], 0);

function rand01(seed) {
  // mulberry32, one draw
  let t = (seed + 0x6d2b79f5) | 0;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}

function imageInfo(idx) {
  const r = rand01(idx * 7 + 1);
  const category = r < 0.7 ? 'empty' : r < 0.95 ? 'animal' : r < 0.98 ? 'human' : 'vehicle';
  let pick = rand01(idx * 13 + 5) * SPECIES_TOTAL;
  let species = SPECIES[0];
  for (const s of SPECIES) { if ((pick -= s[2]) <= 0) { species = s; break; } }
  const w = 0.18 + rand01(idx * 3 + 2) * 0.3;
  const h = w * (0.7 + rand01(idx * 5 + 3) * 0.4);
  const x = rand01(idx * 11 + 4) * (0.95 - w);
  const y = 0.3 + rand01(idx * 17 + 6) * (0.6 - h * 0.8);
  const conf = 0.55 + rand01(idx * 19 + 7) * 0.44;
  const detections = [];
  if (category !== 'empty') {
    detections.push({ label: category, conf: round(conf, 3), bbox: [x, y, w, Math.min(h, 0.92 - y)].map((v) => round(v, 4)) });
    if (category === 'animal' && rand01(idx * 23) < 0.3) {
      const x2 = Math.min(0.9 - w * 0.7, x + w + 0.03);
      detections.push({ label: 'animal', conf: round(conf * 0.8, 3), bbox: [x2, y + 0.04, w * 0.7, h * 0.7].map((v) => round(v, 4)) });
    }
  } else if (rand01(idx * 29) < 0.2) {
    detections.push({ label: 'animal', conf: 0.08, bbox: [0.6, 0.55, 0.1, 0.1] }); // below threshold (grass)
  }
  return {
    idx, sha256: `sha${idx.toString(16).padStart(8, '0')}`, category, detections,
    label: `mammalia;…;${species[0]}`, commonName: species[1], confidence: round(0.6 + rand01(idx * 31) * 0.39, 3),
  };
}

const round = (v, d) => Math.round(v * 10 ** d) / 10 ** d;

// ---------------------------------------------------------------------------
// State

/** @type {any[]} newest first */
const jobs = [];
const detectQueue = []; // { job, item }
const classifyQueue = [];
const workers = new Map();
const seenSha = new Set();
const chaos = { enabled: false, killEverySec: 20, lastKillAt: 0 };
let throttled = false;
let eventSeq = 1;
let pendingEvents = [];
const eventLog = []; // newest last, for GET /events
const recoveryMs = [];

function emit(type, message, extra = {}) {
  const event = { id: eventSeq++, at: new Date().toISOString(), type, message, taskId: null, workerId: null, ...extra };
  pendingEvents.push(event);
  eventLog.push(event);
  if (eventLog.length > 500) eventLog.shift();
}

function flushEvents() {
  if (!pendingEvents.length) return;
  broadcast({ type: 'task_events', events: pendingEvents });
  pendingEvents = [];
}

function hex(n) { return randomBytes(n).toString('hex').slice(0, n); }

function addWorker(stage) {
  const cid = hex(12);
  const id = `${stage}-${cid.slice(0, 6)}`;
  workers.set(id, {
    id, stage, status: 'ALIVE', containerId: cid, tasksCompleted: 0, current: null, taskEndsAt: 0,
    latencies: [], lastHeartbeatAt: Date.now(), reassignedCount: 0, killedAt: null, deadAt: null, registeredAt: Date.now(),
    rssMb: stage === 'detect' ? 780 + Math.round(Math.random() * 80) : 1350 + Math.round(Math.random() * 120),
  });
}

for (let i = 0; i < Number(process.env.MOCK_DETECTORS ?? 4); i++) addWorker('detect');
for (let i = 0; i < Number(process.env.MOCK_CLASSIFIERS ?? 2); i++) addWorker('classify');

function createJob(name, indices, sampleSize = null) {
  const job = {
    id: randomUUID(), name, sampleSize, status: 'running', createdAt: new Date().toISOString(), createdMs: Date.now(),
    finishedAt: null, items: [], cacheHits: 0, finalTimes: [],
  };
  for (const idx of indices) {
    const info = imageInfo(idx);
    const item = { id: randomUUID(), info, final: null, finalAt: 0, cacheHit: false, taskId: randomUUID(), epoch: 0 };
    job.items.push(item);
    if (seenSha.has(info.sha256)) {
      finalize(job, item, true);
    } else {
      detectQueue.push({ job, item, stage: 'detect' });
    }
  }
  if (job.cacheHits > 0) {
    emit('cache_hit', `${job.cacheHits.toLocaleString()} cache hits — ${name} skipped inference for already-seen photos`);
  }
  jobs.unshift(job);
  checkDone(job);
  return job;
}

function finalize(job, item, cacheHit = false) {
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
  job.finishedAt = new Date().toISOString();
  const s = summary(job);
  emit('job_done', `${job.name} done — ${s.total.toLocaleString()} photos in ${(s.elapsedMs / 1000).toFixed(1)}s, ${s.impact.emptyPct.toFixed(0)}% empty`);
}

function summary(job) {
  const categories = { empty: 0, animal: 0, human: 0, vehicle: 0, failed: 0 };
  const species = new Map();
  let processed = 0;
  let pendingDetect = 0;
  let pendingClassify = 0;
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
    throttled, classifyQueue: classifyQueue.length,
    impact: {
      emptyPct: processed ? round((categories.empty / processed) * 100, 1) : 0,
      hoursSaved: round((categories.empty * REVIEW_SECONDS_PER_IMAGE) / 3600, 2),
    },
  };
}

function workerView(w) {
  const dead = w.status === 'DEAD';
  return {
    id: w.id, stage: w.stage, status: w.status, state: dead ? 'dead' : w.current ? 'busy' : 'idle',
    containerId: w.containerId, tasksCompleted: w.tasksCompleted,
    currentTaskIds: w.current ? [w.current.item.taskId] : [],
    currentImageUrl: w.current && !dead ? imgUrl(w.current.item.info.idx) : null,
    avgLatencyMs: w.latencies.length ? Math.round(w.latencies.reduce((a, b) => a + b, 0) / w.latencies.length) : null,
    rssMb: dead ? null : w.rssMb + Math.round(Math.random() * 20),
    lastHeartbeatAt: new Date(w.lastHeartbeatAt).toISOString(), reassignedCount: w.reassignedCount,
    registeredAt: new Date(w.registeredAt).toISOString(), diedAt: w.deadAt ? new Date(w.deadAt).toISOString() : null,
  };
}

const imgUrl = (idx) => `/api/mock/img/${idx}.svg`;

// ---------------------------------------------------------------------------
// Simulation loop

function taskDuration(stage) {
  const base = stage === 'detect' ? 350 + Math.random() * 350 : 1100 + Math.random() * 700;
  return base / SPEED;
}

function kill(w, source) {
  w.killedAt = Date.now();
  emit('worker_killed', `SIGKILL sent to ${w.id}${source === 'chaos' ? ' (chaos)' : ''}`, { workerId: w.id });
  flushEvents(); // kill events are pushed immediately
}

function tick() {
  const now = Date.now();

  // Backpressure with hysteresis.
  if (!throttled && classifyQueue.length > HIGH_WATER) {
    throttled = true;
    emit('throttled', `Backpressure on — classify queue ${classifyQueue.length} > ${HIGH_WATER}; pausing detect dispatch`);
    broadcast({ type: 'throttle', throttled: true, classifyQueue: classifyQueue.length });
  } else if (throttled && classifyQueue.length < LOW_WATER) {
    throttled = false;
    emit('unthrottled', `Backpressure off — classify queue ${classifyQueue.length} < ${LOW_WATER}; resuming detect dispatch`);
    broadcast({ type: 'throttle', throttled: false, classifyQueue: classifyQueue.length });
  }

  for (const w of workers.values()) {
    if (w.status !== 'ALIVE') continue;

    if (w.killedAt) {
      // SIGKILLed: no more heartbeats. The reaper notices after WORKER_TIMEOUT_MS.
      if (now - w.lastHeartbeatAt > WORKER_TIMEOUT_MS) reap(w, now);
      continue;
    }
    if (now - w.lastHeartbeatAt >= HEARTBEAT_MS) w.lastHeartbeatAt = now;

    if (w.current && now >= w.taskEndsAt) complete(w, now);
    if (!w.current) {
      const q = w.stage === 'detect' ? (throttled ? [] : detectQueue) : classifyQueue;
      const task = q.shift();
      if (task) {
        task.item.epoch++;
        if (task.reassignedAt) {
          recoveryMs.push(now - task.reassignedAt);
          task.reassignedAt = null;
        }
        w.current = task;
        w.startedAt = now;
        w.taskEndsAt = now + taskDuration(w.stage);
      }
    }
  }

  // Chaos monkey: kill a random busy worker every killEverySec, replace it shortly after.
  if (chaos.enabled && now - chaos.lastKillAt > chaos.killEverySec * 1000) {
    const victims = [...workers.values()].filter((w) => w.status === 'ALIVE' && !w.killedAt && w.current);
    if (victims.length) {
      const v = victims[Math.floor(Math.random() * victims.length)];
      chaos.lastKillAt = now;
      kill(v, 'chaos');
      setTimeout(() => addWorker(v.stage), 9000);
    }
  }

  for (const job of jobs) if (job.status === 'running') checkDone(job);
}

function complete(w, now) {
  const { job, item } = w.current;
  w.latencies.push(now - w.startedAt);
  if (w.latencies.length > 20) w.latencies.shift();
  w.tasksCompleted++;
  w.current = null;
  if (w.stage === 'detect') {
    item.detected = true;
    if (item.info.category === 'animal') classifyQueue.push({ job, item, stage: 'classify' });
    else finalize(job, item);
  } else {
    finalize(job, item);
  }
}

function reap(w, now) {
  w.status = 'DEAD';
  w.deadAt = now;
  emit('worker_died', `${w.id} missed heartbeats for ${WORKER_TIMEOUT_MS / 1000}s — marked DEAD`, { workerId: w.id });
  if (w.current) {
    const task = w.current;
    w.current = null;
    w.reassignedCount++;
    task.reassignedAt = now;
    (task.stage === 'detect' ? detectQueue : classifyQueue).unshift(task);
    emit('reassigned', `${w.id} died; ${task.stage} task ${task.item.taskId.slice(0, 8)} reassigned (attempt 2)`, {
      workerId: w.id, taskId: task.item.taskId,
    });
    // Sometimes the "dead" worker was only paused and comes back with a stale lease.
    if (Math.random() < 0.5) {
      setTimeout(() => {
        emit('stale_rejected', `Late result from ${w.id} for task ${task.item.taskId.slice(0, 8)} rejected — lease epoch ${task.item.epoch - 1} < ${task.item.epoch}`, {
          workerId: w.id, taskId: task.item.taskId,
        });
      }, 2500);
    }
  }
}

setInterval(tick, 50);

// ---------------------------------------------------------------------------
// WebSocket fan-out, coalesced to ~5 msgs/s per type.

const wss = new WebSocketServer({ noServer: true });

function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const c of wss.clients) if (c.readyState === 1) c.send(data);
}

setInterval(() => {
  flushEvents();
  const running = jobs.filter((j) => j.status === 'running' || Date.now() - Date.parse(j.finishedAt) < 1500);
  for (const j of running) broadcast({ type: 'job_progress', job: summary(j) });
  broadcast({ type: 'worker_update', workers: visibleWorkers() });
}, 200);

function visibleWorkers() {
  return [...workers.values()]
    .filter((w) => !w.deadAt || Date.now() - w.deadAt < 10 * 60_000)
    .sort((a, b) => a.stage.localeCompare(b.stage) || a.registeredAt - b.registeredAt)
    .map(workerView);
}

// ---------------------------------------------------------------------------
// HTTP

function send(res, status, body, type = 'application/json') {
  res.writeHead(status, { 'content-type': type, 'cache-control': 'no-store' });
  res.end(type === 'application/json' ? JSON.stringify(body) : body);
}

function readBody(req) {
  return new Promise((resolve) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks)));
  });
}

let uploadCursor = 100_000; // uploaded photos get fresh content hashes

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;
  const m = (re) => p.match(re);
  let match;

  try {
    if (req.method === 'GET' && p === '/healthz') return send(res, 200, { ok: true });

    if (req.method === 'GET' && p === '/jobs') return send(res, 200, { jobs: jobs.map(summary) });

    if (req.method === 'POST' && p === '/jobs/sample') {
      const body = JSON.parse((await readBody(req)).toString() || '{}');
      const size = Math.min(2000, Number(body.size) || 1000);
      const job = createJob(`sample-${size}`, Array.from({ length: size }, (_, i) => i), size);
      return send(res, 200, { jobId: job.id });
    }

    if (req.method === 'POST' && p === '/jobs') {
      const raw = (await readBody(req)).toString('latin1');
      const count = (raw.match(/filename="/g) ?? []).length;
      if (!count) return send(res, 400, { error: 'NO_FILES' });
      const job = createJob(`upload-${count}`, Array.from({ length: count }, () => uploadCursor++));
      return send(res, 200, { jobId: job.id });
    }

    if (req.method === 'GET' && (match = m(/^\/jobs\/([^/]+)$/))) {
      const job = jobs.find((j) => j.id === match[1]);
      return job ? send(res, 200, summary(job)) : send(res, 404, { error: 'NOT_FOUND' });
    }

    if (req.method === 'GET' && (match = m(/^\/jobs\/([^/]+)\/images$/))) {
      const job = jobs.find((j) => j.id === match[1]);
      if (!job) return send(res, 404, { error: 'NOT_FOUND' });
      const category = url.searchParams.get('category');
      const sp = url.searchParams.get('species');
      const page = Math.max(1, Number(url.searchParams.get('page') ?? 1));
      const pageSize = Math.min(200, Number(url.searchParams.get('pageSize') ?? 48));
      const rows = job.items
        .filter((it) => it.final && (!category || it.final === category) && (!sp || (it.final === 'animal' && it.info.commonName === sp)))
        .sort((a, b) => b.finalAt - a.finalAt);
      const images = rows.slice((page - 1) * pageSize, page * pageSize).map((it) => ({
        id: it.id, url: imgUrl(it.info.idx), cropUrl: imgUrl(it.info.idx), category: it.final,
        commonName: it.final === 'animal' ? it.info.commonName : null,
        speciesLabel: it.final === 'animal' ? it.info.label : null,
        confidence: it.final === 'animal' ? it.info.confidence : null,
        detections: it.info.detections, cacheHit: it.cacheHit,
      }));
      return send(res, 200, { images, total: rows.length, page });
    }

    if (req.method === 'GET' && p === '/workers') return send(res, 200, { workers: visibleWorkers() });

    if (req.method === 'POST' && (match = m(/^\/workers\/([^/]+)\/kill$/))) {
      const w = workers.get(match[1]);
      if (!w) return send(res, 404, { error: 'NOT_FOUND' });
      if (w.status !== 'ALIVE' || w.killedAt) return send(res, 409, { error: 'NOT_ALIVE' });
      kill(w, 'api');
      return send(res, 200, { ok: true });
    }

    if (p === '/chaos') {
      if (req.method === 'POST') {
        const body = JSON.parse((await readBody(req)).toString() || '{}');
        chaos.enabled = Boolean(body.enabled);
        if (body.killEverySec) chaos.killEverySec = Math.max(3, Number(body.killEverySec));
        chaos.lastKillAt = Date.now();
      }
      return send(res, 200, { enabled: chaos.enabled, killEverySec: chaos.killEverySec });
    }

    if (req.method === 'GET' && p === '/metrics') {
      const all = [...workers.values()];
      return send(res, 200, {
        throttled, queues: { detect: detectQueue.length, classify: classifyQueue.length },
        workers: { alive: all.filter((w) => w.status === 'ALIVE').length, dead: all.filter((w) => w.status === 'DEAD').length },
        recoveryMs, latency: { p50: null, p95: null },
      });
    }

    if (req.method === 'GET' && p === '/events') {
      const limit = Math.min(500, Number(url.searchParams.get('limit') ?? 200));
      return send(res, 200, { events: eventLog.slice(-limit).reverse() });
    }

    if (req.method === 'GET' && p === '/config') {
      return send(res, 200, {
        animalConfThreshold: 0.2, heartbeatMs: HEARTBEAT_MS, workerTimeoutMs: WORKER_TIMEOUT_MS, leaseMs: 15000,
        humanReviewSecondsPerImage: REVIEW_SECONDS_PER_IMAGE,
      });
    }

    if (req.method === 'GET' && (match = m(/^\/mock\/img\/(\d+)\.svg$/))) {
      res.setHeader('cache-control', 'public, max-age=3600');
      res.writeHead(200, { 'content-type': 'image/svg+xml' });
      return res.end(renderSvg(Number(match[1])));
    }

    send(res, 404, { error: 'NOT_FOUND' });
  } catch (err) {
    console.error(err);
    send(res, 500, { error: String(err) });
  }
});

server.on('upgrade', (req, socket, head) => {
  if (new URL(req.url, 'http://x').pathname !== '/events') return socket.destroy();
  wss.handleUpgrade(req, socket, head, (ws) => {
    ws.send(JSON.stringify({ type: 'worker_update', workers: visibleWorkers() }));
    if (jobs[0]) ws.send(JSON.stringify({ type: 'job_progress', job: summary(jobs[0]) }));
    ws.send(JSON.stringify({ type: 'throttle', throttled, classifyQueue: classifyQueue.length }));
  });
});

server.listen(PORT, () => {
  console.log(`Wildebeest mock coordinator on http://localhost:${PORT}`);
  const auto = Number(process.env.MOCK_AUTOSTART ?? 0);
  if (auto) createJob(`sample-${auto}`, Array.from({ length: auto }, (_, i) => i), auto);
});

// ---------------------------------------------------------------------------
// Placeholder camera-trap frames: a savanna scene with a silhouette at the bbox.

function renderSvg(idx) {
  const info = imageInfo(idx % 100_000);
  const night = rand01(idx * 37) < 0.35;
  const W = 640, H = 480;
  const sky = night ? ['#2a2d2b', '#4a4f4b'] : ['#9fb7c4', '#e3d9bf'];
  const ground = night ? ['#555a55', '#3a3e3a'] : ['#b59a5e', '#7d6a3c'];
  const shade = night ? '#1c1f1c' : '#3b2f1d';
  const horizon = 0.38 + rand01(idx * 41) * 0.08;
  let shapes = '';
  // acacia
  const tx = rand01(idx * 43) * W;
  shapes += `<rect x="${tx - 4}" y="${H * horizon - 40}" width="8" height="46" fill="${shade}" opacity=".7"/>`;
  shapes += `<ellipse cx="${tx}" cy="${H * horizon - 44}" rx="70" ry="14" fill="${shade}" opacity=".7"/>`;
  // grass
  for (let i = 0; i < 70; i++) {
    const gx = rand01(idx * 101 + i) * W;
    const gy = H * horizon + 20 + rand01(idx * 103 + i) * (H * (1 - horizon) - 40);
    shapes += `<line x1="${gx}" y1="${gy}" x2="${gx + (rand01(i + idx) - 0.5) * 12}" y2="${gy - 10 - rand01(i * 3 + idx) * 22}" stroke="${shade}" stroke-width="1.5" opacity=".35"/>`;
  }
  for (const d of info.detections) {
    if (d.conf < 0.2) continue;
    const [x, y, w, h] = [d.bbox[0] * W, d.bbox[1] * H, d.bbox[2] * W, d.bbox[3] * H];
    if (d.label === 'animal') {
      shapes += `<g fill="${shade}">` +
        `<ellipse cx="${x + w * 0.45}" cy="${y + h * 0.45}" rx="${w * 0.33}" ry="${h * 0.22}"/>` +
        `<ellipse cx="${x + w * 0.85}" cy="${y + h * 0.25}" rx="${w * 0.11}" ry="${h * 0.12}"/>` +
        `<rect x="${x + w * 0.72}" y="${y + h * 0.22}" width="${w * 0.08}" height="${h * 0.3}" transform="rotate(25 ${x + w * 0.76} ${y + h * 0.37})"/>` +
        [0.2, 0.32, 0.58, 0.68].map((f) => `<rect x="${x + w * f}" y="${y + h * 0.55}" width="${w * 0.05}" height="${h * 0.42}"/>`).join('') +
        `</g>`;
    } else if (d.label === 'human') {
      shapes += `<g fill="${shade}"><circle cx="${x + w / 2}" cy="${y + h * 0.12}" r="${Math.min(w, h) * 0.12}"/><rect x="${x + w * 0.3}" y="${y + h * 0.25}" width="${w * 0.4}" height="${h * 0.72}" rx="6"/></g>`;
    } else {
      shapes += `<g fill="${shade}"><rect x="${x}" y="${y + h * 0.3}" width="${w}" height="${h * 0.5}" rx="8"/><rect x="${x + w * 0.2}" y="${y}" width="${w * 0.55}" height="${h * 0.4}" rx="6"/><circle cx="${x + w * 0.2}" cy="${y + h * 0.85}" r="${h * 0.15}" fill="#111"/><circle cx="${x + w * 0.8}" cy="${y + h * 0.85}" r="${h * 0.15}" fill="#111"/></g>`;
    }
  }
  const hh = String(Math.floor(rand01(idx * 47) * 24)).padStart(2, '0');
  const mm = String(Math.floor(rand01(idx * 53) * 60)).padStart(2, '0');
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">
<defs><linearGradient id="s" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="${sky[0]}"/><stop offset="1" stop-color="${sky[1]}"/></linearGradient>
<linearGradient id="g" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="${ground[0]}"/><stop offset="1" stop-color="${ground[1]}"/></linearGradient></defs>
<rect width="${W}" height="${H}" fill="url(#s)"/><rect y="${H * horizon}" width="${W}" height="${H * (1 - horizon)}" fill="url(#g)"/>
${shapes}
<rect y="${H - 26}" width="${W}" height="26" fill="#000" opacity=".8"/>
<text x="10" y="${H - 8}" font-family="monospace" font-size="14" fill="#ddd">CAM${String((idx % 12) + 1).padStart(2, '0')}  2026-09-${String((idx % 28) + 1).padStart(2, '0')} ${hh}:${mm}  ${night ? 'IR' : '24°C'}  #${idx}</text>
</svg>`;
}
