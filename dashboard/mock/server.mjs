// Dev-only mock of the Wildebeest coordinator's dashboard API (docs/CONTRACTS.md, including
// the "v2 additions": GET /system + WebSocket `system`, pause, GET /benchmarks, native workers).
// The simulation itself lives in sim.mjs; this file is only HTTP + WebSocket plumbing.
//
//   node mock/server.mjs                 # listens on :3000 (same as the coordinator)
//   PORT=3999 node mock/server.mjs       # then: API_TARGET=http://localhost:3999 npm run dev
//
// Knobs: MOCK_DETECTORS (3 containers; plus one native MPS worker), MOCK_CLASSIFIERS (1),
// MOCK_SPEED (1), MOCK_AUTOSTART (e.g. 1000 to start a sample job on boot),
// MOCK_LEGACY=1 (behave like a v1 coordinator), MOCK_REPLACE_KILLED=0, MOCK_SEED_HISTORY=0.

import http from 'node:http';
import { readFileSync } from 'node:fs';
import { WebSocketServer } from 'ws';
import { renderSvg } from './scene.mjs';
import * as sim from './sim.mjs';

const PORT = Number(process.env.PORT ?? 3000);
const BENCHMARKS = JSON.parse(readFileSync(new URL('./benchmarks.json', import.meta.url), 'utf8'));

// ---------------------------------------------------------------------------
// WebSocket fan-out: events every 100 ms, progress/workers every 200 ms, system 2×/s.

const wss = new WebSocketServer({ noServer: true });

function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const c of wss.clients) if (c.readyState === 1) c.send(data);
}

let lastThrottle = sim.throttleState().throttled;
setInterval(() => {
  const events = sim.takeEvents();
  if (events.length) broadcast({ type: 'task_events', events });
  const t = sim.throttleState();
  if (t.throttled !== lastThrottle) {
    lastThrottle = t.throttled;
    broadcast({ type: 'throttle', ...t });
  }
}, 100);

setInterval(() => {
  const running = sim.jobs.filter((j) => j.status === 'running' || Date.now() - Date.parse(j.finishedAt) < 1500);
  for (const j of running) broadcast({ type: 'job_progress', job: sim.summary(j) });
  broadcast({ type: 'worker_update', workers: sim.visibleWorkers() });
}, 200);

if (!sim.LEGACY) setInterval(() => broadcast({ type: 'system', system: sim.systemSnapshot() }), 500);

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
const readJson = async (req) => JSON.parse((await readBody(req)).toString() || '{}');

let uploadCursor = 100_000; // uploaded photos get fresh content hashes

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;
  const route = (method, re) => req.method === method && p.match(re);
  let m;

  try {
    if (route('GET', /^\/healthz$/)) return send(res, 200, { ok: true });
    if (route('GET', /^\/jobs$/)) return send(res, 200, { jobs: sim.jobs.map(sim.summary) });

    if (route('POST', /^\/jobs\/sample$/)) {
      const size = Math.min(2000, Number((await readJson(req)).size) || 1000);
      const job = sim.createJob(`sample-${size}`, Array.from({ length: size }, (_, i) => i), size);
      return send(res, 200, { jobId: job.id });
    }

    if (route('POST', /^\/jobs$/)) {
      const count = ((await readBody(req)).toString('latin1').match(/filename="/g) ?? []).length;
      if (!count) return send(res, 400, { error: 'NO_FILES' });
      const job = sim.createJob(`upload-${count}`, Array.from({ length: count }, () => uploadCursor++));
      return send(res, 200, { jobId: job.id });
    }

    if ((m = route('GET', /^\/jobs\/([^/]+)$/))) {
      const job = sim.jobs.find((j) => j.id === m[1]);
      return job ? send(res, 200, sim.summary(job)) : send(res, 404, { error: 'NOT_FOUND' });
    }

    if ((m = route('GET', /^\/jobs\/([^/]+)\/images$/))) {
      const job = sim.jobs.find((j) => j.id === m[1]);
      if (!job) return send(res, 404, { error: 'NOT_FOUND' });
      return send(res, 200, sim.jobImages(job, {
        category: url.searchParams.get('category'),
        species: url.searchParams.get('species'),
        page: Math.max(1, Number(url.searchParams.get('page') ?? 1)),
        pageSize: Math.min(200, Number(url.searchParams.get('pageSize') ?? 48)),
      }));
    }

    if (route('GET', /^\/workers$/)) return send(res, 200, { workers: sim.visibleWorkers() });

    if ((m = route('POST', /^\/workers\/([^/]+)\/(kill|pause)$/))) {
      const w = sim.workers.get(m[1]);
      if (!w) return send(res, 404, { error: 'NOT_FOUND' });
      const [status, body] = m[2] === 'kill' ? sim.kill(w) : sim.pause(w, Number((await readJson(req)).ms) || 20_000);
      return send(res, status, body);
    }

    if (p === '/chaos') {
      if (req.method === 'POST') {
        const body = await readJson(req);
        sim.chaos.enabled = Boolean(body.enabled);
        if (body.killEverySec) sim.chaos.killEverySec = Math.max(3, Number(body.killEverySec));
        sim.chaos.lastKillAt = Date.now();
      }
      return send(res, 200, { enabled: sim.chaos.enabled, killEverySec: sim.chaos.killEverySec });
    }

    if (route('GET', /^\/metrics$/)) return send(res, 200, sim.metrics());

    if (route('GET', /^\/events$/)) {
      return send(res, 200, { events: sim.recentEvents(Math.min(500, Number(url.searchParams.get('limit') ?? 200))) });
    }

    if (route('GET', /^\/config$/)) {
      const c = sim.CONFIG;
      return send(res, 200, {
        animalConfThreshold: 0.2, heartbeatMs: c.heartbeatMs, workerTimeoutMs: c.workerTimeoutMs, leaseMs: c.leaseMs,
        humanReviewSecondsPerImage: 3,
      });
    }

    if (!sim.LEGACY && route('GET', /^\/system$/)) return send(res, 200, sim.systemSnapshot());
    if (!sim.LEGACY && route('GET', /^\/benchmarks$/)) return send(res, 200, BENCHMARKS);

    if ((m = route('GET', /^\/mock\/img\/(\d+)\.svg$/))) {
      res.writeHead(200, { 'content-type': 'image/svg+xml', 'cache-control': 'public, max-age=3600' });
      return res.end(renderSvg(Number(m[1])));
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
    ws.send(JSON.stringify({ type: 'worker_update', workers: sim.visibleWorkers() }));
    if (sim.jobs[0]) ws.send(JSON.stringify({ type: 'job_progress', job: sim.summary(sim.jobs[0]) }));
    ws.send(JSON.stringify({ type: 'throttle', ...sim.throttleState() }));
    if (!sim.LEGACY) ws.send(JSON.stringify({ type: 'system', system: sim.systemSnapshot() }));
  });
});

server.listen(PORT, () => {
  console.log(`Wildebeest mock coordinator on http://localhost:${PORT}${sim.LEGACY ? ' (legacy v1 mode)' : ''}`);
  const auto = Number(process.env.MOCK_AUTOSTART ?? 0);
  if (auto) sim.createJob(`sample-${auto}`, Array.from({ length: auto }, (_, i) => i), auto);
});
