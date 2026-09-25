// Phase 6 benchmark harness.
//
// For each detector count in BENCH_DETECTORS (default 1,2,3,4 — what fits an 8 GB Docker VM;
// use BENCH_DETECTORS=1,2,4,6,8 with ~16 GB for Docker), with classifiers at about
// 1 per 3 detectors (minimum 1), it clears the result cache, runs a fixed BENCH_IMAGES-image
// sample job, and records wall time, throughput, p50/p95 per-image processing latency and peak
// container memory. Then it measures recovery time after a SIGKILL and the cache-hit rerun time.
//
// Output: benchmarks/results.csv, benchmarks/results.md, benchmarks/throughput.png
//
// Usage: npx tsx benchmark.ts   (from scripts/, with data/sample downloaded)
import { execSync, spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
// BENCH_TAG puts a run in its own folder, e.g. BENCH_TAG=fake for the fake-model sweep.
const OUT = path.join(ROOT, "benchmarks", process.env.BENCH_TAG ?? "");
const API = process.env.COORDINATOR_URL ?? "http://localhost:3000";
const IMAGES = Number(process.env.BENCH_IMAGES ?? 1000);
const DETECTORS = (process.env.BENCH_DETECTORS ?? "1,2,3,4").split(",").map(Number);
const RECOVERY_DETECTORS = Number(process.env.BENCH_RECOVERY_DETECTORS ?? 4);
const RECOVERY_IMAGES = Number(process.env.BENCH_RECOVERY_IMAGES ?? 300);
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL ?? "postgres://forgegrid:forgegrid@localhost:15432/forgegrid",
});

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const classifiersFor = (detectors: number) => Math.max(1, Math.round(detectors / 3));

async function api(method: string, route: string, body?: unknown) {
  const res = await fetch(API + route, {
    method,
    headers: body ? { "content-type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`${method} ${route} → ${res.status} ${await res.text()}`);
  return res.json();
}

async function waitFor(check: () => Promise<boolean>, timeoutMs: number, what: string) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      if (await check()) return;
    } catch {
      /* coordinator restarting */
    }
    await sleep(500);
  }
  throw new Error(`timed out waiting for ${what}`);
}

async function scaleTo(detectors: number, classifiers: number) {
  // Remove every existing worker container first so each run starts with fresh, idle workers.
  execSync("docker compose rm -sf detector classifier", { cwd: ROOT, stdio: "inherit" });
  execSync(`docker compose up -d --build --scale detector=${detectors} --scale classifier=${classifiers}`, {
    cwd: ROOT,
    stdio: "inherit",
  });
  await waitFor(async () => {
    const { workers } = await api("GET", "/workers");
    const alive = workers.filter((w: any) => w.status === "ALIVE");
    return (
      alive.filter((w: any) => w.stage === "detect").length === detectors &&
      alive.filter((w: any) => w.stage === "classify").length === classifiers
    );
  }, 10 * 60_000, "workers to register");
}

/** Samples `docker stats` every 2s and keeps the peak memory (MiB) seen per worker stage. */
function startMemorySampler() {
  const peak = { detect: 0, classify: 0 };
  let running = true;
  const toMiB = (s: string) => {
    const m = s.match(/([\d.]+)\s*(KiB|MiB|GiB|kB|MB|GB)/);
    if (!m) return 0;
    const n = Number(m[1]);
    return m[2].startsWith("G") ? n * 1024 : m[2].startsWith("K") || m[2] === "kB" ? n / 1024 : n;
  };
  (async () => {
    while (running) {
      const out = await new Promise<string>((resolve) => {
        const p = spawn("docker", ["stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}"]);
        let buf = "";
        p.stdout.on("data", (d) => (buf += d));
        p.on("close", () => resolve(buf));
      });
      for (const line of out.trim().split("\n")) {
        const [name, usage] = line.split("\t");
        const mib = toMiB(usage ?? "");
        if (name?.includes("detector")) peak.detect = Math.max(peak.detect, mib);
        if (name?.includes("classifier")) peak.classify = Math.max(peak.classify, mib);
      }
      await sleep(2000);
    }
  })();
  return { stop: () => ((running = false), peak) };
}

/** Leftover jobs (e.g. from an interrupted run) would compete for workers, so cancel them first. */
async function cancelRunningJobs() {
  const { jobs } = await api("GET", "/jobs");
  for (const j of jobs.filter((j: any) => j.status === "running")) await api("POST", `/jobs/${j.id}/cancel`);
}

async function runJob(size: number) {
  await cancelRunningJobs();
  const started = Date.now();
  const { jobId } = await api("POST", "/jobs/sample", { size, countryCode: "TZA" });
  await waitFor(async () => (await api("GET", `/jobs/${jobId}`)).status === "done", 90 * 60_000, `job ${jobId}`);
  return { jobId, wallMs: Date.now() - started, job: await api("GET", `/jobs/${jobId}`) };
}

/** Per-image processing latency = time spent in worker-held stages (detect + classify). */
async function latencyPercentiles(jobId: string) {
  const { rows } = await pool.query(
    `select percentile_cont(0.5) within group (order by ms) as p50,
            percentile_cont(0.95) within group (order by ms) as p95
       from (select sum(extract(epoch from (t.finished_at - t.started_at)) * 1000) as ms
               from tasks t join images i on i.id = t.image_id
              where i.job_id = $1 and t.state = 'SUCCEEDED'
              group by i.id) per_image`,
    [jobId],
  );
  return { p50: Math.round(rows[0].p50 ?? 0), p95: Math.round(rows[0].p95 ?? 0) };
}

/** Kill one busy detector mid-job; recovery = kill → every task it held is claimed by a live worker. */
async function measureRecovery() {
  await scaleTo(RECOVERY_DETECTORS, classifiersFor(RECOVERY_DETECTORS));
  await api("POST", "/admin/clear-cache");
  await cancelRunningJobs();
  const { jobId } = await api("POST", "/jobs/sample", { size: RECOVERY_IMAGES, countryCode: "TZA" });
  await waitFor(async () => (await api("GET", `/jobs/${jobId}`)).processed >= RECOVERY_IMAGES * 0.3, 30 * 60_000, "30% progress");

  const { workers } = await api("GET", "/workers");
  const victim = workers.find((w: any) => w.status === "ALIVE" && w.stage === "detect" && w.currentTaskIds.length > 0);
  const heldTasks: string[] = victim.currentTaskIds;
  const killedAt = new Date();
  await api("POST", `/workers/${victim.id}/kill`);

  await waitFor(async () => {
    const { rows } = await pool.query(
      `select count(*)::int as n from tasks where id = any($1) and (state = 'SUCCEEDED' or (state = 'LEASED' and worker_id <> $2))`,
      [heldTasks, victim.id],
    );
    return rows[0].n === heldTasks.length;
  }, 5 * 60_000, "killed worker's tasks to be reclaimed");

  const { rows } = await pool.query(
    `select max(at) as reclaimed_at from (
        select min(e.at) as at from task_events e
         where e.task_id = any($1) and e.type = 'claimed' and e.worker_id <> $2 and e.at >= $3
         group by e.task_id) firsts`,
    [heldTasks, victim.id, killedAt],
  );
  await waitFor(async () => (await api("GET", `/jobs/${jobId}`)).status === "done", 60 * 60_000, "recovery job");
  const recoveryMs = rows[0].reclaimed_at ? new Date(rows[0].reclaimed_at).getTime() - killedAt.getTime() : NaN;
  return { recoveryMs, tasksReclaimed: heldTasks.length };
}

function writeCsv(rows: Record<string, number | string>[]) {
  const header = Object.keys(rows[0]);
  fs.writeFileSync(path.join(OUT, "results.csv"), [header.join(","), ...rows.map((r) => header.map((h) => r[h]).join(","))].join("\n") + "\n");
}

/**
 * macOS "System-wide memory free percentage" (NaN elsewhere). When the host runs short, macOS
 * swaps out the Docker VM's memory and every number here becomes meaningless, so it is recorded.
 */
function hostFreePct(): number {
  try {
    const out = execSync("memory_pressure", { encoding: "utf8" });
    return Number(out.match(/free percentage: (\d+)%/)?.[1] ?? NaN);
  } catch {
    return NaN;
  }
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const rows: Record<string, number | string>[] = [];
  if (hostFreePct() < 20) console.warn(`WARNING: only ${hostFreePct()}% of host memory is free; results will be distorted.`);

  for (const detectors of DETECTORS) {
    const classifiers = classifiersFor(detectors);
    console.log(`\n=== ${detectors} detectors / ${classifiers} classifiers ===`);
    await scaleTo(detectors, classifiers);
    await api("POST", "/admin/clear-cache");
    const sampler = startMemorySampler();
    let minFreePct = hostFreePct();
    const freeTimer = setInterval(() => (minFreePct = Math.min(minFreePct, hostFreePct())), 30_000);
    const { jobId, wallMs, job } = await runJob(IMAGES);
    const peak = sampler.stop();
    clearInterval(freeTimer);
    const lat = await latencyPercentiles(jobId);
    const row = {
      detectors,
      classifiers,
      images: IMAGES,
      total_s: +(wallMs / 1000).toFixed(1),
      throughput_img_s: +(IMAGES / (wallMs / 1000)).toFixed(2),
      p50_ms: lat.p50,
      p95_ms: lat.p95,
      speedup: 0,
      peak_detector_mib: Math.round(peak.detect),
      peak_classifier_mib: Math.round(peak.classify),
      empty_pct: job.impact?.emptyPct ?? "",
      host_min_free_pct: minFreePct,
    };
    row.speedup = +(row.throughput_img_s / (rows[0]?.throughput_img_s as number ?? row.throughput_img_s)).toFixed(2);
    rows.push(row);
    writeCsv(rows); // keep partial results if a later run fails
    console.log(row);
  }

  console.log("\n=== cache-hit rerun ===");
  const rerun = await runJob(IMAGES); // same images as the last run, so all cached
  console.log(`rerun: ${rerun.job.cacheHits}/${IMAGES} cache hits in ${rerun.wallMs} ms`);

  console.log("\n=== recovery after SIGKILL ===");
  const recovery = await measureRecovery();
  console.log(recovery);

  fs.writeFileSync(
    path.join(OUT, "extra.json"),
    JSON.stringify({ cacheRerunMs: rerun.wallMs, cacheHits: rerun.job.cacheHits, images: IMAGES, ...recovery }, null, 2),
  );

  execSync(`"${path.join(ROOT, ".venv/bin/python")}" plot_benchmark.py "${OUT}"`, { cwd: path.join(ROOT, "scripts"), stdio: "inherit" });
  await pool.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
