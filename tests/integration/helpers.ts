// Shared helpers for the Dockerized integration tests.
// The tests drive the real stack (docker compose) through the coordinator's HTTP API
// and check the outcome directly in Postgres.
import { execSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";

export const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
export const API = process.env.COORDINATOR_URL ?? "http://localhost:3000";
export const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://wildebeest:wildebeest@localhost:15432/wildebeest";
export const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:16379";

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export function compose(args: string, env: Record<string, string> = {}) {
  execSync(`docker compose ${args}`, {
    cwd: REPO_ROOT,
    stdio: "inherit",
    env: { ...process.env, ...env },
  });
}

/** Bring the stack up with the given worker counts and wait until they have all registered. */
export async function startStack(detectors: number, classifiers: number, env: Record<string, string> = {}) {
  compose(`up -d --build --scale detector=${detectors} --scale classifier=${classifiers}`, env);
  await waitFor(async () => {
    const workers = await aliveWorkers();
    return (
      workers.filter((w) => w.stage === "detect").length >= detectors &&
      workers.filter((w) => w.stage === "classify").length >= classifiers
    );
  }, 10 * 60_000, "workers to register");
}

export async function api<T = any>(method: string, route: string, body?: unknown): Promise<{ status: number; body: T }> {
  const res = await fetch(API + route, {
    method,
    headers: body ? { "content-type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : undefined };
}

export async function waitFor(check: () => Promise<boolean>, timeoutMs: number, what: string, everyMs = 500) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      if (await check()) return;
    } catch {
      // coordinator may still be starting; keep polling
    }
    await sleep(everyMs);
  }
  throw new Error(`timed out waiting for ${what}`);
}

export interface WorkerInfo {
  id: string;
  stage: "detect" | "classify";
  status: "ALIVE" | "DEAD" | "STOPPED";
  state: "idle" | "busy" | "dead";
  containerId: string;
  currentTaskIds: string[];
}

export async function aliveWorkers(): Promise<WorkerInfo[]> {
  const { body } = await api<{ workers: WorkerInfo[] }>("GET", "/workers");
  return body.workers.filter((w) => w.status === "ALIVE");
}

export async function getJob(jobId: string) {
  return (await api("GET", `/jobs/${jobId}`)).body;
}

export async function waitForJobDone(jobId: string, timeoutMs = 15 * 60_000) {
  await waitFor(async () => (await getJob(jobId)).status === "done", timeoutMs, `job ${jobId} to finish`, 1000);
  return getJob(jobId);
}

export function db() {
  return new pg.Pool({ connectionString: DATABASE_URL, max: 2 });
}
