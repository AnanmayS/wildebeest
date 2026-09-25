import type http from "node:http";
import { WebSocket, WebSocketServer } from "ws";
import { config } from "./config.js";
import type { Db } from "./db.js";

// ---------------------------------------------------------------------------------------------
// task_events rows
// ---------------------------------------------------------------------------------------------

export interface EventInput {
  type: string;
  taskId?: string | null;
  workerId?: string | null;
  detail?: Record<string, unknown> | null;
}

export interface EventRow {
  id: number;
  at: string;
  type: string;
  taskId: string | null;
  workerId: string | null;
  detail: Record<string, unknown> | null;
}

/** Inserts task_events rows in one statement and returns them (with id/at) for publishing. */
export async function recordEvents(db: Db, events: EventInput[]): Promise<EventRow[]> {
  if (events.length === 0) return [];
  const { rows } = await db.query(
    `insert into task_events (task_id, worker_id, type, detail)
     select * from unnest($1::uuid[], $2::text[], $3::text[], $4::jsonb[])
     returning id, at, type, task_id, worker_id, detail`,
    [
      events.map((e) => e.taskId ?? null),
      events.map((e) => e.workerId ?? null),
      events.map((e) => e.type),
      events.map((e) => (e.detail ? JSON.stringify(e.detail) : null)),
    ],
  );
  return rows.map((r) => ({
    id: Number(r.id),
    at: new Date(r.at).toISOString(),
    type: r.type,
    taskId: r.task_id,
    workerId: r.worker_id,
    detail: r.detail,
  }));
}

// Types the dashboard's event log shows. High-volume bookkeeping events (enqueued, claimed,
// succeeded, released) stay in Postgres only.
const LOG_TYPES = new Set([
  "worker_died",
  "worker_killed",
  "reassigned",
  "lease_expired",
  "stale_rejected",
  "cache_hit",
  "throttled",
  "unthrottled",
  "failed",
  "job_done",
  "job_cancelled",
  "worker_paused",
  "worker_resumed",
  "heartbeat_refused",
  "released",
  "redriven",
  "speculated",
]);

const short = (id: string | null | undefined) => (id ? id.slice(0, 8) : "?");

export function describeEvent(e: EventRow): string {
  const d = (e.detail ?? {}) as Record<string, any>;
  switch (e.type) {
    case "worker_died":
      if (d.via === "docker_event") {
        const how = d.exitCode != null ? `exit ${d.exitCode}` : (d.dockerAction ?? "container gone");
        return `${e.workerId} died (docker: ${how}; detected in ${d.detectMs ?? "?"} ms)`;
      }
      return `${e.workerId} died (no heartbeat for ${Math.round((d.silentMs ?? 0) / 1000)}s)`;
    case "worker_killed":
      return `SIGKILL sent to ${e.workerId}${d.source === "chaos" ? " (chaos)" : ""}`;
    case "worker_paused":
      return `${e.workerId} paused for ${Math.round((d.ms ?? 0) / 100) / 10}s${d.source === "chaos" ? " (chaos)" : ""}`;
    case "worker_resumed":
      return `${e.workerId} resumed after ${Math.round((d.pausedMs ?? 0) / 100) / 10}s`;
    case "redriven":
      return `task ${short(e.taskId)}… redriven from the DLQ`;
    case "released":
      return `task ${short(e.taskId)}… released by ${e.workerId} (${d.reason ?? "released"}); no attempt used`;
    case "heartbeat_refused":
      return `${e.workerId} woke up after being declared dead; heartbeat refused (410), it re-registers`;
    case "reassigned":
      return d.charged === false
        ? `${e.workerId} died; task ${short(e.taskId)}… reassigned (our own fault injection, no attempt used)`
        : `${e.workerId} died; task ${short(e.taskId)}… reassigned (attempt ${d.attempts ?? "?"})`;
    case "lease_expired":
      return `lease on task ${short(e.taskId)}… held by ${e.workerId} expired; requeued (attempt ${d.attempts ?? "?"})`;
    case "stale_rejected":
      return `stale result from ${e.workerId} for task ${short(e.taskId)}… rejected (epoch ${d.leaseEpoch} ≠ ${d.currentEpoch})`;
    case "cache_hit":
      return `${d.count} cache hits`;
    case "throttled":
      return `backpressure on: classify queue ${d.classifyQueue} > ${d.highWater}; detect paused`;
    case "unthrottled":
      return `backpressure off: classify queue ${d.classifyQueue} < ${d.lowWater}`;
    case "failed":
      return d.final
        ? `task ${short(e.taskId)}… failed permanently after ${d.attempts} attempts${d.error ? `: ${d.error}` : ""}`
        : `task ${short(e.taskId)}… failed on ${e.workerId} (attempt ${d.attempts}); retrying${d.error ? `: ${d.error}` : ""}`;
    case "job_done":
      return `job ${d.name ?? short(d.jobId as string)} done: ${d.total} images`;
    case "job_cancelled":
      return `job ${d.name ?? short(d.jobId as string)} cancelled (${d.tasks} pending tasks dropped)`;
    default:
      return e.type;
  }
}

/** Event-log items in the websocket `task_events` shape; `detail` is passed through unchanged. */
export function toLogItem(r: EventRow) {
  return {
    id: r.id,
    at: r.at,
    type: r.type,
    taskId: r.taskId,
    workerId: r.workerId,
    message: describeEvent(r),
    detail: r.detail ?? null,
  };
}

/** GET /events?limit=N: the most recent event-log items, newest first. */
export async function recentLogEvents(db: Db, limit: number) {
  const { rows } = await db.query(
    `select id, at, type, task_id, worker_id, detail from task_events
      where type = any($1::text[]) order by id desc limit $2`,
    [[...LOG_TYPES], limit],
  );
  return rows.map((r) =>
    toLogItem({
      id: Number(r.id),
      at: new Date(r.at).toISOString(),
      type: r.type,
      taskId: r.task_id,
      workerId: r.worker_id,
      detail: r.detail,
    }),
  );
}

// ---------------------------------------------------------------------------------------------
// WebSocket hub
//
// Each client gets at most one message per tick (100 ms → ~10 msgs/s). Updates are coalesced
// into per-client "slots": the latest job_progress per job, one worker_update flag, the latest
// throttle state, a `system` snapshot flag raised every SYSTEM_INTERVAL_MS (~2×/s), and an
// append-only batch of task events. Each tick sends the next non-empty slot round-robin, so task
// events are batched and delayed, never dropped (up to a cap).
// ---------------------------------------------------------------------------------------------

type Builders = {
  jobSummary: (jobId: string) => Promise<unknown | null>;
  workerList: () => Promise<unknown[]>;
  system?: () => Promise<unknown>;
};

interface ClientState {
  ws: WebSocket;
  jobs: Set<string>;
  workers: boolean;
  throttle: Record<string, unknown> | null;
  system: boolean;
  events: Array<Record<string, unknown>>;
  cursor: number;
}

const TICK_MS = 100;
/**
 * Job summaries and the worker list are rebuilt at most this often, however fast completions
 * arrive: a summary counts over the job's images and the worker list joins leases, so rebuilding
 * them every 100 ms tick under load would be most of the coordinator's read traffic.
 */
const REBUILD_MIN_MS = 500;
const MAX_EVENTS_PER_MESSAGE = 200;
const MAX_EVENT_BACKLOG = 2000;

class Hub {
  private clients = new Set<ClientState>();
  private builders: Builders | null = null;
  private timer: NodeJS.Timeout | null = null;
  private systemTimer: NodeJS.Timeout | null = null;
  private wss: WebSocketServer | null = null;
  private latestJobId: string | null = null;
  // Per-tick caches so N clients cost one DB query, not N.
  private tickCache = new Map<string, Promise<unknown>>();
  // Rate-limited builds (job summaries, worker list): when each was built and last changed.
  private built = new Map<string, { at: number; value: Promise<unknown> }>();
  private changedAt = new Map<string, number>();

  setBuilders(b: Builders) {
    this.builders = b;
  }

  attach(server: http.Server) {
    this.wss = new WebSocketServer({ server, path: "/events" });
    this.wss.on("connection", (ws) => {
      const state: ClientState = {
        ws,
        jobs: new Set(),
        workers: true,
        throttle: null,
        system: true,
        events: [],
        cursor: 0,
      };
      if (this.latestJobId) state.jobs.add(this.latestJobId);
      this.clients.add(state);
      ws.on("close", () => this.clients.delete(state));
      ws.on("error", () => this.clients.delete(state));
    });
    this.timer = setInterval(() => void this.tick(), TICK_MS);
    this.systemTimer = setInterval(() => {
      for (const c of this.clients) c.system = true;
    }, config.systemIntervalMs);
  }

  close() {
    if (this.timer) clearInterval(this.timer);
    if (this.systemTimer) clearInterval(this.systemTimer);
    this.timer = null;
    this.systemTimer = null;
    for (const c of this.clients) c.ws.terminate();
    this.clients.clear();
    this.wss?.close();
    this.wss = null;
  }

  jobChanged(jobId: string) {
    this.latestJobId = jobId;
    this.changedAt.set(`job:${jobId}`, Date.now());
    for (const c of this.clients) c.jobs.add(jobId);
  }

  workersChanged() {
    this.changedAt.set("workers", Date.now());
    for (const c of this.clients) c.workers = true;
  }

  throttleChanged(throttled: boolean, classifyQueue: number) {
    for (const c of this.clients) c.throttle = { type: "throttle", throttled, classifyQueue };
  }

  publishEvents(rows: EventRow[]) {
    const visible = rows.filter((r) => LOG_TYPES.has(r.type)).map(toLogItem);
    if (visible.length === 0) return;
    for (const c of this.clients) {
      c.events.push(...visible);
      if (c.events.length > MAX_EVENT_BACKLOG) c.events.splice(0, c.events.length - MAX_EVENT_BACKLOG);
    }
  }

  private cached<T>(key: string, make: () => Promise<T>): Promise<T> {
    let p = this.tickCache.get(key);
    if (!p) {
      p = make();
      this.tickCache.set(key, p);
    }
    return p as Promise<T>;
  }

  /**
   * The latest build of `key` if nothing changed since it was made; a new build if the last one is
   * older than REBUILD_MIN_MS; otherwise null ("too soon": the caller keeps the client's flag set
   * and sends on a later tick, so the final state is never lost, just delayed ≤ REBUILD_MIN_MS).
   */
  private rateLimited<T>(key: string, make: () => Promise<T>): Promise<T> | null {
    const now = Date.now();
    const entry = this.built.get(key);
    const changed = this.changedAt.get(key) ?? 0;
    if (entry && changed < entry.at) return entry.value as Promise<T>;
    if (entry && now - entry.at < REBUILD_MIN_MS) return null;
    const value = make();
    this.built.set(key, { at: now, value });
    value.catch(() => this.built.delete(key));
    if (this.built.size > 200) {
      for (const [k, e] of this.built) if (now - e.at > 60_000) this.built.delete(k);
    }
    return value;
  }

  private async nextMessage(c: ClientState): Promise<unknown | null> {
    const slots = ["events", "job", "workers", "throttle", "system"] as const;
    for (let i = 0; i < slots.length; i++) {
      const slot = slots[(c.cursor + i) % slots.length];
      if (slot === "events" && c.events.length > 0) {
        c.cursor = (c.cursor + i + 1) % slots.length;
        return { type: "task_events", events: c.events.splice(0, MAX_EVENTS_PER_MESSAGE) };
      }
      if (slot === "job" && c.jobs.size > 0 && this.builders) {
        const jobId = c.jobs.values().next().value as string;
        const pending = this.rateLimited(`job:${jobId}`, () => this.builders!.jobSummary(jobId));
        if (!pending) continue;
        c.cursor = (c.cursor + i + 1) % slots.length;
        c.jobs.delete(jobId);
        const job = await pending;
        return job ? { type: "job_progress", job } : null;
      }
      if (slot === "workers" && c.workers && this.builders) {
        const pending = this.rateLimited("workers", () => this.builders!.workerList());
        if (!pending) continue;
        c.cursor = (c.cursor + i + 1) % slots.length;
        c.workers = false;
        return { type: "worker_update", workers: await pending };
      }
      if (slot === "throttle" && c.throttle) {
        c.cursor = (c.cursor + i + 1) % slots.length;
        const msg = c.throttle;
        c.throttle = null;
        return msg;
      }
      if (slot === "system" && c.system && this.builders?.system) {
        c.cursor = (c.cursor + i + 1) % slots.length;
        c.system = false;
        const system = await this.cached("system", () => this.builders!.system!());
        return { type: "system", system };
      }
    }
    return null;
  }

  private ticking = false;

  private async tick() {
    if (this.ticking || this.clients.size === 0) return;
    this.ticking = true;
    this.tickCache.clear();
    try {
      await Promise.all(
        [...this.clients].map(async (c) => {
          if (c.ws.readyState !== WebSocket.OPEN) return;
          const msg = await this.nextMessage(c);
          if (msg) c.ws.send(JSON.stringify(msg));
        }),
      );
    } catch (err) {
      console.error("[events] tick failed", (err as Error).message);
    } finally {
      this.ticking = false;
    }
  }
}

export const hub = new Hub();
