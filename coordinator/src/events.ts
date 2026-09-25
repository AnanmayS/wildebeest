import type http from "node:http";
import { WebSocket, WebSocketServer } from "ws";
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
]);

const short = (id: string | null | undefined) => (id ? id.slice(0, 8) : "?");

export function describeEvent(e: EventRow): string {
  const d = (e.detail ?? {}) as Record<string, any>;
  switch (e.type) {
    case "worker_died":
      return `${e.workerId} died (no heartbeat for ${Math.round((d.silentMs ?? 0) / 1000)}s)`;
    case "worker_killed":
      return `SIGKILL sent to ${e.workerId}${d.source === "chaos" ? " (chaos)" : ""}`;
    case "reassigned":
      return `${e.workerId} died; task ${short(e.taskId)}… reassigned (attempt ${d.attempts ?? "?"})`;
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
    default:
      return e.type;
  }
}

/** Event-log items in the websocket `task_events` shape. */
export function toLogItem(r: EventRow) {
  return { id: r.id, at: r.at, type: r.type, taskId: r.taskId, workerId: r.workerId, message: describeEvent(r) };
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
// throttle state, and an append-only batch of task events. Each tick sends the next non-empty
// slot round-robin, so task events are batched and delayed, never dropped (up to a cap).
// ---------------------------------------------------------------------------------------------

type Builders = {
  jobSummary: (jobId: string) => Promise<unknown | null>;
  workerList: () => Promise<unknown[]>;
};

interface ClientState {
  ws: WebSocket;
  jobs: Set<string>;
  workers: boolean;
  throttle: Record<string, unknown> | null;
  events: Array<Record<string, unknown>>;
  cursor: number;
}

const TICK_MS = 100;
const MAX_EVENTS_PER_MESSAGE = 200;
const MAX_EVENT_BACKLOG = 2000;

class Hub {
  private clients = new Set<ClientState>();
  private builders: Builders | null = null;
  private timer: NodeJS.Timeout | null = null;
  private wss: WebSocketServer | null = null;
  private latestJobId: string | null = null;
  // Per-tick caches so N clients cost one DB query, not N.
  private tickCache = new Map<string, Promise<unknown>>();

  setBuilders(b: Builders) {
    this.builders = b;
  }

  attach(server: http.Server) {
    this.wss = new WebSocketServer({ server, path: "/events" });
    this.wss.on("connection", (ws) => {
      const state: ClientState = { ws, jobs: new Set(), workers: true, throttle: null, events: [], cursor: 0 };
      if (this.latestJobId) state.jobs.add(this.latestJobId);
      this.clients.add(state);
      ws.on("close", () => this.clients.delete(state));
      ws.on("error", () => this.clients.delete(state));
    });
    this.timer = setInterval(() => void this.tick(), TICK_MS);
  }

  close() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    for (const c of this.clients) c.ws.terminate();
    this.clients.clear();
    this.wss?.close();
    this.wss = null;
  }

  jobChanged(jobId: string) {
    this.latestJobId = jobId;
    for (const c of this.clients) c.jobs.add(jobId);
  }

  workersChanged() {
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

  private async nextMessage(c: ClientState): Promise<unknown | null> {
    const slots = ["events", "job", "workers", "throttle"] as const;
    for (let i = 0; i < slots.length; i++) {
      const slot = slots[(c.cursor + i) % slots.length];
      if (slot === "events" && c.events.length > 0) {
        c.cursor = (c.cursor + i + 1) % slots.length;
        return { type: "task_events", events: c.events.splice(0, MAX_EVENTS_PER_MESSAGE) };
      }
      if (slot === "job" && c.jobs.size > 0 && this.builders) {
        c.cursor = (c.cursor + i + 1) % slots.length;
        const jobId = c.jobs.values().next().value as string;
        c.jobs.delete(jobId);
        const job = await this.cached(`job:${jobId}`, () => this.builders!.jobSummary(jobId));
        return job ? { type: "job_progress", job } : null;
      }
      if (slot === "workers" && c.workers && this.builders) {
        c.cursor = (c.cursor + i + 1) % slots.length;
        c.workers = false;
        const workers = await this.cached("workers", () => this.builders!.workerList());
        return { type: "worker_update", workers };
      }
      if (slot === "throttle" && c.throttle) {
        c.cursor = (c.cursor + i + 1) % slots.length;
        const msg = c.throttle;
        c.throttle = null;
        return msg;
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
