import { Redis } from "ioredis";
import { config } from "./config.js";
import { setWakeRelay, wake, type Stage } from "./dispatcher.js";
import { hub, type EventRow, type HubRelay } from "./events.js";
import { getRedis } from "./redis.js";
import { serviceTimes } from "./speculation.js";
import { telemetry } from "./telemetry.js";

// The cluster bus: what one coordinator replica must tell the others, over one Redis pub/sub
// channel (docs/decisions/h-ha.md).
//
// Behind the load balancer a dashboard's WebSocket lands on one replica, a worker's completion on
// another. Everything the dashboard sees must therefore reach every replica:
//   - hub notifications: event-log rows, "job X changed", "workers changed", throttle changes;
//   - telemetry records (completion samples, pushes, recoveries, fencing counts, speculation's
//     per-worker service times...), so that every replica builds the same `system` snapshot and
//     worker list from the same in-memory telemetry, and a newly elected leader's speculation loop
//     starts with the cluster's history;
//   - long-poll wake-ups (CLAIM_MODE=postgres), so a worker waiting on replica B learns about a job
//     created on replica A without waiting for its 250 ms re-check;
//   - "the leader resigned", so followers run an election round at once.
// Job summaries and worker lists are not sent: each replica rebuilds them from Postgres when told
// they changed. Messages are batched per replica every FLUSH_MS. Pub/sub is at-most-once: a lost
// message costs a dashboard update or a telemetry sample, never state (Postgres holds all state).

/**
 * Pub/sub channels are global to a Redis server, not per database, so the channel carries the
 * database index: two stacks (or the test suite) sharing one Redis server on different DBs must
 * not hear each other.
 */
export function busChannel(redisUrl = config.redisUrl): string {
  let db = "0";
  try {
    db = new URL(redisUrl).pathname.replace(/^\//, "") || "0";
  } catch {
    /* default DB */
  }
  return `wildebeest:cluster:${db}`;
}
const FLUSH_MS = 50;

type TelemetryRecord = [method: string, args: unknown[]];

export interface BusMessage {
  from: string;
  events?: EventRow[];
  jobs?: string[];
  workers?: boolean;
  throttle?: { throttled: boolean; classifyQueue: number };
  wake?: Stage[];
  telemetry?: TelemetryRecord[];
  resigned?: boolean;
}

// Telemetry methods whose effects every replica must see. recordCompletion is sent as its sample
// only (the complete_ms write-behind belongs to the replica that handled the request).
const REPLICATED = [
  "recordCompletion",
  "recordFinalized",
  "recordPushed",
  "recordSweep",
  "recordStale",
  "recordJobCreated",
  "recordRecovery",
  "recordClaimed",
] as const;
type Replicated = (typeof REPLICATED)[number];

export class ClusterBus implements HubRelay {
  private sub: Redis | null = null;
  private outbox: Required<Omit<BusMessage, "from" | "resigned" | "throttle">> & { throttle: BusMessage["throttle"] | null } =
    ClusterBus.empty();
  private timer: NodeJS.Timeout | null = null;
  private originals: Partial<Record<Replicated, (...args: any[]) => unknown>> = {};
  private serviceTimesRecord: typeof serviceTimes.record | null = null;
  readonly stats = { sent: 0, received: 0 };
  private readonly channel: string;

  constructor(
    readonly instance: string,
    private readonly opts: { onResigned?: () => void; redisUrl?: string } = {},
  ) {
    this.channel = busChannel(opts.redisUrl);
  }

  private static empty() {
    return { events: [] as EventRow[], jobs: [] as string[], workers: false, throttle: null, wake: [] as Stage[], telemetry: [] as TelemetryRecord[] };
  }

  async start() {
    this.sub = new Redis(this.opts.redisUrl ?? config.redisUrl, { maxRetriesPerRequest: null });
    this.sub.on("error", (err) => console.error("[cluster] subscriber:", err.message));
    this.sub.on("message", (_channel: string, raw: string) => this.receive(raw));
    await this.sub.subscribe(this.channel);
    hub.setRelay(this);
    setWakeRelay((stages) => this.wake(stages));
    this.replicateTelemetry();
  }

  async stop() {
    await this.flush().catch(() => {});
    hub.setRelay(null);
    setWakeRelay(null);
    for (const m of Object.keys(this.originals)) delete (telemetry as any)[m]; // back to the class methods
    this.originals = {};
    if (this.serviceTimesRecord) delete (serviceTimes as any).record; // back to the class method
    this.serviceTimesRecord = null;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    const s = this.sub;
    this.sub = null;
    await s?.quit().catch(() => s.disconnect());
  }

  // ---- outgoing ------------------------------------------------------------------------------

  jobChanged(jobId: string) {
    if (!this.outbox.jobs.includes(jobId)) this.outbox.jobs.push(jobId);
    this.schedule();
  }

  workersChanged() {
    this.outbox.workers = true;
    this.schedule();
  }

  throttleChanged(throttled: boolean, classifyQueue: number) {
    this.outbox.throttle = { throttled, classifyQueue };
    this.schedule();
  }

  publishEvents(rows: EventRow[]) {
    this.outbox.events.push(...rows);
    this.schedule();
  }

  wake(stages: Stage[]) {
    for (const s of stages) if (!this.outbox.wake.includes(s)) this.outbox.wake.push(s);
    this.schedule();
  }

  /** Sent at once: followers run an election round without waiting for the lease to lapse. */
  async announceResignation() {
    await getRedis().publish(this.channel, JSON.stringify({ from: this.instance, resigned: true } satisfies BusMessage));
  }

  private schedule() {
    if (this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      this.flush().catch((err) => console.error(`[cluster] publish failed: ${err.message}`));
    }, FLUSH_MS);
    this.timer.unref?.();
  }

  async flush() {
    const o = this.outbox;
    this.outbox = ClusterBus.empty();
    const msg: BusMessage = { from: this.instance };
    if (o.events.length) msg.events = o.events;
    if (o.jobs.length) msg.jobs = o.jobs;
    if (o.workers) msg.workers = true;
    if (o.throttle) msg.throttle = o.throttle;
    if (o.wake.length) msg.wake = o.wake;
    if (o.telemetry.length) msg.telemetry = o.telemetry;
    if (Object.keys(msg).length === 1) return;
    await getRedis().publish(this.channel, JSON.stringify(msg));
    this.stats.sent++;
  }

  // ---- incoming ------------------------------------------------------------------------------

  receive(raw: string) {
    let msg: BusMessage;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    if (!msg || msg.from === this.instance) return;
    this.stats.received++;
    try {
      if (msg.resigned) this.opts.onResigned?.();
      if (msg.events?.length) hub.publishEvents(msg.events, true);
      for (const j of msg.jobs ?? []) hub.jobChanged(j, true);
      if (msg.workers) hub.workersChanged(true);
      if (msg.throttle) hub.throttleChanged(msg.throttle.throttled, msg.throttle.classifyQueue, true);
      if (msg.wake?.length) wake(msg.wake, true);
      for (const [method, args] of msg.telemetry ?? []) this.applyTelemetry(method, args);
    } catch (err) {
      console.error(`[cluster] bad message from ${msg.from}: ${(err as Error).message}`);
    }
  }

  // ---- telemetry replication -----------------------------------------------------------------

  /**
   * Wraps the telemetry singleton's record methods so each call is also sent to the other
   * replicas; received records are applied through the unwrapped methods (never re-sent).
   */
  private replicateTelemetry() {
    const t = telemetry as any;
    for (const m of REPLICATED) {
      const original = t[m].bind(telemetry);
      this.originals[m] = t[m];
      t[m] = (...args: any[]) => {
        const record = this.encode(m, args);
        const out = original(...args);
        if (record) {
          this.outbox.telemetry.push(record);
          this.schedule();
        }
        return out;
      };
    }
    // Speculation's per-worker service times (speculation.ts) are recorded by whichever replica
    // handled the completion, and read by the leader's speculation loop and by every replica's
    // worker list: replicate them the same way, so a newly elected leader starts with full history.
    const record = serviceTimes.record.bind(serviceTimes);
    this.serviceTimesRecord = serviceTimes.record;
    serviceTimes.record = (workerId, stage, ms, at = Date.now()) => {
      record(workerId, stage, ms, at);
      this.outbox.telemetry.push(["serviceTime", [workerId, stage, ms, at]]);
      this.schedule();
    };
  }

  private encode(m: Replicated, args: any[]): TelemetryRecord | null {
    switch (m) {
      case "recordCompletion":
        return ["recordSample", [args[0]]];
      case "recordClaimed": {
        // Only claims that close a recovery matter to the others (checked before the call, which
        // forgets them), so the hot path sends nothing when no recovery is open.
        const ids = (args[0] as string[]).filter((id) => telemetry.isRecovering(id));
        return ids.length ? ["recordClaimed", [ids, args[1], args[2] ?? Date.now()]] : null;
      }
      case "recordRecovery": {
        const r = args[0];
        return ["recordRecovery", [{ ...r, killedAt: r.killedAt?.toISOString() ?? null, detectedAt: r.detectedAt.toISOString(), requeuedAt: r.requeuedAt.toISOString() }]];
      }
      default:
        return [m, args];
    }
  }

  private applyTelemetry(method: string, args: any[]) {
    if (method === "recordSample") return telemetry.recordSample(args[0]);
    if (method === "serviceTime") return this.serviceTimesRecord?.apply(serviceTimes, args as never);
    if (method === "recordRecovery") {
      const r = args[0];
      args = [{ ...r, killedAt: r.killedAt ? new Date(r.killedAt) : null, detectedAt: new Date(r.detectedAt), requeuedAt: new Date(r.requeuedAt) }];
    }
    const fn = this.originals[method as Replicated];
    if (fn) fn.apply(telemetry, args);
  }
}
