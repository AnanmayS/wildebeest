import { AsyncLocalStorage } from "node:async_hooks";
import os from "node:os";
import { config } from "./config.js";
import { query } from "./db.js";
import { getDocker } from "./docker.js";
import { recoverWorkers, type DeathEvidence } from "./recovery.js";

// The fast failure detector: instead of inferring death from 6 s of heartbeat silence, listen for
// it. Docker emits a container `die` event (exit code 137 for SIGKILL) and an `oom` event the moment
// a container stops; we map the container to its worker and run the same recovery path as the
// reaper (recovery.ts). The heartbeat timeout stays as the backstop for anything we can't see:
// native workers, other hosts, a missed event.
//
// Docker documents no delivery guarantee for the event stream, so:
//  - on stream end/error we reconnect with `since=<last event time>`, which replays what we missed
//    (duplicates are harmless: marking DEAD is guarded on ALIVE);
//  - after every (re)connect we reconcile against `docker ps`: an ALIVE worker whose container
//    Docker reports as exited/dead is recovered as well.
//
// Several Compose stacks can share one Docker host. Events are filtered server-side by our own
// project label and checked again client-side, and a container is only mapped to a worker that
// registered with that exact container ID, so another project's containers can never kill ours.

export const PROJECT_LABEL = "com.docker.compose.project";

export interface DockerEvent {
  Type?: string;
  Action?: string;
  status?: string; // pre-1.22 API name for Action
  id?: string;
  Actor?: { ID?: string; Attributes?: Record<string, string> };
  time?: number;
  timeNano?: number;
}

/** The slice of the Docker API the watcher needs (dockerode satisfies it; tests pass a fake). */
export interface DockerEventApi {
  getEvents(opts: { since?: string; filters: Record<string, string[]> }): Promise<NodeJS.ReadableStream>;
  listContainers(opts: { all: boolean; filters: Record<string, string[]> }): Promise<Array<{ Id: string; State: string }>>;
}

/** Docker's `since` takes "<seconds>.<nanoseconds>". */
export function dockerSince(timeNano: bigint): string {
  const s = timeNano / 1_000_000_000n;
  const ns = timeNano % 1_000_000_000n;
  return `${s}.${ns.toString().padStart(9, "0")}`;
}

/**
 * The event's time in nanoseconds. `timeNano` (~1.8e18) is larger than 2^53, so JSON.parse would
 * round it; read the digits from the raw line instead so the replay point is exact.
 */
function eventTimeNano(ev: DockerEvent, raw: string): bigint | null {
  const exact = /"timeNano"\s*:\s*(\d+)/.exec(raw);
  if (exact) return BigInt(exact[1]);
  if (typeof ev.time === "number") return BigInt(ev.time) * 1_000_000_000n;
  return null;
}

const DEAD_STATES = new Set(["exited", "dead"]);

export class DeathWatch {
  private stream: NodeJS.ReadableStream | null = null;
  private stopped = false;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private reconnectDelayMs: number;
  /** Replay point for the next connection: the last event we saw, or when we last connected. */
  private since: bigint | null = null;
  private chain: Promise<unknown> = Promise.resolve();
  readonly stats = { connected: false, connects: 0, events: 0, recovered: 0, lastEventAt: null as string | null };

  constructor(
    private readonly docker: DockerEventApi,
    readonly project: string,
    private readonly opts: { minReconnectMs?: number; maxReconnectMs?: number } = {},
  ) {
    this.reconnectDelayMs = opts.minReconnectMs ?? 500;
  }

  get filters() {
    return { type: ["container"], event: ["die", "oom"], label: [`${PROJECT_LABEL}=${this.project}`] };
  }

  async start() {
    this.stopped = false;
    await this.connect();
  }

  stop() {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    const s = this.stream as (NodeJS.ReadableStream & { destroy?: () => void }) | null;
    this.stream = null;
    s?.removeAllListeners();
    s?.destroy?.();
    this.stats.connected = false;
  }

  /** Resolves once every event received so far has been handled (tests). */
  idle(): Promise<unknown> {
    return this.chain;
  }

  private async connect() {
    if (this.stopped) return;
    const since = this.since;
    this.since ??= BigInt(Date.now()) * 1_000_000n;
    let stream: NodeJS.ReadableStream;
    try {
      stream = await this.docker.getEvents({ ...(since !== null ? { since: dockerSince(since) } : {}), filters: this.filters });
    } catch (err) {
      console.warn(`[deathwatch] cannot subscribe to Docker events: ${(err as Error).message}`);
      this.scheduleReconnect();
      return;
    }
    if (this.stopped) return;
    this.stream = stream;
    this.stats.connected = true;
    this.stats.connects++;
    this.reconnectDelayMs = this.opts.minReconnectMs ?? 500;
    console.log(
      `[deathwatch] watching die/oom events for Compose project "${this.project}"` +
        (since !== null ? ` (replaying since ${dockerSince(since)})` : ""),
    );

    let buffer = "";
    // Bound to the context connect() runs in: when a leader started the watch, its fence, so the
    // recovery an event triggers is term-guarded whatever context the stream emits from.
    stream.on("data", AsyncLocalStorage.bind((chunk: Buffer | string) => {
      buffer += chunk.toString();
      let nl: number;
      while ((nl = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, nl).trim();
        buffer = buffer.slice(nl + 1);
        if (line) this.enqueue(line);
      }
    }));
    let ended = false;
    const onEnd = (why: string) => {
      if (ended || this.stream !== stream) return;
      ended = true;
      this.stream = null;
      this.stats.connected = false;
      if (!this.stopped) console.warn(`[deathwatch] event stream ${why}; reconnecting`);
      this.scheduleReconnect();
    };
    stream.on("end", AsyncLocalStorage.bind(() => onEnd("ended")));
    stream.on("close", AsyncLocalStorage.bind(() => onEnd("closed")));
    stream.on("error", AsyncLocalStorage.bind((err: Error) => onEnd(`failed (${err.message})`)));

    // Anything that died while we weren't listening (startup, or the gap before this reconnect).
    this.chain = this.chain.then(() => this.reconcile()).catch(logError("reconcile"));
  }

  private scheduleReconnect() {
    if (this.stopped || this.reconnectTimer) return;
    const delay = this.reconnectDelayMs;
    this.reconnectDelayMs = Math.min(this.reconnectDelayMs * 2, this.opts.maxReconnectMs ?? 10_000);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.connect();
    }, delay);
    this.reconnectTimer.unref?.();
  }

  /** Events are handled strictly one at a time, in arrival order. */
  private enqueue(line: string) {
    let ev: DockerEvent;
    try {
      ev = JSON.parse(line);
    } catch {
      return;
    }
    this.chain = this.chain.then(() => this.handle(ev, line)).catch(logError("event"));
  }

  async handle(ev: DockerEvent, raw = JSON.stringify(ev)) {
    const action = ev.Action ?? ev.status;
    if ((ev.Type && ev.Type !== "container") || (action !== "die" && action !== "oom")) return;
    // Defence in depth: the server-side label filter should already guarantee this.
    if (ev.Actor?.Attributes?.[PROJECT_LABEL] !== this.project) return;
    const containerId = ev.Actor?.ID ?? ev.id;
    if (!containerId) return;
    this.stats.events++;
    const nano = eventTimeNano(ev, raw);
    if (nano !== null && (this.since === null || nano > this.since)) this.since = nano;
    const diedAt = nano !== null ? new Date(Number(nano / 1_000_000n)) : new Date();
    this.stats.lastEventAt = diedAt.toISOString();

    const exitCode = ev.Actor?.Attributes?.exitCode;
    await this.recoverContainers([containerId], diedAt, {
      diedAt,
      action,
      exitCode: exitCode !== undefined && exitCode !== "" ? Number(exitCode) : null,
    });
  }

  /**
   * Maps container IDs to ALIVE container workers and recovers them. Workers register with the
   * short (12-char) container ID, Docker reports the full one, so the match is a prefix match on
   * at least 12 characters. A worker that registered *after* the death (a replayed event for a
   * container that has since been restarted) is left alone.
   */
  private async recoverContainers(containerIds: string[], diedBefore: Date | null, evidence: DeathEvidence) {
    const { rows } = await query<{ id: string }>(
      `select w.id from workers w, unnest($1::text[]) as c(full_id)
        where w.status = 'ALIVE' and w.runtime = 'container' and length(w.container_id) >= 12
          and left(c.full_id, length(w.container_id)) = w.container_id
          and ($2::timestamptz is null or w.registered_at <= $2::timestamptz)`,
      [containerIds, diedBefore],
    );
    if (rows.length === 0) return;
    const out = await recoverWorkers(
      rows.map((r) => r.id),
      "docker_event",
      () => evidence,
    );
    this.stats.recovered += out.dead.length;
  }

  /** `docker ps -a` for our project: ALIVE workers whose container has exited are dead. */
  async reconcile() {
    const containers = await this.docker.listContainers({
      all: true,
      filters: { label: [`${PROJECT_LABEL}=${this.project}`] },
    });
    const gone = containers.filter((c) => DEAD_STATES.has(c.State)).map((c) => c.Id);
    if (gone.length > 0) await this.recoverContainers(gone, null, { reconciled: true });
  }
}

function logError(what: string) {
  return (err: unknown) => console.error(`[deathwatch] ${what} failed: ${(err as Error).message}`);
}

// ---------------------------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------------------------

/**
 * Our own Compose project: COMPOSE_PROJECT if set, else the `com.docker.compose.project` label on
 * the container we are running in (inside Docker, os.hostname() is the short container ID).
 */
export async function resolveProject(): Promise<string | null> {
  if (config.composeProject) return config.composeProject;
  const info = await getDocker().getContainer(os.hostname()).inspect();
  return info?.Config?.Labels?.[PROJECT_LABEL] ?? null;
}

/**
 * The coordinator replica that pauses a worker (POST /workers/:id/pause) unpauses it on a timer, so
 * a worker container still paused after its pause should have ended was left behind by a replica
 * that died mid-pause. Leaving it frozen would strand its memory forever. Run by the leader when
 * elected and every few seconds after. Only containers of registered workers whose injected pause
 * is over are touched: never a pause another live replica is still timing, and never a container
 * that isn't a worker (e.g. a coordinator replica frozen by a failover test).
 */
export async function unpauseLeftovers(project = active?.project ?? null) {
  if (!project) return;
  try {
    const frozen = await getDocker().listContainers({
      filters: { label: [`${PROJECT_LABEL}=${project}`], status: ["paused"] },
    });
    if (frozen.length === 0) return;
    const { rows } = await query<{ full_id: string }>(
      `select c.full_id from unnest($1::text[]) as c(full_id)
        where exists (select 1 from workers w
                       where w.runtime = 'container' and length(w.container_id) >= 12
                         and left(c.full_id, length(w.container_id)) = w.container_id
                         and w.paused_until is not null and w.paused_until < now() - interval '2 seconds')`,
      [frozen.map((c) => c.Id)],
    );
    for (const { full_id } of rows) {
      await getDocker().getContainer(full_id).unpause();
      console.log(`[deathwatch] unpaused ${full_id.slice(0, 12)}, left paused by a coordinator that went away`);
    }
  } catch (err) {
    console.warn(`[deathwatch] could not check for paused containers: ${(err as Error).message}`);
  }
}

let active: DeathWatch | null = null;

export const deathWatchStatus = () =>
  active ? { enabled: true, project: active.project, ...active.stats } : { enabled: false };

/** Starts the watcher unless DOCKER_EVENTS=off or we can't tell which project is ours. */
export async function startDeathWatch(): Promise<DeathWatch | null> {
  if (config.dockerEvents === "off") {
    console.log("[deathwatch] disabled (DOCKER_EVENTS=off); heartbeat timeout only");
    return null;
  }
  let project: string | null = null;
  try {
    project = await resolveProject();
  } catch (err) {
    console.log(`[deathwatch] disabled: not running in a Compose container (${(err as Error).message}); heartbeat timeout only`);
    return null;
  }
  if (!project) {
    console.log("[deathwatch] disabled: no Compose project label on this container; heartbeat timeout only");
    return null;
  }
  await unpauseLeftovers(project);
  stopDeathWatch(); // at most one watcher per process, whichever leader term started it
  const watch = new DeathWatch(getDocker() as unknown as DockerEventApi, project);
  active = watch;
  await watch.start();
  return watch;
}

/** Stops `watch` (default: the active one). */
export function stopDeathWatch(watch: DeathWatch | null = active) {
  watch?.stop();
  if (active === watch) active = null;
}
