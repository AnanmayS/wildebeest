import { randomUUID } from "node:crypto";
import pg from "pg";
import { config } from "./config.js";
import { applicationName, getPool, nodeId, query, withoutFence, type Fence, type FencedError } from "./db.js";
import { hub, recordEvents } from "./events.js";

// Leader election for coordinator replicas (docs/decisions/h-ha.md): River's lease row plus an
// explicit fencing term.
//
//   acquire  UPDATE coordinator_leader SET holder = me, term = term + 1, expires_at = now() + TTL
//            WHERE expires_at <= now()                        (at most one candidate wins a term)
//   renew    UPDATE ... SET expires_at = now() + TTL WHERE holder = me AND term = myTerm
//   resign   UPDATE ... SET expires_at = now() WHERE holder = me AND term = myTerm   (SIGTERM)
//
// Only Postgres's clock is used, so coordinator clock skew doesn't matter. The lease decides who
// may *become* leader; it does not make a leader's writes safe on its own (a leader can be paused
// past its lease and wake up believing it still leads). That is the term's job: every leader-only
// transaction starts with wb_leader_guard(term) (db.ts, withFence), which a newer term fails.
//
// Step-down rules (client-go's RenewDeadline < LeaseDuration): a renewal that matches no row means
// someone took over → step down at once. A renewal that errors (Postgres unreachable, our
// connection terminated) is retried every tick, and we step down once LEADER_TTL_MS − one tick has
// passed without a successful renewal, i.e. before any other replica can acquire the lease. A
// guard rejection (FencedError) also steps down at once and is recorded as `leader_fenced`.
//
// The election runs on its own connection (not the shared pool), so a saturated pool can't starve
// renewals, and a lock wait behind our own guarded transactions is bounded by lock_timeout.

export interface PreviousTerm {
  holder: string | null;
  term: number;
  renewedAt: Date | null;
  expiresAt: Date | null;
  resigned: boolean;
}

export interface Election {
  fence: Fence;
  since: Date;
  previous: PreviousTerm | null;
}

export interface LeaderRow {
  holder: string | null;
  term: number;
  since: Date | null;
  expiresAt: Date | null;
}

export interface ElectorOptions {
  ttlMs: number;
  renewMs: number;
  databaseUrl?: string;
  /** Became leader. Called without awaiting; the elector keeps renewing meanwhile. */
  onElected: (e: Election) => void | Promise<void>;
  /** Stopped being leader: stop every leader-only loop now. */
  onDeposed: (reason: string, fence: Fence) => void;
}

const ACQUIRE = `
  with prev as (select holder, term, renewed_at, expires_at, resigned_at from coordinator_leader where id = 1)
  update coordinator_leader l
     set holder = $1, instance = $2, term = l.term + 1, since = now(), renewed_at = now(),
         expires_at = now() + ($3::int * interval '1 millisecond'), resigned_at = null
    from prev
   where l.id = 1 and l.expires_at <= now()
  returning l.term, l.since, prev.holder as prev_holder, prev.term as prev_term,
            prev.renewed_at as prev_renewed_at, prev.expires_at as prev_expires_at,
            prev.resigned_at as prev_resigned_at`;

const RENEW = `
  update coordinator_leader set renewed_at = now(), expires_at = now() + ($4::int * interval '1 millisecond')
   where id = 1 and holder = $1 and instance = $2 and term = $3
  returning expires_at`;

const RESIGN = `
  update coordinator_leader set expires_at = now(), resigned_at = now()
   where id = 1 and holder = $1 and instance = $2 and term = $3`;

// Heartbeat this replica's row and read who leads, in one statement.
const OBSERVE = `
  with me as (
    insert into coordinator_nodes (instance, id, role, term) values ($1, $2, $3, $4)
    on conflict (instance) do update set last_seen_at = now(), role = excluded.role, term = excluded.term
  )
  select holder, term, since, expires_at from coordinator_leader where id = 1`;

export class Elector {
  /** This process's incarnation: two processes with the same COORDINATOR_ID never share a term. */
  readonly instance = randomUUID();
  private fence: Fence | null = null;
  private lastRenewOk = 0;
  private client: pg.Client | null = null;
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private inFlight: Promise<void> | null = null;
  view: LeaderRow | null = null;
  readonly stats = {
    elections: 0,
    stepDowns: 0,
    fencedRejections: 0,
    lastStepDown: null as null | { at: string; term: number; reason: string },
  };

  constructor(
    readonly id: string,
    private readonly opts: ElectorOptions,
  ) {}

  isLeader(): boolean {
    return this.fence !== null;
  }

  /** The fence of the term we hold, or null. */
  currentFence(): Fence | null {
    return this.fence;
  }

  /** True while `fence` is still the term we hold (a deposed term never comes back). */
  holds(fence: Fence): boolean {
    return this.fence === fence;
  }

  get role(): "leader" | "follower" {
    return this.fence ? "leader" : "follower";
  }

  /** Runs the first election round, then keeps ticking every LEADER_RENEW_MS. */
  async start() {
    this.running = true;
    await this.tick();
    this.schedule();
  }

  /** Stops ticking; a leader resigns so a follower can take over at once instead of after the TTL. */
  async stop() {
    this.running = false;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    await this.inFlight?.catch(() => {});
    const fence = this.fence;
    if (fence) {
      this.stepDown("shutting down");
      await this.connection()
        .then((c) => c.query(RESIGN, [fence.holder, fence.instance, fence.term]))
        .catch((err) => console.warn(`[leader] resign failed: ${err.message}`));
    }
    const c = this.client;
    this.client = null;
    await c?.end().catch(() => {});
  }

  /** Runs an election round now (e.g. the leader just announced it resigned). */
  kick() {
    if (!this.running) return;
    void this.tick().catch(() => {});
  }

  private schedule() {
    if (!this.running) return;
    this.timer = setTimeout(() => {
      void this.tick().finally(() => this.schedule());
    }, this.opts.renewMs);
    this.timer.unref?.();
  }

  private async connection(): Promise<pg.Client> {
    if (this.client) return this.client;
    const c = new pg.Client({
      connectionString: this.opts.databaseUrl ?? config.databaseUrl,
      application_name: applicationName(),
      connectionTimeoutMillis: this.opts.renewMs * 2,
      // A renewal may wait behind our own guarded transactions (they hold the row FOR SHARE); a
      // candidate's acquire may wait behind the leader's. Neither may hang an election tick.
      lock_timeout: this.opts.renewMs,
      statement_timeout: this.opts.renewMs * 2,
    });
    c.on("error", () => {
      if (this.client === c) this.client = null;
    });
    await c.connect();
    this.client = c;
    return c;
  }

  /** One election round. Single flight: overlapping calls share the round in progress. */
  tick(): Promise<void> {
    this.inFlight ??= this.round().finally(() => {
      this.inFlight = null;
    });
    return this.inFlight;
  }

  private async round() {
    try {
      const c = await this.connection();
      if (this.fence) await this.renew(c, this.fence);
      if (!this.fence) await this.tryAcquire(c);
      const { rows } = await c.query(OBSERVE, [this.instance, this.id, this.role, this.fence?.term ?? null]);
      const r = rows[0];
      this.view = r
        ? { holder: r.holder, term: Number(r.term), since: r.since, expiresAt: r.expires_at }
        : null;
    } catch (err) {
      const c = this.client;
      this.client = null;
      c?.end().catch(() => {});
      const fence = this.fence;
      if (fence && performance.now() - this.lastRenewOk > this.opts.ttlMs - this.opts.renewMs) {
        this.stepDown(`could not renew the lease for ${Math.round(performance.now() - this.lastRenewOk)} ms (${(err as Error).message})`);
      } else if (!fence) {
        console.warn(`[leader] election round failed: ${(err as Error).message}`);
      }
    }
  }

  private async renew(c: pg.Client, fence: Fence) {
    const { rowCount } = await c.query(RENEW, [fence.holder, fence.instance, fence.term, this.opts.ttlMs]);
    if (rowCount) {
      this.lastRenewOk = performance.now();
      return;
    }
    this.stepDown("superseded: another replica holds a newer term");
  }

  private async tryAcquire(c: pg.Client) {
    const { rows } = await c.query(ACQUIRE, [this.id, this.instance, this.opts.ttlMs]);
    if (rows.length === 0) return;
    const r = rows[0];
    const fence: Fence = {
      holder: this.id,
      instance: this.instance,
      term: Number(r.term),
      onRejected: (err) => this.fenced(fence, err),
    };
    this.fence = fence;
    this.lastRenewOk = performance.now();
    this.stats.elections++;
    const previous: PreviousTerm | null =
      Number(r.prev_term) > 0
        ? {
            holder: r.prev_holder,
            term: Number(r.prev_term),
            renewedAt: r.prev_renewed_at,
            expiresAt: r.prev_expires_at,
            resigned: r.prev_resigned_at !== null,
          }
        : null;
    console.log(
      `[leader] ${this.id} elected leader, term ${fence.term}` +
        (previous ? ` (previous: ${previous.holder}, term ${previous.term}, ${previous.resigned ? "resigned" : "lease expired"})` : ""),
    );
    Promise.resolve()
      .then(() => this.opts.onElected({ fence, since: new Date(r.since), previous }))
      .catch((err) => console.error(`[leader] onElected failed: ${(err as Error).message}`));
  }

  /** Stop leading now. `resign` also gives the lease up, so the next round can elect anyone. */
  stepDown(reason: string, opts: { resign?: boolean } = {}) {
    const fence = this.fence;
    if (!fence) return;
    this.fence = null;
    this.stats.stepDowns++;
    this.stats.lastStepDown = { at: new Date().toISOString(), term: fence.term, reason };
    console.warn(`[leader] ${this.id} stepped down from term ${fence.term}: ${reason}`);
    try {
      this.opts.onDeposed(reason, fence);
    } catch (err) {
      console.error(`[leader] onDeposed failed: ${(err as Error).message}`);
    }
    if (opts.resign) {
      void this.connection()
        .then((c) => c.query(RESIGN, [fence.holder, fence.instance, fence.term]))
        .catch(() => {});
    }
  }

  /** The database refused a statement of `fence`'s term: record it and step down. */
  private fenced(fence: Fence, err: FencedError) {
    this.stats.fencedRejections++;
    console.warn(`[leader] write of term ${fence.term} rejected by the fence: ${err.message}`);
    // An audit record, not a leader-only write: it goes in outside the fence (leader_term NULL).
    withoutFence(() =>
      recordEvents(getPool(), [
        {
          type: "leader_fenced",
          detail: { holder: fence.holder, term: fence.term, currentTerm: err.currentTerm, currentHolder: err.currentHolder },
        },
      ])
        .then((rows) => hub.publishEvents(rows))
        .catch(() => {}),
    );
    if (this.fence === fence) this.stepDown(`fenced: term ${err.currentTerm ?? "?"} is current`);
  }
}

// ---------------------------------------------------------------------------------------------
// This process's elector, for GET /system (`leader`) and GET /cluster
// ---------------------------------------------------------------------------------------------

let current: Elector | null = null;

export function setElector(e: Elector | null) {
  current = e;
}

export const getElector = () => current;

/** `system.leader`: the leader as this replica last saw it (null until an election has run). */
export function leaderView(): { id: string; term: number; since: string } | null {
  const v = current?.view;
  if (!v || !v.holder || v.term <= 0 || !v.since) return null;
  return { id: v.holder, term: v.term, since: new Date(v.since).toISOString() };
}

export async function clusterStatus() {
  const [leader, nodes] = await Promise.all([
    query(`select holder, term, since, renewed_at, expires_at, expires_at > now() as valid from coordinator_leader where id = 1`),
    query(
      `select id, instance, role, term, started_at, last_seen_at from coordinator_nodes
        where last_seen_at > now() - interval '1 minute' order by id, started_at`,
    ),
  ]);
  const l = leader.rows[0];
  return {
    self: current
      ? { id: current.id, instance: current.instance, role: current.role, term: current.currentFence()?.term ?? null, ...current.stats }
      : { id: nodeId(), instance: null, role: "follower", term: null },
    leader: l?.holder
      ? {
          id: l.holder,
          term: Number(l.term),
          since: l.since,
          renewedAt: l.renewed_at,
          expiresAt: l.expires_at,
          valid: l.valid,
        }
      : null,
    nodes: nodes.rows.map((n) => ({
      id: n.id,
      instance: n.instance,
      role: n.role,
      term: n.term === null ? null : Number(n.term),
      startedAt: n.started_at,
      lastSeenAt: n.last_seen_at,
    })),
  };
}
