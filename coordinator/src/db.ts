import { AsyncLocalStorage } from "node:async_hooks";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { config } from "./config.js";

export type Db = pg.Pool | pg.PoolClient;

let pool: pg.Pool | null = null;

export function getPool(): pg.Pool {
  if (!pool) {
    // application_name names the replica in pg_stat_activity (the failover test terminates a
    // leader's backends by it).
    pool = new pg.Pool({ connectionString: config.databaseUrl, max: 20, application_name: applicationName() });
    pool.on("error", (err) => console.error("[db] idle client error", err.message));
  }
  return pool;
}

export async function closePool() {
  if (pool) {
    const p = pool;
    pool = null;
    await p.end();
  }
}

export const nodeId = () => config.coordinatorId || os.hostname();
export const applicationName = () => `wildebeest-coordinator:${nodeId()}`.slice(0, 63);

// ---------------------------------------------------------------------------------------------
// Leader fencing (docs/decisions/h-ha.md)
//
// Leader-only code (the repair sweep, the reaper, the death watch, chaos, queue rebuilds) runs
// inside withFence(). While a fence is in the async context, every query() and tx() on the
// shared pool becomes a transaction whose first statement is wb_leader_guard(term): it locks the
// leader row FOR SHARE and aborts unless this process still holds that term. So a leader-only
// path cannot forget the check, and a deposed leader that wakes up from a pause can't write:
// its first statement fails with WBL01 and it steps down. Code outside a fence (the API path,
// fenced per row by lease epochs) is untouched.
// ---------------------------------------------------------------------------------------------

export interface Fence {
  holder: string;
  instance: string;
  term: number;
  /** Called when the guard rejects this term (the elector steps down and records it). */
  onRejected?: (err: FencedError) => void;
}

/** The database refused a leader-only statement: this term has been superseded. */
export class FencedError extends Error {
  readonly code = "WBL01";
  constructor(
    readonly term: number,
    readonly currentTerm: number | null,
    readonly currentHolder: string | null,
    message: string,
  ) {
    super(message);
  }
}

const fences = new AsyncLocalStorage<Fence>();

/** Runs fn (and everything it starts: timers, streams, promises) as the leader of `fence.term`. */
export function withFence<T>(fence: Fence, fn: () => T): T {
  return fences.run(fence, fn);
}

/** Runs fn outside any fence (e.g. to record that a fence rejected us). */
export function withoutFence<T>(fn: () => T): T {
  return fences.exit(fn);
}

export const currentFence = (): Fence | null => fences.getStore() ?? null;

function toFencedError(err: any, fence: Fence): FencedError {
  let detail: { currentTerm?: number; currentHolder?: string } = {};
  try {
    detail = JSON.parse(err.detail ?? "{}");
  } catch {
    /* keep the message */
  }
  return new FencedError(fence.term, detail.currentTerm ?? null, detail.currentHolder ?? null, err.message);
}

const connectionLost = (err: any) =>
  (typeof err?.code === "string" && /^(08|57P)/.test(err.code)) ||
  /Connection terminated|ECONNRESET|EPIPE|not queryable/i.test(String(err?.message ?? ""));

/** Shorthand for a one-off query on the shared pool (guarded when running inside a fence). */
export function query<R extends pg.QueryResultRow = any>(text: string, params?: unknown[]): Promise<pg.QueryResult<R>> {
  if (!fences.getStore()) return getPool().query<R>(text, params);
  return tx((c) => c.query<R>(text, params));
}

/** Runs fn inside BEGIN/COMMIT; rolls back on any thrown error. Inside a fence, BEGIN is guarded. */
export async function tx<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
  const fence = fences.getStore();
  const client = await getPool().connect();
  let broken: Error | undefined;
  // The server may end the session between two statements (idle-in-transaction timeout after the
  // process was frozen, pg_terminate_backend). pg reports that as an 'error' event on the client,
  // which the pool only listens for while the client is idle: without this listener it crashes
  // the process. The next statement then fails and the connection is discarded below.
  const onError = (err: Error) => {
    broken = err;
  };
  client.on("error", onError);
  try {
    // BEGIN + guard in one round trip (simple protocol; the literals are escaped by the driver).
    await client.query(
      fence
        ? `begin; select wb_leader_guard(${client.escapeLiteral(fence.holder)}, ` +
            `${client.escapeLiteral(fence.instance)}, ${Math.trunc(fence.term)})`
        : "begin",
    );
    const result = await fn(client);
    await client.query("commit");
    return result;
  } catch (err: any) {
    // A connection the server closed (terminated backend, idle-in-transaction timeout) must not
    // go back to the pool.
    if (connectionLost(err)) broken = err;
    else await client.query("rollback").catch(() => {});
    if (fence && err?.code === "WBL01") {
      const fenced = toFencedError(err, fence);
      fence.onRejected?.(fenced);
      throw fenced;
    }
    throw err;
  } finally {
    client.removeListener("error", onError);
    client.release(broken);
  }
}

export const MIGRATIONS_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../migrations");

/**
 * Applies migrations/*.sql in filename order, each in its own transaction, and records them
 * in schema_migrations. An advisory lock keeps two processes from migrating at once.
 */
export async function migrate(dir = MIGRATIONS_DIR): Promise<string[]> {
  const client = await getPool().connect();
  const applied: string[] = [];
  try {
    await client.query("select pg_advisory_lock(424242)");
    await client.query(
      "create table if not exists schema_migrations (version text primary key, applied_at timestamptz not null default now())",
    );
    const { rows } = await client.query<{ version: string }>("select version from schema_migrations");
    const done = new Set(rows.map((r) => r.version));
    const files = fs.readdirSync(dir).filter((f) => f.endsWith(".sql")).sort();
    for (const file of files) {
      if (done.has(file)) continue;
      const sql = fs.readFileSync(path.join(dir, file), "utf8");
      await client.query("begin");
      try {
        await client.query(sql);
        await client.query("insert into schema_migrations (version) values ($1)", [file]);
        await client.query("commit");
        applied.push(file);
      } catch (err) {
        await client.query("rollback");
        throw new Error(`migration ${file} failed: ${(err as Error).message}`);
      }
    }
  } finally {
    await client.query("select pg_advisory_unlock(424242)").catch(() => {});
    client.release();
  }
  return applied;
}
