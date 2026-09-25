import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { config } from "./config.js";

export type Db = pg.Pool | pg.PoolClient;

let pool: pg.Pool | null = null;

export function getPool(): pg.Pool {
  if (!pool) {
    pool = new pg.Pool({ connectionString: config.databaseUrl, max: 20 });
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

/** Shorthand for a one-off query on the shared pool. */
export function query<R extends pg.QueryResultRow = any>(text: string, params?: unknown[]) {
  return getPool().query<R>(text, params);
}

/** Runs fn inside BEGIN/COMMIT; rolls back on any thrown error. */
export async function tx<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
  const client = await getPool().connect();
  try {
    await client.query("begin");
    const result = await fn(client);
    await client.query("commit");
    return result;
  } catch (err) {
    await client.query("rollback").catch(() => {});
    throw err;
  } finally {
    client.release();
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
