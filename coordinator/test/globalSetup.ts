import pg from "pg";

// Recreates the forgegrid_test database once per `npm test` run. Migrations are applied by
// the test helpers (through the real migrate()), so they are exercised too.
export default async function setup() {
  const url = new URL(process.env.TEST_DATABASE_URL ?? "postgres://forgegrid:forgegrid@localhost:15432/forgegrid_test");
  const dbName = url.pathname.slice(1);
  const admin = new URL(url);
  admin.pathname = "/postgres";
  const client = new pg.Client({ connectionString: admin.toString() });
  try {
    await client.connect();
  } catch (err) {
    throw new Error(
      `cannot reach Postgres at ${admin.host} (${(err as Error).message}). ` +
        "Start it with `docker compose up -d postgres redis minio` from the repo root.",
    );
  }
  await client.query(`drop database if exists ${dbName} with (force)`);
  await client.query(`create database ${dbName}`);
  await client.end();
}
