import { config } from "./config.js";
import { query, tx } from "./db.js";
import { reaperStalls } from "./reaper.js";

// A cheap live check of the properties the design promises, run at most every 5 s and shown on
// the dashboard (`system.invariants`). Each query is scoped so it stays cheap on big jobs:
//
//  duplicateResults  tasks accepted as SUCCEEDED more than once (two `succeeded` events): a
//                    fencing failure. Only events newer than the previous check are examined, and
//                    the count is cumulative, because a violation never heals.
//                    (Result rows themselves are keyed by (sha256, model_version), so a second
//                    row per image is impossible by schema; the event check catches a second
//                    *accepted* completion, which is the real risk.)
//  stuckLeases       LEASED tasks the reaper should already have taken back: on a non-ALIVE
//                    worker silent for longer than WORKER_TIMEOUT_MS + 2 reaper ticks, or with a
//                    lease that expired more than 2 ticks ago (plus the reaper's own stall grace).
//  lostImages        images of running jobs with no final category and no PENDING/LEASED task,
//                    i.e. work that fell through the cracks and would keep the job running forever.
//
// Every query runs with a statement timeout (INVARIANT_TIMEOUT_MS, 2 s): a check must never become
// a long-running transaction. One did (docs/decisions/f-fixups.md): lostImages, planned while a big
// job's rows were still being inserted into a freshly truncated `tasks` table (0 pages, so an
// estimate of 0 rows), chose a nested loop over a sequential scan of tasks and then executed
// against the committed job: 120,000 × 120,000 rows, 24 minutes, on both replicas, holding back
// vacuum and blocking a TRUNCATE. lostImages is also run with nested loops off, so its plan is a
// hash anti-join (linear in unfinished images + live tasks) whatever the statistics say. A check
// that times out keeps its previous value and is retried on the next pass.

export interface InvariantReport {
  checkedAt: string;
  duplicateResults: number;
  stuckLeases: number;
  lostImages: number;
  ok: boolean;
}

let last: InvariantReport | null = null;
let duplicateTotal = 0;
let cursor: number | null = null; // highest task_events.id already examined
let running: Promise<InvariantReport> | null = null;
let lastStuck = 0;
let lastLost = 0;

/** Only the newest events are examined on the first pass, so startup stays cheap on a big history. */
const FIRST_PASS_EVENTS = 100_000;

export function checkInvariants(): Promise<InvariantReport> {
  running ??= run().finally(() => (running = null));
  return running;
}

async function run(): Promise<InvariantReport> {
  const slackMs = 2 * config.reapIntervalMs + reaperStalls.graceMs();
  const { rows: top } = await query<{ max: string | null }>(`select max(id) as max from task_events`);
  const maxId = Number(top[0].max ?? 0);
  const from = cursor ?? Math.max(0, maxId - FIRST_PASS_EVENTS);

  const [dups, stuck, lost] = await Promise.all([
    maxId > from
      ? bounded<{ n: number }>(
          `select count(*)::int as n from (
             select e.task_id from task_events e
              where e.type = 'succeeded'
                and e.task_id in (select task_id from task_events
                                   where type = 'succeeded' and id > $1 and id <= $2)
              group by e.task_id having count(*) > 1) d`,
          [from, maxId],
        )
      : Promise.resolve({ n: 0 }),
    bounded<{ n: number }>(
      `select count(*)::int as n from tasks t left join workers w on w.id = t.worker_id
        where t.state = 'LEASED'
          and ((w.status is distinct from 'ALIVE'
                and coalesce(w.last_heartbeat_at, '-infinity') < now() - ($1::int * interval '1 millisecond'))
               or t.lease_expires_at < now() - ($2::int * interval '1 millisecond'))`,
      [config.workerTimeoutMs + slackMs, slackMs],
    ),
    bounded<{ n: number }>(
      `select count(*)::int as n from images i join jobs j on j.id = i.job_id
        where j.status = 'running' and i.final_category is null
          and not exists (select 1 from tasks t where t.image_id = i.id and t.state in ('PENDING', 'LEASED'))`,
      [],
      { noNestLoop: true },
    ),
  ]);

  // A timed-out duplicate check leaves the cursor where it was, so those events are examined again.
  if (dups) cursor = maxId;
  duplicateTotal += dups?.n ?? 0;
  if (stuck) lastStuck = stuck.n;
  if (lost) lastLost = lost.n;
  last = {
    checkedAt: new Date().toISOString(),
    duplicateResults: duplicateTotal,
    stuckLeases: lastStuck,
    lostImages: lastLost,
    ok: duplicateTotal === 0 && lastStuck === 0 && lastLost === 0,
  };
  if (!last.ok) console.warn(`[invariants] VIOLATION ${JSON.stringify(last)}`);
  return last;
}

/**
 * One read-only query in its own transaction with a statement timeout (and, for lostImages, no
 * nested loops). Returns the first row, or null when it timed out.
 */
async function bounded<T>(sql: string, params: unknown[], opts: { noNestLoop?: boolean } = {}): Promise<T | null> {
  try {
    return await tx(async (c) => {
      await c.query(
        `set local statement_timeout = ${Math.max(1, Math.trunc(config.invariantTimeoutMs))}` +
          (opts.noNestLoop ? "; set local enable_nestloop = off" : ""),
      );
      const { rows } = await c.query(sql, params);
      return rows[0] as T;
    });
  } catch (err: any) {
    if (err?.code !== "57014") throw err; // query_canceled (statement_timeout)
    console.warn(`[invariants] a check took longer than ${config.invariantTimeoutMs} ms; keeping its last value`);
    return null;
  }
}
/** The latest report, running a first check if there has been none yet. */
export async function latestInvariants(): Promise<InvariantReport> {
  return last ?? checkInvariants();
}

/** Test hook. */
export function resetInvariants() {
  last = null;
  duplicateTotal = 0;
  cursor = null;
  lastStuck = 0;
  lastLost = 0;
}
