import { config } from "./config.js";
import { query } from "./db.js";
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
      ? query<{ n: number }>(
          `select count(*)::int as n from (
             select e.task_id from task_events e
              where e.type = 'succeeded'
                and e.task_id in (select task_id from task_events
                                   where type = 'succeeded' and id > $1 and id <= $2)
              group by e.task_id having count(*) > 1) d`,
          [from, maxId],
        )
      : Promise.resolve({ rows: [{ n: 0 }] }),
    query<{ n: number }>(
      `select count(*)::int as n from tasks t left join workers w on w.id = t.worker_id
        where t.state = 'LEASED'
          and ((w.status is distinct from 'ALIVE'
                and coalesce(w.last_heartbeat_at, '-infinity') < now() - ($1::int * interval '1 millisecond'))
               or t.lease_expires_at < now() - ($2::int * interval '1 millisecond'))`,
      [config.workerTimeoutMs + slackMs, slackMs],
    ),
    query<{ n: number }>(
      `select count(*)::int as n from images i join jobs j on j.id = i.job_id
        where j.status = 'running' and i.final_category is null
          and not exists (select 1 from tasks t where t.image_id = i.id and t.state in ('PENDING', 'LEASED'))`,
    ),
  ]);

  cursor = maxId;
  duplicateTotal += dups.rows[0].n;
  last = {
    checkedAt: new Date().toISOString(),
    duplicateResults: duplicateTotal,
    stuckLeases: stuck.rows[0].n,
    lostImages: lost.rows[0].n,
    ok: duplicateTotal === 0 && stuck.rows[0].n === 0 && lost.rows[0].n === 0,
  };
  if (!last.ok) console.warn(`[invariants] VIOLATION ${JSON.stringify(last)}`);
  return last;
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
}
