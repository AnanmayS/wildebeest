-- Coordinator HA (item #12, docs/decisions/h-ha.md): several coordinator replicas serve the API,
-- one of them is the leader and runs the singleton work (repair sweep and throttle decisions,
-- reaper, death watch reaction, chaos, queue rebuild). Leadership is a lease on one row, River
-- style, and every acquisition bumps `term`: the fencing token that every leader-only transaction
-- checks (wb_leader_guard) before it writes anything.

create table if not exists coordinator_leader (
  id          smallint primary key default 1 check (id = 1),
  holder      text null,                 -- COORDINATOR_ID of the current (or last) leader
  instance    text null,                 -- that process's random incarnation id
  term        bigint not null default 0, -- +1 on every acquisition, never reused
  since       timestamptz null,          -- when this term started
  renewed_at  timestamptz null,
  expires_at  timestamptz not null default '-infinity',
  resigned_at timestamptz null,          -- set by a graceful step-down (SIGTERM), cleared on acquire
  -- Cluster-wide switches that used to live in the coordinator's memory. The leader decides
  -- `throttled`; any replica can change chaos (POST /chaos), only the leader acts on it.
  throttled       boolean not null default false,
  chaos_enabled   boolean not null default false,
  chaos_every_sec int not null default 20
);
insert into coordinator_leader (id) values (1) on conflict (id) do nothing;

-- Every live coordinator process, heartbeated once per election tick. When one stops appearing
-- the leader rebuilds the Redis ready queues: IDs a dead replica had popped but not leased yet
-- (complete-and-claim-next) would otherwise stay "queued" in Postgres and in no Redis list.
create table if not exists coordinator_nodes (
  instance     text primary key,
  id           text not null,
  started_at   timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  role         text not null default 'follower',
  term         bigint null
);

-- Which leader term wrote an event. Stamped automatically inside guarded transactions (the guard
-- sets wb.leader_term for the transaction), NULL for everything else (API-path writes). Terms must
-- never go backwards in id order: an event of term t inserted after an event of term t+1 would be
-- a write accepted from a deposed leader. The default is set separately from ADD COLUMN so existing
-- rows aren't rewritten.
alter table task_events add column if not exists leader_term bigint null;
alter table task_events alter column leader_term set default nullif(current_setting('wb.leader_term', true), '')::bigint;

-- The fence. First statement of every leader-only transaction: lock the leader row FOR SHARE and
-- abort (SQLSTATE WBL01) unless the caller still holds `p_term`. A takeover is an UPDATE of that
-- row, so it waits until every transaction that passed the guard has finished: terms never
-- overlap, whatever pauses or clock jumps the old leader suffers. The lock is held only for the
-- transaction; an old leader frozen mid-transaction loses its session after 2 s idle, so it can't
-- hold a takeover off for longer than that.
create or replace function wb_leader_guard(p_holder text, p_instance text, p_term bigint) returns bigint
language plpgsql as $$
declare
  cur coordinator_leader%rowtype;
begin
  select * into cur from coordinator_leader where id = 1 for share;
  if not found or cur.term <> p_term or cur.holder is distinct from p_holder
     or cur.instance is distinct from p_instance then
    raise exception 'leader term % of % is stale: term % is held by %', p_term, p_holder, cur.term, cur.holder
      using errcode = 'WBL01',
            detail = json_build_object('currentTerm', cur.term, 'currentHolder', cur.holder)::text;
  end if;
  perform set_config('wb.leader_term', p_term::text, true);
  perform set_config('idle_in_transaction_session_timeout', '2000', true);
  return p_term;
end $$;

-- The leader's audit of queued rows (dispatcher.repairLostQueued) reads the rows Postgres counts as
-- sitting in a Redis list; only those few rows are indexed.
create index if not exists tasks_queued_idx on tasks (pushed_at) where state = 'PENDING' and queued;
