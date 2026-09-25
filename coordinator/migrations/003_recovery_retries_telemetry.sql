-- P1: recovery fast path, error classification / retry hygiene, and per-task telemetry.

-- Retry accounting. `attempts` keeps its meaning: the attempts that count against MAX_ATTEMPTS.
-- It is always task_errors + lease_losses. `releases` counts returns that cost nothing:
-- /release (infrastructure errors), deregister / re-register, and leases lost because the
-- coordinator itself killed or paused the worker.
alter table tasks
  add column task_errors  int not null default 0,
  add column lease_losses int not null default 0,
  add column releases     int not null default 0,
  -- Full-jitter backoff after a task error: the dispatcher leaves the task alone until then.
  add column not_before   timestamptz null,
  -- When the task last became PENDING (created, failed, requeued, released, redriven).
  add column pending_at   timestamptz not null default now(),
  -- When the dispatcher last pushed the ID to a Redis ready queue.
  add column pushed_at    timestamptz null,
  -- Worker-measured { claimMs, fetchMs, inferMs, uploadMs } from the accepted complete.
  add column timings      jsonb null,
  -- Coordinator-side handling time of the accepted complete request (written behind, batched).
  add column complete_ms  real null;

-- Existing rows keep `attempts`; their breakdown starts at 0.
update tasks set pending_at = enqueued_at;

-- DLQ view: FAILED tasks, newest first.
create index tasks_failed_idx on tasks (finished_at desc) where state = 'FAILED';

-- Worker runtime and coordinator-induced faults.
alter table workers
  add column runtime   text not null default 'container' check (runtime in ('container', 'native')),
  add column device    text not null default 'cpu' check (device in ('cpu', 'mps', 'cuda')),
  add column paused_at timestamptz null;
