-- When the dispatcher could first have pushed the task: it was PENDING, past its retry backoff,
-- and not held back by DETECT_QUEUE_TARGET or backpressure. dispatchWaitMs = pushed_at - eligible_at
-- is then pure orchestration (tick delay + push latency); a backlog waiting in Postgres behind a
-- full queue counts as queue wait instead (docs/CONTRACTS.md, system.timings).
alter table tasks add column eligible_at timestamptz null;
