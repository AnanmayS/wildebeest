# Wildebeest coordinator

Node/TypeScript service that owns all task state. Workers only ever `BLMOVE` task IDs out of Redis;
every state change goes through this service's HTTP API (see `docs/CONTRACTS.md`).

```
src/
  config.ts      env + defaults (mutable object, tests shorten timeouts)
  db.ts          pg pool, tx helper, SQL migration runner (migrations/*.sql, schema_migrations)
  redis.ts       key names: queue:{stage}, processing:{workerId}, worker:{id}:alive, wildebeest:throttled
  storage.ts     MinIO: ensure bucket, idempotent put, cached presigned URLs (S3_PUBLIC_ENDPOINT)
  jobs.ts        job creation (upload / sample / synthetic), cache lookup, JobSummary + gallery queries
  tasks.ts       the state machine: claim, complete, fail, release, requeueLostLeases (attempt accounting)
  results.ts     idempotent result writes, categorisation, image/job finalisation
  dispatcher.ts  200 ms loop: Postgres PENDING → Redis ready queues, backpressure   (dispatchOnce, pushNow)
  deathwatch.ts  Docker die/oom events for our Compose project → recovery, docker ps reconciliation
  recovery.ts    the one death path: mark DEAD → requeue leases + processing list → LPUSH to queue head
  reaper.ts      1 s loop: heartbeat backstop, lost leases, stall-aware grace       (reapOnce, StallMeter)
  workers.ts     register / heartbeat / deregister / list
  docker.ts      SIGKILL and timed pause through the Docker socket;  chaos.ts  random kills
  dlq.ts         GET /dlq, redrive
  telemetry.ts   in-memory rolling counters, timing samples, recovery records
  system.ts      GET /system + WebSocket `system` snapshot;  invariants.ts  live invariant check
  events.ts      task_events rows + WebSocket hub at /events;  metrics.ts  GET /metrics
  api.ts         Express routes;  index.ts  startup (migrate → bucket → reconcile → serve → loops)
```

## How the fault tolerance works

- **Postgres is the source of truth.** Every transition is one guarded statement,
  `UPDATE tasks ... WHERE id = $1 AND state = 'LEASED' AND lease_epoch = $2`. If another actor got
  there first, zero rows match and the caller loses cleanly.
- **Fencing tokens.** Each claim increments `lease_epoch` and returns it. `complete` and `fail` must
  echo it. A worker that stalled past its lease (GC pause, network partition) and then comes back
  holds an old epoch, so its late result gets `409 STALE_LEASE` (logged as `stale_rejected`), and the
  result from the worker that took over is the one kept.
- **Death detection: listen first, infer second.** The coordinator subscribes to Docker `die`/`oom`
  events for its own Compose project and recovers a killed worker in well under a second
  (`deathwatch.ts`). The heartbeat timeout stays as the backstop: the reaper marks a worker DEAD after
  `WORKER_TIMEOUT_MS` of silence, extended by its own recent stall. Both run the same path
  (`recovery.ts`), which pushes the recovered task IDs to the head of their queue right after commit.
- **Leases + heartbeats.** A heartbeat renews the leases the worker reports holding. Lost leases go
  back to PENDING (attempts+1), or to FAILED after `MAX_ATTEMPTS`, unless the coordinator itself
  killed or paused the worker, which costs no attempt.
- **Retry hygiene.** Workers classify errors: infrastructure trouble → `/release` (free) plus a
  worker-side circuit breaker; bad input → `/fail` with `nonRetryable` (straight to the DLQ); other
  errors → `/fail` with full-jitter backoff before the retry. `GET /dlq` and redrive expose FAILED tasks.
- **Processing lists.** `BLMOVE queue:{stage} processing:{workerId}` means a task ID is never only in
  a worker's memory. Claim-confirm, complete and fail `LREM` the ID. Anything left in a dead worker's
  list was taken but never confirmed; the reaper drains it back to the ready queue (LRANGE + DEL in
  one MULTI, so an ID can't slip in between and get lost).
- **Idempotency.** Results use `INSERT ... ON CONFLICT (sha256, model_version) DO NOTHING`, the
  classify task uses the `unique (image_id, stage)` constraint, and MinIO keys are content-addressed.
- **Duplicates are harmless.** An ID can end up in a queue twice (for example after a drain races a
  claim), but only a PENDING task can be leased, so the second copy is skipped.

## Tests

The tests use real Postgres, Redis and MinIO. They drive the loops directly (`dispatchOnce()`,
`reapOnce()`), and they simulate the passage of time by back-dating heartbeats and leases instead
of sleeping.

```bash
docker compose up -d postgres redis minio     # from the repo root
cd coordinator && npm install && npm test
```

The test run recreates a database called `wildebeest_test`, flushes Redis DB 15 and uses the bucket
`wildebeest-test`. It connects to `localhost:15432`, `localhost:16379` and `localhost:9000`, which
you can override with `TEST_DATABASE_URL`, `TEST_REDIS_URL` and `TEST_S3_ENDPOINT`.
