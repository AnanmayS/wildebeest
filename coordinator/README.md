# ForgeGrid coordinator

Node/TypeScript service that owns all task state. Workers only ever `BLMOVE` task IDs out of Redis;
every state change goes through this service's HTTP API (see `docs/CONTRACTS.md`).

```
src/
  config.ts      env + defaults (mutable object, tests shorten timeouts)
  db.ts          pg pool, tx helper, SQL migration runner (migrations/*.sql, schema_migrations)
  redis.ts       key names: queue:{stage}, processing:{workerId}, worker:{id}:alive, forgegrid:throttled
  storage.ts     MinIO: ensure bucket, idempotent put, cached presigned URLs (S3_PUBLIC_ENDPOINT)
  jobs.ts        job creation (upload / sample), cache lookup, JobSummary + gallery queries
  tasks.ts       the state machine: claimConfirm, completeTask, failTask, requeueLostLeases, release
  results.ts     idempotent result writes, categorisation, image/job finalisation
  dispatcher.ts  200 ms loop: Postgres PENDING → Redis ready queues, backpressure   (dispatchOnce)
  reaper.ts      1 s loop: dead workers, lost leases, processing-list drain          (reapOnce)
  workers.ts     register / heartbeat / deregister / list
  docker.ts      SIGKILL through the Docker socket;  chaos.ts  random kills
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
- **Leases + heartbeats.** A heartbeat renews all of the worker's leases. The reaper marks a worker
  DEAD after `WORKER_TIMEOUT_MS` of silence and moves its tasks back to PENDING (attempts+1), or to
  FAILED after `MAX_ATTEMPTS`. It does the same for any lease that simply expired.
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

The test run recreates a database called `forgegrid_test`, flushes Redis DB 15 and uses the bucket
`forgegrid-test`. It connects to `localhost:15432`, `localhost:16379` and `localhost:9000`, which
you can override with `TEST_DATABASE_URL`, `TEST_REDIS_URL` and `TEST_S3_ENDPOINT`.
