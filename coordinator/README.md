# Wildebeest coordinator

Node/TypeScript service that owns all task state. Workers move task IDs out of Redis (`BLMOVE`/`LMOVE`) or long-poll
`POST /tasks/claim`. Every state change goes through this service's HTTP API (see `docs/CONTRACTS.md`), which is where
fencing is enforced. Two replicas run behind `coordinator-lb` (HAProxy). Both serve the whole API; the one holding
the Postgres leader lease also runs the singleton loops.

```
src/
  index.ts         startup: migrate → bucket → WebSocket hub → join the cluster (startHa) → serve; SIGTERM resigns
  config.ts        env + defaults (a mutable object, so tests shorten timeouts in place)
  db.ts            pg pool, tx(), migrations; withFence(): every transaction in a leader context starts with
                   wb_leader_guard(term); a server-ended session fails the transaction instead of crashing
  migrate.ts       `npm run migrate`
  redis.ts         key names: queue:{stage}, processing:{workerId}, spec:{workerId}, worker:{id}:alive, markers
  storage.ts       MinIO: bucket, idempotent put, cached presigned URLs (null for synthetic/ keys)

  jobs.ts          job creation (upload / sample / synthetic as one SQL statement), cache lookup, summaries, gallery
  tasks.ts         the state machine: one-statement claim (leaseSql), claim-confirm, Postgres-mode claim,
                   complete / complete-batch via wb_complete (+ next), fail, release, requeueLostLeases
                   (attempt accounting, own-fault and unacknowledged-lease attribution, copy promotion)
  results.ts       idempotent result writes, categorisation, wb_finish_job (job row locked only near the end)
  dispatcher.ts    push after commit (pushNew / pushNow / kickDetect), 200 ms repair sweep + backpressure,
                   detectQueueTarget, Redis rebuild, queued-row audit incl. orphaned processing-list IDs
  workers.ts       register / heartbeat (renews reported taskIds, returns cancel) / deregister / list
  speculation.ts   straggler speculation: policy loop, per-worker service times, probation, offers

  recovery.ts      the one death path: guarded mark DEAD → requeue leases + drain processing list → LPUSH to head
  deathwatch.ts    Docker die/oom events for our Compose project, reconnect with since=, docker ps reconciliation
  reaper.ts        1 s backstop: heartbeat timeouts, expired leases, dead workers' lists, job-finish sweep;
                   StallMeter (grace = the reaper's own recent lateness)
  docker.ts        SIGKILL and timed pause through the Docker socket (attribution stamped first)
  chaos.ts         random kills (leader only; switch shared in the leader row)
  dlq.ts           GET /dlq, redrive

  leader.ts        election: lease row coordinator_leader (holder, instance, term, expires_at), renew, resign,
                   step-down rules, dedicated connection
  ha.ts            wires a replica into the cluster: leader work inside its fence, replica heartbeats, followers
  loops.ts         every(); startLeaderLoops (sweep, reaper, speculation, audits), startReplicaLoops, reconcileAsLeader
  cluster.ts       Redis pub/sub bus between replicas: event-log rows, change notifications, long-poll wake-ups,
                   replicated telemetry (incl. speculation service times)

  telemetry.ts     in-memory timings, throughput, recovery records, fencing (replicated over the bus)
  system.ts        GET /system and the WebSocket `system` snapshot (cached, shared by all clients)
  invariants.ts    live invariant check, each query bounded by statement_timeout
  events.ts        task_events rows + the WebSocket hub at /events (paced, coalesced)
  metrics.ts       GET /metrics (JSON; recovery from task_events)
  prom.ts          GET /metrics/prom (Prometheus RED/USE; gauges computed at scrape time)
  otel.ts          per-image trace hooks (producer, derived attempt spans, requeue, complete)
  otel-sdk.ts      SDK bootstrap (only when OTEL_EXPORTER_OTLP_ENDPOINT is set)
  otel-preload.ts  ESM loader hook for http/pg/ioredis instrumentation (`node --import`)
  api.ts           Express routes

migrations/        001 init … 006 hot path (wb_complete, wb_finish_job, fillfactor/autovacuum) · 007 speculation
                   (task_attempts, wb_complete + copies) · 008 leader (coordinator_leader, wb_leader_guard,
                   coordinator_nodes) · 009 trace context · 010 fix-ups (idempotent completion retry)
scripts/failover.py  leader failover campaign (kill / pause / stop / pg_terminate_backend) + stale-term audit
```

## How the fault tolerance works

- **Postgres is the source of truth.** Every transition is one guarded statement, e.g.
  `UPDATE tasks … WHERE id = $1 AND state = 'LEASED' AND lease_epoch = $2`. If another actor got there first, zero
  rows match and the caller loses cleanly.
- **Fencing tokens.** Each claim increments `lease_epoch` and returns it; `complete`, `fail` and `release` must echo
  it. A worker that stalled past its lease holds an old epoch, and its late result gets `409 STALE_LEASE`.
- **Speculative copies** have their own epochs and are valid only while the lease they shadow is current. The first
  commit wins; the loser gets `409 ALREADY_DONE`. A copy whose original lost its lease is promoted instead of
  requeued.
- **Death detection: listen first, infer second.** Docker `die`/`oom` events (`deathwatch.ts`) and the heartbeat
  timeout (`reaper.ts`) run the same guarded path (`recovery.ts`), which pushes recovered IDs to the head of their
  queue right after the requeue commits.
- **Leases and heartbeats.** A heartbeat renews exactly the leases the worker reports holding. A lost lease costs an
  attempt (`MAX_ATTEMPTS = 3`), unless we caused it (our kill or pause) or the worker never learned of it (a claim
  response lost with a replica).
- **Retry hygiene.** Infrastructure errors → `/release` (free) plus a worker-side circuit breaker; bad input → `/fail`
  `nonRetryable` (DLQ); other errors → `/fail` with full-jitter backoff. `GET /dlq` and redrive expose FAILED tasks.
- **Processing lists.** A worker's claim moves IDs into `processing:{workerId}`, so an ID is never only in a worker's
  memory. `claim-confirm` empties the list. A dead worker's list is drained back to the ready queue. An ID left in a
  live worker's list by a lost reply is confirmed by that worker's next claim, or taken back by the leader's audit
  after 3 heartbeats.
- **Leader term.** Leader-only work runs inside `withFence(term)`; `wb_leader_guard` takes the leader row
  `FOR SHARE` and aborts if the term moved on, so a deposed leader's statements are refused by the database.
- **Idempotency.** Results use `INSERT … ON CONFLICT (sha256, model_version) DO NOTHING` and are read back; classify
  tasks are unique per `(image_id, stage)`; MinIO keys are content-addressed. A completion retried after its first
  send committed is answered `200`.
- **Duplicates are harmless.** An ID can end up in a queue twice, but only a `PENDING` task can be leased, so the
  second copy is skipped.

## Tests

The tests use real Postgres, Redis and MinIO. They drive the loops directly (`dispatchOnce()`, `reapOnce()`) and
simulate the passage of time by back-dating heartbeats and leases instead of sleeping. `npm test` runs the suite
twice, with `CLAIM_MODE=hybrid` (167 tests) and `CLAIM_MODE=postgres` (146, plus 21 Redis-only tests skipped).

```bash
docker compose up -d postgres redis minio     # from the repo root
cd coordinator && npm install && npm test
```

The test run recreates a database called `wildebeest_test`, flushes Redis DB 15 and uses the bucket
`wildebeest-test`. It connects to `localhost:15432`, `localhost:16379` and `localhost:9000`, which you can override
with `TEST_DATABASE_URL`, `TEST_REDIS_URL` and `TEST_S3_ENDPOINT`.
