# P2: hot-path throughput

Owner: P2 agent. Scope: `coordinator/**`, worker `runtime.py`, `storage.py`, `__main__.py`, `worker/tests/**`,
compose environment entries, and the "P2 refinements" section of `docs/CONTRACTS.md`. Implements report items #3
(event-driven dispatch), #4 (job-row lock), #5 (collapse round trips), #9 (queue hygiene) and the
`CLAIM_MODE=postgres` A/B.

## What changed, by item

**#3 Push-after-commit dispatch** (`dispatcher.ts`). Whoever commits a transaction that makes tasks PENDING pushes
them right after the commit: job creation (classify tasks, detect top-up), a detect completion's new classify task,
requeue (reaper / death watch, as in P1), release, redrive, deregister, and the end of a retry backoff (a timer, not
the next sweep). The row itself is the outbox: it commits with `queued=false`, the push flips it with a guarded
`UPDATE … WHERE queued = false RETURNING` and then RPUSH/LPUSHes, so exactly one pusher wins and a crash between
COMMIT and push leaves a row the 200 ms tick repairs. Detect admission is a single-flight, coalescing top-up
(`kickDetect`) triggered by every detect claim, so the queue refills as fast as workers drain it instead of 50 IDs
per 200 ms tick. It still skips while backpressure is on. The queue target scales:
`max(DETECT_QUEUE_MIN, 2 × Σ live detect workers' claim window)`, where each worker reports its window in its
heartbeat; `DETECT_QUEUE_TARGET > 0` pins it (tests use 5). `system.dispatcher.mode` reports `DISPATCH_MODE`
(push by default; `tick` restores P1 for A/B). `eligible_at` keeps P1's meaning: a push from the event path uses
the previous top-up as the window (a task ready before it but not pushed was held back → backlog); classify
pushes and retries are eligible from ready.

**#4 Job-row lock** (`wb_finish_job`, migration 006). A finalisation counts the job's unfinalised images without a
lock, bounded (`LIMIT threshold + 1` over the existing partial index `images_job_unfinished_idx`), and only locks
the job row when the count is at most the threshold (leases in flight + live workers, refreshed each tick). Far
from the end of a job nothing touches the job row. Near the end, two "last" images are serialised exactly as in
P1 (the second waits for the lock, then re-checks with a fresh snapshot). If the threshold was too low and both
skipped the lock, the reaper's 1 s `finishCompletedJobs` sweep marks the job done (tested); worst case the job
is done ≤ 1 s late.

**#5 Collapse round trips.**
- *Claim* is one statement: pick → guarded UPDATE → `claimed` events → lease details (`leaseSql` in `tasks.ts`),
  1 Postgres round trip instead of 5 (BEGIN, UPDATE, SELECT, INSERT, COMMIT).
- *Complete* is one statement, `select * from wb_complete(...)`: a PL/pgSQL function that, per item, does the
  fenced UPDATE, the idempotent result insert and read-back, categorisation, finalisation or classify-task
  creation, the `succeeded` / `stale_rejected` / `enqueued` events, then the worker counter and one job check per
  touched job. 1 round trip instead of ~12. Validation stays in Node before the statement (a malformed result
  never touches state), so the old "peek the stage" read is gone: both result shapes are parsed and the function
  marks a detect task without detections `invalid`.
- *Batching:* `POST /tasks/complete-batch` runs the same function over many items: per-row epoch check, one
  stale row never fails the batch, accepted rows share one commit (group commit).
- *Complete-and-claim-next:* `next: k` on complete / complete-batch claims the worker's next k in the same
  request (hybrid: `LPOP queue:{stage} k` + the lease statement; postgres: the SKIP LOCKED claim).
- *Worker* (`runtime.py`): holds a window of leases (running + backlog), buffers results and reports them in one
  request that also asks for the next leases; claim batch k = ceil(RTT ÷ service time) measured online
  (`ClaimSizer`, EWMA, bounded by the coordinator's floor/cap); hybrid claims move up to k IDs with one
  `MULTI/EXEC` of LMOVEs; depth-1 prefetch downloads/decodes the next image on a background thread
  (`Storage.prefetch`) while the current one runs. Heartbeats report every held task ID (running, prefetched,
  finished-but-unreported).

**`CLAIM_MODE=postgres`.** No Redis ready queue: workers long-poll `POST /tasks/claim`, one
`UPDATE … WHERE id IN (retries FOR UPDATE SKIP LOCKED LIMIT k ∪ new work FOR UPDATE SKIP LOCKED LIMIT k−r)
RETURNING`, with the same lease/epoch/fencing columns and the same `claimed` events. Retries come first (partial
index `tasks_retry_idx`), new detect work is not admitted under backpressure (classify backlog = PENDING classify
rows). Long-polls are woken in-process by the same post-commit hooks that push in hybrid mode, and re-check every
250 ms. Workers learn the mode at register and from every heartbeat response.

**#9 Queue hygiene** (migration 006). `tasks`: `fillfactor=70`, `autovacuum_vacuum_scale_factor=0.01`,
`autovacuum_analyze_scale_factor=0.02`; the `lease_expires_at` index is dropped so a lease renewal (the heartbeat's
only write to `tasks`) can be a HOT update; the reaper finds expired leases through the LEASED partial index on
`worker_id` (≈ workers × batch rows). `task_events`: insert-driven autovacuum at 2% (visibility map for index-only
scans) and analyze at 2%. `workers`: `fillfactor=50`. The heartbeat is one statement and skips `tasks` entirely when
the worker holds nothing. Dashboard reads: job summaries and the worker list are rebuilt at most every 500 ms for
all WebSocket clients together (a client whose flag is set later still gets the final state); `/metrics` is shared
for 1 s and its recovery join only covers the 50 most recent deaths of the last hour; `/system` already was cached
(400 ms).

**Day-partitioned `task_events`: not done, on purpose.** Partitioning pays off by dropping old partitions instead
of `DELETE` + vacuum. Wildebeest has no retention policy: nothing is ever deleted from `task_events`, and the table
is append-only, so it produces no dead tuples to vacuum. Partitioning would add a partition-maintenance job, turn
the `bigserial` primary key into `(id, at)`, and make every `id`-range scan (the invariant checker, `/events`) visit
each partition. The trigger to revisit it is a retention requirement ("keep 7 days of history").

**Small wiring.** `runtime.py` reads `WORKER_CONTAINER_ID` (register `containerId`), `WORKER_HOSTNAME` (worker ID
override, so two native workers on one host don't collide), and `WORKER_DEVICE` at register time (after the model
has loaded; `DEVICE=auto` sets it). A heartbeat retries once on a connection error (native worker false-DEAD seen
by the native agent). `Worker(stage=, handler=, redis_client=, coordinator_url=)`, `run()` and a new `stop()` keep
`swarm.py` working unchanged.

## Decisions

1. **PL/pgSQL for complete, a data-modifying CTE for claim.** Completion branches (detect → animal → cached
   classification? → finalise or enqueue; stale → event), which reads as ordinary code in PL/pgSQL and as a puzzle
   in one CTE. Claim is a straight pipeline, so a CTE is the clearer form. The categorisation rule now exists twice
   (`results.ts` for job-creation cache hits, `wb_categorize` for completions); a test runs both over the same cases.
2. **Lock order inside a batch:** task rows up front in id order, results in sha256 order, jobs in id order. The
   heartbeat renews with `SKIP LOCKED`, so it never waits on (or deadlocks with) a completing batch; a lease skipped
   once is renewed 2 s later, well inside the 15 s lease.
3. **The row is the outbox.** No outbox table and no LISTEN/NOTIFY (NOTIFY takes a global lock through commit; the
   report's Recall.ai reference). The repair sweep is the recovery for a lost push; it is also why a push failure is
   only logged.
4. **The coordinator LPOPs for claim-next** instead of moving IDs through a processing list: it is the consumer, and
   it leases them in the same request. If it dies in between, the rows are still PENDING and the startup rebuild
   re-pushes them.
5. **MULTI/EXEC instead of a Lua script for the worker's batch move.** Same atomicity and one round trip, and the
   worker image and unit tests (fakeredis without a Lua engine) need no scripting support. A trailing nil LMOVE
   costs microseconds.
6. **Claim sizing k = ceil(RTT ÷ service).** In a synchronous worker the per-task orchestration cost is RTT ÷ k, so
   this k keeps it at or below one service time. Real models (≈5 ms RTT, 300–1100 ms service) get k = 1, the report's
   consistency check; 0 ms tasks run to the cap (16). The result buffer flushes at k by default, so a real-model worker
   reports every task at once and a 0 ms worker sends one request per 16 tasks.
7. **Prefetch holds one extra lease.** A worker holds the running task plus the next one (whose image is downloading).
   The cost: a killed worker loses two tasks, and at the end of a job the last few tasks can wait for a busy worker.
   Both are small next to hiding the download (`fetchMs` p50 0 ms in the sample run below). `PREFETCH=0` turns it off;
   the swarm never enables it.
8. **Postgres-mode timings.** There is no push step, so a claim stamps `pushed_at = eligible_at = claimed`:
   `dispatchWaitMs` is 0 by construction and the whole wait counts as queue wait.
9. **`claimMode` in every heartbeat response.** Found in the live run: after restarting the coordinator in the other
   mode, running workers kept BLMOVE-ing an empty Redis queue because they only learn the mode at register.

## Measurements

Quick closed-loop probe, not the rigorous sweep (the benchmark agent does that). Compose project `wb-p2`, Docker
Desktop on the M2 shared with other agents' stacks, fake backend. Each point: N `swarm.py` worker loops in one
container, one synthetic job sized for ~10–15 s, throughput over the 10th–90th percentile of completions (checked
against wall clock). 3 trials per point, interleaved across configurations, each configuration on a fresh volume;
the table shows the median and the three values. `before` = the P1 images (commit 71f4ea9 coordinator + worker).
`after-hybrid-nobatch` = P2 with `MAX_CLAIM_BATCH=1`, `COMPLETE_BATCH=1` (push dispatch, one-statement claim and
complete, but one task per request), to separate those from batching.

- **overhead ms/task** = N × 1000 / throughput − measured handler time (the swarm's mean handler time is 0 ms or
  ~6.1–7.2 ms for "5 ms": `time.sleep` overshoots in the container). It includes queueing behind the dispatcher cap.
- **claimMs / completeMs** = p50 over the job's tasks from `tasks.timings` / `tasks.complete_ms`; with batching
  these are per-task shares. **dispatchWait** = p50/p95 of `pushed_at − eligible_at` over the job.
- **overheadPct** = `system.timings.overheadPct` (60 s window). It is degenerate for 0 ms tasks (service time ≈ the
  overhead itself), so read it for the 5 ms rows.

| config | task ms | workers | tasks/s (median, trials) | overhead ms/task | claimMs p50 | completeMs p50 | dispatchWait p50/p95 | overheadPct |
|---|---|---|---|---|---|---|---|---|
| before | 0 | 1 | **227** (208, 227, 227) | 4.40 | 1.4 | 1.2 | 2.1 / 4.6 | 100.3 |
| after-hybrid-nobatch | 0 | 1 | **210** (176, 210, 266) | 4.75 | 3.7 | 1.2 | 0.6 / 1.1 | 140.2 |
| after-hybrid | 0 | 1 | **872** (654, 872, 877) | 1.15 | 1.1 | 0.3 | 0.7 / 1.1 | 9.3 |
| after-postgres | 0 | 1 | **886** (879, 886, 946) | 1.13 | 1.0 | 0.3 | 0.0 / 0.0 | 8.2 |
| before | 0 | 4 | **239** (211, 239, 240) | 16.75 | 2.5 | 2.6 | 2.6 / 9.8 | 100.3 |
| after-hybrid-nobatch | 0 | 4 | **731** (611, 731, 847) | 5.47 | 4.1 | 1.3 | 0.6 / 1.3 | 140.6 |
| after-hybrid | 0 | 4 | **1740** (1640, 1740, 4254) | 2.30 | 1.3 | 0.5 | 0.7 / 2.2 | 10.2 |
| after-postgres | 0 | 4 | **1683** (1451, 1683, 1884) | 2.38 | 2.0 | 0.6 | 0.0 / 0.0 | 8.6 |
| before | 0 | 16 | **240** (240, 240, 242) | 66.75 | 6.4 | 11.5 | 3.0 / 7.2 | 101.5 |
| after-hybrid-nobatch | 0 | 16 | **1328** (1299, 1328, 1338) | 12.04 | 10.5 | 1.7 | 0.7 / 1.8 | 132.6 |
| after-hybrid | 0 | 16 | **5886** (5590, 5886, 6621) | 2.72 | 1.7 | 0.7 | 1.0 / 5.8 | 11.9 |
| after-postgres | 0 | 16 | **6005** (3431, 6005, 6231) | 2.66 | 2.2 | 1.1 | 0.0 / 0.0 | 9.5 |
| before | 5 | 1 | **78** (72, 78, 81) | 6.27 | 2.0 | 2.0 | 2.5 / 118.7 | 98.4 |
| after-hybrid-nobatch | 5 | 1 | **93** (75, 93, 98) | 4.14 | 3.3 | 1.0 | 0.6 / 1.0 | 127.6 |
| after-hybrid | 5 | 1 | **98** (93, 98, 98) | 4.08 | 3.3 | 1.0 | 0.6 / 1.1 | 12.4 |
| after-postgres | 5 | 1 | **94** (92, 94, 95) | 4.33 | 3.4 | 1.1 | 0.0 / 0.0 | 9.8 |
| before | 5 | 4 | **235** (230, 235, 237) | 10.83 | 2.1 | 2.6 | 2.7 / 12.8 | 88.5 |
| after-hybrid-nobatch | 5 | 4 | **394** (319, 394, 420) | 4.17 | 3.4 | 1.0 | 0.6 / 1.0 | 112.5 |
| after-hybrid | 5 | 4 | **388** (346, 388, 388) | 4.22 | 3.4 | 1.0 | 0.6 / 1.2 | 13.6 |
| after-postgres | 5 | 4 | **375** (375, 375, 389) | 4.49 | 3.2 | 1.0 | 0.0 / 0.0 | 10.8 |
| before | 5 | 16 | **240** (239, 240, 241) | 60.13 | 5.3 | 6.1 | 3.1 / 6.6 | 83.7 |
| after-hybrid-nobatch | 5 | 16 | **1102** (860, 1102, 1169) | 7.83 | 5.9 | 1.4 | 0.7 / 1.6 | 96.0 |
| after-hybrid | 5 | 16 | **1321** (1222, 1321, 1403) | 5.48 | 3.7 | 1.1 | 0.7 / 2.0 | 19.7 |
| after-postgres | 5 | 16 | **1372** (1340, 1372, 1459) | 5.15 | 3.2 | 0.9 | 0.0 / 0.0 | 13.1 |

What the numbers say:

- **The P1 ceiling was the dispatcher.** P1 is flat at ~240 tasks/s at every worker count and both task times:
  50 IDs per 200 ms tick = 250/s. Per-task overhead grew with workers (4.4 → 67 ms) because the loops queued
  behind the cap.
- **Push dispatch + one-statement claim/complete** (nobatch row) removes the cap: 240 → 1,328 tasks/s at 16
  workers and 0 ms, 240 → 1,102 at 5 ms. dispatchWait p50 drops from 2–3 ms (p95 up to 119 ms at 1 worker × 5 ms,
  the tick) to 0.6–0.7 ms. completeMs 1.2–11.5 → 1.0–1.7 ms.
- **Batching** (claim batch + complete-batch + next) is what matters for 0 ms tasks: 872 / 1,740 / 5,886 tasks/s at
  1 / 4 / 16 workers (3.8× / 7.3× / 24.5× over P1), 1.1–2.7 ms of overhead per task. At 5 ms tasks the RTT ÷
  service rule picks k = 1 (RTT ≈ 3–4 ms < 6 ms service), so 1 and 4 workers look like the nobatch row
  (~4 ms per task = one combined complete+claim round trip); at 16 workers batching still adds ~20% (1,102 → 1,321),
  most likely because round trips slow under contention until RTT ÷ service exceeds 1 and k grows (not instrumented).
- **Hybrid vs Postgres claims are a wash here**: within noise at every point (e.g. 5,886 vs 6,005 at 16 × 0 ms,
  1,321 vs 1,372 at 16 × 5 ms). Postgres mode saves the Redis hop and has dispatchWait 0 by construction; hybrid
  keeps the queue in Redis. On this box neither substrate is the bottleneck below ~6k tasks/s; the single
  Python swarm process (16 threads, one GIL) and the single Node process are the likelier limits. The benchmark
  agent's multi-container sweep should settle it.
- 1-worker points are the per-loop floor: ~1.1 ms per task at 0 ms tasks (was 4.4), ~4.1 ms at 5 ms (was 6.3).

Other checks on the same stack (fake backend, `FAKE_MODEL_DELAY_MS=50`, 3 detectors + 1 classifier, prefetch on):
400 real sample images through MinIO: done, 0 failed, invariants ok, `fetchMs` p50 **0 ms** (prefetch hid the
download), `dispatchWaitMs` p50 0, `completeMs` p50 1.3 ms, `lastSweepRepaired` 0, effective detect target 12
(3 workers × window 2 × 2). The same job in `CLAIM_MODE=postgres`: identical categories, 0 failed. Killing a detector
mid-job (600 images, postgres mode): detected via `docker_event`, both of its leases (running + prefetched)
re-claimed 341 ms after the kill, job done with 0 failed, invariants ok.

First (noisier) baseline taken earlier while another agent's real-model detectors saturated the VM: P1 at 31 / 57 /
49 tasks/s (0 ms; 1 / 4 / 16 workers), completeMs p50 up to 225 ms at 16 workers: the job-row lock plus the
loaded VM. Treat all numbers here as indicative.

## Behaviour changes to existing tests (all intentional)

- `dispatch-cache` "tops queue:detect up to DETECT_QUEUE_TARGET": now runs with `DISPATCH_MODE=tick` (it tests
  the sweep); push mode has its own tests.
- `retries` "release … dispatched again at once" and "a zero jitter draw retries immediately": the task is already
  at the head of the queue after the release/fail call; `dispatchOnce().retried` is 0, not 1.
- `state-machine` "puts recovered work at the head": the claim refilled the queue to its target, so the queue holds
  target + 1 with the retry on top (was target).
- `telemetry` idle snapshot: `dispatcher.mode` is `push`. "counts a backlog … as queue wait": the backlog now waits
  before the first claim (a claim refills the queue at once). Synthetic job: the first queue-target's worth of
  tasks is `queued` right after creation.
- `test_runtime.py` heartbeat metrics include `claimBatch`.
- Test helpers set the `queues-built` marker after flushing Redis (as startup does), and `pullAndClaim` waits for
  the background top-up it triggers. `npm test` runs the whole suite twice, `CLAIM_MODE=hybrid` and `postgres`;
  Redis-specific tests are `skipIf(postgres)` (14 in the postgres run).

## Known gaps

- Measurements are from one shared, noisy VM (one 4-worker P2 trial read 4,254 tasks/s, another 1,640) and one
  swarm process per point; no USL fit, no open-loop run, no 60-minute run for `n_dead_tup` / claim-latency drift
  (report #9's long-horizon check). The benchmark agent owns those.
- Batching is synchronous: a worker idles during its report round trip. An asynchronous reporter thread would
  overlap it (the RabbitMQ rule assumes that), but would change the worker's failure handling; not done.
- With k = 1 (real models, and 5 ms tasks at low concurrency) the gain is push dispatch and one-statement
  claim/complete only; the per-task round trip remains.
- At job end, a worker holds up to its window of leases (k, and 2 with prefetch) that another idle worker could have
  run: the tail of a 0 ms job can be up to 16 tasks per worker long. Not measured.
- `invariants.lostImages` still scans every unfinalised image of running jobs every 5 s (fine for 20k, not for a
  1M synthetic job).
- `fillfactor` applies to new pages only; `n_dead_tup` and HOT ratios were not measured.
- Postgres mode wakes all long-polls of a stage on each commit (a small thundering herd); fine at tens of workers.
- Classify tasks created at job creation (cache hits) and synthetic classify jobs are pushed 1,000 at a time, the rest
  by the sweep (counted in `lastSweepRepaired`, though nothing was lost).
- A coordinator crash between the claim-next `LPOP` and its lease statement leaves those IDs out of Redis until the
  restart's rebuild (which a crash implies).
- Not done: `WORKER_HOSTNAME` is read but `scripts/native_worker.sh` (not mine) doesn't set it yet.
