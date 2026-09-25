# Measurement and correctness program (bench agent)

Owner: benchmark agent. Covers report items #2 (orchestration-ceiling benchmark) and #6 (invariant
checker + fault matrix). Files: `bench/`, `tests/invariants/`, `worker/wildebeest_worker/swarm.py`,
`docker-compose.faults.yml`, `benchmarks/ceiling/`, `benchmarks/faults/`, `benchmarks/summary.json`.

## How to run

```bash
uv pip install --python .venv/bin/python -r bench/requirements.txt    # psycopg, numpy, scipy, matplotlib

# "after": the working tree, synthetic tasks (needs POST /jobs/synthetic)
.venv/bin/python bench/run.py --target . --mode synthetic --out benchmarks/ceiling/after
.venv/bin/python bench/run.py --target . --mode synthetic --env CLAIM_MODE=postgres --out benchmarks/ceiling/after-postgres
.venv/bin/python tests/invariants/faults.py --target . --runs 12 --seed 42          # → benchmarks/faults/after
.venv/bin/python tests/invariants/faults.py --target . --runs 12 --seed 42 --env CLAIM_MODE=postgres --out benchmarks/faults/after-postgres

# "before": the frozen checkout, sample jobs (works on the old code)
.venv/bin/python bench/run.py --target $BEFORE --mode sample --out benchmarks/ceiling/before
.venv/bin/python tests/invariants/faults.py --target $BEFORE --runs 12 --seed 42    # → benchmarks/faults/before

.venv/bin/python -m pytest tests/invariants -q                                      # checker unit tests (no Docker)
.venv/bin/python bench/run.py --out benchmarks/ceiling/after --report-only           # re-render charts/summary only
```

Each command brings the checkout's Compose stack up as its own project (`wb-b` for this repo,
`wb-before` otherwise) on the benchmark ports (Postgres 35432, Redis 36379, MinIO 39000/39001,
coordinator 33000), fake backend only, and tears it down with `down -v` at the end (`--keep-up`
leaves it running). Run one at a time. Useful flags: `--suites ceiling,openloop,recovery`,
`--task-ms 0,5,50`, `--workers 1,2,4,8,16,32,64`, `--trials 3`, `--kills 22`, `--no-build`,
`--env CLAIM_MODE=postgres` (any Compose variable; for the hybrid-vs-Postgres A/B), and for faults
`--only kill,pause` / `--faults-per-run 5`.

Suggested Makefile targets (I don't own the Makefile):

```make
BEFORE ?= ../wb-before
bench-after:      ; $(PYTHON) bench/run.py --target . --mode synthetic --out benchmarks/ceiling/after
bench-before:     ; $(PYTHON) bench/run.py --target $(BEFORE) --mode sample --out benchmarks/ceiling/before
faults:           ; $(PYTHON) tests/invariants/faults.py --target . --runs 12 --seed 42
test-invariants:  ; $(PYTHON) -m pytest tests/invariants -q
```

## Decisions

1. **Python harness, not TypeScript.** The analysis needs scipy (USL fit with bounds, t-intervals,
   bootstrap) and matplotlib, both already in the root `.venv`; psycopg was the only addition
   (`bench/requirements.txt`). `scripts/benchmark.ts` is left as is: it measures the real-model sweep.
2. **Swarm of worker loops, not worker containers.** `swarm.py` runs N ordinary `runtime.Worker`
   objects as threads (each registers as `detect-sw<k>-<i>`, heartbeats, BLMOVEs, claim-confirms and
   completes over HTTP) with a handler that only sleeps. It depends on `Worker(stage, handler,
   redis_client, coordinator_url, hostname=…)`, `run()` and `stopping`/`stop()`; `run()` installs
   signal handlers, which Python only allows on the main thread, so the swarm makes those calls
   no-ops off the main thread and owns SIGTERM itself. The harness starts ⌈N/8⌉ swarm containers of
   the worker image with `docker compose run` (8 loops each, `--per-container`) so client CPU and
   the GIL stay out of the measurement, and mounts `swarm.py` read-only so the frozen "before" image
   works without a rebuild.
3. **The fake handler returns no detections.** Every task then finalises exactly one image (the
   `finalizeImage` + `maybeFinishJob` hot path runs on every task) and no classify tasks exist, so
   "tasks/s" is one number, not two queues in series (the confound the report found in the 8-worker
   row of the old sweep). It does not download the image (`SWARM_FETCH=bytes` to include a MinIO GET).
4. **Overhead subtracts the measured handler time, not the nominal task time.** `time.sleep(5 ms)`
   takes 6.4 ms p50 (18.7 ms p99) inside a Docker Desktop container, `sleep(1 ms)` takes 2 ms. The swarm
   reports its actual mean handler time (`SWARM_STATS` line on exit) and the harness uses it.
   Per-task overhead = mean per-loop cycle (start of task i → start of task i+1 on the same loop) −
   handler time, at N = 1. `tasks.timings` p50s are recorded too when the column exists.
5. **Steady state = 10th–90th percentile of completion times** of the run; throughput = tasks in that
   window ÷ its length. K is sized from earlier points to last ~15 s (`--duration`), capped at 60 s
   (leftovers are cancelled). Trials are the outer loop, so drift spreads over all points.
6. **Stack reset between points** (`Stack.reset_state`): cancel jobs, stop swarms, remove worker
   containers, `TRUNCATE` jobs/images/tasks/task_events/result tables, delete workers, clear the Redis
   queues and processing lists, `VACUUM ANALYZE`. Equivalent to a fresh stack without the restart;
   it only ever runs against the harness's own projects. Without it later points would run against
   ever-larger tables.
7. **Open loop.** Arrival times are fixed up front (evenly spaced at the offered rate); a 10 ms
   ticker submits whatever is due from a 48-thread pool, so a slow submission never delays the next
   one. Latency = image `finalized_at` − *intended* arrival (host↔VM clock offset measured by
   lowest-RTT `clock_timestamp()` probes), i.e. no coordinated omission; unfinished arrivals count as
   +∞ in the percentiles. Exact percentiles (numpy) over every measured task instead of an
   HdrHistogram: at ≤ 10⁴ samples per point exact is cheap. Sample mode (old code) submits
   `POST /jobs` uploads of 8×8 JPEGs with 16 random trailing bytes (unique sha, so the cache never
   short-circuits them) named by sequence number; synthetic mode submits one `/jobs/synthetic` per
   tick. The report splits the median into intended→image row committed, →claimed (dispatch + queue),
   →completed (lease). Offered load = 50/80/100/110 % of the best closed-loop mean at 5 ms tasks,
   with the N that achieved it.
8. **Recovery = kill → every task taken from the victim re-claimed**, all from `task_events`
   (Postgres clock, no skew): `worker_killed` → `worker_died` → last `reassigned`/`lease_expired`/
   `released` → first later `claimed` per task. Regular fake-backend detector containers
   (300 ms model, 4 detectors + 2 classifiers, backlog of sample jobs), victim = a detect worker with
   a lease per `GET /workers`; a kill that lands between tasks is recorded as `idle` and not counted.
   The dead container is removed before the pool is scaled back up, so Compose can't restart the same
   worker ID (which would release its tasks through re-registration and hide the recovery path).
9. **Shared machine.** Other agents' stacks (real-model detectors) share the 8-vCPU Docker VM and
   the host. The harness samples `docker stats` in the background, records the mean CPU of *other*
   projects' containers per point, waits (up to 10 min, then 60 s once it has timed out) for them to
   drop below 150 % before each point, and repeats a point (up to twice) when they averaged more than
   225 % during it. The column is in every results table.
10. **summary.json** is regenerated by every run from `benchmarks/ceiling/{before,after}/results.json`,
    `benchmarks/results.csv` (real-model sweep) and `benchmarks/faults/{after,before}/results.json`, in
    exactly the contract shape. `ceiling` shows the "after" run (hybrid) when it exists, else "before";
    `after-postgres` appears in `benchmarks/ceiling/results.md` (the comparison table) but not in summary.json,
    which has no slot for it.
    `overhead.*.perTaskMs` = N = 1 cycle − handler time; `usl` is the 0 ms fit.
11. **Invariant checker reads the coordinator's own history** (`tasks`, `task_events`, `images`, `jobs`,
    result tables). I3/I4 need the `claimed` and `succeeded` events to keep `detail.leaseEpoch`;
    completions without it are counted as "unverifiable", not as violations. I5 is sampled live once
    a second (confirmed on two consecutive samples, suppressed while the coordinator is down and for
    12 s after it restarts, since nothing can reap then). I7: a job still `running` 300 s after the last
    fault is a violation; the runner then records where each unfinished task is (Postgres state and
    which Redis list holds its ID).
12. **Fault injection without touching the coordinator.** SIGKILL through `POST /workers/:id/kill`
    (works on both versions); pause with `docker pause` directly (20 s, > `LEASE_MS`); network faults via
    toxiproxy between workers and coordinator/Redis (`docker-compose.faults.yml`: workers get
    `COORDINATOR_URL=http://toxiproxy:8666`, `REDIS_URL=redis://toxiproxy:8679`; the runner creates the
    proxies through the API on host port 38474); MinIO `docker stop` 30 s; Redis `FLUSHALL` + SIGKILL +
    start (redis:7 saves on shutdown, so a plain restart would not lose data); coordinator SIGKILL +
    start. Schedules come from a seeded deck of the 7 types (each type appears ~evenly); the schedule
    is repeatable, the interleaving with real time is not.
13. **The runner never retries a POST after a timeout.** An earlier version retried `POST /jobs/sample` on a
    10 s client timeout; on a cold coordinator (first job hashes and uploads 1,200 images) that created a second
    job, and the tracked job then queued behind it. The first before matrix had one extra I7 violation from that
    (run 9, 336 tasks still queued behind the duplicate); rerun with the fix, run 9 passed and the other four
    I7 violations reproduced exactly.

## Results so far

### Before (frozen pre-improvement checkout, sample mode, `benchmarks/ceiling/before/`)

Apple M2, Docker Desktop 8 vCPU / 8 GB, shared with other agents' stacks (neighbour CPU recorded per point).
3 trials per point, mean ± 95% CI.

| Worker loops | 0 ms tasks (tasks/s) | 5 ms | 50 ms |
|---|---|---|---|
| 1 | 175 ± 64 | 76 ± 17 | 15.9 ± 2.3 |
| 2 | 236 ± 19 | 140 ± 93 | 31.8 ± 1.6 |
| 4 | 237 ± 12 | 240 ± 11 | 63.8 ± 2.1 |
| 8 | 243 ± 4 | 240 ± 2 | 129 ± 14 |
| 16 | 240 ± 3 | 235 ± 24 | 238 ± 17 |
| 32 | 241 ± 3 | 237 ± 4 | 233 ± 27 |
| 64 | 236 ± 6 | 231 ± 17 | 207 ± 82 |

- **The ceiling is the dispatcher, exactly as the report predicted:** every series flattens at ~240 tasks/s,
  just under the 50 IDs per 200 ms tick = 250/s refill cap, from N = 2 (0 ms), N = 4 (5 ms) or N = 16 (50 ms)
  on. Past the cap, extra loops only add waiting: lease p50 at 0 ms grows from 1.8 ms (N = 1) to 33 ms (N = 64),
  and the per-loop cycle grows linearly with N (Little's law holds at 0.99–1.00 on every point).
- **USL fit (0 ms, 7 points, 21 trials):** λ = 187 tasks/s per loop [169, 201], α = 0.72 [0.63, 0.79],
  β = 0.0013 [0.0009, 0.0016], R² = 0.73. The low R² is itself the finding: a hard rate cap is not the smooth
  contention/crosstalk curve USL models, so α absorbs the cap. The 50 ms series fits well (R² 0.96, α ≈ 0.009).
- **Per-task overhead (N = 1, cycle − measured handler time):** 5.8 ± 2.3 ms at 0 ms tasks, 6.8 ± 3.4 ms at
  5 ms, 11.1 ± 7.2 ms at 50 ms; 7.9 ± 2.3 ms averaged (the `summary.json` value). About 2.9 ms of it is inside
  the lease (claim-confirm committed → complete committed); the rest is BLMOVE + claim-confirm + the loop.
  (The report's 46 ms/cycle figure came from the 300 ms fake model with image download + decode.)
- **Open loop (5 ms tasks, 8 loops, max 240/s):** p50 / p99 / p99.9 from intended arrival = 156 / 394 / 435 ms
  at 50 % load, 195 / 519 / 567 ms at 80 %, 3.5 / 4.1 / 4.2 s at 100 %, 5.5 / 6.7 / 6.8 s at 110 % (achieved
  210/s: the queue grows for the whole run). At 50 % the median task already waits 137 ms between its image row
  committing and being claimed: that is the 200 ms dispatcher tick (mean wait ≈ half a tick plus queueing).
- **Recovery (22 SIGKILLs of busy fake-backend detector containers, 2 more landed between tasks):**
  kill → all tasks re-claimed p50 5,562 ms, p95 6,659 ms, max 6,736 ms. Detection is ~95 % of it
  (kill → marked DEAD p50 5,306 ms, heartbeat timeout); requeued → re-claimed p50 313 ms.

### After smoke (commit ab9e570, synthetic mode, hybrid, 1 trial — harness check only, not results)

The harness runs unchanged against the P2 code: 0 ms tasks 1,470 tasks/s at N = 1 and 3,515 at N = 16;
5 ms tasks 92 / 379 / 538 at N = 1 / 4 / 16; open loop at 50 % of 538/s: p50 93 ms, p99 540 ms; recovery
170–430 ms over 3 kills, detected `via docker_event` in 74–82 ms. One oddity to recheck in the full run: 0 ms
at N = 4 gave only 378 tasks/s (per-loop cycle 10.5 ms, same as at 5 ms), well below N = 1; see requests.
A 1-run fault smoke (kill, coordinator restart, Redis connection reset, 20 s pause) passed with 0 violations and
1 fenced late result.

### Fault matrix, before (`benchmarks/faults/before/`)

12 runs × 5 faults (seed 42; kill 9, coordinator restart 9, pause 9, reset 9, latency 8, MinIO 8, Redis 8),
1,200-image fake-backend job per run, 3 detectors + 2 classifiers.

| Runs | Faults injected | Violations | By invariant | Fenced late results (409) | Failed images | Re-executions |
|---|---|---|---|---|---|---|
| 12 | 60 | **4** | I7 job_finishes: 4 | 52 | 71 | 203 |

- **No safety violation:** I1–I6 held in every run: no double success, no stale epoch accepted (52 late
  results fenced off with 409, most after 20 s pauses and connection resets), epochs strictly increasing,
  categories always matching the one stored result, no stuck lease on a dead worker.
- **Four liveness violations (I7), one cause:** runs 2, 4, 8 and 12, exactly the runs with a `reset` toxic on
  the worker→Redis path, left 1–5 tasks `PENDING`/`queued` in the processing lists of *live* workers, and the
  job never finished (request 1 below). Run 6 also had a Redis reset but a later FLUSHALL made the dispatcher
  rebuild the queues, which rescued the orphans.
- **MinIO outages cost real work:** every run with a 30 s MinIO stop finalised 7–15 healthy images as `failed`
  (attempts burned on an infrastructure error) and re-ran 18–42 tasks.


## Requests for the coordinator / worker owners

1. **Orphaned task IDs in a live worker's processing list (found by the fault matrix on the before code).**
   When a worker's connection to Redis is reset while `BLMOVE` is in flight, Redis moves the ID into
   `processing:{workerId}` but the worker never sees the reply. The task stays `PENDING` with `queued = true`,
   sits in the processing list of a worker that is still `ALIVE`, and nothing ever looks at it again: the
   reaper only drains lists of non-ALIVE workers, and the dispatcher only pushes `queued = false` rows. The job
   never finishes (I7). Every before run with a `reset` toxic on the worker→Redis path ended this way (one or two
   orphans per live worker), except one where a later Redis FLUSHALL made the dispatcher rebuild the queues.
   Suggested fix (either side): the worker `LRANGE`s its own processing list after a Redis error and
   claim-confirms what it finds; or the reaper re-pushes IDs that have sat in an ALIVE worker's processing list
   longer than a few heartbeats (needs a timestamp, e.g. `tasks.pushed_at`, which P1 added). The P2 worker's
   `MULTI`/`LMOVE` path has the same exposure in principle; the one after-code smoke run with a Redis reset
   passed, so the full after matrix should say whether it is fixed.
2. **MinIO outages still burn attempts on the before code** (7–12 images finalised `failed` per run with a 30 s
   MinIO stop, 3 detectors). This is P1's error-classification work (`/release`); the after matrix should show
   0 failed images for MinIO-only runs.
3. **Recheck claim batching at small N (P2).** In the smoke run, 0 ms tasks at 4 loops ran at 378 tasks/s, i.e.
   ~95/s per loop, the same per-loop rate as with 5 ms tasks, while 1 loop did 1,470/s. One trial with busy
   neighbours, so maybe noise, but the coincidence suggests a floor of ~10 ms per task per loop at N = 4, e.g. the
   50 ms `COMPLETE_FLUSH_MS` timer divided by a claim batch of ~5. The full after run will have 3 trials.
4. **Keep `detail.leaseEpoch` on `claimed` and `succeeded` events** (the checker's I3/I4 rely on it; P2 kept
   it in `wb_complete`). If batching ever drops per-task events, the checker reports "unverifiable completions"
   instead of silently passing.
5. Makefile targets suggested above; `bench/requirements.txt` needs installing into `.venv` (psycopg is new).

