# P3: straggler speculation

Owner: P3 agent. Scope: `coordinator/**` (migration `007_speculation.sql`, new `speculation.ts`), worker `runtime.py`
(+ `worker/tests/test_speculation.py`), the "P3 refinements" section of `docs/CONTRACTS.md`, coordinator env in
`docker-compose.yml`. Implements report item #11 ("Stragglers are the tail-latency story"): in a heterogeneous pool
(d-native: CPU containers next to a fast MPS worker) the slowest worker sets the job's tail, because the last tasks it
holds finish long after everyone else has run dry.

## The problem with the old model

A second claim bumped `lease_epoch`, which fenced the first holder out: "speculation" would have been pre-emption, and
if the new holder turned out slower, or died, the task had lost its best attempt. What's needed is a task that has
**two valid attempts at once**, of which exactly one result is accepted, without weakening any existing guarantee
(exactly one result per image; a worker declared dead, or paused past its lease, is fenced).

## Design: the lease plus one shadowing copy

- The `tasks` row keeps describing the task's **lease** exactly as before (`worker_id`, `lease_epoch`,
  `lease_expires_at`, `started_at`). Every single-attempt path (claim, complete, fail, release, heartbeat, reaper) is
  unchanged for a task that is never speculated, which is almost all of them. The hot path gains nothing but a
  `greatest(lease_epoch, spec_epoch)` in the claim.
- A **speculative copy** is a `task_attempts(task_id, epoch, worker_id, shadow_epoch, state, …)` row with its own
  epoch, reserved in `tasks.spec_epoch`. Epochs stay unique per task (a later claim takes
  `greatest(lease_epoch, spec_epoch) + 1`), so no report can ever be confused with another attempt's.
- **Validity by shadowing.** A copy records the lease epoch it was started next to (`shadow_epoch`) and is valid only
  while `task.state = LEASED and task.lease_epoch = shadow_epoch`. Anything that ends that lease (a fail, a release, a
  requeue, a later claim) invalidates the copy without touching `task_attempts`, so none of the existing transitions
  had to learn about copies to stay correct. The reaper still sweeps such copies to `dropped` (tidiness, and so their
  worker counts as idle again), but correctness doesn't depend on it.
- **First commit wins, fenced by task state.** `wb_complete` (replaced in 007) first tries the lease (unchanged
  `state = LEASED and lease_epoch = epoch`), then the copy (`running` and shadowing the current lease; the task row is
  already locked by the statement). Either success flips the task to `SUCCEEDED` in the same statement that locked it,
  so the second attempt's report finds the task no longer `LEASED` and matches nothing: exactly-one-result is the same
  row-lock argument as before. On a copy win the task row takes the copy's worker, epoch and start (the record describes
  the accepted attempt); the copy becomes `won`. On a lease win a running copy becomes `lost`.
- **Losers vs. fenced.** A report that matched nothing is `already_done` iff its attempt was still valid when the other
  one won: it is the `lost` copy, or the lease epoch a `won` copy shadowed (`wb_late_outcome`). Everything else is
  `stale` (STALE_LEASE with its `stale_rejected` event), as before. Single complete / fail / release answer
  **409 `ALREADY_DONE`**; complete-batch reports `"already_done"`. Not a fencing event: no event, not counted.
  I chose 409 over `200 {ok:false}` because a pre-P3 worker already treats any 409 on complete as "discard" (it would
  have counted a 200 as done), and because a conflict is what it is.
- **Promotion.** If a speculated task's lease is lost (expired, worker not ALIVE, deregister or re-register) while its
  copy is healthy (running, its worker ALIVE, its own lease fresh), the copy *becomes* the lease: the task row takes its
  worker/epoch/lease/start, the attempt is `promoted`, nothing is requeued or charged, and the old lease's epoch is now
  fenced like any lost lease. This is the case where speculation also saves a recovery: the paused/killed straggler's
  work is already running elsewhere. It runs inside `requeueLostLeases` (reaper and death watch) and
  `releaseWorkerTasks`, before the requeue statement, in the same transaction.
- **Why a paused original still gets STALE_LEASE.** Its lease expires, the reaper promotes the copy
  (`lease_epoch := copy epoch`), and when the original wakes up its epoch matches neither the lease nor a `lost`/`won`
  attempt: stale, with `stale_rejected`, before *and* after the copy's result is in. Same for an original whose worker
  was declared dead (and its heartbeat gets 410). If the copy's own worker dies or its lease runs out, the reaper drops
  the copy (`dropped`) and its late report is stale. The only report that gets `already_done` is one whose attempt was
  valid at the instant the other attempt committed.
- **Cancel on the next heartbeat.** The heartbeat statement returns `cancel` = held tasks that were speculated
  (`spec_epoch is not null`) on which this worker holds no valid attempt. The loser drops a waiting lease at once; a
  running one can't be interrupted (it's a model call), so its result is dropped when the handler returns and nothing is
  reported; a finished-but-unreported result is reported anyway and the answer decides. Restricting cancels to
  speculated tasks keeps every pre-P3 behaviour (a lost lease is still discovered through STALE_LEASE, which is what
  the fencing demo shows).
- A copy's `fail`/`release` ends only the copy (nothing charged, event with `detail.speculative`); the original carries
  on and will hit a real error itself.

## Policy

Every `SPECULATE_INTERVAL_MS` (250 ms), per stage (`speculation.ts`):

1. `SPECULATION=on` and a baseline of ≥ `SPECULATE_MIN_SAMPLES` (5) completions of the stage.
2. The stage has **no PENDING task** (nothing left to hand out: queued in Redis, held back, or a retry in backoff).
3. An **idle worker** exists: ALIVE, heartbeated within 2 × `HEARTBEAT_MS`, holds no lease and no copy/offer.
4. Candidates: LEASED tasks older (since claim) than `max(SPECULATE_MIN_MS = 1 s, SPECULATE_MULTIPLIER = 3 × stage
   p50)`, oldest first, never speculated before. Spark's defaults are the same multiplier (3) and a 0.9 quantile; we
   use "the queue is empty" instead of a quantile of finished tasks, because a job's tail is exactly when the queue is
   empty and a worker is idle.
5. Target: the **fastest** idle worker by its own recent p50 (heterogeneous pool), never the holder, not on probation,
   not cooling down; one copy per idle worker per pass.
6. **Probation**: p50 > `SPECULATE_PROBATION_MULTIPLIER` (3) × stage p50 → no copies to that worker (a copy on a slow
   worker is a second straggler). It keeps its normal work; it's a routing hint, not an eviction. Shown in
   `system.speculation.probation` and `probation: true` on the worker.

"Service time" is the coordinator's claimed → complete-handled time, the same clock as a task's age, so the
comparison is like for like (with prefetch both include the wait behind the running task, which is right: a task
queued behind a straggler is a straggler).

Delivery: an offer reserves the copy's epoch (`spec_epoch`) and an `offered` attempt for one worker. Hybrid mode
`RPUSH`es the ID to `spec:{workerId}`, which the worker's claim `MULTI` moves from before the shared queue, and
claim-confirm leases it as a copy; postgres mode wakes the long-poll and `/tasks/claim` returns it. Offers not taken
within 3 s are dropped and that worker isn't offered another for 30 s; an untaken offer doesn't use up the task's one
copy. Targeting a worker needed a per-worker list: a copy in the shared queue would go to whichever idle worker
blocked first, not the fastest. The worker's idle `BLMOVE` now waits 250 ms instead of 1 s (`IDLE_WAIT_MS`), because
the first measurement showed offer → lease taking 750–1000 ms, most of the saving.

## Measurements

Compose project `wb-p3` on the shared Docker Desktop VM (M2, other agents' stacks running), fake backend, synthetic
detect job of 200 images (~27% become classify tasks). Pool: 3 detectors at `FAKE_MODEL_DELAY_MS=300`, **one throttled
detector at 3000 ms (10×)**, 2 classifiers at 300 ms, prefetch on (every worker holds running + next). Makespan =
job `createdAt` → `finishedAt`. Trials interleaved off/on, the coordinator restarted with `SPECULATION` flipped and
fresh workers for each trial (so every trial starts with no service-time history).

| speculation | makespan, 5 interleaved trials (ms) | median | mean | copies launched / won / wasted (total) |
|---|---|---|---|---|
| off | 27,592 · 27,935 · 28,780 · 28,810 · 29,574 | **28.8 s** | 28.5 s | — |
| on | 22,129 · 23,264 · 23,949 · 24,342 · 24,956 | **23.9 s** | 23.7 s | 8 / 7 / 1 |

**−4.8 s median (−17%)**; the ranges don't overlap (worst "on" 25.0 s < best "off" 27.6 s). 0 failed, invariants ok,
0 duplicate results and 0 `stale_rejected` in every trial. The throttled worker was on probation in every run
(p50 ≈ 6.0–6.3 s vs a stage p50 of 630–720 ms). Each run launched 1–2 copies (the straggler's running task and the one
prefetched behind it); the one `wasted` copy was for a running task the original finished 50 ms after the copy
started.

An earlier 3+3 run with the worker's idle `BLMOVE` still at 1 s gave 24.2 s off vs 22.0 s on (−2.1 s). In that run
`task_attempts` showed offer → lease taking 750–1,000 ms, which is why `IDLE_WAIT_MS` is now 250 ms. A 3+3 run right
after that change, on a busier VM, gave 27.8 s vs 24.4 s medians. The table above is the final code.

Ideal makespan with the throttled worker contributing nothing at the end is ≈ 200 ÷ (3 × 3.3/s + 0.33/s) ≈ 19.5 s
plus the classify tail; the gap between "on" and that is the detection delay (threshold ≈ 3 × 620 ms ≈ 1.9 s since
claim), the offer pick-up (≤ 250 ms) and one fast service time.

Other checks on the same stack:

- **Kill the straggler while its copy runs** (3 detectors at 2.5 s, one at 60 s, 24 images): `speculated` (task
  running 18.1 s, threshold 15.2 s) → `POST /workers/:id/kill` → `worker_died` via Docker event →
  `reassigned` "its speculative copy … took over (epoch 2)" **33 ms after the kill request** → `speculation_won`
  (`promoted: true`) 2.1 s later → job done, 0 failed, invariants ok. Nothing was requeued.
- **Pause the straggler for 30 s while its copy runs:** the copy won 2.3 s after the pause (`speculation_won`, the
  original told to cancel), the job finished, the paused worker was declared dead by heartbeat 2.6 s later, resumed →
  `heartbeat_refused` (410). When it finished its in-flight task, it posted that result once and got `ALREADY_DONE`
  ("result … discarded: another attempt finished first"), then re-registered. `fencing.staleRejected` stayed 0.

Every trial: 0 failed, `invariants.ok`, `duplicateResults` 0, and no `stale_rejected` from speculation (losers get
`already_done`).

## Tests

- `coordinator/test/speculation.test.ts` (23 tests, run in both `CLAIM_MODE=hybrid` and `postgres`): copy lease and
  epochs; copy first → one result, original gets `already_done` + cancel (also for fail/release); original first →
  copy `already_done`, `wasted`; both completing concurrently → exactly one `succeeded`; an original paused past its
  lease → promoted, original STALE before and after the copy's win; an original declared dead → promoted, 410 + STALE;
  a copy whose worker died or whose lease expired → dropped, STALE; a copy's fail ends only the copy; the original's
  fail → retry, copy cancelled, the retry's epoch skips the copy's; deregister hands over to the copy; API 409
  `ALREADY_DONE`, heartbeat `cancel`, batch status; a rejected single complete claims no `next`; policy thresholds
  (baseline, multiplier, 1 s floor), queue-empty and idle-worker conditions, `SPECULATION=off`, at most once, never
  the holder, fastest idle worker, probation (no copies, flagged in `/system` and `/workers`), untaken offers dropped
  with cooldown and re-offer; `/system` lease `copy` and `/workers` fields; a job where every task is speculated with
  alternating winners ends with one `succeeded` per task.
- `worker/tests/test_speculation.py` (10 tests): claim takes an offered copy first; heartbeat `cancel` drops waiting
  leases and the running task's result, leaves a finished-unreported one to the report; `ALREADY_DONE` single and
  batch; postgres mode gets copies from the claim.
- Existing tests: all pass. Changed: `telemetry.test.ts` exact-shape checks now include the additive `copy` lease field
  and the additive `speculation` fields.

## Behaviour changes

- A single `complete` that the coordinator rejects (409 STALE_LEASE / ALREADY_DONE) no longer claims `next`. Before,
  the claimed leases were dropped with the 409 and sat LEASED until they expired and were charged an attempt (a P2 bug
  that speculation would have made frequent). complete-batch still honours `next` (it answers 200).
- Idle hybrid workers poll `BLMOVE` every 250 ms instead of 1 s.
- `wb_complete` is replaced by migration 007 (the 006 body plus the copy path).

## For the lead to merge

- **Migration ordering hazard:** 007 does `create or replace function wb_complete`. If the HA (008) or OTel (009)
  migration also replaces `wb_complete` (e.g. to add a leader-term fence), it must start from 007's body, or the copy
  path silently disappears (copies would then always get STALE_LEASE, which is safe but disables speculation's win).
- The speculation loop (`loops.ts`) should run on the leader only once HA lands, like the dispatcher and reaper;
  per-worker service times are in memory (`speculation.ts`), so with stateless API replicas the leader only sees the
  completions it handles. Counters are from `task_events` and are the same on every replica.
- README: the tail-latency story can quote the table above; the state diagram gains
  `LEASED → LEASED (copy promoted)` and "first commit wins" for speculated tasks.
- Dashboard (not touched): `speculation_won` / `speculation_wasted` are new log types; `leases[].copy`,
  `speculation.probation`, and the worker `probation` / `speculativeTaskIds` fields are there to draw.

## Known gaps

- A running loser can't be interrupted (model calls aren't cancellable); it finishes and its result is dropped.
  The fake backend could honour a cancel flag, but `fake.py` was out of scope.
- One copy per task, ever. A copy that dies isn't replaced (the original is still running).
- Service-time history is per coordinator process and starts empty after a restart (5 completions of a stage before
  anything is speculated).
- Offers to a worker running a pre-P3 image are never taken (it doesn't read `spec:{id}`); they expire and that worker
  cools down. Harmless, just no speculation onto it.
- Only makespan with one throttled worker was measured; no real-model heterogeneous run (the MPS worker) yet.
- `recovery` records don't count promoted tasks (nothing was requeued for them).
