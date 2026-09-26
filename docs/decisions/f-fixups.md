# F: final fix-ups

Owner: fix-up agent. Scope: `coordinator/**` (migration `010_fixups.sql`), worker `runtime.py` (+ `worker/tests/**`),
the "Final refinements" section of `docs/CONTRACTS.md`. Works through b-bench.md's requests 1 and 3, h-ha.md's
"retried complete answered 409" gap, and the coordinator gaps the dashboard's final pass found (e-dashboard.md).

## 1. Task IDs orphaned in a live worker's processing list (b-bench request 1)

**The bug.** A worker's claim moves IDs `queue:{stage} → processing:{workerId}` (the MULTI of LMOVEs, or BLMOVE when the
queue was empty). If the connection resets while that command is in flight, Redis has moved the IDs but the reply is
lost. redis-py 8 then either raises, or (its default `Retry`, 10 attempts on `ConnectionError`/`TimeoutError`)
silently sends the command again, which moves *more* IDs and returns only those. Either way the first IDs sit in the
processing list of a worker that is ALIVE and doesn't know it has them: the task is PENDING with `queued = true`, the
reaper only drains lists of workers that are not ALIVE, the repair sweep only pushes `queued = false` rows, and the
HA queued-row audit counted an ID in *any* processing list as accounted for. The job never finishes (the before fault
matrix's four I7 violations). The same happens when the IDs moved fine but claim-confirm never got through (five
failed tries).

**Did HA's audit already cover it?** No. `repairLostQueued` (every 5 s) only un-queued IDs that were in *no* Redis
list; its own test asserted that an ID in a worker's processing list is left alone. The new test
`fixups.test.ts › orphaned IDs … are re-dispatched by the leader's audit within a bounded time` reproduces the orphan
(an ID LMOVEd into `processing:{alive worker}` and never confirmed, the worker heartbeating) and fails on the old
audit.

**Fix, both sides:**

- *Worker (the common case, no delay).* The claim MULTI now starts with `LRANGE processing:{me} 0 -1`. claim-confirm
  empties the list, so anything already in it was moved by a claim whose reply was lost; those IDs are confirmed
  together with the newly moved ones. Because it is inside the same MULTI, a *silently retried* claim reads back what
  the lost attempt moved in the same round trip. A lost BLMOVE (the idle path) is picked up by the next claim. No
  extra round trip, one extra command on an empty list.
- *Leader (backstop).* The audit now also looks at IDs of PENDING + queued rows (pushed > 5 s ago) that sit **only**
  in an ALIVE worker's processing list. It remembers where it saw them; one still there `ORPHAN_GRACE_MS` later
  (default 3 heartbeats = 6 s) is taken back: a fenced `UPDATE … SET pushed_at = now()` (still PENDING + queued),
  then `LREM` from that list and, only if the LREM removed it, `LPUSH` to the queue head. Bound: grace + 2 audit
  intervals ≈ 16 s. It covers a worker that stops claiming (circuit breaker open, stuck) and pre-fix workers.

**Why it is safe against a claim that is genuinely in flight.** The guarded UPDATE goes first, so a deposed leader
touches nothing in Redis (`a deposed leader's audit is fenced off before it touches Redis`). A confirm that commits
between the sightings LREMs the ID itself, so the audit's LREM removes nothing and nothing is pushed (`the audit waits
out the grace, and leaves alone an ID that was confirmed meanwhile`). A confirm that arrives *after* the re-push still
leases the task (it's PENDING); the extra queue entry is skipped by whoever pulls it and not put back
(`is safe against a claim-confirm still in flight: one lease, the extra queue entry is skipped`). An ID that is also
still in a ready queue is left alone. On the worker side, confirming an ID twice is harmless for the same reason.

Tests: `coordinator/test/fixups.test.ts` (5 audit tests, hybrid only); `worker/tests/test_orphans.py`: a claim whose
reply was lost is confirmed on the next claim; a silently retried claim confirms what the lost attempt moved; a lost
BLMOVE reply; a claim-confirm that never got through; no leftovers means an ordinary claim. The fake coordinator in
the worker tests now LREMs on claim-confirm, like the real one.

## 2. Claim batching at small N (b-bench request 3)

**What the request suspected:** a per-loop floor of ~10 ms per task from `COMPLETE_FLUSH_MS` (50 ms) ÷ a claim batch of
~5, seen once in a 1-trial smoke run (0 ms tasks: 1,470/s at N = 1, 378/s at N = 4).

**What it was.** Not the worker. With 0 ms tasks the worker's claim batch runs to the cap (16), its result buffer
flushes on count or as soon as the backlog is empty, and the flush carries `next` (complete-and-claim-next), so it
never sits on results while blocking on a claim: per-loop cycles were 0.6–1.5 ms at N = 1 and 4 in every clean run.
The swarm containers used 6 % CPU. The slow points all had **Postgres at 500 %+ CPU** and, every time I looked,
the live invariant check's `lostImages` query from both coordinator replicas running for tens of seconds or minutes
(33 s, 100 s, and once **24 minutes**, blocking the harness's `TRUNCATE` and with it the whole benchmark).

The mechanism: the harness truncates the tables between points, so `tasks` is physically empty when the next point
creates its job (one statement inserting K images and then K tasks). An invariant check that is *planned* in that
window sees `tasks` with 0 pages, i.e. an estimated 0 rows, and picks a nested loop anti-join whose inner side is a
sequential scan of `tasks`; in READ COMMITTED it then *executes* with a fresh snapshot in which the job has
committed: K × K rows (120,000² for the stuck one). Both replicas run the check every 5 s, the window is ~1–2 s for a
big job, so it hits often, and it scales with K, which is why it showed up at the larger points (the harness sizes K
from the previous point's rate, so N = 1's first trial always gets a small K and looked fast). While it runs it burns
a core per replica and holds back vacuum's xmin horizon, so the hot path's updates slow down too (inside
`wb_complete`, the image update went to 4 ms mean). Outside the benchmark the same thing can happen on a fresh
database's first big job.

**Fix** (`invariants.ts`): every live invariant query runs in its own transaction with
`statement_timeout = INVARIANT_TIMEOUT_MS` (2 s), and `lostImages` with `enable_nestloop = off`, which makes its plan a
hash (or merge) anti-join, linear in unfinished images + live tasks whatever the statistics say (62 ms for
75,000 unfinished images and 100,000 tasks, vs 112 ms for the nested loop's *good* case). A check that times out keeps its last value and is retried on the next pass (the duplicate-results cursor
doesn't advance). Test: `live invariant checks are bounded` (a check blocked by a lock is cut off after the timeout,
nothing left running).

Nothing in the worker needed changing for this; `COMPLETE_FLUSH_MS` only matters when a batch is partly full and
the backlog isn't empty, which with k = ceil(RTT ÷ service) and flush-on-empty does not happen in steady state.

### Measurements

`bench/run.py --suites ceiling --task-ms 0,5 --workers 1,4,16 --trials 3 --mode synthetic` on the benchmark ports;
before = master at b8e563e (images built from a clean worktree), after = this change. Apple M2, Docker Desktop 8 vCPU,
shared with another agent's stack (neighbour CPU in the per-point logs). Tasks/s, per trial and mean.

| task | N | before, trials 1 / 2 / 3 | before mean | after, trials 1 / 2 / 3 | after mean |
|---|---|---|---|---|---|
| 0 ms | 1 | 1,420 / 1,140 / 1,553 | 1,371 | 1,351 / 2,467 / 563 | 1,460 |
| 0 ms | 4 | 1,115 / 3,047 / 2,664 | 2,276 | 7,184 / 2,985 / 2,302 | **4,157** |
| 0 ms | 16 | 711 / 1,058 / 875 | 881 | 3,749 / 3,495 / 1,661 | **2,968** |
| 5 ms | 1 | 97 / 92 / 74 | 88 | 94 / 86 / 98 | 92 |
| 5 ms | 4 | 391 / 360 / 362 | 371 | 424 / 367 / 395 | 395 |
| 5 ms | 16 | 1,347 / 997 / 1,284 | 1,209 | 1,448 / 1,502 / 1,531 | **1,493** |

K per point ranged from 3,600 to 200,000 (the harness sizes it from the previous point's rate). Per-loop cycle at
N = 16, 0 ms: before 14.7–22.4 ms, after 4.3–9.6 ms; at 5 ms the cycle is ~10.5 ms at every N after (the 6.4 ms a
`sleep(5 ms)` really takes in a container, plus one round trip).

Reading it:

- **The before sweep had the stall**: its 0 ms N = 16 points (711–1,058/s, lease p50 26–47 ms) and N = 4 trial 1
  (1,115/s) ran while the runaway `lostImages` queries were burning cores; I watched them in `pg_stat_activity`
  (33 s, 100 s). A rerun of the before sweep stopped for good behind a 24-minute one.
- **After, adding loops no longer collapses throughput:** 0 ms goes 1,460 → 4,157 → 2,968 (means) and 5 ms scales
  88 → 395 → 1,493 (16.2× for 16 loops; before 13.7×). At 0 ms, N = 16 is below N = 4: that is Postgres saturated
  (500 %+ CPU in `wb_complete` and the lease statement, lease p50 22–27 ms at N = 16), a throughput ceiling rather
  than a per-loop floor. On a freshly started stack without neighbours the same images did 4,300–6,900/s at N = 16
  and 3,900–5,000/s at N = 4 (ad-hoc points at K = 20,000–100,000, before and after images alike when the stall
  didn't hit).
- **Noise is large** (a shared Docker VM; another agent's stack reached 270 % CPU during the before run). The after
  sweep's third trial was slower at every N (563 / 2,302 / 1,661) with no runaway query in the logs; I didn't find a
  cause in the time I had (not table bloat: `workers` stayed at 2 pages; no session held back the xmin horizon when
  I looked). With 3 trials the 0 ms means carry wide intervals; the 5 ms series is tight.

## 3. Idempotent completion retry (h-ha gap)

A worker retries a complete whose answer it never got: the replica that committed it was killed or frozen before
answering, or the connection reset after the COMMIT. Before, the retry matched no LEASED row: 409 STALE_LEASE, a
`stale_rejected` event, a count in `system.fencing` (the dashboard showed "epoch 1 ≠ 1"), although the result it
carried was the one kept. Migration `010_fixups.sql` replaces `wb_complete` (007's version, which 008/009 left
alone) with one addition: before deciding `already_done` / `stale`, an item whose task is `SUCCEEDED` **with the
item's epoch, credited to the sending worker** gets status `duplicate`, and nothing is written. Node answers it like an
accepted write (`200 {ok: true}` with `leases` when `next` was asked for; batch item `"ok"`) without recording the
completion a second time (no timing sample, no service time, no `tasks_completed`). Epochs are unique per task, so
(task, epoch) is one attempt; the worker check keeps anyone else's report on the old path. The check runs before
`wb_late_outcome`, so a speculative copy that won (task SUCCEEDED under the copy's epoch) is also idempotent, while
the original it beat still gets `already_done` (P3 semantics unchanged). fail/release retries are unchanged.

Tests: `fixups.test.ts › idempotent completion retry` (single complete over HTTP incl. `next`; complete-batch with
one row already committed; an older epoch and another worker's report still fenced; a winning copy's retry ok and the
losing original `already_done`). `hot-path.test.ts` "fences in the statement" now expects the duplicate to be ok.

## 4. Other fixes (dashboard final pass, e-dashboard.md "Coordinator gaps")

- **`GET /benchmarks`** was never implemented (404), so the dashboard's "Measured" panel never appeared. It now serves
  `summary.json` from `BENCHMARKS_DIR` (default `/benchmarks`, which Compose mounts read-only on both replicas;
  `coordinator-2` inherits the mount through `extends`), 404 `NO_BENCHMARKS` without one.
- **Recovery records that never closed.** `handleDeaths` pushed the requeued IDs before `telemetry.recordRecovery`
  opened the record, so a worker could claim first and `reclaimedAt` stayed null forever. The record now opens before
  the push. Two related races are closed by a small memory of recent claims (latest claim per task, 30 s): IDs drained
  from the dead worker's processing list are pushed by the drain itself, and with two replicas the claim can be
  handled by the replica that hasn't yet received the record over the bus (it then re-announces the claim, so the
  replica that opened the record closes it). A task's latest claim counts only if it isn't the dead worker's own.
- **Speculation baselines** kept 10 minutes / 200 samples, so a job of 5 ms tasks left a baseline that put every
  worker of a later 1 s job on probation (and switched speculation off for it). Samples now expire after
  `SPECULATE_WINDOW_MS` (60 s); a worker is compared with the stage p50 over the same stretch of time as its own
  samples, and probation needs ≥ 5 samples of the worker and ≥ 20 of the stage (a replica that just started or took
  over has a handful, which is how a classifier was flagged right after a failover).
- **`grafanaUrl` in `GET /config`** from `GRAFANA_PUBLIC_URL` (null when unset).
- The queued-row audit's interval is configurable (`QUEUED_AUDIT_MS`), mostly for tests.
- **`lease_expired` with `charged: true` on live workers after a leader kill** (e-dashboard gap 4). Not the
  processing-list orphan (those tasks are PENDING and never expire): it is h-ha.md's "response lost with its replica"
  gap. A claim committed on the killed replica (claim-confirm, or the `next` leases of a complete), the response died
  with it, the worker never learned of the lease, and it expired `LEASE_MS` later and was charged an attempt. The
  fault matrix shows the same for a connection reset on the worker→coordinator path: 106–113 such expiries per
  5-reset run, and 1–2 images finalised `failed` after three of them. Now such a lease is **not charged**: when a lease
  expires on an ALIVE worker that heartbeated at least one interval after the claim but never renewed the lease
  (still `lease_expires_at = started_at + LEASE_MS`: no heartbeat ever listed the task), the loss costs a release, not
  an attempt, and the event says `charged: false, unacknowledged: true`. A worker that went silent right after the
  claim is charged as before (it may have run the task). The lease still waits out `LEASE_MS` before the task is
  requeued; releasing it early needs heartbeat-driven release with a per-heartbeat sequence number (so a heartbeat
  built before the claim's answer arrived can't release it). Not done. Tests: `leases lost with their response`.

## End-to-end check: connection resets (fault matrix)

`tests/invariants/faults.py --only reset --runs 3 --seed 7` (fake backend, 1,200-image job per run, 3 detectors + 2
classifiers routed through toxiproxy, 5 × 5 s `reset_peer` per run on the worker→coordinator or worker→Redis path),
after images, `SPECULATION=off` (see below):

| run | faults (path) | job | violations | failed images |
|---|---|---|---|---|
| 1 | coord, redis, coord, coord, coord | done 51.7 s after the last fault | 0 | 0 |
| 2 | coord ×3, redis ×2 | done 42.6 s after | 0 | 0 |
| 3 | coord, redis, coord, redis, redis | done 35.5 s after | 0 | 0 |

Run 3 had 51 lease expiries (lost responses on the coordinator path), all `charged: false, unacknowledged: true`. A
replay of run 3 caught the worker-side recovery at the second Redis reset: `found 1 task ID(s) in
processing:classify-a404c688e84d that an earlier claim moved without our knowing (lost reply); confirming them now`.
Every Redis-reset run of the before matrix ended in I7 (the job never finished).

With speculation on (the default), 2 of 3 runs each reported one I3/I4 pair ("completion accepted with epoch 2,
latest claim was 1"): a speculative copy that won. The checker reads only `claimed` events and predates P3; copies are
leased with a `speculated {epoch}` event (coordinator/scripts/failover.py translates those before calling the checker,
for exactly this reason). Those runs (before the unacknowledged-lease change) also finalised up to 2 images `failed`.

## Not fixed / for others

- **Requests for the owners of files I don't edit:**
  - `tests/invariants/checker.py` (or `faults.py`): treat `speculated` events (`detail.epoch`) as claims, as
    `coordinator/scripts/failover.py` does; otherwise every won speculative copy is reported as an I3/I4 violation.
  - `docker-compose.yml`: pass `GRAFANA_PUBLIC_URL: ${GRAFANA_PUBLIC_URL:-}` to the coordinator (both replicas inherit
    it), and the Makefile's observability target could set it to `http://localhost:${GRAFANA_PORT:-3300}/d/wildebeest-overview`.
  - `bench/stack.py` `reset_state()`: `VACUUM ANALYZE` right after the job is created (or after TRUNCATE, `ANALYZE` once
    the job exists) would give every point real statistics; with the fix above it is no longer needed for correctness,
    but it removes the remaining plan lottery from the hot path's first seconds.
- `system.recovery` / `fencing` are still per process (e-dashboard gap 6).
- A lease lost with its response still waits out `LEASE_MS` (15 s) before the task runs again (see above).
- A retried `/fail` or `/release` whose first send committed is still answered 409 STALE_LEASE with a
  `stale_rejected` event (only completes were made idempotent, as asked); harmless, the task already moved on.
- The after sweep's slow third trial (item 2) is unexplained.
