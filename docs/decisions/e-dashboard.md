# E: dashboard redesign ("show the system at work")

Owner: dashboard agent. Scope: `dashboard/**` only. Built against docs/CONTRACTS.md "v2 additions";
the mock (`dashboard/mock/`) implements the contract so the page could be built before the coordinator.

## What the page is now

Above the fold at 1440×900 (verified in headless Chrome and the in-app browser):

1. **How work flows** (hero). The real topology, left to right: Job → Dispatcher (push rate, repair sweep)
   → backpressure valve → `queue:detect` tank (depth vs target) → detector lanes → `queue:classify` tank
   (high/low watermarks drawn at their real heights) → classifier lanes → Result store (finalised count,
   img/s trend, exactly-once badge from `invariants.duplicateResults`). A dashed feedback rail runs from
   the classify tank back to the valve; it turns amber and the valve closes while throttled. A bottom
   rail shows the "no animal → final after detect" bypass with its share.
   Each worker lane: state stripe (solid = busy, dim = idle, hatched amber = frozen, red = dead),
   device badge (CPU/MPS/CUDA), `native` badge, lease chip (task id + epoch, violet when epoch > 1,
   underline = share of the lease used), p50 ms/task, tasks done, Pause and Kill buttons (disabled
   for native workers and, for Pause, on a v1 coordinator). Packets on the lane wires are spawned only
   by diffs of the worker's held task IDs: in = claim, out = completion, amber back-to-queue = requeue
   after death. Dead lanes fold to one line, then to a "+N dead" line after 20 s.
2. **Why it's fast.** "Where a task's time goes": waterfall of the `timings` p50 steps with p95 column,
   orchestration steps in the accent, work in grey, waiting hatched; queue wait gets a visibly broken
   bar when it would squash everything else. Headline = `overheadPct`. Hover/focus a step for a
   one-line explanation. **Measured** (`GET /benchmarks`): throughput-vs-workers on a log₂ axis per
   task size with a dashed linear reference and USL peak, plus before/after bars for recovery p50 and
   orchestration ms per task. The throughput trend lives in the Result store node; it becomes its own
   panel when timings or benchmarks are missing.
3. **Why it's robust.** Latest failure → recovery: total time, a to-scale bar against the heartbeat
   timeout ("6 s just to notice"), and the steps Failure → Detected (Docker event / heartbeat) →
   Requeued → Reclaimed by X with the time between each. Fencing: last stale rejection
   ("09bc28 woke up holding e1 · task now at e2 · 409 STALE_LEASE") and the live lease table
   (retried leases first). Invariants: duplicate results / stuck leases / lost images, plus the
   fault-matrix totals from benchmarks.

Below the fold ("What the job found"): job progress + funnel + impact, species leaderboard, event log,
photo gallery with filters. Header: job strip, chaos switch, upload, "Rerun (cache)", sample size + Load.

Every panel has a one-sentence caption. Motion is limited to the lease packets, lane colour changes,
bar grow-ins and counters; `prefers-reduced-motion` removes packets and animations.

## Degrading on an older coordinator

Checked with `MOCK_LEGACY=1` (no `/system`, `/benchmarks`, pause, runtime/device, or event `detail`):
the hero still renders from `worker_update` + `/metrics` (queues) + job summary, with a "v1 coordinator"
badge; the waterfall and Measured panels are hidden and the throughput panel takes their place (fed
by a client-side history of `job.throughput`); recovery is rebuilt from `worker_killed` →
`worker_died` → `reassigned` events ("reclaimed: not reported (v1)"); fencing falls back to the last
`stale_rejected` event (epochs parsed from its message); Pause is disabled. `/system` is re-probed every 30 s.

## Contract gaps: requests for the coordinator

1. **`detail` on event-log items.** `toLogItem()` in coordinator/src/events.ts drops `detail`, so the
   WebSocket `task_events` and `GET /events` items have none. The dashboard reads `worker_died.detail.via`
   and `detectMs`, `stale_rejected.detail.leaseEpoch` and `currentEpoch`, and `worker_paused.detail.ms`.
   Please pass `detail` through unchanged. Without it the dashboard falls back (parses "epoch 3 ≠ 4" from
   the message, assumes a 20 s pause), but the lane can't say "Docker event in 39 ms".
2. **Log types.** Add `worker_paused`, `worker_resumed`, `speculated` (and `released`, if you want it in the
   log; the mock emits it for ~0.2% of tasks) to `LOG_TYPES`. The dashboard styles all four.
3. **`recovery[].reclaimedBy`** (worker id that re-claimed the task). Not in the contract; the brief asks
   for "reclaimed by worker X". Optional in the dashboard types. Also please state the array's order;
   the dashboard takes the record with the latest `detectedAt`.
4. **`timings.dispatchWaitMs` inflates `overheadPct` under backlog.** As defined (pushed − became PENDING),
   a task that waits in Postgres because `queue:detect` is at its target counts that wait as dispatch
   time, and `overheadPct` counts dispatch time as orchestration. On a 1,000-image job this would report
   seconds of "orchestration" per task. Suggest measuring dispatch wait from when the task became
   eligible (queue had room / not throttled), or folding backlog into `queueWaitMs`. The mock does the
   latter: dispatch ≈ push latency (1–4 ms), backlog in queue wait.
5. **Pause demo can end without a 409.** After `docker unpause`, the worker's heartbeat thread usually
   gets `410 WORKER_DEAD` first and `runtime.process()` then discards the result locally, so no
   `stale_rejected` is recorded. Correct, but the demo's punchline disappears. Options: let the worker
   still POST the result when `dead` is set (it is fenced anyway), or have the coordinator record an event
   when a dead worker's heartbeat is refused (e.g. `worker_rejoined` with the epochs it held). The
   dashboard shows whichever arrives; with neither, the lane simply returns to alive.
6. **`stages.*.completedPerSec` window** isn't specified; the dashboard's tooltip says "last 10 s".
7. `worker_update` items must carry `runtime` and `device` (the contract says `GET /workers` and
   `worker_update` both do; just confirming both paths are wired).
8. `docker pause` produces no `die` event, so a paused worker is caught by the heartbeat timeout. The
   lane's pause track marks the actual declared-dead moment and the lease expiry to scale.

## Notes

- Backpressure placement: the brief sketched the gate between detector lanes and `queue:classify`. The
  throttle actually gates detect admission (dispatcher → `queue:detect`) based on classify depth, so the
  valve sits on that wire and a feedback rail connects it to the classify tank's watermarks.
- The mock's benchmarks (`dashboard/mock/benchmarks.json`) are USL-shaped placeholders, labelled
  "MOCK DATA … not a measurement" in their `machine` field, which the page shows.
- The mock seeds one past stale rejection (epoch 3 ≠ 4) so the fencing panel isn't empty on first load
  (`MOCK_SEED_HISTORY=0` disables it); live pauses produce real epochs (typically 1 ≠ 2).
- Fixed a pre-existing bug in `useTween`: rAF timestamps can precede the effect's `performance.now()`,
  and the negative progress made counters overshoot to large negative numbers.

## Final pass (after P3 speculation, coordinator HA, observability)

Checked against the real merged coordinator (two replicas behind `coordinator-lb`, fake workers), not only the
mock: Compose project `wb-dash`, 3 detectors + 1 classifier at `FAKE_MODEL_DELAY_MS=300`, plus one
`docker compose run` detector at 3000 ms (later 10 s) as a straggler, page at 1440×900.

### What was added

- **Coordinators (pipeline header).** One pill per replica from `GET /cluster` (polled every 2 s; 404 = single
  coordinator, falls back to `system.leader`): `coord-2 leader · term 4`, `coord-1 standby`, `coord-1 down`.
  A leader whose `renewedAt` stops moving (or `leader.valid = false`) turns amber: `coord-1 silent 3 s / 5 s lease`.
  Replaces the old leader badge.
- **Leader failover line (robust column).** Hidden until a failover exists. Live while it happens ("leader coord-1
  silent 3 s · when its 5.0 s lease expires the standby takes term 4; workers keep going"), then the record from the
  newest `leader_elected` with a `previousHolder`: "coord-1 lost → coord-2 elected, term 4, in 5.9 s · 0 stale-term
  writes". Time = previous leader's last renewal → `leader_elected.at` (after the new leader's first sweep, the HA
  test's clock). Stale-term writes = `leader_fenced` events for the old term (tooltip: leaderless ms, reconcile ms,
  first-sweep counts, reason). A restart of the same replica id reads "coord-1 restarted, re-elected".
- **Speculation.** Lanes: a dashed sky ghost chip `COPY d7cb1f e2` for `speculativeTaskIds`, `+copy` on the
  original's chip when `leases[].copy` is set, a `PROBATION` badge (tooltip: its p50 vs 3× stage p50) and the lane's
  ms/task in amber. "Why it's fast" gains a one-line **Stragglers** strip: "8 of 9 copies won · 1 wasted · latest:
  92eaad 5.5 s > 1.9 s → 53b998, won" + "1 on probation"; hover explains the rule. Hidden when
  `speculation.enabled === false`.
- **Events.** Styled: `speculated` (Copy launched), `speculation_won`, `speculation_wasted`, `heartbeat_refused`,
  `redriven`, `leader_elected`, `leader_lost`, `leader_fenced`. A lane that gets `heartbeat_refused` says "woke after
  being declared dead · heartbeat refused (410)" until its stale result verdict arrives.
- **Recovery.** A dead straggler whose copy was promoted shows "copy on X took over" instead of "nothing in flight".
  Docker deaths show the coordinator's share: "die event, 8 ms after exit" (`detail.exitToDeadMs`); the rest of
  `detectMs` is Docker Desktop's own `docker kill` (250–490 ms seen).
- **Grafana.** `VITE_GRAFANA_URL` at build time (`docker compose build --build-arg VITE_GRAFANA_URL=http://localhost:3300/d/wildebeest-overview dashboard`),
  or a `grafanaUrl` in `GET /config` if the coordinator ever serves one: "Open traces in Grafana ↗" in the header
  subtitle. Nothing otherwise.
- **Mock.** One of the three mock detectors is a straggler (~8× slower, `MOCK_STRAGGLER=0` to drop it); the sim runs
  P3's policy (queue empty + idle worker, 3× stage p50, fastest idle target, probation, first commit wins, promotion
  on the original's death, wasted copies) and emits the P3 events and fields. HA: two replicas, 5 s lease,
  `GET /cluster`, `POST /mock/kill-leader` (and `?mode=pause`: the old leader wakes and is fenced); the kill closes
  every WebSocket so the reconnect path runs. `MOCK_SEED_HISTORY` also seeds a past failover (coord-2 → coord-1,
  term 7, 5.3 s).

### Mismatches found against the real coordinator (fixed in the page)

1. `avgLatencyMs: 0` and `stages.*.p50ServiceMs: 0` for workers/stages with no samples rendered as "0 ms/task" and
   "p50 0 ms": 0 now means "no data".
2. `recovery[].killedAt` is null when the coordinator didn't cause the death; the page parsed it to NaN. It now
   starts the clock at `detectedAt − detail.detectMs`.
3. The recovery record can stay open forever (see gap 2 below): "Reclaimed: waiting for a worker" never resolved.
   The page now remembers every lease it sees in `system.leases` (claim time = `system.at − ageMs`) and closes the
   timeline from that when the record doesn't ("by X", tooltip says it's read from the lease table). Requeue time
   is the earlier of the `reassigned` events and `requeuedAt` (taken after the push).
4. After a coordinator restart `system.recovery` is empty and the fallback said "not reported (v1)" with a 4 ms
   total (it started the clock at the death). The event fallback now starts at the kill/pause event and uses lease
   sightings for the reclaim.
5. Same-epoch `stale_rejected` ("epoch 1 ≠ 1", state SUCCEEDED, same holder) appeared right after a failover: a worker
   re-sending a completion whose first attempt had committed on the killed replica. The fencing panel showed "woke up
   holding e1 · task now at e1". Repeats are no longer shown as zombies (the panel prefers the newest real fencing,
   else says "re-sent its e1 result after it had already committed · no duplicate written"; lanes show no verdict).
6. `fencing.staleRejected` counts since the answering replica started (0 after a failover while the log still shows
   rejections): the count is now the max of that and the log's.
7. "How work flows" wrapped onto two lines once the header held the coordinator pills.

### Seen live (real coordinator)

- Kill (busy detector, via the page): recovery **683 ms** kill → reclaimed by another detector; detected +259 ms
  (die event 38 ms after exit), requeued +19 ms, reclaimed +405 ms. An earlier kill: 497 ms to detection, of which
  489 ms was `docker kill` itself and 8 ms the coordinator.
- Pause (20 s): declared dead by heartbeat after 5.7 s, 2 tasks requeued, reclaimed; on wake-up the lane showed
  "woke with e1 · task at e2 → result rejected", fencing panel "0f0d72897c11 woke up holding e1 · task now at e2 ·
  409 STALE_LEASE", then `heartbeat_refused`.
- Synthetic 20,000 tasks (4 detectors at 5 ms): 181 → 392 detect/s, 325–393 images/s finalised, dispatcher 270–459
  pushes/s, waterfall 9.8–15% orchestration, invariants 0/0/0.
- Leader kill mid-job (twice): pills went `coord-2 silent 2 s / 5 s lease` → `coord-1 leader · term 2` +
  `coord-2 down`; line "coord-2 lost → coord-1 elected, term 2, in 5.5 s · 0 stale-term writes" (leaderless 5,439
  ms, reconcile 101 ms); second time "coord-1 lost → coord-2 elected, term 4, in 5.9 s". The page stayed "Live"
  and the job kept finalising (~390 img/s during the first).
- Speculation: straggler on probation (p50 6,016 ms vs stage 617 ms); copies launched at the tail of each job
  (e.g. "task d7cb1f… running 12.7 s (threshold 6.1 s); copy on 89c7e0…", won 1.0 s later); the ghost chip
  `COPY d7cb1f e2` and `+copy` on the straggler's chip were on screen together.

### Coordinator gaps (for the coordinator owner; not edited here)

1. **No `GET /benchmarks`.** The route in "v2 additions" was never implemented (404; `benchmarks/summary.json`
   is mounted at `/benchmarks` but nothing serves it), so the "Measured" panel never shows on the real stack.
2. **Recovery records that never close.** `recoverDeadWorkers` pushes the requeued IDs (`pushNow`) *before*
   `telemetry.recordRecovery` opens the record, so a worker can claim the task first; `recordClaimed` then finds
   nothing open and the record keeps `reclaimedAt: null` forever (seen: task claimed at .383, record opened at .531).
   Open the record (or register the task IDs) before the push.
3. **Duplicate completes after a failover are logged as fencing.** A retried complete for a task already SUCCEEDED
   with the same epoch and holder is recorded as `stale_rejected` ("epoch 1 ≠ 1") and counted in
   `fencing.staleRejected`. It should be idempotent (200, or `ALREADY_DONE`) and not a fencing event.
4. **Orphaned leases after a leader kill.** ~27 s after killing a replica, three `lease_expired` events with
   `charged: true` for tasks held by live workers (claims/completes whose responses died with the replica). Costs an
   attempt each; the untracked `worker/tests/test_orphans.py` suggests this is being worked on.
5. **Per-replica service times.** `probation` / `p50ServiceMs` / speculation thresholds come from the answering
   replica's memory, so they differ between replicas, reset on restart, and right after a failover a classifier was
   briefly flagged on probation. Also the stage p50 window (last 200 completions) kept 5 ms synthetic tasks as the
   baseline for a later 1 s job, which put every worker on probation and disabled speculation for that job.
6. **`system.recovery` / `fencing` are per process.** After a restart or failover the robust panels fall back to the
   event log; persisting the last few recovery records (or rebuilding them from `task_events`) would keep them.
7. **`GET /config` has no `grafanaUrl`.** The link needs a build arg today; a `GRAFANA_PUBLIC_URL` env surfaced in
   `/config` would make it runtime-configurable. `docker-compose.yml` doesn't pass `VITE_GRAFANA_URL` either.
