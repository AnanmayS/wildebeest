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
