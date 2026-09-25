# P1: coordinator recovery fast path, retry hygiene, telemetry

Owner: P1 coordinator agent. Scope: `coordinator/**`, worker `runtime.py`, `__main__.py`, `storage.py`, `fake.py`,
`worker/tests/**`, coordinator env in `docker-compose.yml`, and worker-protocol refinements in `docs/CONTRACTS.md`
(section "P1 refinements"). Implements report items #1 and #7 plus the `system` telemetry the new dashboard reads.

## Decisions

1. **One death path, three detectors.** Docker `die`/`oom` events, `docker ps` reconciliation and the heartbeat
   timeout all call `recovery.ts`: guarded `UPDATE workers … WHERE status='ALIVE'` → `worker_died` event →
   `requeueLostLeases({workerIds})` → drain the processing list → `pushNow()` the IDs to the queue head. The guard
   makes the event and the reaper race-safe (exactly one wins); a false positive is harmless because fencing already
   absorbs it. *Why:* no second, subtly different recovery implementation to keep correct.
2. **Project scoping is enforced three times.** Server-side filter `label=com.docker.compose.project=<ours>`,
   a client-side check of the same label on each event, and a container → worker mapping that only matches workers
   that registered with that exact container ID (prefix match on ≥ 12 chars) and are `runtime='container'`.
   Our project comes from `COMPOSE_PROJECT` or our own container's labels (`inspect(os.hostname())`); if neither is
   available the watcher stays off and heartbeats alone detect deaths. *Why:* several stacks share this Docker host.
3. **Reconnect with `since=<last event>`, then reconcile.** On stream end/error we reconnect with exponential backoff
   and `since` = the newest event time seen (or the last connect time). Every (re)connect then reconciles against
   `docker ps -a` for our project: an ALIVE worker whose container is `exited`/`dead` is recovered. Reconciliation
   only convicts on a positive "exited" from Docker, never on absence (a container that isn't listed may belong to
   something we can't see). Replayed events can't kill a newer incarnation: the mapping requires
   `registered_at <= event time`. `timeNano` is read from the raw JSON line as a BigInt, because the number (~1.8e18)
   exceeds 2^53 and `JSON.parse` rounds it (found by a flaky test).
4. **Recovered work is pushed immediately**, by the recovery path itself, after the requeue commits (LPUSH to the
   head). This also applies to the reaper's lease-expiry requeues. Behaviour change: `dispatchOnce().retried` no
   longer sees these tasks; three existing tests were updated to assert the new, faster behaviour.
5. **Attempt accounting.** `attempts` keeps its meaning (the only counter checked against `MAX_ATTEMPTS`) and is
   always `task_errors + lease_losses`. `releases` counts free returns: `/release`, deregister, re-register, and
   leases lost to *our own* kill or pause. Keeping `attempts` as the budget avoided renaming a column other code and
   tests read. Non-retryable `/fail` goes to FAILED regardless of budget.
6. **Attribution rule for our own faults.** A kill ends the incarnation, so every lease held by an incarnation we
   SIGKILLed is free (`killed_at >= registered_at`). A pause is bounded: leases of tasks started before the injected
   pause ended are free (`paused_at >= registered_at and started_at <= paused_until`). The first version used
   `killed_at >= started_at` and the live run caught the race: the Docker kill call takes a few hundred ms on Docker
   Desktop, the worker finished its task and claimed the next one in that window, and that task was charged. Stamps
   are written *before* the Docker call (the `die` event can beat the HTTP response) and rolled back if Docker
   refuses, so a refused kill never excuses a later genuine loss. Known imprecision: a task in flight during a short
   pause that later loses its lease for an unrelated reason also gets a free retry. That's acceptable because the
   pause was ours.
7. **Detection time is measured from the best known moment of death**: our kill/pause request to this incarnation,
   else the container's exit time from the event, else the last heartbeat. `worker_died.detail.exitToDeadMs`
   isolates the coordinator's share for Docker events.
8. **Reaper self-awareness (`StallMeter`).** The reaper loop measures how late each tick starts
   (gap − interval − 250 ms slack) and adds the sum of stalls from the last `max(WORKER_TIMEOUT_MS, LEASE_MS)` to
   both the heartbeat timeout and every lease. It is driven by the loop only (`reapOnce(grace)` defaults to the
   meter), so tests calling `reapOnce()` directly are unaffected. It fired for real: while other agents' real-model
   workers saturated the Docker VM, reaper ticks ran 1–2 s late and grace grew to ~5 s instead of convicting workers
   for heartbeats the coordinator hadn't processed. Logged only for stalls ≥ 1 s; exposed in `/metrics.reaper`.
9. **Retry backoff is full jitter, coordinator-side** (`not_before`), `random(0, min(30 s, 500 ms · 2^(errors−1)))`,
   honoured by both the dispatcher sweep and `pushNow`. Releases get no backoff: the worker's circuit breaker is the
   backoff for infrastructure trouble, and a released task should run on a healthy worker at once.
10. **Worker error classes live in `runtime.py`** (`InfraError`, `NonRetryableError`, `classify_error`); `storage.py`
    translates botocore/PIL errors into them (`StorageUnavailable`, `CorruptImage`, `MissingObject`). `classify_error`
    also recognises raw botocore/requests/redis errors in case a handler bypasses `Storage`. A missing object is
    non-retryable: originals are written before the job exists, so a 404 won't heal (redrive exists if it does).
11. **Circuit breaker:** trips on the first infrastructure error (before the `/release` is even sent), claims nothing
    while open (so it holds no leases that could expire), releases the unstarted rest of a batch, probes `HEAD bucket`
    with full-jitter backoff (0.5 s base, 30 s cap), and never touches the heartbeat thread.
12. **Report retry budget = one lease length** (`leaseMs` from register), full jitter, on connection errors and
    502/503/504 only. Aborted early once the worker has been declared dead.
13. **A zombie reports once.** A worker that got `410` while a task was in flight still posts that result once (it is
    fenced with 409 and recorded as `stale_rejected`) and a `heartbeat_refused` event is written once per death. Asked
    for by the dashboard agent: without it the pause demo usually ended with no visible fence.
14. **Telemetry is in memory, Postgres keeps the per-task record.** Hot-path recording is an array push; summaries are
    built at most every 400 ms and shared by `GET /system` and every WebSocket client. `tasks.pushed_at`,
    `eligible_at`, `timings` are written in the statements that already run; `complete_ms` (only known after commit) is
    written behind, batched once per second. Counts reset on coordinator restart (documented in CONTRACTS).
15. **`dispatchWaitMs` is measured from eligibility**, not from "became PENDING" (dashboard request): a task ready
    before the previous sweep but not pushed by it was held back by the queue target/backpressure, so it is eligible
    only from the sweep that pushed it; its backlog counts as queue wait. Stored as `tasks.eligible_at`
    (migration 004). In tick mode `dispatchWaitMs` = tick delay + push latency (p50 11 ms at 0 ms tasks).
16. **Invariant check is incremental and cheap**: duplicate completions only over `task_events` newer than the last
    check (cumulative count), stuck leases over the LEASED partial indexes, lost images over the unfinalised partial
    index of *running* jobs. ~85 ms for a 20k-image running job.
17. **Synthetic jobs are one SQL statement** (`generate_series` → images + tasks via a materialised CTE), no
    `enqueued` events, not counted in the cache hit rate. 100k tasks in 3.5 s on the loaded shared VM (0.1 s for 2k).
    The fake backend wraps its handlers (`fake.synthetic_aware`) so `synthetic/` keys never touch MinIO or upload a
    crop; `detector.py`/`classifier.py` are untouched.
18. **Pause is coordinator-timed** (`setTimeout` → unpause) and `resumeAll()` runs on shutdown; at startup the death
    watch unpauses any container of our project left paused by a previous coordinator.
19. **`presign()` returns null for `synthetic/` keys**, so leases/DLQ/gallery never carry URLs to objects that don't exist.

## Measurements (fake workers, `FAKE_MODEL_DELAY_MS=300`, 3 detectors + 1 classifier, Docker Desktop on the M2)

| Kill via API → … | quiet VM (15 kills) | VM saturated by other agents (15 kills) |
|---|---|---|
| detected, via `docker_event` | p50 158 ms, p95 261 ms, max 542 ms | p50 444 ms, p95 3.1 s |
| of which exit → DEAD (coordinator) | p50 8 ms, max 26 ms | p50 ~20 ms, max 631 ms |
| all lost work re-claimed | p50 218 ms, p95 336 ms, max 405 ms | p50 485 ms, p95 3.5 s |
| heartbeat only (`DOCKER_EVENTS=off`), re-claimed | 5.1–7.2 s (5 kills) | 5.6–13.5 s (6 kills; reaper stall grace up to 5.5 s) |

The rest of the detection time is Docker's own kill → container-exit latency, which the coordinator can't shorten.

Other end-to-end checks (compose project `wb-p1`, fake workers):
- **Pause 20 s past the 15 s lease:** `worker_paused` → `worker_died` via heartbeat (6.2 s) → `reassigned` with
  `charged: false` (attempts 0) → re-claimed by another worker (epoch 2) → `worker_resumed` → `heartbeat_refused` →
  late result `stale_rejected` (epoch 1 ≠ 2). `system.fencing.last` shows it.
- **MinIO stopped for 15 s mid-job (400 sample images):** job done, 0 FAILED, 0 attempts charged, 4 releases; all 3
  detector breakers opened and closed within ~2–3 s of MinIO returning.
- **10k synthetic tasks at 0 ms:** created in ~1 s, done in 80.8 s (≈124 detect/s with 3 detectors, bounded by the
  serial worker round trips and the 250/s dispatcher cap, which are P2's targets). Invariants ok, 0 failed.

## For the lead to merge

- **README "How fault tolerance works"**: death detection is now Docker events first (heartbeat backstop); recovery
  row in the headline table should use the measured numbers above (p50 ~0.2 s quiet, not 5.8 s); SIGKILL paragraph
  ("the reaper declares it DEAD within about 6 s") is now "within ~0.2 s via the `die` event, 6 s heartbeat backstop";
  the state diagram gains `LEASED → PENDING: release / our own kill or pause (no attempt)` and
  `LEASED → FAILED: fail {nonRetryable}` and `FAILED → PENDING: redrive`; "Retries jump the queue" now happens
  immediately after the requeue commits.
- **DECISIONS.md**: #12 (only lease expiry, death and `/fail` count) is refined by decisions 5–6 here; #36 (retries
  LPUSHed by the dispatcher) is now done by the recovery path itself (decision 4); `released` is now shown in the
  event log (dashboard request), contrary to #12's note.
- **INTERVIEW_NOTES.md** still says a heartbeat renews every leased task of the worker (report's drift note) — not fixed
  here (not my file).
- `docker-compose.yml` coordinator env gained `DOCKER_EVENTS`, `RETRY_BASE_MS`, `RETRY_MAX_MS`. `WORKER_DEVICE` /
  `WORKER_RUNTIME` are read by the worker if set (native workers).
- Hand-off for **P2**: `pushNow()` and the `Sweep`/`eligible_at` logic in `dispatcher.ts` are where push-after-commit
  plugs in (`setDispatcherMode("push")` in `system.ts` flips the snapshot). `completeTask` records its waterfall sample
  through `telemetry.recordCompletion(timingSample(...))`; batch completes should do the same per row.
  `requeueLostLeases` already takes `workerIds` and returns the requeued IDs.
- Hand-off for **P3 (speculation)**: `speculated` is already a log type; `system.speculation` is hard-coded zeros in
  `system.ts`.

## Known gaps

- `docker pause` emits no `die`, so paused workers are caught by the heartbeat timeout (by design; it's the demo).
- Native workers and workers on other hosts are detected by heartbeat only.
- Telemetry (timings window, recovery records, fencing count) is per coordinator process and resets on restart.
- If the reaper's general sweep grabs a dead worker's tasks a few ms before the death path does, that worker's
  recovery record shows `tasks: 0` (the tasks are still recovered and pushed).
- The worker's report budget is one lease; a coordinator outage longer than that still drops finished results
  (the lease then expires and the task reruns, as before).
- Chaos mode still only kills; pausing is API-only.
- Recovery itself is several round trips (~30–70 ms from DEAD mark to push under load); P2 could fold them.
