# H: coordinator high availability

Owner: HA agent. Scope: `coordinator/**` (migration 008, `leader.ts`, `ha.ts`, `cluster.ts`, fencing in `db.ts`),
`docker-compose.yml`, `lb/haproxy.cfg`, `dashboard/nginx.conf` (proxy target only), the "HA refinements" section of
`docs/CONTRACTS.md`. Implements report item #12: a Postgres lease-row leader with term fencing, stateless API
replicas, and a failover test with p50/p95 and "0 writes accepted from a stale term".

## The design in one paragraph

Two coordinator replicas sit behind an HAProxy (`coordinator-lb`). Both serve the whole API and their own
WebSocket clients. One of them holds a lease on the single row `coordinator_leader` (River's design: 5 s TTL,
renewed every 1 s, Postgres `now()` only) and runs the singleton work. Every acquisition bumps `term`, and every
leader-only transaction starts with `wb_leader_guard(term)`, which locks the leader row `FOR SHARE` and aborts
unless that term is still current. A takeover is an `UPDATE` of the same row, so it waits for every transaction
that already passed the guard: terms never overlap, however long the old leader was frozen. What used to live in
one process's memory and must agree across replicas moved out: the throttle flag and the chaos switch into the
leader row, WebSocket fan-out and telemetry onto a Redis pub/sub bus.

```
            workers ─┐                         ┌─ coordinator (coord-1) ─┐
  dashboard nginx ───┼──> coordinator-lb ──────┤                         ├──> Postgres  (coordinator_leader: holder, term, expires_at)
                     │    (haproxy, /healthz)  └─ coordinator-2 (coord-2)┘      Redis    (queues + wildebeest:cluster pub/sub)
                                                 one of them leads: sweep, reaper, death watch, chaos, rebuilds
```

## Decisions

1. **Lease row + explicit term, checked by the resource.** Not Redis `SET NX` (TTLs are wall-clock, Redis here is
   non-persistent, and the term would have to be checked in Postgres anyway), not an advisory lock (a dropped
   session silently transfers leadership while the old process keeps sweeping). Acquire:
   `UPDATE … SET holder, instance, term = term + 1, expires_at = now() + TTL WHERE expires_at <= now()`; at most one
   candidate matches, the rest re-check the new row version and match nothing. Renew:
   `… WHERE holder = me AND instance = mine AND term = myTerm`. Resign (SIGTERM) sets `expires_at = now()` and
   announces it on the bus, so a follower takes over in one round instead of after the TTL. `instance` is a random
   id per process, so a restarted replica with the same `COORDINATOR_ID` is a stranger to its own old lease.

2. **The fence lives in the database access layer, not at call sites.** Leader-only code runs inside
   `withFence(term, …)` (an `AsyncLocalStorage` context). While a fence is in context, `query()` and `tx()` open a
   transaction whose first statement (sent together with `BEGIN`, one round trip) is `wb_leader_guard`. So the
   reaper, the sweep, recovery, rebuilds, chaos stamps, the death-watch reaction, `recordEvents` on the pool: all are
   guarded without a single `if (isLeader)` in them, and a code path added later can't forget the check. The API
   path runs without a fence and is unchanged (it is fenced per row by lease epochs). The existing tests call
   `dispatchOnce()`/`reapOnce()` directly and are untouched. Streams and timers started by the leader inherit the
   context; the Docker event stream's listeners are bound to it explicitly (`AsyncLocalStorage.bind`), because an
   EventEmitter runs listeners in the emitter's context.

3. **`FOR SHARE` makes terms linearizable.** Checking the term and then writing would let a takeover commit in
   between (READ COMMITTED). Holding a share lock on the leader row for the whole transaction serialises every
   leader-only transaction before the next term begins. Two costs, both bounded: the leader's own renewal waits
   behind its guarded transactions (ms; `lock_timeout` on the election connection), and an old leader frozen
   *inside* a guarded transaction would hold a takeover off, so the guard sets
   `idle_in_transaction_session_timeout = 2 s` for its transaction. Tested: a guarded transaction sleeping 400 ms
   makes the takeover wait for its commit, and its event gets the older term and the smaller id.

4. **The guard stamps the term.** `wb_leader_guard` also sets `wb.leader_term` for the transaction, and
   `task_events.leader_term` defaults to it. Every event a leader-only transaction writes carries its term; API-path
   events carry NULL. "Zero writes accepted from a stale term" becomes a query: no event may carry a lower term than
   an event inserted before it. The failover test runs it after every run.

5. **Step-down rules.** A renewal that matches no row → step down at once (someone took over). A renewal that
   errors → retry every tick; step down once `TTL − one tick` has passed without a successful renewal, i.e. before
   any follower can acquire (client-go's RenewDeadline < LeaseDuration). A guard rejection → step down at once and
   write a `leader_fenced` event (outside any term). There is deliberately **no local "my lease has probably
   expired" check before a sweep**: after a long pause the old leader's first statements go to the database and are
   refused there. That is the point of fencing (the lock service's view is advisory, the resource decides), and it
   is what makes the rejections countable. Non-database side effects are ordered after a guarded statement (the
   chaos kill's attribution stamp, the rebuild's `UPDATE`) or are harmless when repeated (pushing an ID twice).

6. **Election runs on its own connection.** A dedicated `pg.Client` (not the pool), `lock_timeout` = one tick,
   `statement_timeout` = two ticks, so a saturated pool can't starve renewals and a round can't hang. Its
   `application_name` (like the pool's) is `wildebeest-coordinator:<id>`, which the test uses to kill a leader's
   backends.

7. **What is leader-only.** Repair sweep + throttle hysteresis (`dispatchOnce`), reaper (heartbeat deaths, lost
   leases, drain of dead workers' processing lists, expired speculation offers, the job-finish safety sweep),
   straggler speculation, the Docker death watch and its reaction, chaos kills, leftover-pause cleanup,
   replica-loss detection, the queued-row audit, and the startup reconciliation. What every
   replica runs: the whole HTTP API including push-after-commit (P2's pushes are guarded per row by
   `queued = false`, so any replica may push), the invariant check (read-only), the `complete_ms` write-behind for
   completions it handled, and, while following, a 500 ms refresh of the throttle flag and worker/lease counts.

8. **Startup reconciliation moved from "process start" to "won an election".** A follower's start touches no shared
   state (tested: a follower leaves the Redis queues and `queued` flags alone; the same process, once elected,
   rebuilds them). The rebuild is unconditional on election: the previous leader may have died between an `LPOP`
   (complete-and-claim-next) and the lease statement. The worker/lease grace is now only for a **cold start**: if
   nobody processed a heartbeat for 2 × `HEARTBEAT_MS`. After an ordinary failover the surviving replica kept
   serving heartbeats and renewing leases, and a blanket grace would only delay catching a worker that died
   meanwhile.

9. **A replica can die without being the leader, so replicas heartbeat too.** P2 accepted that IDs popped for
   claim-next could be lost in a crash "until the restart's rebuild". With replicas there is no restart. Each
   process upserts its row in `coordinator_nodes` every election round; the leader deletes rows silent for longer
   than the TTL and then rebuilds the queues (forget first, rebuild second, so whatever the lost replica dropped is
   covered). A rebuild is now also self-healing if it stops half way: it deletes the queues *and* the
   `queues-built` marker in one `DEL` and sets the marker only after the Postgres half committed, so a deposed
   leader whose `UPDATE` is fenced off after its `DEL` went through leaves the marker missing and the real leader
   rebuilds on its next sweep.

10. **Shared state: Postgres for decisions, pub/sub for notifications.** The throttle flag is decided by the leader
    (a fenced `UPDATE coordinator_leader SET throttled`) and read by every replica every 500 ms, because detect
    top-ups and Postgres-mode claims happen on every replica. The chaos switch is in the same row (any replica
    accepts `POST /chaos`, only the leader's loop kills). Everything else is a notification on the
    `wildebeest:cluster:<db>` channel (pub/sub is server-wide, so the Redis DB index is in the name), batched every 50 ms: event-log rows, "job X / workers changed", throttle changes,
    Postgres-mode long-poll wake-ups, "leader resigned", and **telemetry records**. Job summaries and worker lists
    are not sent; each replica rebuilds them from Postgres when told something changed. Pub/sub is at-most-once,
    which is fine: a lost message costs a dashboard refresh or a timing sample, never state.

11. **Telemetry: replicate the records, not the snapshot.** Options were (a) the leader computes the `system`
    snapshot and publishes it, (b) compute it from Postgres, (c) replicate the in-memory telemetry. (a) makes every
    follower's `GET /system` a stale copy and routes all completions' samples to the leader anyway; (b) is what P1
    decided against for cost. Chosen: (c). The telemetry singleton's record methods are wrapped so each record also
    goes on the bus; peers apply it through the unwrapped method. A completion is sent as its waterfall sample only
    (the `complete_ms` write-behind stays with the replica that handled it), and a claim is sent only when it closes
    an open recovery, so the hot path sends nothing extra per claim. Checked live: a WebSocket client on each
    replica saw the same event log (including the leader's `worker_died`/`reassigned` and the follower's
    `worker_killed`), the same throughput series and the same recovery record, `reclaimedBy` included; timing sample
    counts agreed to within the 50 ms batching (518 vs 528 of a 60 s window).

12. **HAProxy, not nginx, in front of the replicas.** Active health checks (`GET /healthz` every second, out after
    2 failures) are what make a *frozen* replica leave the rotation: a paused container still accepts TCP
    connections, so passive failure detection only notices after a request times out. `on-marked-down
    shutdown-sessions` cuts the requests stuck on it, so workers get an error they retry rather than waiting out
    their 10 s timeout. Only connection failures are retried by the balancer (the request never reached a replica);
    anything later is answered 502/504 and the worker retries it itself. Docker DNS resolution at runtime picks up a
    replica recreated with a new IP. The dashboard's nginx just points at `coordinator-lb`.

13. **Worker retry budgets already cover a replica dying mid-request; `runtime.py` is unchanged.**
    complete/fail/release retry connection errors and 502/503/504 for a full lease (15 s) against a balancer that
    reroutes within ~2 s; claim-confirm and `/tasks/claim` retry 5×; a heartbeat retries once (a single late
    heartbeat is far inside the 6 s worker timeout); register loops for 2 min. In the runs below no worker was ever
    declared dead by a coordinator failover.

14. **Leftover pauses.** The replica that pauses a worker times the unpause. If it dies, the leader unpauses worker
    containers whose injected pause ended more than 2 s ago (every 5 s and on election). The old rule, "unpause every
    paused container of the project at startup", would have unpaused a live replica's in-progress pause, and in the
    failover test it would have unpaused the frozen *coordinator*.

15. **A session the server ends mid-transaction must not crash the replica.** Found by the first pause campaign:
    the frozen leader's guarded transaction was ended by the 2 s idle-in-transaction timeout; on waking, `pg`
    delivered the FATAL as an `error` event on a checked-out client, which the pool only listens to while a
    client is idle, and the process died (the job still finished, and the replica restarted as a follower, but the
    run recorded 0 fenced attempts because the zombie never got to try). `tx()` now listens for that event while
    it holds a client, fails the transaction, and discards the connection instead of returning it to the pool. The
    same applies to `pg_terminate_backend` (tested). The fix matters beyond HA: before it, terminating a
    coordinator backend in the middle of any transaction would have crashed it too.

16. **An audit of queued rows, because a pop can be lost without anyone dying.** Found by the second campaign's
    `terminate` runs: complete-and-claim-next `LPOP`s IDs and then leases them; when the connection running the
    lease statement was terminated, the put-back failed too, and those rows stayed `queued = true` in Postgres
    and in no Redis list. The job hung until the next leader election rebuilt the queues (P2 recorded the same gap
    for a crash, "until the restart's rebuild"). Now the leader, every 5 s, compares rows queued for more than 5 s
    (a small partial index, `tasks_queued_idx`) against one `MULTI` snapshot of the ready queues and every
    worker's processing and speculation lists, and marks the missing ones unqueued; the repair sweep pushes them
    again. A false positive (an ID between a pop and its lease at that instant) becomes a duplicate queue entry,
    which claim-confirm already makes harmless.

17. **Speculation (P3) runs on the leader, and its history is replicated.** `speculateOnce` is a leader loop
    (inside the fence, so its offers and `task_attempts` writes are term-guarded). Its per-worker service times are
    recorded by whichever replica handled a completion; the bus replicates them like the other telemetry, so the
    leader sees every completion (not half), every replica's worker list shows the same `p50ServiceMs`/probation,
    and a newly elected leader starts with the cluster's history instead of none. Migration 008 does not touch
    `wb_complete`/`wb_late_outcome` (007's versions stand).

## Failover test

`coordinator/scripts/failover.py` against the `wb-ha` stack: 2 replicas + HAProxy, 3 fake detectors + 2 fake
classifiers (`FAKE_MODEL_DELAY_MS=50`), hybrid claims. Each run: wait for both replicas and the balancer to be
healthy, start a 400-task synthetic job, inject one fault into the **leader** 2 s in, wait for the job, then check
the job's history with the invariant checker (`tests/invariants/checker.py`: single success, result rows, fenced
completions, epochs, terminal images), the stale-term audit over all of `task_events`, and failed images. The
checker predates speculation, so the script presents P3's `speculated {epoch}` events to it as the claims they are
(without that, every won copy reads as "completion accepted with epoch 2, latest claim was 1").

- **kill**: `docker kill -s KILL` the leader. Failover time = kill (Postgres `clock_timestamp()` just before the
  signal) → the new leader's `leader_elected` event, which it writes after its election reconciliation and first
  full sweep. The killed replica is started again and rejoins as a follower.
- **pause**: `docker pause` the leader for 8 s (past its 5 s lease), then unpause and watch it for 3 s.
- **terminate**: `pg_terminate_backend` every backend with the leader's `application_name` (election connection
  and pool).

- **stop** (extra): `docker stop` (SIGTERM) the leader. It resigns and says so on the bus.

### Results (hybrid claims, 12 runs per scenario, 48 runs, ~24k tasks)

Docker Desktop on the M2, shared with other agents' stacks. Failover times on the database clock.

| fault on the leader | runs OK | failover p50 | p95 | min / max | leaderless p50* | stale-term writes | fenced attempts |
|---|---|---|---|---|---|---|---|
| SIGKILL | 12/12 | **5.34 s** | **5.66 s** | 4.29 / 5.66 s | 5.36 s | **0** | n/a |
| `docker pause` 8 s | 12/12 | **5.45 s** | **5.87 s** | 5.30 / 5.87 s | 5.51 s | **0** | **70** (5–6 in every run) |
| SIGTERM (resign) | 12/12 | **0.18 s** | **0.42 s** | 0.11 / 0.42 s | 0.40 s | 0 | n/a |
| `pg_terminate_backend` (7–10 backends) | 12/12 | no failover | | | | 0 | 0 |

\* previous leader's last renewal → new term. The rest of the failover is the new leader's reconciliation and first
sweep: p50 80–150 ms depending on the scenario (queue rebuild, forget the lost replica, grace check, one dispatcher and one reaper pass),
max 440 ms.

- **Failover ≈ the 5 s TTL**, as designed: the follower acquires on its first round after the lease lapses
  (rounds every 1 s), and the kill lands anywhere up to 1 s after the last renewal, hence 4.3–5.7 s. p95 − p50 is
  0.3–0.4 s.
- **Zero writes accepted from a stale term** across all 48 runs (the audit covers every `task_events` row stamped
  with a term, across ~70 terms). In every pause run the woken leader *tried*: 5–6 leader-only statements (dispatcher
  sweep, reaper, chaos tick, replica check, speculation) were refused by `wb_leader_guard`, recorded as
  `leader_fenced`, and it stepped down; nothing of its old term committed after the takeover.
- **The jobs never noticed.** Every job finished with 0 failed images and no invariant violation (single success,
  result rows, fenced completions, epochs, terminal images). No worker was declared dead (`reassigned` 0), and no
  lease expired (`lease_expired` 0): the orphaned leases of lost responses were picked up by speculation
  instead. Job time (400 detect tasks + their classify tasks) p50: 10.6 s with a SIGKILL, 11.9 s with a pause,
  10.4 s with a SIGTERM, 8.8 s with terminated backends; the pause costs most because requests stuck on the frozen
  replica wait until the balancer cuts them (~2–3 s).
- **Terminated backends didn't cost the lead**: the election connection reconnects on the next round and renews
  the same term (the renew deadline is 4 s); in-flight statements failed and were retried by their loops or the
  workers. Two bugs this scenario found are fixed above (decisions 15 and 16).
- `stale_rejected` events (16 in total, mostly pause runs) are retries of completions whose first attempt committed
  on the replica that then died or froze; the result was kept (see gaps).

### Results (`CLAIM_MODE=postgres`, 6 runs per scenario, 24 runs, ~12k tasks)

| fault on the leader | runs OK | failover p50 | p95 (= max) | stale-term writes | fenced attempts |
|---|---|---|---|---|---|
| SIGKILL | 6/6 | 5.32 s | 5.49 s | 0 | n/a |
| `docker pause` 8 s | 6/6 | 5.28 s | 5.52 s | 0 | 30 (5 in every run) |
| SIGTERM (resign) | 6/6 | 0.10 s | 0.13 s | 0 | n/a |
| `pg_terminate_backend` | 6/6 | no failover | | 0 | 0 |

Same picture: no violations, no failed images, no worker declared dead, no lease expired. Over both campaigns:
**72 runs, 72 OK, 0 stale-term writes, 100 fenced attempts refused**.

## Tests

`test/ha.test.ts` (17 tests; two are hybrid-only): single leader per term and renewals keep it;
takeover after lapse bumps the term and the old leader steps down on its next renewal; resignation hands over
without the TTL; a restarted process with the same id waits out its old lease; step-down on the renew deadline;
a deposed leader's reaper pass, dispatcher sweep, rebuild and death-watch reaction are rejected with nothing written
(and `leader_fenced` recorded), while the current leader's pass succeeds with its term stamped; a guarded
transaction holds a takeover off until it commits (terms never overlap, audit query = 0); leader work runs only on
the leader, stops when deposed, moves on resignation; a follower's start leaves the queues alone and its election
rebuilds them and writes `leader_lost`/`leader_elected`; cold-start grace only when no heartbeat was processed; the
throttle flag reaches followers; the bus relays hub notifications and telemetry, applies a peer's without echoing
it and ignores its own; recoveries and the claims that close them replicate; a session the server ends
mid-transaction fails the transaction without crashing the process; the queued-row audit re-dispatches a popped and
lost ID but leaves one sitting in a worker's processing list alone.

`npm test` (both modes, on top of P3): hybrid 139 passed; postgres 123 passed, 16 skipped (Redis-only tests). The
only existing test touched is a P1 race in `recovery.test.ts` ("handles events split across chunks"): it read the
`worker_died` event right after seeing the DEAD mark, which commits a moment earlier; it now waits for the event
(it failed about 1 run in 10 on the loaded VM).

## Known gaps

- **Failover ≈ the lease TTL** by design (River: 5 s). The API keeps serving throughout; only leader-only work
  (death detection, lease expiry, repair sweep, throttle decisions) pauses. A graceful stop (SIGTERM) hands over in
  one election round. A shorter TTL trades failover time against false takeovers when a leader stalls.
- **A response lost with its replica costs a lease expiry.** A claim (claim-confirm, `/tasks/claim`, or `next` on a
  complete) that committed on a replica that died or was frozen before answering leaves a lease the worker never
  learned about; it expires after `LEASE_MS` (15 s) and is charged an attempt (`lease_expired`). This is what made
  the pause runs' jobs longer. Since P3, speculation usually rescues such a task first: at the end of a job the
  orphaned lease looks like a straggler and an idle worker gets a copy (seen in the runs, ~7 s after the fault). A fix would be heartbeat-driven release of leases a worker has not acknowledged,
  which needs a per-heartbeat sequence number to be safe against delayed heartbeats; not done.
- **A retried complete whose first attempt committed is answered 409** and logged `stale_rejected` (the result is
  kept; the worker's log says "discarded"). Making `wb_complete` treat same-epoch/same-worker/already-SUCCEEDED as
  `ok` would fix the wording; it is P2's function and the speculation work is likely to change it.
- **Telemetry history is per replica process**: a replica started later has a shorter `recovery`/`fencing`/timings
  history than its peer (counts are cluster-wide from the moment it joined).
- **Single Postgres, single Redis.** HA covers the coordinator only. Postgres failover would need its own story (and
  the lease/term design keeps working on a promoted replica only with synchronous replication).
- Two replicas top up `queue:detect` independently, so it can briefly hold up to 2 × its target (harmless), and
  `eligible_at` (P1's dispatch-wait split) is computed against each replica's own previous top-up.
- `ALREADY_PAUSED` is per replica (Docker rejects a double pause with 502).
- Postgres-mode long-poll wake-ups cross replicas on the bus; if one is lost, the 250 ms re-check covers it.
- Not done: partitioned ownership (Temporal-style shards with a term each) for scaling the singleton work itself.

## For the lead to merge

- Rebased on P3 (02e6488). Any new leader-only loop belongs in `startLeaderLoops` (`loops.ts`) so it runs inside
  the fence; any new in-memory telemetry that `/system` or the worker list reads must be replicated in
  `coordinator/src/cluster.ts` (as done for `serviceTimes`), or replicas will disagree.
- **OpenTelemetry work**: `db.ts`'s `query()`/`tx()` now wrap guarded transactions; a span around them should
  include the guard. `index.ts` now starts the cluster through `startHa()`.
- **README**: architecture diagram gains the balancer and the second replica; "single coordinator" limitations
  become "failover ≈ 5 s, measured below"; the headline table can quote the failover p50/p95 and "0 stale-term
  writes in N runs".
- **Compose**: the coordinator's host port moved to `coordinator-lb` (same `COORDINATOR_PORT`); scripts that
  `docker compose exec coordinator` still reach replica 1. `docker compose up --scale coordinator-2=0` runs a
  single replica behind the same balancer.
- **Bench harness and fault matrix (eb869d2) need a one-line follow-up each** (not my files): `bench/stack.py`
  `up_infra()` and `tests/invariants/faults.py` start `("postgres", "redis", "minio", "coordinator")`; add
  `"coordinator-2", "coordinator-lb"` or nothing answers on `COORDINATOR_PORT`. `faults.py`'s toxiproxy upstream
  should be `coordinator-lb:3000`, and its "coordinator" restart fault now restarts one replica (a real failover:
  consider restarting the leader, found via `GET /cluster`). `coordinator/scripts/failover.py` already loads
  `tests/invariants/checker.py` from the repo.
- `DECISIONS.md` #15 (reconciliation at startup) is refined by decision 8 here.
