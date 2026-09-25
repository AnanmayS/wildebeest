# O: observability (tracing, Prometheus metrics, Grafana)

Owner: observability agent. Implements report item #10 ("Tracing turns this into something a reader can see"):
per-image OpenTelemetry traces across coordinator and workers, Prometheus RED/USE metrics, a bundled
`grafana/otel-lgtm` stack with a provisioned dashboard, and a measured on/off/sampled overhead. Built on top of P3
(speculation) and HA (two coordinator replicas).

Files: `coordinator/src/otel.ts` (trace hooks), `otel-sdk.ts` (SDK bootstrap), `otel-preload.ts` (ESM loader
hook), `prom.ts` (metrics), migration `009_trace_context.sql`; one-line hooks in `tasks.ts`, `jobs.ts`,
`recovery.ts`, `telemetry.ts`, `api.ts`, `index.ts`, `cluster.ts`; `worker/wildebeest_worker/tracing.py` with hooks
in `runtime.py` and `storage.py`; `worker/requirements-otel.txt` (+ one Dockerfile layer); `observability/**`
(Prometheus config, Grafana provisioning, dashboard + its generator, `overhead.py`); `docs/observability/**`; the
`lgtm` Compose service and OTEL env entries; "Observability refinements" in `docs/CONTRACTS.md`.

## How to use it

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://lgtm:4318 docker compose --profile observability up -d
# head sampling (keep 10% of images): add OTEL_TRACES_SAMPLER=parentbased_traceidratio OTEL_TRACES_SAMPLER_ARG=0.1
```

- **Grafana:** `http://localhost:${GRAFANA_PORT:-3300}` (anonymous admin) → Dashboards → Wildebeest → **Wildebeest**
  (`/d/wildebeest-overview`). Rows: RED per stage (completions/s, errors/s, handler p95), latency (service time and
  queue wait p50/p95), saturation (queue depth, leases in flight, workers, backpressure), fault tolerance (fenced
  writes, recoveries, recovery p50/p95, invariant violations, cache hits), three Tempo tables, and worker RSS / claim
  windows from heartbeats. `docs/observability/grafana-dashboard.png` is the dashboard after a 400-image job with
  one detector paused past its lease (its late write fenced off) and another SIGKILLed.
- **Find a reclaimed task's trace:** the dashboard table *Reclaimed tasks: traces with a requeue* lists every
  image whose attempt was lost; click the trace ID. Or Explore → Tempo → TraceQL:
  `{ name =~ "requeue (detect|classify)" }`. Other useful queries: fenced zombie writes
  `{ span.wildebeest.write.accepted = false }`; one image `{ span.wildebeest.task_id = "<uuid>" }`; speculative
  copies `{ span.wildebeest.speculative = true }`. Tempo's HTTP API is on `${TEMPO_PORT:-3200}`
  (`GET /api/v2/traces/<traceId>`), Prometheus on `${PROMETHEUS_PORT:-9090}`.
- **Tracing off** (the default, `OTEL_EXPORTER_OTLP_ENDPOINT` empty): the coordinator's preload returns at once (no
  loader hook, no SDK), the worker never imports OpenTelemetry, and every hook returns on its first line.
  `/metrics/prom` is always on.

## What a trace looks like

One trace per image (task), following the OTel messaging conventions:

```
create detect            PRODUCER  job creation; its traceparent is stored in tasks.traceparent
├─ lease detect          SERVER    one per attempt, claim → end: epoch, worker, speculative, outcome event
│   └─ process detect    CONSUMER  worker: the handler run
│       ├─ fetch / infer / upload     worker phases (infer = between the last storage read and the first write)
│       └─ settle detect CLIENT    worker's complete/fail/release request (traceparent sent per item)
│           └─ POST /tasks/:id/complete   SERVER, http instrumentation (single-task reports only)
│               ├─ complete detect         per task, write.accepted true/false (also inside complete-batch)
│               └─ pg.query / lpop / …     pg and ioredis instrumentation
├─ requeue detect        INTERNAL  leader's reaper / death watch: reassigned | lease_expired (+ via, detect_ms, charged)
├─ lease detect          SERVER    attempt 2, higher epoch …
└─ create classify       PRODUCER  under the detect completion that created it (same trace)
```

**The demo** (`docs/observability/reclaimed-task-trace.{png,json,txt}`, events in `reclaimed-task-events.json`),
on the two-replica HA stack with 2 s fake tasks: a detector was SIGKILLed through the API while running the task.

- `create detect` was emitted by coord-2 (it handled the job request).
- Attempt 1, `lease detect` epoch 1 on `detect-5b2e…`, 1.40 s, ends in ERROR "lease lost: detect-5b2ec9a2ff62 died
  (docker_event)" with a `reassigned` event (`death.via=docker_event`, `charged=false`). coord-1 emitted it, as the
  leader whose death watch requeued the task. `requeue detect` follows at the same instant.
- Attempt 2, `lease detect` epoch 2 on `detect-340c…`, contains the worker's `process detect` (fetch 21 ms, infer
  2.0 s, settle) → `POST /tasks/:id/complete` → `complete detect` (accepted) and its pg/ioredis spans. coord-2
  handled that complete, so it emitted the attempt span.
- The killed worker's own `process` span is missing: it died holding it open. Its `fetch` span had already been
  exported and shows up as an orphan near the start.

The task's `task_events` agree: claimed epoch 1 → reassigned → claimed epoch 2 → succeeded. Three processes
(coord-1, coord-2, the second detector) wrote one coherent trace, with no shared memory between them.

## Decisions

1. **The trace context lives on the task row** (`tasks.traceparent`, migration 009). It is written in the
   transaction that creates the task, before the push. Redis carries only IDs, and process memory dies with its
   process and is not shared between HA replicas; the row is what every attempt, the requeue and every replica see.
   009 only adds a nullable column (no rewrite, no function changes), so it doesn't touch 007's `wb_complete` or
   008's guard. Classify tasks are created inside `wb_complete`, so instead of redefining it their context is
   written right after the detect completion commits and before `pushNew`.
2. **One trace per image, PRODUCER root per task.** Every producer links to a `create job` span rather than being
   its child. Keeping each image its own trace keeps traces small and lets the head sampler decide per image.
3. **An attempt is a coordinator span (`lease {stage}`) with a derived ID, emitted when the attempt ends.**
   - Why the coordinator: a SIGKILLed worker never exports the span it has open, so a worker-side "attempt"
     would vanish in exactly the case the trace is meant to show.
   - Why not an open span: with HA, a claim and its complete or requeue usually land on different replicas.
     The first version kept the span open in memory, and half the attempts could never be closed.
   - How: the span IDs of the PRODUCER span and of every attempt are derived from the task ID and epoch
     (`sha256(taskId/create)`, `sha256(taskId/epoch)`, first 16 hex; a custom `IdGenerator` lets the SDK use
     them). A claim (claim-confirm, `/tasks/claim`, complete + `next`, speculative copy) only computes the attempt's
     context and returns it in the lease. The replica that sees the attempt end emits the span, starting at
     `tasks.started_at`: complete (OK), fail/release, the leader's requeue (ERROR "lease lost … (docker_event)" /
     "lease expired"), or the other attempt winning a speculation race (`lost_race`, not an error).
   - The requeue gets the lost attempt's epoch, claim time and trace context from the requeue statement's own
     `RETURNING`, so no second read can race the next claim.
   - A small per-replica cache of what it claimed adds the claim path, the `speculative` flag and the loser of a
     race. It is never required.
4. **Batched completes are attributed per task.** Each complete-batch item carries its own `traceparent` (the
   worker's per-task `settle` span); the coordinator makes one `complete {stage}` span per item in that item's
   trace (`batch_size` attribute). Single-task reports also send the `traceparent` header, so the http
   instrumentation continues the trace; batched reports send none, because a batch has no single parent. An
   untraced worker's items fall back to the row's context (one query per batch, only for such items).
5. **Selective library instrumentation.** Only http, pg and ioredis (no auto-instrumentations bundle).
   - Loading: through `node --import ./dist/otel-preload.js`, because the coordinator is an ES module. That is
     the import-in-the-middle loader in message-channel mode, so only those modules are wrapped.
   - Scope: an incoming request is traced only if the caller sent a `traceparent`, which means a worker's
     single-task report. Heartbeats, claims, batched completes and dashboard polls start no traces. pg/ioredis
     spans appear only inside a traced request, and pool-connect spans are off.
   - `OTEL_NODE_DISABLED_INSTRUMENTATIONS=http,pg,ioredis` switches libraries off one by one.
6. **Head sampling is decided once, at creation, and followed everywhere.** The PRODUCER span is a root, so
   `parentbased_traceidratio` samples images. The unsampled context is stored too (flags `00`), so the lease,
   the worker (`parentbased_*`) and every coordinator span agree, and nothing of an unsampled image is exported.
7. **Metrics are Prometheus (prom-client), not OTLP.**
   - Hot path: counters and histograms are fed from the points that already feed `/system`. That is one line
     each in `telemetry.ts` (completion, stale rejection, recovery opened/closed, job created), plus
     `failTask` and the requeue.
   - Scrape time: gauges are computed from the cached `system` snapshot and Postgres, so scraping adds nothing
     to the hot path.
   - Workers: their metrics ride on the heartbeat that already carries them. The coordinator exports them as
     `wildebeest_worker_*{worker,stage}`, so no worker or swarm loop needs an endpoint. The JSON `/metrics` is
     unchanged.
   - **HA:** each replica counts only what it handled. Telemetry replicated over the cluster bus is applied
     inside `fromBus()` and not counted again. Prometheus scrapes both replicas (`coordinator_id` label); the
     dashboard sums counters and takes `max` of the cluster-wide gauges.
8. **The worker's `infer` span is derived, not wrapped.** The handlers (`detector.py`, `classifier.py`, `fake.py`)
   are untouched: `storage.py` marks when the last read ended and the first write began, and the runtime emits
   `infer` over that interval with explicit timestamps. A speculative lease's process span carries
   `wildebeest.speculative=true`.
9. **Compose.**
   - `grafana/otel-lgtm` runs behind the `observability` profile: collector, Tempo, Prometheus, Loki and Grafana
     in one dev container.
   - Our Prometheus config and dashboard provider are mounted in, and every host port is env-overridable.
   - OTEL env is on both coordinator replicas (`coordinator-2` extends `coordinator`) and on the workers.
   - `OTEL_BSP_MAX_QUEUE_SIZE` defaults to 32768, because job creation emits one producer span per task in a
     burst.
   - The worker image installs `requirements-otel.txt` as a layer after the model download, so the multi-GB
     layers stay cached.
   - Dashboard rates use fixed `[30s]` windows: the bundled Prometheus datasource declares a 60 s scrape
     interval, which would make `$__rate_interval` 4 minutes.

## Overhead

`observability/overhead.py`. Closed loop, fake handler (`FAKE_MODEL_DELAY_MS` 0 or 5 ms; the swarm's measured
handler time is ~6–8 ms for "5"), 16 worker loops in one `swarm.py` container, synthetic jobs. Each run
truncates the tables, recreates the coordinator(s) with the configuration's env, runs a warm-up job, then a
measured job. Each point has 3 trials, interleaved and rotated across configurations. tasks/s is measured over
the 10th–90th percentile of completions.

CPU per task is cgroup `usage_usec` over the job divided by tasks, which is steadier than throughput on this shared
VM. "spans/task" is what the collector accepted. Configurations:

- `off`: `OTEL_EXPORTER_OTLP_ENDPOINT` empty.
- `on`: everything traced (`parentbased_always_on`).
- `sampled-10%`: `parentbased_traceidratio` 0.1.
- `coordinator-only` / `workers-only`: tracing in one tier.

**Final code, HA stack** (2 replicas + haproxy, coordinator CPU summed over both; `overhead-ha.json`):

| workers × task | config | tasks/s median (trials) | vs off | overhead ms/task | coordinator CPU µs/task | swarm CPU µs/task | lgtm CPU µs/task | spans/task | job create ms |
|---|---|---|---|---|---|---|---|---|---|
| 16 × 0 ms | off | **6050** (6050, 5769, 7038) |  | 2.65 | 235 | 113 | 65 | 0.0 | 354 |
| 16 × 0 ms | on | **3403** (3403, 3522, 3105) | -44% | 4.70 | 349 | 284 | 192 | 5.0 | 857 |
| 16 × 0 ms | sampled-10% | **5796** (5796, 5834, 5142) | -4% | 2.76 | 286 | 151 | 39 | 0.6 | 618 |
| 16 × 5 ms | off | **1380** (1506, 1380, 1344) |  | 5.29 | 695 | 432 | 43 | 0.0 | 173 |
| 16 × 5 ms | on | **1155** (1204, 1155, 1148) | -16% | 6.54 | 923 | 658 | 213 | 6.3 | 460 |
| 16 × 5 ms | sampled-10% | **1312** (1373, 1274, 1312) | -5% | 5.69 | 883 | 506 | 61 | 0.6 | 356 |

**Wider sweep, earlier build** (single coordinator, before the HA rebase and the stateless attempt spans; the VM
was busier, so throughput trials spread widely; `overhead-single-coordinator.json`):

| workers × task | config | tasks/s median (trials) | vs off | overhead ms/task | coordinator CPU µs/task | swarm CPU µs/task | lgtm CPU µs/task | spans/task | job create ms |
|---|---|---|---|---|---|---|---|---|---|
| 4 × 0 ms | off | **2606** (2459, 2606, 5068) |  | 1.53 | 260 | 114 | 34 | 0.0 | 295 |
| 4 × 0 ms | on | **1614** (1667, 971, 1614) | -38% | 2.48 | 438 | 420 | 273 | 5.7 | 662 |
| 4 × 0 ms | sampled-10% | **3746** (3746, 1746, 4110) | +44% | 1.07 | 258 | 168 | 84 | 0.6 | 482 |
| 4 × 0 ms | coordinator-only | **3229** (4618, 1394, 3229) | +24% | 1.24 | 310 | 126 | 100 | 3.0 | 604 |
| 4 × 0 ms | workers-only | **2370** (2734, 1534, 2370) | -9% | 1.69 | 276 | 358 | 182 | 3.0 | 266 |
| 16 × 0 ms | off | **4930** (5245, 3249, 4930) |  | 3.25 | 171 | 112 | 36 | 0.0 | 501 |
| 16 × 0 ms | on | **1407** (1407, 770, 2003) | -71% | 11.36 | 489 | 526 | 615 | 5.1 | 1218 |
| 16 × 0 ms | sampled-10% | **3683** (3683, 1038, 4331) | -25% | 4.34 | 239 | 186 | 76 | 0.6 | 749 |
| 16 × 0 ms | coordinator-only | **3027** (3098, 827, 3027) | -39% | 5.29 | 319 | 134 | 207 | 2.2 | 1066 |
| 16 × 0 ms | workers-only | **1805** (942, 1805, 3127) | -63% | 8.86 | 340 | 508 | 364 | 3.0 | 501 |
| 4 × 5 ms | off | **367** (367, 338, 374) |  | 4.72 | 1242 | 668 | 1008 | 0.0 | 129 |
| 4 × 5 ms | on | **278** (238, 278, 321) | -24% | 7.67 | 1776 | 1087 | 1191 | 6.6 | 306 |
| 4 × 5 ms | sampled-10% | **362** (360, 362, 376) | -1% | 4.85 | 1272 | 701 | 232 | 0.8 | 228 |
| 4 × 5 ms | coordinator-only | **367** (367, 380, 367) | +0% | 4.78 | 1288 | 622 | 382 | 3.2 | 323 |
| 4 × 5 ms | workers-only | **336** (336, 284, 356) | -8% | 5.49 | 1298 | 1078 | 562 | 3.1 | 137 |
| 16 × 5 ms | off | **1048** (1048, 1260, 1005) |  | 7.93 | 516 | 415 | 96 | 0.0 | 183 |
| 16 × 5 ms | on | **495** (324, 495, 537) | -53% | 19.99 | 1283 | 1150 | 1509 | 5.5 | 1011 |
| 16 × 5 ms | sampled-10% | **838** (519, 838, 978) | -20% | 10.93 | 782 | 543 | 468 | 0.6 | 501 |
| 16 × 5 ms | coordinator-only | **1086** (1086, 1151, 888) | +4% | 7.53 | 639 | 375 | 138 | 3.0 | 779 |
| 16 × 5 ms | workers-only | **795** (795, 1070, 773) | -24% | 10.13 | 586 | 722 | 140 | 3.0 | 270 |

What the numbers say:

- **Full tracing is not free on a no-op pipeline.** On the final build, tracing everything costs 44% of
  throughput at 16 loops × 0 ms and 16% at 5 ms tasks, 1–2 ms of extra wall time per task. That is the
  Platformatic effect the report warned about, at a smaller scale because the instrumentation is selective.
  - In CPU: the coordinators pay +110–230 µs per task and the Python swarm +170–230 µs, for 5–6 spans per task.
  - The OTLP backend on the same VM pays +130–170 µs per task on top.
- **Head sampling at 10% brings it within noise:** −4% and −5% (+50–190 µs coordinator CPU per task; the 16 × 5
  ms figure is the noisier one). The remaining cost is the always-on part: creating non-recording spans,
  stamping every row's traceparent, and the extra bytes per lease and report.
- **The worker tier is the more expensive one per span** (earlier sweep, `workers-only` vs `coordinator-only`).
  Python span creation plus the batch exporter thread competing for the GIL with 16 worker loops. The swarm's
  measured `time.sleep(5 ms)` stretches from ~6 to ~8–11 ms under full tracing, a symptom of that contention.
  The coordinator's selective instrumentation costs less per span.
- **Job creation** takes 2–3× longer with tracing on (one producer span and one row update per task, in
  the creating transaction): 354 → 857 ms for 15,000 tasks.
- **At real-model speeds this is noise.** MegaDetector tasks take 300–1,100 ms, and the per-task costs above
  (well under 1 ms of CPU per tier) are below 0.3% of that. Full tracing is the right setting for demos and
  debugging. For the orchestration-ceiling benchmark, use tracing off or `parentbased_traceidratio` 0.01–0.1,
  and say so next to any number.
- **Noise:** the earlier sweep ran while other agents' stacks loaded the VM, so its throughput trials vary up
  to 5×. Some "+" rows (sampled faster than off) are noise, not effects; read its CPU columns, not its
  throughput. The final HA run ran on a quieter VM: trials within ~15%.

## Tests

- `coordinator/test/tracing.test.ts` (11 tests, both claim modes via `npm test`):
  - traceparent stored per task and returned in the lease (same trace, the attempt's derived span);
  - single and batched completes land in each task's trace under the worker's settle span;
  - SIGKILL → requeue → epoch-2 attempt in one trace, with the fenced zombie write;
  - a requeue after a coordinator restart;
  - **another replica closing an attempt it never saw claimed** (context from the row, span ID from
    task/epoch, start = claim time);
  - the classify task in the image's trace;
  - a speculative copy as its own attempt, the loser ending `lost_race` / `already_done`;
  - synthetic jobs stamped;
  - an unsampled image exports nothing;
  - tracing off: no spans and no traceparent anywhere;
  - `GET /metrics/prom` counters, histograms and gauges.
- `worker/tests/test_tracing.py` (16 tests):
  - traceparent extraction: valid, unsampled, malformed, and tracing off;
  - the process span under the lease context, with fetch/infer/upload/settle children;
  - a per-item traceparent in complete-batch;
  - a failed task and its fail report;
  - an unsampled lease exports nothing;
  - tracing off adds nothing to requests.
- All suites pass after rebasing on HA: coordinator 150 (hybrid) / 134 + 16 skipped (postgres), worker 153.

## Gaps

- **Measured on one shared laptop VM**, with a closed-loop fake workload and 3 trials per point. Only the final
  HA run is clean. No open-loop or real-model overhead run, because real models make the cost negligible by
  arithmetic.
- **The otel-lgtm container is a dev backend.** It idles at ~1.0–1.3 GB. Under the full on/off sweep
  (millions of spans) its Tempo was OOM-killed inside the 1.8 GB limit, and it was recreated between runs.
  For long benchmarks, sample or raise `LGTM_MEM_LIMIT`.
- **Classify traceparent race.** A classify task's traceparent is written just after the detect completion
  commits. In `CLAIM_MODE=postgres` a long-poll could, in principle, lease it in that ~1 ms window; that lease
  then carries no traceparent and the classify attempt is untraced (the rest of the image's trace is fine).
  Doing it inside `wb_complete` would mean redefining 007's function.
- **Attempts that end off the hooked paths emit no lease span:**
  - deregister/re-register releases and job cancel;
  - a lease promoted to its speculative copy (P3's promotion path is inside the requeue statement, and
    promoted tasks aren't in `Requeued`);
  - a copy that is dropped or cancelled without reporting;
  - the loser of a speculation race, when the winner completed on another replica and the loser never reports.

  Their tasks' traces are otherwise intact.
- **Tempo search sometimes shows `<root span not yet received>`** for fresh traces, because the PRODUCER span is
  emitted at creation and later spans arrive in other batches. Opening the trace shows the full tree.
- **No Loki log correlation or exemplars.** Worker logs aren't shipped; Prometheus histograms carry no trace
  exemplars.
- **No trace spans for job-level HTTP.** Dashboard requests (`POST /jobs…`) are not HTTP-traced; the
  `create job` span is the link target instead.
- **Worker image build path.** The committed `worker/Dockerfile` adds `requirements-otel.txt` as its own layer.
  Docker Desktop's credential helper hung for every BuildKit build on this machine during this work, so the
  images used for the measurements were built with the classic builder: the P2 worker image plus the same pip
  layer and the new package code. They were not built from the committed Dockerfile.
- **The HA overhead run covers 16 loops only.** The 4-loop points and the per-tier attribution exist only for the
  earlier single-coordinator build.
