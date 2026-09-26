# Decisions

Choices made where the original spec was ambiguous or where the build deviated from it. Newest at the bottom.

1. **Contracts file.** Exact payloads, Redis keys and ports live in `docs/CONTRACTS.md` so the
   coordinator, workers and dashboard could be built in parallel against one spec.
2. **`tasks.queued` column + dispatcher loop.** Backpressure means stage 1 tasks must be held back
   while `queue:classify` is above the high-water mark, so tasks are not pushed to Redis at job
   creation. A 200 ms dispatcher loop moves `PENDING` tasks into Redis (detect tasks only when not
   throttled, and only up to `DETECT_QUEUE_TARGET`). Postgres stays the source of truth; Redis only holds IDs.
   *Updated by #54–55:* tasks are now pushed right after the commit that makes them `PENDING`; the 200 ms tick is only
   a repair sweep, and the detect target scales with the live pool. The `queued` column is what makes both safe.
3. **Duplicate queue entries are tolerated.** Claim-confirm only leases `PENDING` tasks with a
   guarded `UPDATE ... WHERE state='PENDING'`, so an ID that ends up in a queue twice is skipped, not double-processed.
4. **Model version comes from env, not from the worker.** The coordinator needs the detector model
   version at job-creation time for the cache check, before any worker has run. Both sides read
   `DETECTOR_MODEL_VERSION` / `CLASSIFIER_MODEL_VERSION` from the same Compose anchor.
5. **`MODEL_BACKEND=fake`.** Workers have a deterministic fake backend (hash-based detections, fixed delay)
   so coordinator logic can be tested quickly without model weights. Demos and benchmarks use `speciesnet`.
6. **Dashboard behind nginx at `/api`.** One origin for the browser (port 8080) avoids CORS and
   lets the WebSocket share the proxy.
7. **MinIO image `pgsty/minio`.** `minio/minio` and `quay.io/minio/minio` can no longer be pulled anonymously
   (verified 2026-09-24). `pgsty/minio` is a maintained fork with the same CLI and bundled `mc`, so it is a drop-in.
8. **Host ports 15432 / 16379.** Postgres and Redis publish on 15432 and 16379 (override with
   `POSTGRES_HOST_PORT` / `REDIS_HOST_PORT`) so the stack doesn't collide with a local Postgres or Redis.
   Inside the Compose network services still use 5432 / 6379.
9. **Coordinator: worker IDs are `{stage}-{hostname}`.** If the same ID registers again (the worker process restarted), whatever the previous incarnation held is released back to `PENDING` (no attempt cost) and its `processing:` list is drained before it can claim again. `WORKER_HOSTNAME` overrides the hostname part, so two native workers on one Mac don't share an ID.
10. **Coordinator: a heartbeat renews only the leases in the `taskIds` the worker reports.** (Originally it renewed every `LEASED` task of that worker; changed because a task a live worker had given up on — e.g. its report retries ran out during a coordinator outage — would then stay `LEASED` forever and the job could never finish. Now that lease simply expires and the reaper reassigns it.) Since P2 the worker reports every task it holds: running, prefetched, and finished but not yet reported. Renewal uses `SKIP LOCKED`, so it never waits on a completing batch (a lease skipped once is renewed 2 s later), and a heartbeat with no `taskIds` doesn't touch `tasks` at all.
11. **Coordinator: claim-confirm from a non-`ALIVE` worker, or for the other stage's task, leases nothing** (200 with `leases: []`, per the contract's "absent" rule) and RPUSHes the IDs back if they are still waiting. The worker learns it was declared dead from its next heartbeat (410).
12. **Coordinator: graceful releases (deregister, re-register) don't count as attempts** and write a `released` task_event. *Refined by #46:* `/release`, leases lost to our own kills/pauses, and unacknowledged leases (#90) are free too; `attempts` = task errors + lease losses. `released` is now shown in the event log.
13. **Coordinator: categories come from the stored result.** Results are `INSERT ... ON CONFLICT DO NOTHING` and then read back; the image is categorised from the kept row, not the submitted one, so `images.final_category` always agrees with `detection_results`. Partial cache hits are used too: a cached detection with no cached classification marks the image `cache_hit` and creates only the classify task; a detect completion whose photo was already classified (another job) finalises without a classify task.
14. **Coordinator: job completion race.** The transaction that finalises an image locks the job row, then re-checks "no unfinalised images" in a new statement, so two "last" images finishing at once can't leave the job `running`. The reaper also sweeps for finished-but-running jobs each second as a safety net. *Updated by #56:* the lock is now taken only near the end of a job.
15. **Coordinator: restart reconciliation.** On startup the ready queues are cleared and every `PENDING` task is marked `queued=false`, so the dispatcher rebuilds the queues from Postgres. `ALIVE` workers' heartbeats and all leases get a grace period so the coordinator's own downtime isn't read as every worker dying at once. *Updated by #70:* with two replicas this runs when a replica *wins an election*, not at process start, and the grace applies only on a cold start (no heartbeat processed for 2 × `HEARTBEAT_MS`).
16. **Coordinator: job/worker-level events use `task_events` with `task_id` NULL**: `worker_died`, `worker_killed`, `job_done`, `throttled`/`unthrottled`, and one aggregated `cache_hit` row per job (detail `{jobId, count}`) instead of one per image.
17. **Coordinator: recovery time is derived from `task_events`**: for a DEAD worker, the time from its kill (if killed through the API) or DEAD mark until every task `reassigned` away from it has a later `claimed` event. `/metrics.recoveryMs` lists one value per recovered worker (bounded to the 50 most recent deaths of the last hour). The benchmark harness computes the same thing offline; `system.recovery` adds a live, step-by-step record (#43).
18. **Coordinator: chaos mode never kills the last `ALIVE` worker of a stage.** Workers use `restart: "no"`, so killing the last detector or classifier would stall the job forever.
19. **Coordinator: sample selection.** Rows from `labels.csv` (only files that exist) are sorted by name within each `label`, then interleaved by relative position within their label. Any prefix keeps the dataset's mix (about 70% empty), and a 300-image sample is a prefix of the 1,000-image one, so they share cache entries. Without `labels.csv`, filenames are sorted. A `size` above the number available is clamped.
20. **Coordinator: `processed` includes failed images** (`processed` = images with any final category, `failed` is a subset), so the progress bar reaches 100% even when a task runs out of attempts.
21. **Coordinator: WebSocket pacing.** Each client gets at most one message per 100 ms. Pending updates are coalesced per client (latest job summary per job, one worker-list flag, latest throttle state, and an append-only batch of task events) and sent round-robin, so event-log items are batched and never dropped.
22. **Coordinator: malformed results get a 400 and keep the lease.** `/complete` validates the result shape before the guarded UPDATE, so a buggy worker can still `/fail` the task or let the lease expire.
23. **Worker: SpeciesNet API actually used (speciesnet 5.0.5, PyTorch).** Read from the installed package
   source. `speciesnet.DEFAULT_MODEL` is `kaggle:google/speciesnet/pyTorch/v4.0.3a/1`; `ModelInfo(model_name)`
   downloads it with `kagglehub.model_download()` (cache dir = `KAGGLEHUB_CACHE`) and then fetches the
   MegaDetector weights named in its `info.json` (`md_v5a.0.1.pt` from GitHub) into the same folder. The model
   `info.json` says version `4.0.3a`, type `always_crop`. The high-level `SpeciesNet(model_name, components=...)`
   class and its `detect()` / `classify(detections_dict=...)` / `predict()` methods work on file paths and
   thread/process pools, so the workers use the component classes it wraps, one image at a time:
   `SpeciesNetDetector(model_name)` → `.predict(filepath, .preprocess(pil_img))` returns
   `{"detections": [{"category","label","conf","bbox":[x,y,w,h]}]}` (normalised, top-left origin, sorted by conf,
   everything above 0.01); `SpeciesNetClassifier(model_name, device="cpu")` → `.predict(filepath,
   .preprocess(pil_img, bboxes=[BBox(x,y,w,h)]))` returns the top-5 `classifications` (the `always_crop` model crops
   to `bboxes[0]`); `SpeciesNetEnsemble(model_name, geofence=True)` → `.combine(filepaths, classifier_results,
   detector_results, geolocation_results={fp: {"country": "TZA"}}, partial_predictions={})` returns
   `prediction`, `prediction_score`, `prediction_source`, `model_version`. `SpeciesNetDetector` has no device
   argument and picks MPS on a Mac, so `SpeciesNetDetectorModel` moves it back to CPU.
24. **Worker: model version strings.** `DETECTOR_MODEL_VERSION=speciesnet-md_v5a.0.1`,
   `CLASSIFIER_MODEL_VERSION=speciesnet-v4.0.3a` (compose `x-model-env` defaults), pinned by
   `SPECIESNET_MODEL=kaggle:google/speciesnet/pyTorch/v4.0.3a/1` in the worker so a package upgrade can't silently
   change the model. Classifier labels file `...labels.20260609.txt` (2,498 labels), geofence/taxonomy releases
   `20260609`.
25. **Worker: the classifier uses the package's own ensemble + geofence, per image.** `SpeciesNetEnsemble.combine()`
   takes dicts keyed by filepath and works fine with a single image, so every classify task gets the real
   ensemble (detector + classifier heuristics, roll-ups to genus/family, geofence for the lease's `countryCode`,
   default `TZA`). The classifier crops to the *top animal* box (not the top box overall, which could be a person);
   the ensemble still sees all detections. `label` is the ensemble's `prediction`; `raw` keeps `predictionSource`
   and the classifier's top 5. The ensemble can answer `blank` (classifier very sure there is no animal) — in the
   baseline this happened for 62 of 140 false-positive detections and for 0 of 582 real animals, so the
   coordinator could finalise `commonName == "blank"` as `empty` (+3.1 points of empty-vs-animal accuracy).
26. **Worker: common names.** `wildebeest_worker.labels.common_name()` collapses SpeciesNet taxonomy strings to the
   ground-truth vocabulary by genus/family (any zebra, *Connochaetes* → wildebeest, *Panthera leo* → lion,
   Elephantidae → elephant, Giraffidae → giraffe, *Eudorcas*/*Nanger*/*Gazella* → gazelle, *Syncerus* → buffalo,
   *Crocuta*/*Hyaena*/*Parahyaena*/Hyaenidae → hyena, *Phacochoerus* → warthog, *Aepyceros* → impala); special
   labels become `blank`, `animal`, `human`, `vehicle`, `unknown`; anything else keeps SpeciesNet's own common
   name minus a trailing " species". The full taxonomy string is stored as `label`.
27. **Worker: detections below 0.1 are dropped** before reporting (SpeciesNet keeps everything above 0.01). Nothing
   downstream looks below `ANIMAL_CONF_THRESHOLD=0.2`, and it keeps `detection_results` rows small.
28. **Worker: weights are baked into the image** (`KAGGLEHUB_CACHE=/opt/models/kagglehub`, downloaded in a
   `RUN` step), not into the `model-cache` volume. Containers start with no network access to Kaggle/GitHub
   (verified with `--network none`) and scaling to N workers can't race N concurrent downloads into one volume.
   The image is 3.75 GB (CPU-only torch from the PyTorch CPU wheel index; yolov5 pulls in OpenCV, pandas, scipy).
   The `/models` volume is left mounted and only used as `TORCH_HOME`.
29. **Worker: `TORCH_NUM_THREADS=2` per worker** (compose anchor). Docker Desktop has 8 vCPUs; a full-width
   intra-op pool per worker makes several workers fight over the same cores. With 2 threads, ~4 busy workers
   saturate the CPU, which is where the benchmark curve should flatten. A 1- vs 2-thread recheck on a loaded VM was
   inconclusive (2 threads tied or won at 2 workers), so 2 stays the default (#81).
30. **Worker: batch claims.** On SIGTERM or 410 the remaining unstarted leases are simply not processed:
   deregister (or the reaper) returns them to `PENDING`. On 410 the main loop re-registers (worker IDs are
   `{stage}-{hostname}`, so it gets the same ID back; the coordinator releases anything the old incarnation held).
   `/workers/register` retries for up to 120 s at startup. *Superseded in part:* claims now move up to k IDs in one
   `MULTI` of `LMOVE`s with k sized online (#59), blocking in `BLMOVE` for 250 ms only when the queue is empty (#65);
   reports retry for a full lease length (#49), a heartbeat retries once, and a worker told 410 still posts its
   in-flight result once so the fence is visible (#50).
31. **Worker memory, latency and capacity (measured at 1280 px, the default at the time; #35 made 640 px the
   default, which drops detector peak RSS to ~1.3 GB and latency to 0.66 s with #41).** In the linux/arm64 image on Docker Desktop (Apple M2,
   8 vCPU, 8 GB), `python -m wildebeest_worker.measure`, `TORCH_NUM_THREADS=2`:
   | | Detector (MegaDetector v5a, 1280 px) | Classifier (SpeciesNet v4.0.3a + ensemble) |
   |---|---|---|
   | RSS after model load | 1.08–1.10 GB | 0.81 GB |
   | RSS steady state (after inference) | 1.26–1.35 GB | 0.88 GB |
   | Peak RSS | 1.52–1.60 GB | 0.98 GB |
   | Model load | 2–5 s | 3–4 s |
   | Model latency per image | 3.8–4.1 s median on a quiet host, 6–7 s while the host was busy | 0.6–1.1 s |
   Native macOS on the same M2 is much faster (detector 1.1 s, classifier 0.2 s with Apple Accelerate); the Linux
   arm64 CPU wheels are ~1.7× slower on convolutions and ~4× on matmul (measured), and containers get no GPU.
   Compose limits: `DETECTOR_MEM_LIMIT=2000m`, `CLASSIFIER_MEM_LIMIT=1300m` (peak + ~25% headroom).
   Capacity: infra (postgres, redis, minio, coordinator, dashboard) uses ~0.35 GB, and the Docker VM needs
   ~0.5 GB, leaving ~7 GB on an 8 GB Docker Desktop: **3 detectors + 2 classifiers** fit at peak (6.8 GB);
   4 + 1 fits at steady state (6.3 GB) but can OOM if peaks line up. On a **16 GB laptop** with Docker given
   ~12 GB: **6 detectors + 1 classifier** or 5 + 2. CPU runs out first anyway: 8 vCPUs / 2 threads ≈ 4 busy
   workers. The 8-detector benchmark point needs ~15 GB and won't fit on either machine at 1280 px.
32. **Worker: `DETECTOR_IMG_SIZE` knob (default 1280 at the time; 640 since #35).** SpeciesNet has no argument for detector input size, so
   the worker sets the `SpeciesNetDetector.IMG_SIZE` class constant before loading. At 640 px the detector is
   ~2.5–2.7× faster in the container (2.4 s vs 6.7 s under the same load) and peak RSS drops to 1.3 GB. On 400
   baseline images, empty-vs-animal accuracy was 93.8% at 640 vs 91.5% at 1280, recall 95.5% vs 96.4%
   (1 fewer animal found out of 111). Recommended for the live demo; set `DETECTOR_MODEL_VERSION` to e.g.
   `speciesnet-md_v5a.0.1-640` with it so the two sizes never share cache entries.
33. **Dataset: Snapshot Serengeti season 1 from LILA.** `scripts/download_sample.py` downloads only the ~19 MB
   per-season COCO Camera Traps metadata (`SnapshotSerengetiS01.json.zip`, URL from lila.science), writes a
   compact CSV index to `data/metadata/`, and fetches single images from the `snapshotserengeti-unzipped`
   Azure mirror with a 16-thread pool. Snapshot Serengeti labels are per sequence, so it uses only sequences with
   exactly one label and one random frame per sequence (seed 42): 1,400 empty + 60 each of zebra, wildebeest,
   lion (female+male), elephant, giraffe, gazelle (Thomson's+Grant's), buffalo, hyena (spotted+striped),
   warthog, impala. Images are the 2048×1536 originals (1.2 GB total), saved as `S1_<site>_<roll>_PICTnnnn.jpg`.
34. **Classifier "blank" overrules the detector.** When stage 2 labels the animal crop `blank`, the image is
   finalised as `empty` (species fields cleared; the classification row is still stored for the cache). In the
   2,000-image baseline this raised empty-vs-animal accuracy from 92.1% to 95.2% and lost no real animals.
35. **Detector input size 640 by default.** In a Linux container MegaDetector takes ~4 s/image at 1280 px on
   the M2; 640 px is ~2.5× faster with equal accuracy on a 400-image check (93.8% vs 91.5% empty-vs-animal,
   one fewer animal found out of 111). The size is part of `DETECTOR_MODEL_VERSION` (`…-640`), so switching
   sizes never reuses cached results from the other size.
36. **Retries jump the queue.** The first chaos run measured ~2 min from a kill until the dead worker's task
   was re-claimed, because the reassigned task was RPUSHed behind ~50 queued tasks. Tasks that were already started
   (`started_at` set) are now LPUSHed to the head of their queue and bypass both `DETECT_QUEUE_TARGET` and
   backpressure, since that work was admitted before its worker died. Processing-list drains also go to the head.
   *Updated by #45:* the recovery path itself pushes them right after the requeue commits, not the next dispatcher tick.
37. **Default demo scale is 3 detectors + 1 classifier.** In the first chaos run (4 detectors + 2 classifiers on an
   8 GB Docker VM that also runs other projects' containers) one detector was OOM-killed at its 2 GB limit. The system
   recovered it like any other death, but the demo default stays within memory.
38. **Backpressure test uses the fake model backend.** Phase 4 asks for backpressure with 6 detectors and
   1 classifier; six real detectors (~1.1 GB each) don't fit this 8 GB Docker VM. Backpressure is coordinator logic
   (queue depth → dispatcher), so `cache-backpressure.test.ts` runs the workers with `MODEL_BACKEND=fake` and separate
   fake model versions. The pipeline, chaos and cache tests use the real models.
39. **Benchmark sweep defaults to 1, 2, 3, 4 detectors.** The PRD sweep (1, 2, 4, 6, 8) needs ~15 GB for Docker.
   On this 8-vCPU / 8 GB VM, 4 detectors × 2 torch threads already saturate the CPU, so the flattening point is visible.
   `BENCH_DETECTORS=1,2,4,6,8` runs the full sweep on a bigger machine.
40. **Redis data loss is detected and repaired without a restart.** Whenever the queues are built from Postgres the
   coordinator sets `wildebeest:queues-built`. Each dispatcher tick checks it; if it is gone (Redis restarted without
   persistence, or was flushed), the dispatcher runs the same rebuild as startup: clear the ready queues and mark every
   `PENDING` task unqueued so it is pushed again. Duplicate IDs this may create are harmless.
41. **CPU inference tuning: channels_last, nothing else.** Measured in the worker image (linux/arm64 on an M2,
   2 torch threads, median over 12 Serengeti images, same images for every variant):

   | Variant | MegaDetector v5a @ 640 | SpeciesNet classifier |
   | --- | --- | --- |
   | fp32 baseline | 0.81 s | 0.37 s |
   | Conv+BN `fuse()` | 0.81 s | – |
   | **channels_last weights** | **0.66 s** (identical detections) | **0.31 s** (identical labels) |
   | oneDNN bf16 fast-math (`DNNL_DEFAULT_FPMATH_MODE=BF16`) | 1.13–1.27 s | – |
   | ONNX Runtime, opset 17, 2 intra-op threads (± arm64 bf16 GEMM) | 0.97–0.99 s | – |
   | 1 / 4 torch threads | 0.92 s / 0.61 s | – |

   Only channels_last is adopted (`worker/wildebeest_worker/tuning.py`). The VM does expose bf16/i8mm, but oneDNN's
   bf16 path was slower for these convolutions. Two threads per worker stays the default: 4 threads buys ~8%, so on
   8 vCPUs more 2-thread workers beat fewer 4-thread ones. Not tried: MegaDetector v1000 "cedar" (YOLOv9c, ~half the
   FLOPs, GPL) — it needs threshold retuning and a new accuracy baseline, and its author reports only ~2× over MDv5a
   at 1280, which is what 640 px already buys; a smaller model's main win here would be memory, i.e. more workers.
42. **Renamed ForgeGrid → Wildebeest.** The herd keeps moving when one animal falls, which is the fault-tolerance
   story, and the sample data is Serengeti. Renamed everywhere (package `wildebeest_worker`, database, bucket, Redis
   keys, images). The original product spec (not in this repo) used the old name.

## Improvement program (2026-09-25)

Entries 43 onward come from the improvement program driven by
[the research report](../reports/Wildebeest%20distributed%20systems%20improvements.md). Each phase has a detailed record
in [docs/decisions/](decisions) with the full measurements, tests and known gaps; these entries are the summary.
All measurements are on one Apple M2 laptop, Docker Desktop 8 vCPU / 8 GB, usually shared with other workloads.

### P1: recovery fast path and retry hygiene ([p1-coordinator.md](decisions/p1-coordinator.md))

43. **Docker `die`/`oom` events are the primary failure detector; heartbeats are the backstop.** The event, a
   `docker ps` reconciliation and the heartbeat timeout all call one path (`recovery.ts`): a guarded
   `UPDATE workers … WHERE status = 'ALIVE'`, requeue, drain, push. The guard makes the racers race-safe, and a false
   positive is harmless because fencing absorbs it. *Why:* a positive death signal beats a faster timeout, and one
   recovery implementation is easier to keep correct than three. *Effect:* kill → all lost work re-claimed p50 218 ms,
   p95 336 ms over 15 kills on a quiet VM, against 5.1–7.2 s heartbeat-only (before-benchmark: 5.6 s p50 over 22 kills).
44. **The death watch only convicts what it can prove.** Events are filtered to our Compose project three ways
   (server-side label filter, client-side check, container ID registered by the worker). On reconnect it replays with
   `since=<last event>` and reconciles against `docker ps -a`, convicting only on a positive "exited", never on absence.
   A replayed event can't kill a newer incarnation (`registered_at <= event time`). `timeNano` is read as a BigInt
   because it exceeds 2^53 (found by a flaky test). *Why:* several stacks share the Docker host.
45. **Recovered work is pushed by the recovery path itself**, `LPUSH` to the queue head right after the requeue
   commits, instead of waiting for the next dispatcher tick. Supersedes the mechanism of #36.
46. **Attempt accounting separates "the task failed" from "we lost it".** `attempts` (still the counter checked against
   `MAX_ATTEMPTS`) = task errors + lease losses. `releases` counts free returns: `/release`, deregister, re-register,
   and leases lost to *our own* SIGKILL or pause. Kill/pause stamps are written before the Docker call (the `die` event
   can beat the HTTP response) and rolled back if Docker refuses. The first version keyed on `killed_at >= started_at`
   and a live run caught the race (the worker claimed its next task during the few hundred ms the kill took); it now
   keys on the incarnation (`killed_at >= registered_at`). *Why:* chaos mode was finalising healthy images as `failed`.
47. **The reaper knows when it is the slow one** (`StallMeter`, Lifeguard's self-awareness). It measures how late each
   tick starts and adds recent stalls to every heartbeat timeout and lease. *Effect:* seen live while other stacks
   saturated the VM: ticks ran 1–2 s late and grace grew to ~5 s instead of convicting healthy workers.
48. **Errors are classified before they are reported.** Infrastructure errors (S3 unreachable or 5xx, timeouts) →
   `/release` (free) and a worker-side circuit breaker that stops claiming and probes `HEAD bucket` with full-jitter
   backoff while heartbeats continue. Bad input (undecodable image, missing object) → `/fail` `nonRetryable`, straight to
   the DLQ (`GET /dlq`, `POST /dlq/:id/redrive`). Anything else → `/fail` with coordinator-side full-jitter backoff
   (`not_before`, 500 ms base, 30 s cap). Releases get no backoff: the breaker is the backoff. *Effect:* MinIO stopped
   for 15 s mid-job → 0 failed images, 0 attempts charged (before: 7–15 healthy images `failed` per 30 s outage).
49. **Report retry budget = one lease length** (full jitter; connection errors and 502/503/504 only), up from ~3 s.
   *Why:* any coordinator restart longer than 3 s used to drop finished results and force a rerun.
50. **A zombie reports once.** A worker that got `410 WORKER_DEAD` with a task in flight still posts that result once
   (it is fenced with 409 and recorded), and a `heartbeat_refused` event is written once per death. *Why:* correct
   either way, but without it the pause demo usually ended with no visible fence.
51. **Telemetry lives in memory; Postgres keeps the per-task record.** Hot-path recording is an array push, summaries are
   built at most every 400 ms and shared by `GET /system` and every WebSocket client. `dispatchWaitMs` is measured from
   when a task became *eligible* (not held back by the queue target or backpressure), so backlog counts as queue wait
   rather than as orchestration overhead.
52. **Synthetic jobs are one SQL statement** (`generate_series` → images + tasks), for the ceiling benchmark: 100k tasks
   in 3.5 s. The fake backend never touches MinIO for `synthetic/` keys.
53. **The live invariant check is incremental**: duplicate completions only over events newer than the last check,
   stuck leases over the LEASED partial indexes. ~85 ms for a 20k-image running job. (Bounded further by #89.)

### P2: hot path ([p2-hotpath.md](decisions/p2-hotpath.md))

54. **Push after commit; the task row is the outbox.** Whoever commits a transaction that makes tasks `PENDING` (job
   creation, a detect completion's classify task, requeue, release, redrive, the end of a retry backoff) pushes them
   right after the commit, via a guarded `UPDATE … WHERE queued = false RETURNING` so exactly one pusher wins. The
   200 ms tick is a repair sweep for a crash between COMMIT and push. No outbox table and no `LISTEN/NOTIFY` (a
   transaction that notifies takes a global lock through commit). `DISPATCH_MODE=tick` restores the old dispatcher for
   A/B runs. *Effect:* the ~250 tasks/s refill cap is gone: 240 → 1,328 tasks/s at 16 loops × 0 ms before any
   batching; dispatch wait p50 2–3 ms → 0.6 ms.
55. **The detect queue target scales with the pool**: `max(DETECT_QUEUE_MIN = 8, 2 × Σ live detect workers' claim
   windows)`, reported in heartbeats and refilled by a single-flight, coalescing top-up on every claim.
   `DETECT_QUEUE_TARGET > 0` pins it (tests use 5). Backpressure still gates admission.
56. **The job row is locked only near the end of a job** (`wb_finish_job`, migration 006). A finalisation counts the
   job's unfinalised images without a lock (bounded, over a partial index) and locks the job row only when at most
   "leases in flight + live workers" remain. If both "last" images slip past, the reaper's 1 s sweep marks the job done.
   *Why:* every finalisation used to serialise on one row, held through the commit's fsync.
57. **Claim is one statement (a data-modifying CTE); complete is one statement (PL/pgSQL `wb_complete`).** Claim is a
   straight pipeline, which reads well as a CTE; complete branches, which reads well as code. Validation stays in Node
   before the statement. The categorisation rule now exists in `results.ts` and `wb_categorize`; a test runs both over
   the same cases. *Effect:* 5 → 1 and ~12 → 1 Postgres round trips; `completeMs` p50 1.2–11.5 ms → 1.0–1.7 ms.
58. **Batched completes with per-row fencing, and complete-and-claim-next.** `POST /tasks/complete-batch` runs
   `wb_complete` over many items: per-row epoch check, one stale row never fails the batch, accepted rows share one
   commit. `next: k` claims the worker's next leases in the same request; the coordinator `LPOP`s the IDs itself (it is
   the consumer, and a crash leaves them `PENDING` for the rebuild). Lock order: tasks by id, results by sha256, jobs by
   id; heartbeat renewal uses `SKIP LOCKED`. *Why safe:* at-least-once means a buffer lost in a crash only costs reruns.
59. **Claim batch k = ⌈RTT ÷ service time⌉, measured online** (EWMAs, bounded by `CLAIM_BATCH_SIZE`..`MAX_CLAIM_BATCH`
   = 1..16). A synchronous worker's orchestration cost per task is RTT ÷ k, so this keeps it at or below one service
   time. Real models get k = 1 (the report's consistency check); 0 ms tasks run to 16. The batch move is one
   `MULTI`/`EXEC` of `LMOVE`s rather than a Lua script (same atomicity, and fakeredis in the unit tests has no Lua).
   *Effect:* 872 / 1,740 / 5,886 tasks/s at 1 / 4 / 16 loops × 0 ms, 3.8× / 7.3× / 24.5× over the tick dispatcher.
60. **Prefetch depth 1.** A worker holds the running lease plus the next one, whose image downloads on a background
   thread. *Cost:* a killed worker loses two tasks, and a job's tail can wait on a busy worker. *Effect:* `fetchMs` p50
   0 ms on 400 real images. `PREFETCH=0` turns it off; the benchmark swarm never enables it.
61. **Queue hygiene.** `tasks`: `fillfactor = 70`, autovacuum at 1% / analyze at 2%, and the `lease_expires_at` index
   dropped so a lease renewal is a HOT update (the reaper finds expired leases through the LEASED partial index).
   `workers`: `fillfactor = 50`. Heartbeats skip `tasks` when the worker holds nothing; dashboard reads are cached and
   shared. **Day-partitioned `task_events`: not done**: it is append-only with no retention policy, so there is nothing
   to drop and no dead tuples to vacuum. Revisit when a retention requirement exists.
62. **`CLAIM_MODE=postgres` is kept as a flagged A/B, hybrid stays the default.** Postgres mode drops the Redis queue:
   workers long-poll one `UPDATE … WHERE id IN (… FOR UPDATE SKIP LOCKED LIMIT k) RETURNING` with the same epochs and
   events, woken in-process by the same post-commit hooks. *Effect:* within noise of hybrid at every point (5,886 vs
   6,005 tasks/s at 16 × 0 ms; 1,321 vs 1,372 at 16 × 5 ms). The coordinator's test suite runs in both modes. Workers
   learn the mode from every heartbeat response (found live: after a restart in the other mode, workers kept
   `BLMOVE`-ing an empty queue).

### P3: straggler speculation ([p3-speculation.md](decisions/p3-speculation.md))

63. **A task can have two valid attempts: the lease plus one shadowing copy.** A second claim used to bump
   `lease_epoch` and fence the first holder out, so "speculation" would have been pre-emption. Now the `tasks` row keeps
   describing the lease exactly as before, and a copy is a `task_attempts` row with its own epoch (reserved in
   `tasks.spec_epoch`; epochs stay unique per task), valid only while `lease_epoch = shadow_epoch`. Anything that ends
   the lease invalidates the copy without touching it, so no existing transition had to learn about copies.
   `wb_complete` accepts the lease or a valid copy and flips the task to `SUCCEEDED` in the statement that locked it:
   **first commit wins**, by the same row-lock argument as before. The loser gets **409 `ALREADY_DONE`** (a pre-P3
   worker already discards on any 409), which is not a fencing event.
64. **Promotion and cancel.** If a speculated task's lease is lost while its copy is healthy, the copy *becomes* the
   lease: nothing is requeued or charged, and the old epoch is fenced like any lost lease. The heartbeat response
   carries `cancel` for speculated tasks the worker no longer holds a valid attempt on. A waiting lease is dropped; a
   running model call can't be interrupted, so its result is dropped when it returns. *Effect:* killing a straggler whose
   copy was running handed the task over 33 ms after the kill request, with nothing requeued.
65. **Policy: speculate only at the tail.** Every 250 ms per stage: ≥ 5 completions of baseline, no `PENDING` task left,
   an idle worker, and a task older than `max(1 s, 3 × stage p50)` that was never speculated. The copy goes to the
   *fastest* idle worker through a per-worker `spec:{workerId}` list (the shared queue would hand it to whoever blocked
   first). Workers slower than 3 × stage p50 are on probation and get no copies. Idle workers block in `BLMOVE` for
   250 ms instead of 1 s (`IDLE_WAIT_MS`), because offer → lease was taking 750–1,000 ms. *Effect:* 200-image job with
   one 10× throttled detector: 28.8 → 23.9 s median makespan (−17%), 7 of 8 copies won, 0 duplicate results, 0 stale
   rejections.
66. **A single `complete` that is rejected (STALE_LEASE / ALREADY_DONE) no longer claims `next`.** Before, the claimed
   leases were dropped with the 409 and sat `LEASED` until they expired and were charged (a P2 bug speculation would
   have made frequent).

### H: coordinator high availability ([h-ha.md](decisions/h-ha.md))

67. **Leader election is a Postgres lease row with an explicit term** (River's design: 5 s TTL, renewed every 1 s,
   Postgres `now()` only). Acquire is `UPDATE … SET term = term + 1 … WHERE expires_at <= now()`, so at most one
   candidate wins a term. Rejected: Redis `SET NX` (wall-clock TTLs, non-persistent Redis, and the term would have to be
   checked in Postgres anyway) and advisory locks (a dropped session silently transfers leadership while the old process
   keeps sweeping). *Effect, 72 failover runs:* leader SIGKILL 5.34 s p50 / 5.66 s p95; `docker pause` 5.45 s p50;
   SIGTERM 0.18 s p50 (it resigns); `pg_terminate_backend` causes no failover. **0 writes accepted from a stale term,
   100 fenced attempts refused**, every job finished with 0 failed images, no worker declared dead.
68. **The fence lives in the database access layer.** Leader-only code runs inside `withFence(term)`
   (`AsyncLocalStorage`); while a fence is in context, `query()`/`tx()` open every transaction with
   `wb_leader_guard(term)`, sent together with `BEGIN`. The guard locks the leader row `FOR SHARE`, so a takeover (an
   `UPDATE` of that row) waits for every transaction that already passed it: terms never overlap. It also stamps
   `task_events.leader_term`, which turns "no stale-term write" into a query. *Why:* no `if (isLeader)` at call sites,
   and code added later can't forget the check.
69. **Step-down rules, and no local "my lease probably expired" check.** A renewal that matches nothing → step down; a
   renewal that errors → step down once `TTL − one tick` passes without success (before any follower can acquire); a
   guard rejection → step down and record `leader_fenced`. A woken leader's first statements go to the database and are
   refused there: the resource decides. Election runs on its own connection with `lock_timeout` and
   `statement_timeout`, so a saturated pool can't starve renewals.
70. **Leader-only vs every replica.** Leader: repair sweep and throttle decision, reaper, death watch, speculation,
   chaos, replica-loss detection, queued-row audit, reconciliation. Every replica: the whole HTTP API, including
   push-after-commit (guarded per row). Reconciliation runs **on winning an election**, not at process start
   (supersedes #15); the worker grace is only for a cold start. Replicas heartbeat into `coordinator_nodes`; the leader
   rebuilds the queues when one goes silent, since it may have died holding `LPOP`ed IDs.
71. **Shared state: Postgres for decisions, Redis pub/sub for notifications.** The throttle flag and chaos switch live
   in the leader row. Event-log rows, change notifications, long-poll wake-ups and **telemetry records** go over
   `wildebeest:cluster:<db>`, batched every 50 ms. Replicating records rather than a snapshot gives every replica the
   whole cluster's `/system` (speculation's service-time history included). Pub/sub is at-most-once, which costs a
   dashboard refresh or a timing sample, never state.
72. **HAProxy in front of the replicas**, with active `/healthz` checks every second and `shutdown-sessions` on
   mark-down. *Why:* a paused container still accepts TCP, so only an active check makes a frozen replica leave the
   rotation. Workers needed no change: their retry budgets already cover a replica dying mid-request.
73. **Two bugs the failover campaign found.** (a) A session the server ends mid-transaction was delivered as an `error`
   event on a checked-out client and crashed the process; `tx()` now fails the transaction and discards the connection.
   (b) A pop can be lost without anyone dying (the connection running the lease statement terminated after `LPOP`), so
   the leader audits rows queued > 5 s against a `MULTI` snapshot of every Redis list and re-dispatches missing ones.
74. **Speculation runs on the leader**, inside the fence, and its per-worker service times are replicated over the bus.

### O: observability ([o-observability.md](decisions/o-observability.md))

75. **The trace context lives on the task row** (`tasks.traceparent`, migration 009), written in the creating
   transaction. Redis carries only IDs and process memory is not shared between replicas; the row is what every attempt,
   the requeue and every replica see. **One trace per image**, rooted at a PRODUCER span per task (linked to a
   `create job` span), so traces stay small and head sampling decides per image.
76. **An attempt is a coordinator span with a derived ID, emitted when the attempt ends.** A SIGKILLed worker never
   exports the span it has open, and with HA a claim and its complete usually land on different replicas. Span IDs are
   derived from `sha256(taskId/epoch)`, so whichever replica sees the end (complete, fail, requeue, lost race) emits the
   span from `tasks.started_at`. Batched completes are attributed per item via a per-item `traceparent`.
77. **Selective instrumentation and head sampling at creation.** Only http, pg and ioredis, loaded through an ESM
   preload; only requests carrying a `traceparent` start traces. The sampling decision is made once at creation and
   stored in the context flags, so the worker and every replica follow it. Tracing off loads nothing.
78. **Metrics are Prometheus (`prom-client`), not OTLP.** Counters and histograms are fed from the points that already
   feed `/system`; gauges are computed at scrape time; worker metrics ride on the heartbeat. *Effect (tracing overhead,
   HA stack, 16 loops):* full tracing −44% throughput at 0 ms tasks and −16% at 5 ms; 10% head sampling −4% / −5%. At
   real-model speeds this is below 0.3%. Benchmarks run with tracing off or sampled, and say so.

### D-native: heterogeneous workers ([d-native.md](decisions/d-native.md))

79. **One worker runs natively on macOS with PyTorch MPS and joins the same pool.** Docker Desktop can't give a Linux
   container the Mac GPU, and real-model throughput is CPU-bound. `DEVICE=auto|cpu|mps|cuda`; containers default to
   `cpu`; an explicit device that isn't available raises at startup (a "GPU worker" never silently runs on CPU).
   *Effect:* bare MegaDetector 0.12 s/img on MPS (5.2–5.5× a quiet container); pool throughput 2.7–3.4× in three paired
   runs, with the MPS worker taking 91–96% of detections and the bottleneck moving to the classifier (a tandem queue).
80. **MPS results share the CPU model version strings.** Detections match to 3.6e-7 and classifier top-5 scores to
   1e-4, far below the 4 decimals stored and the gap already accepted between oneDNN and Accelerate. Anything that
   really changes numerics (fp16, another input size, TensorRT) gets a new version. `channels_last` is per backend:
   kept for container CPU, off on MPS, and off for the classifier on native macOS CPU (NHWC made it ~9× slower there).
   fp16 on MPS (+20%) not adopted: it would need its own version and an accuracy check.
81. **`TORCH_NUM_THREADS` stays 2.** The 1- vs 2-thread recheck on a loaded VM was inconclusive (2 threads tied or won
   at 2 workers); the possible gain is ≤ 13% of a ~1.7 img/s ceiling, while one MPS worker adds 4–5× that ceiling.

### E: dashboard ([e-dashboard.md](decisions/e-dashboard.md))

82. **The dashboard shows the system, not just the results.** The hero is the real topology (dispatcher →
   backpressure valve → queues with real watermarks → worker lanes → result store). Lease packets are spawned only by
   diffs of each worker's held task IDs, so every animation is a real claim, completion or requeue. Beside it: where a
   task's time goes, the latest failure → recovery timeline to scale, the last fencing rejection, invariants, the
   leader and failover line, and speculation. It was built against a mock that implements the contract, and degrades
   to a v1 coordinator.

### B: measurement and correctness ([b-bench.md](decisions/b-bench.md))

83. **The ceiling harness drives real worker loops, not a model of them.** `swarm.py` runs N ordinary
   `runtime.Worker`s as threads, 8 per container, so client CPU and the GIL stay out of the measurement. The fake
   handler returns no detections, so every task finalises exactly one image and "tasks/s" is one number, not two queues
   in series. Overhead subtracts the *measured* handler time (`sleep(5 ms)` takes 6.4 ms in a container).
84. **Steady state and hygiene.** Throughput is the 10th–90th percentile of completions; 3 trials per point, trials as
   the outer loop; tables truncated and vacuumed between points; neighbour containers' CPU recorded per point and a
   point repeated when they were busy. Every point is checked against Little's law and fit with the USL (bootstrap CIs).
   *Effect:* before: flat at ~240 tasks/s, Little's law 0.998, USL α = 0.72 with R² 0.73 (a hard rate cap is not a
   contention curve, which is itself the finding); per-task overhead 7.9 ms.
85. **Open-loop latency is measured from the intended arrival** (fixed schedule, a 10 ms ticker submitting from a
   48-thread pool), so there is no coordinated omission. *Effect:* before, at 50% load, the median task waited 137 ms
   between its row committing and being claimed: the 200 ms dispatcher tick.
86. **The invariant checker reads the coordinator's own history** (`tasks`, `task_events`, `images`, result tables)
   and checks I1–I7: single success, one result row, fenced completion, increasing epochs, no stuck lease (sampled
   live), terminal images, job finishes. Completions without an epoch are "unverifiable", not silently passed.
   Speculative copies count as claims. It is described as a Jepsen-style harness, never "Jepsen-tested".
87. **The fault matrix injects without touching the coordinator**: SIGKILL, 20 s `docker pause`, toxiproxy latency and
   `reset_peer` on worker→coordinator and worker→Redis, 30 s MinIO stop, Redis `FLUSHALL` + SIGKILL, leader
   coordinator SIGKILL, from a seeded deck. The runner never retries a POST after a timeout (an early version created a
   duplicate job that way). *Effect, before:* 12 runs, 60 faults, 0 safety violations, 52 late results fenced, 4
   liveness violations (#88).

### F: fix-ups ([f-fixups.md](decisions/f-fixups.md))

88. **Task IDs orphaned in a live worker's processing list are recovered on both sides.** A connection reset during
   the claim `LMOVE`/`BLMOVE` moves IDs the worker never learns about (redis-py's default retry makes it worse by moving
   more). The worker's claim `MULTI` now starts with `LRANGE processing:{me}` and confirms leftovers in the same round
   trip. The leader's audit is the backstop: an ID still unconfirmed in an ALIVE worker's list after 3 heartbeats is
   taken back with a fenced `UPDATE`, then `LREM` and, only if that removed it, `LPUSH`. *Effect:* the before matrix's
   4 I7 violations (every run with a Redis reset) → 3 of 3 reset runs finished with 0 violations.
89. **Every live invariant query is bounded**: its own transaction, `statement_timeout = 2 s`, and `lostImages` with
   `enable_nestloop = off`. A check planned while `tasks` was freshly truncated picked a nested loop with a sequential
   inner scan, then executed against the new job: K × K rows (once for 24 minutes), burning a core per replica and
   holding back vacuum. *Effect:* 16 loops × 0 ms mean 881 → 2,968 tasks/s in a paired sweep; 62 ms for 75k unfinished
   images.
90. **A lease lost with its response is not charged.** When a claim commits on a replica that dies before answering (or
   the reply is reset), the worker never learns of the lease. If it expires on an ALIVE worker that heartbeated after the
   claim but never renewed it, the loss costs a release, not an attempt (`charged: false, unacknowledged: true`). It
   still waits out `LEASE_MS` before rerunning. *Effect:* 51 such expiries in one reset run, all uncharged, 0 failed.
91. **A completion retry whose first send committed is idempotent.** `wb_complete` (migration 010) returns `duplicate`
   for an item whose task is `SUCCEEDED` with the item's epoch, credited to the sending worker; Node answers it like an
   accepted write without recording it twice. Before, it got `409 STALE_LEASE` and was counted as fencing. Retried
   `/fail` and `/release` are unchanged.
92. **Smaller fixes.** Recovery records open before the push (a fast claim used to leave them open forever);
   speculation service-time samples expire after 60 s, and probation needs ≥ 5 worker and ≥ 20 stage samples (a 5 ms job
   had put every worker of a later 1 s job on probation); `GET /benchmarks` serves `summary.json`; `GET /config` carries
   `grafanaUrl` from `GRAFANA_PUBLIC_URL`.
