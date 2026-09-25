# Decisions

Choices made where the PRD was ambiguous or where the build deviated from it. Newest at the bottom.

1. **Contracts file.** Exact payloads, Redis keys and ports live in `docs/CONTRACTS.md` so the
   coordinator, workers and dashboard could be built in parallel against one spec.
2. **`tasks.queued` column + dispatcher loop.** Backpressure means stage 1 tasks must be held back
   while `queue:classify` is above the high-water mark, so tasks are not pushed to Redis at job
   creation. A 200 ms dispatcher loop moves `PENDING` tasks into Redis (detect tasks only when not
   throttled, and only up to `DETECT_QUEUE_TARGET`). Postgres stays the source of truth; Redis only holds IDs.
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
9. **Coordinator: worker IDs are `{stage}-{hostname}`.** If the same ID registers again (the worker process restarted), whatever the previous incarnation held is released back to `PENDING` (no attempt cost) and its `processing:` list is drained before it can claim again.
10. **Coordinator: a heartbeat renews only the leases in the `taskIds` the worker reports.** (Originally it renewed every `LEASED` task of that worker; changed because a task a live worker had given up on — e.g. its `/complete` retries ran out during a coordinator outage — would then stay `LEASED` forever and the job could never finish. Now that lease simply expires and the reaper reassigns it.)
11. **Coordinator: claim-confirm from a non-`ALIVE` worker, or for the other stage's task, leases nothing** (200 with `leases: []`, per the contract's "absent" rule) and RPUSHes the IDs back if they are still waiting. The worker learns it was declared dead from its next heartbeat (410).
12. **Coordinator: graceful releases (deregister, re-register) don't count as attempts** and write a `released` task_event, which the event log doesn't show. Only lease expiry, worker death and `/fail` count.
13. **Coordinator: categories come from the stored result.** Results are `INSERT ... ON CONFLICT DO NOTHING` and then read back; the image is categorised from the kept row, not the submitted one, so `images.final_category` always agrees with `detection_results`. Partial cache hits are used too: a cached detection with no cached classification marks the image `cache_hit` and creates only the classify task; a detect completion whose photo was already classified (another job) finalises without a classify task.
14. **Coordinator: job completion race.** The transaction that finalises an image locks the job row, then re-checks "no unfinalised images" in a new statement, so two "last" images finishing at once can't leave the job `running`. The reaper also sweeps for finished-but-running jobs each second as a safety net.
15. **Coordinator: restart reconciliation.** On startup the ready queues are cleared and every `PENDING` task is marked `queued=false`, so the dispatcher rebuilds the queues from Postgres. `ALIVE` workers' heartbeats and all leases get a grace period so the coordinator's own downtime isn't read as every worker dying at once.
16. **Coordinator: job/worker-level events use `task_events` with `task_id` NULL**: `worker_died`, `worker_killed`, `job_done`, `throttled`/`unthrottled`, and one aggregated `cache_hit` row per job (detail `{jobId, count}`) instead of one per image.
17. **Coordinator: recovery time is derived from `task_events`**: for a DEAD worker, the time from its kill (if killed through the API) or DEAD mark until every task `reassigned` away from it has a later `claimed` event. `/metrics.recoveryMs` lists one value per recovered worker.
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
   saturate the CPU, which is where the benchmark curve should flatten.
30. **Worker: batch claims.** The first ID is claimed with `BLMOVE ... 1` (1 s block); extra IDs up to
   `claimBatchSize` use `BLMOVE ... 0.05` so a worker never waits to fill a batch. On SIGTERM or 410 the
   remaining unstarted leases are simply not processed: deregister (or the reaper) returns them to `PENDING`.
   On 410 the in-flight result is discarded without calling `/complete`, and the main loop re-registers (worker IDs
   are `{stage}-{hostname}`, so it gets the same ID back; the coordinator releases anything the old incarnation held). `/complete`, `/fail` and heartbeats retry up to 5 times on connection errors only;
   `/workers/register` retries for up to 120 s at startup.
31. **Worker memory, latency and capacity (measured).** In the linux/arm64 image on Docker Desktop (Apple M2,
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
32. **Worker: `DETECTOR_IMG_SIZE` knob (default 1280).** SpeciesNet has no argument for detector input size, so
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
   keys, images). `docs/PRD.md` keeps the original name because it is a copy of the source spec.
