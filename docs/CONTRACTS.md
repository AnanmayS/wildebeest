# Wildebeest interface contracts

This file pins down the details the PRD (docs/PRD.md) leaves open, so the coordinator,
workers, and dashboard can be built in parallel and still fit together. If you must
change a contract, change it here and note it in docs/DECISIONS.md.

## Services and ports (docker-compose)

| Service     | Image/build     | Port (host) | Notes |
|-------------|-----------------|-------------|-------|
| postgres    | postgres:16     | 15432       | db `wildebeest`, user/pass `wildebeest`/`wildebeest` |
| redis       | redis:7         | 16379       | host ports moved off 5432/6379, which are commonly taken |
| minio       | pgsty/minio     | 9000, 9001  | user/pass `minioadmin`/`minioadmin`, bucket `wildebeest` (coordinator creates it at startup) |
| coordinator | ./coordinator   | 3000        | mounts `/var/run/docker.sock` and `./data/sample:/data/sample:ro` |
| detector    | ./worker        | none        | `WORKER_STAGE=detect`, scalable, `restart: "no"` |
| classifier  | ./worker        | none        | `WORKER_STAGE=classify`, scalable, `restart: "no"` |
| dashboard   | ./dashboard     | 8080        | nginx serving the Vite build; proxies `/api/*` → `coordinator:3000/*` (prefix stripped) and `/api/events` (websocket) → `coordinator:3000/events` |

Workers reach the coordinator at `COORDINATOR_URL=http://coordinator:3000`, Redis at
`REDIS_URL=redis://redis:6379`, MinIO at `S3_ENDPOINT=http://minio:9000`.

## Environment variables (all services read what they need)

PRD section 13 defaults plus:

```
DATABASE_URL=postgres://wildebeest:wildebeest@postgres:5432/wildebeest
REDIS_URL=redis://redis:6379
S3_ENDPOINT=http://minio:9000
S3_PUBLIC_ENDPOINT=http://localhost:9000   # used only to sign URLs the browser loads
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_BUCKET=wildebeest
SAMPLE_DIR=/data/sample                     # contains *.jpg + labels.csv
DETECT_QUEUE_TARGET=50                      # dispatcher keeps queue:detect at most this long
MODEL_BACKEND=speciesnet                    # worker: speciesnet | fake (fake = deterministic, for fast tests)
FAKE_MODEL_DELAY_MS=300                     # worker, fake backend only
```

## Model version strings

- The coordinator needs the detector model version *before* any worker runs (for the cache
  check at job creation). Rule: the coordinator reads `DETECTOR_MODEL_VERSION` and
  `CLASSIFIER_MODEL_VERSION` env vars; the worker reports the same env var as its `modelVersion`,
  so the two always agree. Compose sets them once in the `x-model-env` anchor
  (defaults `speciesnet-md_v5a.0.1` / `speciesnet-v4.0.3a`, matching speciesnet 5.0.5's default model; the worker agent should update the defaults
  in docker-compose.yml to match the installed speciesnet package/model). When running with
  `MODEL_BACKEND=fake`, also set `DETECTOR_MODEL_VERSION=fake-detector-v1` and
  `CLASSIFIER_MODEL_VERSION=fake-classifier-v1` so fake results never pollute the real cache.
- The coordinator stores results under its own env version (it trusts env, and ignores a mismatched
  worker-reported version except to log a warning).

## Object storage keys (bucket `wildebeest`)

- `images/{sha256}.jpg` — original upload (coordinator writes)
- `crops/{sha256}_{classifierModelVersion}.jpg` — classifier crop thumbnail (worker writes;
  sanitise `/` in the version to `_` for the key)

## Redis keys

- `queue:detect`, `queue:classify` — lists of task IDs (uuid strings). Coordinator `RPUSH`es.
- `processing:{workerId}` — list; worker moves an ID here with
  `BLMOVE queue:{stage} processing:{workerId} LEFT RIGHT 1` (1s timeout, loop).
- `worker:{id}:alive` — string, TTL `3 × HEARTBEAT_MS`, set by the coordinator on register/heartbeat.
- `wildebeest:throttled` — "1" while backpressure is engaged (informational; coordinator is the source).

Workers only ever do BLMOVE (and nothing else) against Redis. All other Redis mutation is the coordinator's.
When a claim-confirm, complete, or fail request is handled, the coordinator `LREM`s that task ID from
`processing:{workerId}`.

## Task dispatch (coordinator)

Postgres `tasks` gets one extra column: `queued boolean not null default false` (true = its ID is
currently in a Redis ready queue or a processing list).

A dispatcher loop (every 200 ms) is the only thing that pushes to ready queues:
- classify: every `PENDING` classify task with `queued=false` → set `queued=true`, `RPUSH queue:classify`.
- detect: unless throttled, top up `queue:detect` to `DETECT_QUEUE_TARGET` from `PENDING` detect
  tasks with `queued=false` (oldest first), using `FOR UPDATE SKIP LOCKED`.
- throttle: engage when `LLEN queue:classify > CLASSIFY_QUEUE_HIGH_WATER`, release when
  `< CLASSIFY_QUEUE_LOW_WATER`. Emit a `throttle` ws event and a log line on each change.

Reaper (every 1 s):
1. Workers with `status='ALIVE'` and `last_heartbeat_at < now() - WORKER_TIMEOUT_MS` → `DEAD`
   (emit `worker_update`, event log "worker died").
2. `LEASED` tasks whose `lease_expires_at < now()` or whose worker is `DEAD` → if `attempts + 1 < MAX_ATTEMPTS`
   (attempts counts failed attempts; incremented here) → `PENDING`, `queued=false`, `worker_id=null`,
   task_event `lease_expired` or `reassigned`; else `FAILED` and the image is finalised with
   `final_category='failed'`.
3. For each DEAD worker's `processing:{id}` list: IDs whose task is still `PENDING` with `queued=true`
   get `RPUSH`ed back to their ready queue; then `DEL` the list.

Duplicate IDs in a queue are harmless: claim-confirm only leases tasks in state `PENDING`.

## Worker ↔ coordinator HTTP API (JSON)

`POST /workers/register`
```json
{ "stage": "detect", "hostname": "a1b2c3", "containerId": "a1b2c3d4e5f6" }
→ 200 { "workerId": "detect-a1b2c3", "config": { "heartbeatMs": 2000, "leaseMs": 15000, "claimBatchSize": 1, "animalConfThreshold": 0.2 } }
```
`containerId`: inside Docker, the container hostname is the short container ID; send `socket.gethostname()` for both.

`POST /workers/:id/heartbeat`
```json
{ "taskIds": ["uuid"], "metrics": { "tasksDone": 12, "avgLatencyMs": 950, "rssMb": 812, "currentImageKey": "images/ab…jpg" } }
→ 200 { "ok": true }
→ 410 { "error": "WORKER_DEAD" }   // worker was declared dead: it must drop in-flight work and re-register
```

`POST /tasks/claim-confirm`
```json
{ "workerId": "detect-a1b2c3", "taskIds": ["uuid"] }
→ 200 { "leases": [ { "taskId": "uuid", "leaseEpoch": 3, "stage": "detect", "imageKey": "images/{sha}.jpg",
                      "sha256": "…", "countryCode": "TZA",
                      "detections": null } ] }
```
For classify leases `detections` is the stage 1 detections array (below). Tasks that could not be leased are
simply absent from `leases` (the worker skips them).

`POST /tasks/:id/complete`
```json
{ "workerId": "…", "leaseEpoch": 3, "result": <DetectResult | ClassifyResult> }
→ 200 { "ok": true }
→ 409 { "error": "STALE_LEASE" }
```

DetectResult:
```json
{ "modelVersion": "…", "latencyMs": 900,
  "detections": [ { "label": "animal", "conf": 0.93, "bbox": [x, y, w, h] } ] }
```
`label` ∈ `animal | human | vehicle`; `bbox` normalised 0–1, top-left origin (MegaDetector convention).

ClassifyResult:
```json
{ "modelVersion": "…", "latencyMs": 1100, "label": "<full speciesnet taxonomy string>",
  "commonName": "plains zebra", "confidence": 0.87, "cropKey": "crops/…jpg", "raw": { … } }
```

`POST /tasks/:id/fail` `{ "workerId", "leaseEpoch", "error": "message" }` → 200, or 409 STALE_LEASE.
Fail counts as an attempt: back to `PENDING` or `FAILED` like the reaper.

`POST /workers/:id/deregister` → 200. Marks worker `STOPPED` (not DEAD, no reassignment event storm);
any tasks still leased to it return to PENDING.

Final categorisation after stage 1 (coordinator): if any detection has `label=animal` and
`conf >= ANIMAL_CONF_THRESHOLD` → enqueue classify (unique `(image_id, stage)`, `ON CONFLICT DO NOTHING`),
image stays unfinalised. Else if any `human` ≥ threshold → `human`; else any `vehicle` ≥ threshold →
`vehicle`; else `empty`. After stage 2 → `animal` with species fields.

## Dashboard API (via nginx at `/api`)

`POST /jobs` multipart field `files` (many) + optional `countryCode` → `{ "jobId" }`
`POST /jobs/sample` `{ "size": 1000, "countryCode": "TZA" }` → `{ "jobId" }` (size ≤ images available)
`POST /jobs/:id/cancel` → `{ "ok": true }` (false if it wasn't running): PENDING tasks become `CANCELLED`, job status `cancelled`.
`GET /jobs` → `{ "jobs": [JobSummary] }` newest first (dashboard uses it to find the active job)
`GET /jobs/:id` → JobSummary:
```json
{ "id": "…", "name": "sample-1000", "status": "running|done", "createdAt": "…", "finishedAt": null,
  "total": 1000, "processed": 412, "failed": 0, "cacheHits": 0,
  "categories": { "empty": 280, "animal": 120, "human": 8, "vehicle": 4, "failed": 0 },
  "species": [ { "commonName": "zebra", "count": 41 } ],
  "elapsedMs": 53000, "throughput": 7.8,
  "pending": { "detect": 500, "classify": 30 },
  "impact": { "emptyPct": 70.0, "hoursSaved": 0.58 } }
```
`throughput` = images finalised per second over the last 5 s. `hoursSaved` = empty count × HUMAN_REVIEW_SECONDS_PER_IMAGE / 3600.

`GET /jobs/:id/images?category=animal&species=zebra&page=1&pageSize=48` →
```json
{ "images": [ { "id", "url", "cropUrl", "category", "commonName", "speciesLabel", "confidence",
                "detections": [ { "label", "conf", "bbox" } ], "cacheHit": false } ],
  "total": 120, "page": 1 }
```
URLs are presigned against `S3_PUBLIC_ENDPOINT`.

`GET /workers` → `{ "workers": [ { "id", "stage", "status": "ALIVE|DEAD|STOPPED", "state": "idle|busy|dead",
  "containerId", "tasksCompleted", "currentTaskIds": [], "currentImageUrl": null, "avgLatencyMs", "rssMb",
  "lastHeartbeatAt", "reassignedCount": 0 } ] }` (DEAD/STOPPED workers from the last 10 min included).
`POST /workers/:id/kill` → `{ "ok": true }` (SIGKILL via dockerode)
`POST /chaos` `{ "enabled": true, "killEverySec": 20 }` → `{ "enabled", "killEverySec" }`; `GET /chaos` same shape.
`GET /metrics` → queue depths, throttled flag, worker counts, per-job stats, recovery times
(ms from a worker's DEAD mark — and from its kill time when killed via the API — until all its reassigned tasks were
re-claimed by live workers), p50/p95 per-image latency (image created → finalised).
`GET /healthz` → `{ "ok": true }`
`POST /admin/clear-cache` → `{ "ok": true, "deleted": { "detection": n, "classification": n } }` — deletes all rows from
`detection_results` and `classification_results` (benchmark + integration tests use it; images/tasks are kept).

## WebSocket `/events` (dashboard connects to `/api/events`)

Server → client JSON messages, at most ~10/s per client (coalesce `job_progress` and `worker_update`,
never drop `task_event`s the event log shows — batch them instead):

```json
{ "type": "job_progress", "job": JobSummary }
{ "type": "worker_update", "workers": [ …same as GET /workers… ] }
{ "type": "task_events", "events": [ { "id": 1, "at": "…", "type": "reassigned", "taskId": "…", "workerId": "…", "message": "detect-a1b2 died; task 3f2a… reassigned" } ] }
{ "type": "throttle", "throttled": true, "classifyQueue": 512 }
```
Event log types shown: `worker_died`, `reassigned`, `lease_expired`, `stale_rejected`, `cache_hit`
(one aggregated message per job, e.g. "300 cache hits"), `throttled`, `unthrottled`, `failed`, `job_done`.

## Additions (after the dashboard build)

- **Stable image URLs.** Presigned URLs for a given object key are cached in memory by the coordinator for
  ~50 min (sign with a 1 h expiry) so repeated `worker_update`s carry the same URL and the browser doesn't refetch.
- `GET /events?limit=200` → `{ "events": [ …same shape as the task_events message items… ] }`, newest first,
  so the event log is populated after a reload/reconnect.
- **Kill events.** `POST /workers/:id/kill` and chaos kills write a `worker_killed` task_event (task_id NULL,
  worker_id set, message "SIGKILL sent to detect-a1b2 (chaos)") and push it immediately.
- **JobSummary** gains `"sampleSize": 1000 | null`, `"throttled": boolean`, `"classifyQueue": number`.
- **Workers** gain `"registeredAt"` and `"diedAt"` (null unless DEAD); list sorted by stage, then registeredAt.
- `GET /config` → `{ "animalConfThreshold": 0.2, "heartbeatMs": 2000, "workerTimeoutMs": 6000, "leaseMs": 15000,
  "humanReviewSecondsPerImage": 3 }`.
- `GET /metrics` shape (minimum): `{ "throttled": bool, "queues": { "detect": n, "classify": n },
  "workers": { "alive": n, "dead": n }, "recoveryMs": [n…], "latency": { "p50": ms, "p95": ms } }`.
- `/jobs/:id/images` ordering: most recently finalised first; `pageSize` default 48, max 200.

---

# v2 additions (improvement program, 2026-09-25)

Owners: the coordinator agents implement these; the dashboard and benchmark agents consume them. Coordinator agents
may refine *request/response details of the worker protocol*, but must keep every shape the dashboard reads
(`system` snapshot, events, `/benchmarks`) exactly as written here, and must note any change in this file.

## Worker protocol v2

- Every `complete` (single or batch) may carry `timings`: `{ "claimMs": n, "fetchMs": n, "inferMs": n, "uploadMs": n }`
  measured by the worker. The coordinator stores them with the task (`tasks.timings jsonb`) for the waterfall.
- `POST /tasks/:id/complete` accepts optional `"next": k` → response `{ "ok": true, "leases": [Lease…] }`: the
  coordinator claims up to k more tasks of the worker's stage for it in the same request (complete-and-claim-next).
- `POST /tasks/complete-batch` `{ "workerId", "items": [{ "taskId", "leaseEpoch", "result", "timings" }], "next": k }`
  → `{ "results": [{ "taskId", "status": "ok" | "stale" | "invalid" }], "leases": [Lease…] }`. One stale row never fails the batch.
- `POST /tasks/:id/release` `{ "workerId", "leaseEpoch", "reason" }` → back to PENDING **without** spending an attempt
  (infrastructure errors: MinIO/S3 down, timeouts). Task errors still use `/fail`.
- `CLAIM_MODE=hybrid|postgres` (coordinator env; default hybrid). In `postgres` mode there is no Redis ready queue:
  workers call `POST /tasks/claim` `{ "workerId", "stage", "max": k, "waitMs": 1000 }` (long-poll) → `{ "leases": [...] }`,
  implemented as one `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED LIMIT k) RETURNING`.
- `POST /jobs/synthetic` `{ "count": n, "stage"?: "detect" }` → `{ "jobId" }`: n images with random sha256 and
  `object_key = "synthetic/<sha>"`, no MinIO object. Fake-backend workers skip the image download for `synthetic/` keys.
  Used by the orchestration-ceiling benchmark (0–50 ms fake tasks, up to ~1M tasks).
- `POST /workers/:id/pause` `{ "ms": 20000 }` → `docker pause` the container for ms, then `docker unpause`
  (a SIGSTOP-style freeze; demonstrates fencing on the dashboard). Events `worker_paused` / `worker_resumed`.
- Worker death is detected from Docker `die`/`oom` events (filtered to this Compose project's label
  `com.docker.compose.project`) as well as heartbeats. Event `worker_died` gains `detail.via`:
  `"docker_event" | "heartbeat"` and `detail.detectMs` (ms from kill request or container death to detection).

## `system` snapshot — the dashboard's main data

`GET /system` and a WebSocket message `{ "type": "system", "system": SystemSnapshot }` pushed ~2×/s.

```json
{
  "at": "2026-09-25T18:00:00.000Z",
  "config": { "claimMode": "hybrid", "leaseMs": 15000, "heartbeatMs": 2000, "workerTimeoutMs": 6000,
              "detectQueueTarget": 50, "classifyHighWater": 500, "classifyLowWater": 200, "modelBackend": "speciesnet" },
  "dispatcher": { "mode": "push", "pushedLast10s": 120, "repairSweepsLast10s": 50, "lastSweepRepaired": 0 },
  "queues": { "detect": 12, "classify": 3, "throttled": false },
  "stages": {
    "detect":   { "workersAlive": 3, "inFlight": 3, "completedPerSec": 1.1, "p50ServiceMs": 780 },
    "classify": { "workersAlive": 1, "inFlight": 1, "completedPerSec": 0.3, "p50ServiceMs": 330 }
  },
  "leases": [ { "taskId": "…", "workerId": "detect-ab12", "stage": "detect", "epoch": 2, "ageMs": 640,
                "attempt": 2, "imageUrl": "…" } ],
  "timings": { "windowSec": 60, "samples": 214,
               "p50": { "dispatchWaitMs": 3, "queueWaitMs": 410, "claimMs": 6, "fetchMs": 18, "inferMs": 690,
                        "uploadMs": 0, "completeMs": 9, "totalMs": 1130 },
               "p95": { "…same keys…": 0 },
               "overheadPct": 3.4 },
  "recovery": [ { "workerId": "detect-ab12", "killedAt": "…", "detectedAt": "…", "via": "docker_event",
                  "requeuedAt": "…", "reclaimedAt": "…", "tasks": 1, "totalMs": 820 } ],
  "fencing": { "staleRejected": 3, "last": { "taskId": "…", "workerId": "…", "epoch": 3, "currentEpoch": 4, "at": "…" } },
  "invariants": { "checkedAt": "…", "duplicateResults": 0, "stuckLeases": 0, "lostImages": 0, "ok": true },
  "throughput": [ { "t": "…", "detect": 1.2, "classify": 0.4, "images": 1.2 } ],
  "cache": { "hitsLast10m": 300, "hitRatePct": 50.0 },
  "speculation": { "launched": 0, "won": 0, "wasted": 0 },
  "leader": { "id": "coord-1", "term": 7, "since": "…" }
}
```
- `timings` p50/p95 come from the last `windowSec` of completed tasks: `dispatchWaitMs` = pushed − became PENDING,
  `queueWaitMs` = claimed − pushed, `claimMs`/`fetchMs`/`inferMs`/`uploadMs` from the worker's `timings`,
  `completeMs` = coordinator's handling time of the complete request. `overheadPct` = (claim + complete + dispatchWait)
  ÷ total service time (excluding queue wait), as a percentage.
- `leases` is capped at 40 (oldest first). `throughput` is the last 120 s at 1 s resolution.
- `invariants` is a cheap live check (≤ every 5 s) over the active job(s): result rows per image > 1, LEASED tasks on
  non-ALIVE workers past timeout + 2 reaper ticks, images with no task and no final category.
- `leader` is null until coordinator HA exists. `speculation` zeros until speculation exists.

## `GET /benchmarks`

Serves `benchmarks/summary.json` (mounted read-only into the coordinator at `/benchmarks`), or 404 if absent.
Produced by the benchmark harness:
```json
{ "generatedAt": "…", "machine": "Apple M2, Docker Desktop 8 vCPU / 8 GB",
  "ceiling": { "taskMs": [0, 5, 50], "series": [ { "taskMs": 0, "points": [ { "workers": 1, "throughput": 180.2, "p50Ms": 4.1, "p99Ms": 9.8 } ] } ],
               "usl": { "taskMs": 0, "lambda": 190.0, "alpha": 0.02, "beta": 0.0004 } },
  "real": { "points": [ { "detectors": 1, "classifiers": 1, "throughput": 1.21 } ] },
  "recovery": { "before": { "p50Ms": 6400, "p95Ms": 8700, "samples": 1 }, "after": { "p50Ms": 800, "p95Ms": 1400, "samples": 20 } },
  "overhead": { "before": { "perTaskMs": 46 }, "after": { "perTaskMs": 8 } },
  "faults": { "runs": 12, "faultsInjected": 60, "violations": 0 } }
```
