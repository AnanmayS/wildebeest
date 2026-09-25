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
