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
