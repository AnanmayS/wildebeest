# Open-loop latency at the same offered load (before vs after)

The harness's open-loop suite offers load as a fraction of each version's own closed-loop maximum,
so "50%" meant 120 tasks/s for the old code and 832 tasks/s for the new one. To compare latency at
equal load, the new code was also driven at the old code's rates with the same shape: 8 worker loops
(fake backend, 5 ms tasks), one synthetic job of 8 images submitted per arrival, 20 s per rate.
Script: `bench/openloop_matched.py <jobs/s>` against the default stack
(`MODEL_BACKEND=fake FAKE_MODEL_DELAY_MS=5 docker compose up -d --scale detector=8 --scale classifier=2`).
Latency = image finalised − job created (Postgres clock); submission itself took 5 ms p50 at these rates.

| Offered (tasks/s) | Before p50 / p99 (ms) | After p50 / p95 (ms) |
|---|---|---|
| 120 | 156 / 394 | 17 / 45 |
| 192 | 195 / 519 | 15 / 42 |
| 240 (the old code's ceiling) | 3,517 / 4,095 | 15 / 41 |

At the harness's higher "50%/80%" points for the new code (832–1,331 tasks/s of 8-image jobs) the
median task waited ~1 s in Postgres for room in the deliberately short detect queue: admission
control holding a backlog, not a claim-path cost. Offered rates there were set from a noisy
closed-loop maximum (one 2,745 tasks/s trial). See results.md in this directory.
