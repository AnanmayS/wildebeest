# Fault matrix — wb-before

Generated 2026-09-25T22:44:40Z on Apple M2, Docker Desktop 8 vCPU / 8 GB. Seed 42, 1200-image fake-backend sample job per run (3 detectors + 2 classifiers, 200 ms fake model), workers routed through toxiproxy. Checker: tests/invariants/checker.py.

| Runs | Faults injected | Violations |
|---|---|---|
| 12 | 60 | **4** |

Faults by type: coordinator 9, kill 9, latency 8, minio 8, pause 9, redis 8, reset 9.

Violations by invariant: I7 job_finishes: 4.

| Run | Faults | Job | Finished (s after last fault) | stale_rejected (fenced) | reassigned | lease_expired | Failed images | Re-executions | Violations |
|---|---|---|---|---|---|---|---|---|---|
| 1 | redis, kill, coordinator, latency, minio | done | 92.3 | 0 | 0 | 1 | 8 | 22 | 0 |
| 2 | reset, pause, kill, coordinator, reset | running | 300.6 | 1 | 2 | 0 | 0 | 2 | 1 |
| 3 | redis, pause, latency, minio, pause | done | 41.7 | 1 | 2 | 0 | 7 | 18 | 0 |
| 4 | latency, redis, coordinator, minio, reset | running | 300.6 | 0 | 0 | 0 | 7 | 19 | 1 |
| 5 | kill, pause, latency, coordinator, minio | done | 74.6 | 1 | 2 | 0 | 12 | 30 | 0 |
| 6 | redis, reset, kill, redis, coordinator | done | 89.5 | 0 | 1 | 0 | 0 | 1 | 0 |
| 7 | kill, latency, reset, pause, minio | done | 63.6 | 21 | 3 | 2 | 12 | 29 | 0 |
| 8 | pause, kill, coordinator, redis, reset | running | 300.7 | 1 | 2 | 0 | 0 | 2 | 1 |
| 9 | minio, latency, minio, coordinator, pause | done | 60.4 | 1 | 1 | 0 | 15 | 42 | 0 |
| 10 | redis, kill, latency, reset, redis | done | 65.9 | 12 | 1 | 4 | 0 | 5 | 0 |
| 11 | coordinator, kill, minio, pause, latency | done | 48.7 | 1 | 2 | 0 | 10 | 27 | 0 |
| 12 | reset, coordinator, reset, kill, pause | running | 300.0 | 13 | 2 | 4 | 0 | 6 | 1 |

## Violations

| Run | Invariant | Subject | Detail |
|---|---|---|---|
| 2 | I7 job_finishes | `7b536fd2-c4ec-4d67-9cf8-f852d82794be` | job running 300 s after the last fault; 5 unfinished: a3b6b50b detect PENDING queued=True epoch=0 → processing:detect-211f5671d2ec (ALIVE); e9def4bf detect PENDING queued=True epoch=0 → processing:detect-cada78a400fa (ALIVE); e3e345d4 detect PENDING queued=True epoch=0 → processing:detect-1d924359063d (ALIVE); fab4f31f classify PENDING queued=True epoch=0 → processing:classify-a9ff90d816b2 (ALIVE); 59f813ba classify PENDING queued=True epoch=0 → processing:classify-ae319b68cf7a (ALIVE) |
| 4 | I7 job_finishes | `4331fbbf-0057-4473-8fcb-01e64be581c2` | job running 300 s after the last fault; 4 unfinished: 4d001a76 classify PENDING queued=True epoch=0 → processing:classify-d3ec79630e21 (ALIVE); 917c89c4 detect PENDING queued=True epoch=0 → processing:detect-f0ab184814a2 (ALIVE); ed1f05bf detect PENDING queued=True epoch=0 → processing:detect-b2e270f94186 (ALIVE); 50b2105e detect PENDING queued=True epoch=0 → processing:detect-e1950353d56f (ALIVE) |
| 8 | I7 job_finishes | `eb771f45-b689-4e52-8e5f-d31cdc2853cd` | job running 300 s after the last fault; 5 unfinished: 444137da detect PENDING queued=True epoch=0 → processing:detect-06b0d1643e78 (ALIVE); 3b47fa61 detect PENDING queued=True epoch=0 → processing:detect-96b0097ab3cb (ALIVE); b2197c04 detect PENDING queued=True epoch=0 → processing:detect-981c1c06d5b3 (ALIVE); 4e15a837 classify PENDING queued=True epoch=0 → processing:classify-271f10916080 (ALIVE); 87b2db7f classify PENDING queued=True epoch=0 → processing:classify-fc6f745d1fd9 (ALIVE) |
| 12 | I7 job_finishes | `b0675621-0f26-42be-967b-9ae3297448a7` | job running 300 s after the last fault; 1 unfinished: 8de7ac86 detect PENDING queued=True epoch=0 → processing:detect-a8ed5c895cd8 (ALIVE) |

Columns: *stale_rejected* = late results fenced off by the epoch check (409); *re-executions* = claims beyond one per finished task (work redone after a fault); *failed images* = images finalised `failed` after running out of attempts (terminal, so not a violation, but each is healthy work lost to an infrastructure fault).
