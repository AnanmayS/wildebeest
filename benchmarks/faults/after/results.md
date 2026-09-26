# Fault matrix — current code

Generated 2026-09-26T01:57:08Z on Apple M2, Docker Desktop 8 vCPU / 8 GB. Seed 42, 1200-image fake-backend sample job per run (3 detectors + 2 classifiers, 200 ms fake model), workers routed through toxiproxy. Checker: tests/invariants/checker.py.

| Runs | Faults injected | Violations |
|---|---|---|
| 6 | 30 | **0** |

Faults by type: coordinator 5, kill 4, latency 4, minio 4, pause 4, redis 5, reset 4.

| Run | Faults | Job | Finished (s after last fault) | stale_rejected (fenced) | reassigned | lease_expired | Failed images | Re-executions | Violations |
|---|---|---|---|---|---|---|---|---|---|
| 1 | redis, kill, coordinator, latency, minio | done | 44.6 | 0 | 0 | 0 | 0 | 11 | 0 |
| 2 | reset, pause, kill, coordinator, reset | done | 25.3 | 1 | 4 | 0 | 0 | 4 | 0 |
| 3 | redis, pause, latency, minio, pause | done | 18.3 | 3 | 4 | 0 | 0 | 12 | 0 |
| 4 | latency, redis, coordinator, minio, reset | done | 56.9 | 0 | 0 | 0 | 0 | 8 | 0 |
| 5 | kill, pause, latency, coordinator, minio | done | 34.5 | 2 | 4 | 0 | 0 | 19 | 0 |
| 6 | redis, reset, kill, redis, coordinator | done | 46.6 | 0 | 2 | 0 | 0 | 2 | 0 |

Columns: *stale_rejected* = late results fenced off by the epoch check (409); *re-executions* = claims beyond one per finished task (work redone after a fault); *failed images* = images finalised `failed` after running out of attempts (terminal, so not a violation, but each is healthy work lost to an infrastructure fault).
