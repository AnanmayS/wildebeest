# Benchmark results

1000-image Snapshot Serengeti sample, cache cleared before each run. Latency is per-image processing time (detect + classify task time), excluding queue wait.

![Throughput vs workers](throughput.png)

| Detectors | Classifiers | Total (s) | Throughput (img/s) | Speedup | p50 (ms) | p95 (ms) | Peak detector RSS (MiB) | Peak classifier RSS (MiB) | Min host RAM free |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 346.5 | 2.89 | 1× | 338 | 683 | 84 | 86 | 42% |
| 2 | 1 | 172.5 | 5.8 | 2.01× | 335 | 676 | 79 | 80 | 45% |
| 4 | 1 | 95.2 | 10.5 | 3.63× | 330 | 667 | 79 | 80 | 47% |
| 8 | 3 | 43.2 | 23.16 | 8.01× | 322 | 662 | 79 | 78 | 47% |

- **Cache-hit rerun:** 1000/1000 images served from the content-hash cache in 0.10 s.
- **Recovery after SIGKILL:** 1 in-flight task(s) of a killed detector were reclaimed by live workers 5.8 s after the kill (bounded by WORKER_TIMEOUT_MS = 6 s plus the 1 s reaper tick).
