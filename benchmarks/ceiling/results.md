# Orchestration ceiling: before vs after

Each run directory has its own `results.md` with every point, CI, USL fit, open-loop table and recovery breakdown. Reproduce with `bench/run.py` (see docs/decisions/b-bench.md).

![before vs after](ceiling_compare.png)

### Tasks/s at 0 ms tasks (mean ± 95% CI)

| Worker loops | before |
|---|---|
| 1 | 175.2 ± 64.3 |
| 2 | 235.6 ± 19.4 |
| 4 | 237.1 ± 11.9 |
| 8 | 242.7 ± 4.3 |
| 16 | 240.0 ± 2.6 |
| 32 | 240.6 ± 2.7 |
| 64 | 235.8 ± 6.3 |

### Tasks/s at 5 ms tasks (mean ± 95% CI)

| Worker loops | before |
|---|---|
| 1 | 76.1 ± 16.8 |
| 2 | 139.5 ± 93.3 |
| 4 | 239.8 ± 11.3 |
| 8 | 240.4 ± 1.7 |
| 16 | 235.1 ± 24.1 |
| 32 | 237.4 ± 4.0 |
| 64 | 231.1 ± 17.4 |

### Tasks/s at 50 ms tasks (mean ± 95% CI)

| Worker loops | before |
|---|---|
| 1 | 15.9 ± 2.3 |
| 2 | 31.8 ± 1.6 |
| 4 | 63.8 ± 2.1 |
| 8 | 128.6 ± 14.1 |
| 16 | 238.3 ± 17.0 |
| 32 | 232.5 ± 26.5 |
| 64 | 207.0 ± 82.2 |

### Headline numbers

| | before |
|---|---|
| Peak tasks/s, 0 ms tasks | 242.7 (N=8) |
| Peak tasks/s, 5 ms tasks | 240.4 (N=8) |
| Peak tasks/s, 50 ms tasks | 238.3 (N=16) |
| USL fit, 0 ms | λ=187.0, α=0.7200, β=0.001270 |
| Per-task overhead (ms, N=1) | 7.88 ± 2.27 |
| Recovery p50 / p95 / max (ms) | 5,562 / 6,659 / 6,736 (n=22) |
| Little's law ratio (mean) | 0.998 |
| Task source | sample |

