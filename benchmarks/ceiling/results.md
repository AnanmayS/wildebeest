# Orchestration ceiling: before vs after

Each run directory has its own `results.md` with every point, CI, USL fit, open-loop table and recovery breakdown. Reproduce with `bench/run.py` (see docs/decisions/b-bench.md).

![before vs after](ceiling_compare.png)

### Tasks/s at 0 ms tasks (mean ± 95% CI)

| Worker loops | before | after |
|---|---|---|
| 1 | 175.2 ± 64.3 | 1,320.2 ± 2,133.5 |
| 2 | 235.6 ± 19.4 | 3,398.1 ± 3,743.6 |
| 4 | 237.1 ± 11.9 | 4,087.1 ± 3,121.9 |
| 8 | 242.7 ± 4.3 | 3,147.2 ± 4,890.7 |
| 16 | 240.0 ± 2.6 | 4,480.7 ± 539.4 |
| 32 | 240.6 ± 2.7 | 5,438.4 ± 5,749.2 |
| 64 | 235.8 ± 6.3 | – |

### Tasks/s at 5 ms tasks (mean ± 95% CI)

| Worker loops | before | after |
|---|---|---|
| 1 | 76.1 ± 16.8 | 89.5 ± 2.7 |
| 2 | 139.5 ± 93.3 | 193.2 ± 13.5 |
| 4 | 239.8 ± 11.3 | 377.9 ± 13.9 |
| 8 | 240.4 ± 1.7 | 769.9 ± 9.5 |
| 16 | 235.1 ± 24.1 | 1,500.7 ± 139.6 |
| 32 | 237.4 ± 4.0 | 1,664.1 ± 2,420.7 |
| 64 | 231.1 ± 17.4 | – |

### Tasks/s at 50 ms tasks (mean ± 95% CI)

| Worker loops | before | after |
|---|---|---|
| 1 | 15.9 ± 2.3 | 16.3 ± 0.5 |
| 2 | 31.8 ± 1.6 | 33.0 ± 0.6 |
| 4 | 63.8 ± 2.1 | 65.6 ± 1.4 |
| 8 | 128.6 ± 14.1 | 135.5 ± 1.5 |
| 16 | 238.3 ± 17.0 | 274.8 ± 4.6 |
| 32 | 232.5 ± 26.5 | 499.2 ± 42.5 |
| 64 | 207.0 ± 82.2 | – |

### Headline numbers

| | before | after |
|---|---|---|
| Peak tasks/s, 0 ms tasks | 242.7 (N=8) | 5,438.4 (N=32) |
| Peak tasks/s, 5 ms tasks | 240.4 (N=8) | 1,664.1 (N=32) |
| Peak tasks/s, 50 ms tasks | 238.3 (N=16) | 499.2 (N=32) |
| USL fit, 0 ms | λ=187.0, α=0.7200, β=0.001270 | λ=1,864.5, α=0.3616, β=0.000000 |
| Per-task overhead (ms, N=1) | 7.88 ± 2.27 | 4.82 ± 2.65 |
| Recovery p50 / p95 / max (ms) | 5,562 / 6,659 / 6,736 (n=22) | 163 / 327 / 346 (n=20) |
| Little's law ratio (mean) | 0.998 | 0.998 |
| Task source | sample | synthetic |

