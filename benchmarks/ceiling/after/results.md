# Orchestration ceiling — after

Generated 2026-09-26T01:44:10Z on Apple M2, Docker Desktop 8 vCPU / 8 GB. Task source: **synthetic** mode, fake handler (sleep for the task time, return no detections so every task finalises one image). Throughput is the steady state between the 10th and 90th percentile completion; ± is the 95% t-interval over trials. Latency columns are lease time (claim-confirm → complete committed). The last column is the mean CPU of *other* projects' containers on the shared Docker VM during the point (100 = one core): the machine is shared, and a busy neighbour shows up here.

![ceiling](ceiling.png)

### 0 ms tasks

| Worker loops | Trials | Tasks/s | ± 95% CI | Lease p50 (ms) | Lease p99 (ms) | Cycle (ms) | Overhead/task (ms) | Little's law X·W/N | Other containers' CPU (%) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 3 | 1,320.2 | 2,133.5 | 5.20 | 11.94 | 1.17 | 1.17 | 1.000 | 10 |
| 2 | 3 | 3,398.1 | 3,743.6 | 2.76 | 12.42 | 0.66 | 0.66 | 1.000 | 7 |
| 4 | 3 | 4,087.1 | 3,121.9 | 4.00 | 18.62 | 1.04 | 1.04 | 0.999 | 13 |
| 8 | 3 | 3,147.2 | 4,890.7 | 9.74 | 36.09 | 3.84 | 3.84 | 0.995 | 7 |
| 16 | 3 | 4,480.7 | 539.4 | 20.59 | 67.97 | 3.56 | 3.56 | 0.996 | 9 |
| 32 | 3 | 5,438.4 | 5,749.2 | 37.32 | 146.76 | 6.95 | 6.94 | 0.994 | 13 |

### 5 ms tasks

| Worker loops | Trials | Tasks/s | ± 95% CI | Lease p50 (ms) | Lease p99 (ms) | Cycle (ms) | Overhead/task (ms) | Little's law X·W/N | Other containers' CPU (%) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 3 | 89.5 | 2.7 | 9.42 | 18.59 | 11.17 | 4.34 | 1.000 | 5 |
| 2 | 3 | 193.2 | 13.5 | 8.50 | 20.55 | 10.35 | 3.81 | 1.000 | 10 |
| 4 | 3 | 377.9 | 13.9 | 8.35 | 17.67 | 10.59 | 4.34 | 1.000 | 8 |
| 8 | 3 | 769.9 | 9.5 | 14.83 | 36.09 | 10.39 | 4.37 | 1.000 | 9 |
| 16 | 3 | 1,500.7 | 139.6 | 17.18 | 41.40 | 10.66 | 4.65 | 0.999 | 7 |
| 32 | 3 | 1,664.1 | 2,420.7 | 232.22 | 691.61 | 23.79 | 17.59 | 0.989 | 7 |

### 50 ms tasks

| Worker loops | Trials | Tasks/s | ± 95% CI | Lease p50 (ms) | Lease p99 (ms) | Cycle (ms) | Overhead/task (ms) | Little's law X·W/N | Other containers' CPU (%) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 3 | 16.3 | 0.5 | 56.62 | 70.45 | 61.26 | 8.96 | 1.000 | 6 |
| 2 | 3 | 33.0 | 0.6 | 56.28 | 68.76 | 60.59 | 8.40 | 0.999 | 8 |
| 4 | 3 | 65.6 | 1.4 | 56.44 | 71.93 | 60.94 | 8.79 | 1.000 | 10 |
| 8 | 3 | 135.5 | 1.5 | 55.50 | 65.40 | 59.01 | 7.33 | 0.999 | 4 |
| 16 | 3 | 274.8 | 4.6 | 54.28 | 65.89 | 58.25 | 6.97 | 1.000 | 9 |
| 32 | 3 | 499.2 | 42.5 | 56.13 | 75.38 | 64.15 | 12.80 | 1.000 | 9 |

### Universal Scalability Law fit

X(N) = λN / (1 + α(N−1) + βN(N−1)), least squares over every trial; CIs from 500 bootstrap resamples.

| Task (ms) | Points | λ (tasks/s per loop) | α (contention) | β (crosstalk) | R² | Peak N* | X(N*) |
|---|---|---|---|---|---|---|---|
| 0 | 6 | 1,864.5 [1,083.4, 2,563.4] | 0.3616 [0.1577, 0.5407] | 0.000000 [0.000000, 0.008675] | 0.385 | 6,641,019,890.2 | 5,156.6 |
| 5 | 6 | 110.9 [98.4, 128.9] | 0.0000 [0.0000, 0.0000] | 0.001105 [0.000361, 0.003033] | 0.776 | 30.1 | 1,696.1 |
| 50 | 6 | 17.4 [17.3, 17.6] | 0.0000 [0.0000, 0.0000] | 0.000116 [0.000082, 0.000150] | 0.999 | 92.8 | 812.4 |

### Per-task overhead

- One worker loop, cycle time minus the measured handler time (claim → work → complete → next claim; `time.sleep` overshoots by ~1.4 ms in these containers, so the handler's actual time is subtracted): **4.82 ms** ± 2.65 (all task times); by task time: 0 ms → 1.17 ± 2.50, 5 ms → 4.34 ± 0.40, 50 ms → 8.96 ± 1.61.
- Inside the lease (claim-confirm committed → complete committed, minus task time), p50: 4.04 ms.
- Worker-reported timings (tasks.timings, p50 of the last run): {'claimMs': 6.4, 'fetchMs': 0.0, 'inferMs': 50.9, 'uploadMs': 0.0}.
- Little's law (X × mean cycle ÷ N, should be 1.0 in a closed loop): mean 0.998, range 0.970–1.001.

### Open loop (latency from intended arrival, no coordinated omission)

![open loop](openloop.png)

| Offered | Offered (tasks/s) | Achieved (tasks/s) | p50 (ms) | p90 (ms) | p99 (ms) | p99.9 (ms) | Max (ms) | Unfinished | Submit errors |
|---|---|---|---|---|---|---|---|---|---|
| 50% | 832.06 | 690.12 | 699.55 | 2668.0 | 3383.55 | 3482.56 | 3532.56 | 0 | 0 |
| 80% | 1331.3 | 506.9 | 11290.0 | 24102.61 | 27189.62 | 27522.32 | 27714.48 | 0 | 0 |
| 100% | 1664.12 | 481.93 | 20136.12 | 40814.12 | 43032.83 | 43159.99 | 43200.28 | 0 | 0 |
| 110% | 1830.53 | 742.18 | 15769.1 | 26167.0 | 27637.14 | 27848.56 | 27925.59 | 0 | 0 |

Where the median task spends its time (p50 of each hop, ms):

| Offered | intended arrival → image row committed | → claimed (dispatch + queue) | → completed (lease) |
|---|---|---|---|
| 50% | 613.72 | 39.63 | 40.59 |
| 80% | 11141.51 | 60.46 | 64.82 |
| 100% | 19943.53 | 67.49 | 70.71 |
| 110% | 15643.19 | 43.74 | 49.57 |

### Recovery after SIGKILL

![recovery](recovery.png)

20 kills of busy fake-backend detector containers via `POST /workers/:id/kill` (0 more landed between tasks and are not counted). Detection via: docker_event.

| | p50 (ms) | p95 (ms) | max (ms) |
|---|---|---|---|
| **kill → all tasks re-claimed** | **163** | **327** | **346** |
| kill → worker marked dead | 15 | 37 | 59 |
| kill → tasks requeued | 20 | 45 | 65 |
| requeued → re-claimed | 148 | 312 | 323 |

