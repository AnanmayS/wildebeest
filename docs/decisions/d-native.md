# D-native: heterogeneous scale-out with a native MPS worker

Status: implemented (worker side). Worker changes: `worker/wildebeest_worker/{tuning,detector,classifier}.py`,
`scripts/native_worker.sh`, `worker/requirements-native.txt`, `worker/tests/test_device.py`.

## Why

Real-model throughput is CPU-bound at about 1.3 img/s on the M2. The pipeline already reaches about 80% of bare
MegaDetector in the Docker VM, and Docker Desktop can't give a Linux container the Mac GPU (Metal). To go
faster we need different hardware in the pool. The cheapest option is the laptop's own GPU: one worker process runs
**natively on macOS with PyTorch MPS**, registers with the same coordinator and `BLMOVE`s from the same Redis
queue as the CPU containers. The pool becomes heterogeneous, and nothing in the protocol changes. Leases,
heartbeats, fencing and idempotent writes don't depend on where a worker runs.

## Device selection

`DEVICE=auto|cpu|mps|cuda`, read by `tuning.resolve_device()` when a model loads.

- The default is **`cpu`**. The containers always get CPU, so the container path is unchanged: same device,
  same channels_last weights, same `TORCH_NUM_THREADS`.
- `auto` picks cuda, then mps, then cpu.
- An explicit device that isn't available **raises at startup**. A "GPU worker" never silently runs on the CPU.
- SpeciesNet's detector picks cuda/mps by itself and has no device argument. The worker moves it to the resolved
  device. The classifier takes `device=` directly.
- After resolving, the model calls `announce_device()`. This sets `WORKER_DEVICE` to the real device unless the
  launcher already set it, so `DEVICE=auto` still registers the correct device.
- `PYTORCH_ENABLE_MPS_FALLBACK`: **not needed**. Detector and classifier both ran with fallback **off** on torch
  2.14 with no errors, so every op has an MPS kernel. Turning it on changed nothing (0.120 vs 0.121 s/img). The
  script turns it on anyway as a safety net for other torch versions. If a future op isn't supported, the task
  slows down instead of failing and spending the task's attempts.

### channels_last per backend (measured, `CHANNELS_LAST=0|1` forces it)

Median s/img, 2 torch threads, MegaDetector at 640 px. The native rows use the same 40 images for every variant; the container row is from DECISIONS #41 (12 images):

| Backend | Detector, NHWC | Detector, NCHW | Classifier, NHWC | Classifier, NCHW | Adopted |
|---|---|---|---|---|---|
| Linux container CPU (DECISIONS #41) | **0.66** | 0.81 | **0.31** | 0.37 | NHWC for both (unchanged) |
| native macOS CPU | **0.30** | 0.42 | 2.08 | **0.21–0.23** | NHWC detector only |
| MPS | 0.145 | **0.121** | 0.32 | **0.23** | NCHW for both |

The surprise is the classifier on native macOS CPU: NHWC weights make it **~9× slower**. It happened on
three repeated runs. Probably some SpeciesNet ops fall off the Accelerate fast path when the weights are NHWC.
This only affects a native CPU worker, not a container: `tuning.CHANNELS_LAST_BACKENDS` keys macOS CPU as
`cpu-darwin`. CUDA is unmeasured and stays NCHW.

Not adopted: **fp16 on MPS**. It makes the detector forward pass 20% faster (0.096 vs 0.120 s). It also changes
the numbers the model produces, so it would need its own `DETECTOR_MODEL_VERSION` and an accuracy check, and
20% doesn't justify that.

## Does MPS produce the same results as CPU?

Yes, down to float32 rounding.

- **Detector**, 40 images (20 animal, 20 empty; 33 boxes at conf ≥ 0.1): same boxes, same labels, same order.
  Largest raw confidence difference **3.6e-7**. The stored bboxes (4 decimals) are identical, and every image got
  the same final category. The raw model-head tensor differs by at most 0.005, measured in 640 px box-coordinate units.
- **Classifier**, 20 animal crops, same stage 1 boxes: same ensemble label for all 20 and the same top-5 order.
  Largest top-5 score difference **0.0001**, and the same `confidence` to 4 decimals.
- **Container CPU (Linux, oneDNN) vs MPS vs native CPU**, 40 more images (the first 40 by filename; 35 boxes):
  the stored detections are identical in all three, with the same labels, confidences and bboxes to 4 decimals and
  the same categories. The container detections came from `python -m wildebeest_worker.measure` in the
  `wildebeest-worker` image.

**Decision: MPS results share `DETECTOR_MODEL_VERSION` / `CLASSIFIER_MODEL_VERSION` with CPU results.** The
differences are about 1e-7. That is hundreds of times smaller than the 4 decimals the worker stores, and
smaller than the gap we already accept between thread counts and between oneDNN (containers) and Accelerate
(native CPU). A box could in principle flip only if its confidence sits within about 1e-6 of the 0.1 drop cut or
the 0.2 animal threshold. The content-hash cache is keyed by `(sha256, model_version)`, and treating these as
one model is correct. Anything that really changes numerics must get a new version string: fp16/bf16, a different
input size, TensorRT or ONNX export.

## Measurements

Apple M2 (4P+4E, 16 GB), macOS, torch 2.14.0, speciesnet 5.0.5, MegaDetector v5a at 640 px, 2 torch threads unless
noted. **Other agents' Compose stacks and benchmarks were running on the same machine and the same Docker VM
the whole time** (host load average 4–17, several stacks and 5–9 real-model containers besides ours). Native MPS
numbers barely move with that load. Container numbers move a lot, so the container rows below also give the
quiet-machine figures recorded earlier.

### Bare models (no pipeline), per image

| | MegaDetector v5a @ 640 | SpeciesNet classifier + ensemble |
|---|---|---|
| **native MPS** | **0.121–0.128 s (7.7–8.3 img/s)** | 0.23–0.37 s |
| native macOS CPU, 2 threads | 0.30–0.35 s (2.7–3.3 img/s) | **0.20–0.23 s** |
| native macOS CPU, 4 / 8 threads | 0.31 s / 0.31 s | – |
| container CPU, 2 threads (quiet, DECISIONS #41) | 0.66 s (1.5 img/s) | 0.31 s |
| container CPU, 2 threads (today, contended) | 1.2–1.3 s | – |

- On the detector, MPS is **5.2–5.5× faster than a quiet container** and 2.5–2.9× faster than native CPU. One
  MPS worker alone is about **4–5× the whole container pool's ceiling** (1.5–1.9 img/s).
- More threads barely help native CPU (0.35 → 0.31 s from 2 to 8 threads). PyTorch on macOS runs the convolutions
  on Accelerate/AMX, and the extra cores add almost nothing.
- **MPS doesn't help the classifier.** Single-crop inference in SpeciesNet's model is dominated by small kernels
  and layout changes. Native CPU matches or beats MPS, so a native classifier should use `DEVICE=cpu`. A native MPS
  classifier would also share the GPU with the MPS detector.
- Memory, native MPS detector: the MPS allocator holds 0.54 GB of tensors (1.2 GB driver pool) in unified memory,
  and the **physical footprint is 1.68 GB** (see below). Model load takes 1.7 s (6.8 s including imports). Native
  MPS classifier: 0.21 GB of MPS tensors (1.1 GB driver pool). Neither uses the Docker VM's
  8 GB, so a native worker adds capacity without taking room from the containers.

On macOS, `ps` RSS **undercounts** an MPS worker: Metal buffers aren't in RSS. During a pool run the native detector showed an
RSS of 0.04–0.23 GB, but `footprint <pid>` reported a **physical footprint of 1.68 GB**. Budget about 1.7 GB of host RAM per
native MPS detector. The dashboard's `rssMb` for a native worker is an undercount for the same reason
(runtime.py falls back to peak RSS on macOS).

### End-to-end pool (300 images, cache cleared, same stack)

Stack `wb-d`: 2 CPU detector containers (2 threads each) + 1 CPU classifier container, with and without
`scripts/native_worker.sh detect` (MPS). Throughput is images ÷ wall time from `POST /jobs/sample` to `done`.

Three paired runs, each "without" run directly followed by a "with" run, so both saw about the same background load:

| Run (host load avg) | Without native: wall, img/s | With 1 native MPS detector: wall, img/s | Speed-up |
|---|---|---|---|
| a (7–15) | 561 s, **0.53** | 206 s, **1.46** | 2.7× |
| b (7–13) | 367 s, **0.82** | 107 s, **2.80** | 3.4× |
| c (9–17) | 818 s, **0.37** | 240 s, **1.25** | 3.4× |

(A fourth "with" run added a native MPS classifier as well: 210 s, 1.43 img/s, no better than run a. The shared GPU
slowed the native detector to 0.36 s p50, and the containers' stragglers still set the tail. See below.)
The quiet-machine reference for 2 + 1 containers is 1.34 img/s (benchmarks/results.md). Today's "without" runs
got 0.37–0.82 because of other agents' load in the VM.

**Heterogeneous split (detect tasks per worker, of 300):**

| Run | native MPS | container 1 | container 2 | Native p50 task | Container p50 task |
|---|---|---|---|---|---|
| a | 286 (95%) | 7 | 7 | 228 ms | 14.2–14.5 s |
| b | 274 (91%) | 13 | 13 | 183 ms | 4.4 s |
| c | 288 (96%) | 6 | 6 | 272 ms | 22.6–28.1 s |

What this shows:
- **A pull queue balances heterogeneous workers without being told anything.** Each worker `BLMOVE`s its next task when
  it finishes the last one, so the split follows service rate: the MPS worker did 91–96% of detection. It got no
  speed weights and no scheduler change.
- **The bottleneck moves to the classifier (tandem queue).** In run b, detection finished in 58–60 s (**~5 img/s**
  of detection), but the single CPU classifier container needed until 106 s for its 102 crops (1.0 s mean under
  load). Whole-pool throughput is set by the slowest stage: min(detect rate, classify rate ÷ 0.34 animal
  fraction). To get past about 3 img/s, add classifiers. A second classifier container, or a native classifier
  with `DEVICE=cpu` (0.21 s bare), should roughly double stage 2.
- **Stragglers set the tail.** Each container held a detect task for 4–28 s while the MPS worker would have done
  roughly 12–80 of them in that time. In run c the detect stage ended at 166 s only because a container's last task was
  still running. This is the case for speculative re-execution (report item #11): once the queue is empty, re-run a
  slow container's task on the idle fast worker. A simpler fix: stop CPU detectors from taking new work while a
  much faster worker is idle.
- **Per-task overhead matters at GPU speed.** The native worker's own handler time (fetch + infer) was 266 ms p50
  in run a, but its claim → complete cycle was about 360 ms. Claim-confirm, complete and the MinIO GET all cross
  Docker Desktop's port forwarder into a busy VM. At 0.12 s of GPU time per image, coordination and I/O are more
  than half of each task, so batch claims and complete-and-claim-next (CONTRACTS) pay off directly for this worker.

### 1 vs 2 torch threads per container (bare probes)

MegaDetector at 640 in the worker image, 25 images, median per image, run in three alternating rounds while
other agents' stacks loaded the VM (host load average 9–16). At most 2 detector containers at a time (the
shared-machine rule), so the earlier 4 × 1 vs 4 × 2 comparison couldn't be repeated as is.

| Config | Round 1 | Round 2 | Round 3 | Aggregate img/s |
|---|---|---|---|---|
| 1 × 2 threads | 0.97 s | 0.90 s | 0.89 s | 1.03–1.12 |
| 1 × 1 thread | 1.15 s | 1.20 s | 1.15 s | 0.83–0.87 |
| 2 × 2 threads | 1.77 s | 2.96 s | 1.81 s | 0.68–1.13 |
| 2 × 1 thread | 2.57 s | 2.02 s | 2.65 s | 0.75–0.99 |

- **Inconclusive, and 1 thread did not win here.** At 2 workers, 2 threads tied or beat 1 thread in every round.
  At an equal thread budget, 1 × 2 threads (1.03–1.12 img/s) also beat 2 × 1 thread (0.75–0.99). The per-image time
  of a 1-thread container nearly doubles as soon as a second one runs, which points at shared memory bandwidth or
  VM scheduling, not at compute.
- The earlier quiet-machine result (4 × 1 thread 1.86 img/s vs 4 × 2 threads 1.65) came from a different regime.
  With 4 workers × 2 threads, 8 vCPUs are oversubscribed once the VM's own work is counted, and 1 thread removes that.
- **Decision: keep `TORCH_NUM_THREADS=2`.** 1 thread may only pay off at 4+ detectors on a quiet VM. That check is one
  env change (`TORCH_NUM_THREADS=1 make benchmark`) and should be rerun when the machine is otherwise idle. Either way,
  the gain is ≤ 13% of a ~1.7 img/s ceiling, while one MPS worker adds 4–5× that ceiling.

## What runtime.py must read and send (for P1/P2)

`scripts/native_worker.sh` exports:

| Env var | Value from the script | Meaning |
|---|---|---|
| `WORKER_RUNTIME` | `native` | register `"runtime"` |
| `WORKER_DEVICE` | `$DEVICE`. With `DEVICE=auto`, the resolved device | register `"device"` |
| `WORKER_CONTAINER_ID` | `native-<hostname -s>`, e.g. `native-aro` | register `"containerId"` |
| `DEVICE` | `mps` for `detect`, `cpu` for `classify` (override with env) | read by the models (`tuning.resolve_device`) |

`POST /workers/register` must send:
```json
{ "stage": "detect", "hostname": "<socket.gethostname()>", "containerId": "native-aro",
  "runtime": "native", "device": "mps" }
```
Rules for runtime.py:
1. `runtime` = `WORKER_RUNTIME` if it is `container` or `native`, else `container` if `/.dockerenv` exists, else `native`.
2. `device` = `WORKER_DEVICE`, else `cpu`. Read it **after** the handler (model) is built. `build_handler` runs
   before `Worker(...)` in `__main__.py`, and the model fills in `WORKER_DEVICE` for `DEVICE=auto`. Don't import
   torch in runtime.py (the fake backend and the unit tests don't have it).
3. `containerId` = `WORKER_CONTAINER_ID` if set, else the hostname for containers, else `native-<hostname>`.

Status at the time of writing: runtime.py already does 1 and 2 and sends `runtime`/`device`, and the worker logged
`registered as detect-aro.local (native/mps, …)`. It does **not** read `WORKER_CONTAINER_ID` yet. It derives
`native-{socket.gethostname()}` = `native-aro.local`, which also works (the coordinator only uses containerId for
Docker actions, and those are 409 `NOT_A_CONTAINER` for native workers). Honouring `WORKER_CONTAINER_ID` is a
one-line change and lets a launcher pick the ID.

Recommended, not done (runtime.py isn't mine):
- **`WORKER_HOSTNAME` override.** Worker IDs are `{stage}-{hostname}`. Two native workers of the same stage on
  one Mac (or two Macs with the same hostname) would share an ID, and each re-register releases the other's
  leases. The script refuses to start a second worker of the same stage on one host (pid file). Reading
  `WORKER_HOSTNAME` would allow it deliberately.
- **Retry a heartbeat once on a connection reset.** In one run the native worker's heartbeats failed twice with
  `RemoteDisconnected` (the host → Docker port forwarder dropping idle keep-alive connections under load). The reaper
  marked it `DEAD (silent 6164 ms)`, and it re-registered on its next heartbeat. It was idle at the time, so no task
  was lost, but a busy worker would have lost its lease. A heartbeat is sent with one attempt, and a stale pooled
  connection costs a whole 2 s interval. One immediate retry on `ConnectionError` fixes it (or give heartbeats their
  own `requests.Session`). Workers outside the VM (native, Tailscale) need this more than containers do.

## Runbook: add a native MPS worker during the demo

Prerequisites: Apple-silicon Mac, `uv`, the stack running (`make demo`). The weights are already in
`data/cache/kagglehub` if `scripts/baseline.py` has been run. Otherwise the first start downloads about 500 MB from Kaggle.

1. **Before the demo** (one-time, about 1 min; the venv is 1.3 GB): `scripts/native_worker.sh setup`
2. Start the job on the dashboard with the containers only, and point out the throughput graph (about 1.3 img/s).
3. In a terminal: `scripts/native_worker.sh` (MPS detector on the default ports). For a stack on other ports:
   `COORDINATOR_PORT=43000 REDIS_HOST_PORT=46379 MINIO_PORT=49000 scripts/native_worker.sh`.
   It prints its pid, loads in about 7 s, logs `registered as detect-<host> (native/mps …)`, and starts taking
   tasks. The dashboard shows a new detector with a "native · MPS" badge, and throughput jumps within a second
   or two.
4. Talking points: same queue, same leases and fencing. The scheduler knows nothing about speed: a pull queue
   balances load by itself, because the fast worker simply comes back for more (see the per-worker split above).
   The bottleneck moves to the classifier (tandem queue). Add a classifier
   (`docker compose up -d --scale classifier=2`, or `scripts/native_worker.sh classify`, which runs on native CPU by default).
5. Failure demo: Kill/Pause on the dashboard are disabled for a native worker (409 `NOT_A_CONTAINER`). Use the terminal:
   `kill -9 <pid>` → there is no Docker event for a native worker, so the reaper marks it DEAD after the 6 s
   heartbeat timeout, and a container re-claims its task.
   `kill -STOP <pid>`, wait about 10 s, `kill -CONT <pid>` → the task has been reassigned in the meantime. The thawed
   worker either gets 410 on its next heartbeat and drops its result, or posts it and gets `409 STALE_LEASE`.
   Which one happens depends on which of its threads runs first. Either way the fenced copy wins.
6. Stop it gracefully with Ctrl-C: it finishes the in-flight task, deregisters, and exits.

Troubleshooting: `DEVICE=mps is not available` means the torch build has no MPS support or you aren't on
Apple silicon. `coordinator not reachable` means wrong ports. The script sources `.env` like Compose does. Model
versions must match the stack. The script defaults to the Compose defaults (`speciesnet-md_v5a.0.1-640`,
`DETECTOR_IMG_SIZE=640`). If you changed them in `.env`, the script picks them up.

## Sketch: a GPU spot VM joining over Tailscale (not built, no spend)

- **Instance**: AWS g4dn.xlarge (T4 16 GB, 4 vCPU, 16 GB RAM), about $0.53/h on demand, about $0.30/h spot.
  Deep Learning AMI (NVIDIA driver + Docker + nvidia-container-toolkit).
- **Image**: a linux/amd64 variant of the worker image with CUDA torch instead of the CPU wheel. That needs a new
  build arg for the torch index, which the Dockerfile doesn't have today. Tag it `wildebeest-worker:cuda` and run it
  with `--gpus all -e DEVICE=cuda`. Or run this script's
  approach natively with `DEVICE=cuda`.
- **Network**: Tailscale on the laptop and the VM (`tailscale up --authkey …` in user-data). Point the VM at the
  laptop's tailnet name: `COORDINATOR_URL=http://laptop:3000`, `REDIS_URL=redis://laptop:16379`,
  `S3_ENDPOINT=http://laptop:9000`. Compose already publishes those ports. Limit access with Tailscale ACLs, not
  public security-group rules. Redis has no auth today, so add `requirepass` before anything leaves the laptop.
- **Protocol changes it would need**: WAN round trips (20–80 ms) plus image downloads from the laptop's MinIO over
  the tailnet (0.3–1 MB each) make per-task overhead matter. Use `CLAIM_BATCH_SIZE` 4–8 and in-worker prefetch.
  Raise `WORKER_TIMEOUT_MS` or make it per-runtime: a 6 s silence rule is tight over a WAN, see the false DEAD
  above. `runtime: "remote"` would let the dashboard show it.
- **Chaos for free**: a spot interruption is a SIGKILL with a 2-minute warning. Hook the IMDS
  `spot/instance-action` notice to SIGTERM the worker (graceful deregister). An unannounced reclaim just exercises
  the lease reaper.
- Expected speed: a T4 runs MegaDetector v5a at 640 px at roughly 15–25 img/s in fp32 (estimate, unmeasured), so
  a single classifier would bottleneck immediately. Classifiers would need to scale too, or also run on the GPU.
