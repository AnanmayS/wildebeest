#!/usr/bin/env bash
# Run a Wildebeest worker natively on the host (macOS + Apple GPU via PyTorch MPS), next to the
# containers of a running Compose stack. It pulls from the same Redis queue and registers with the
# same coordinator, so the pool becomes heterogeneous: CPU containers + one GPU worker.
#
#   scripts/native_worker.sh                  # MPS detector against the default stack (ports 3000/16379/9000)
#   scripts/native_worker.sh classify         # native classifier (CPU by default; DEVICE=mps to override)
#   scripts/native_worker.sh setup            # only create/refresh .venv-native, then exit
#   COORDINATOR_PORT=43000 REDIS_HOST_PORT=46379 MINIO_PORT=49000 scripts/native_worker.sh
#   DEVICE=cpu scripts/native_worker.sh       # native CPU (auto | cpu | mps | cuda)
#
# Ctrl-C (SIGINT) is a graceful stop: finish the in-flight task, deregister, exit. The dashboard's
# Kill/Pause buttons don't apply to a native worker (there is no container); to show a crash, run
#   kill -9 <pid>          (the pid is printed at startup; the script execs into the worker)
#   kill -STOP <pid> ... kill -CONT <pid>     (a freeze: shows fencing, like the Pause button)
# and the coordinator's heartbeat timeout recovers its task exactly like a SIGKILLed container.
#
# See docs/decisions/d-native.md for measurements and the runbook.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$ROOT/.venv-native"
PY="$VENV/bin/python"
REQS="$ROOT/worker/requirements-native.txt"

MODE="${1:-detect}"
case "$MODE" in
  detect|classify|setup) ;;
  *) echo "usage: $0 [detect|classify|setup]" >&2; exit 2 ;;
esac

# Same .env Compose reads, so custom ports/model versions match the stack.
if [[ -f "$ROOT/.env" ]]; then set -a; . "$ROOT/.env"; set +a; fi

# ---------------------------------------------------------------- venv (Python 3.11 via uv)
command -v uv >/dev/null || { echo "uv is required: https://docs.astral.sh/uv/" >&2; exit 1; }
if [[ ! -x "$PY" ]]; then
  echo "creating $VENV (Python 3.11)"
  uv venv --python 3.11 "$VENV"
fi
# Reinstall only when the requirement files change.
STAMP="$VENV/.wildebeest-reqs"
WANT="$(cat "$REQS" "$ROOT/worker/requirements.txt" | shasum | cut -d' ' -f1)"
if [[ "$(cat "$STAMP" 2>/dev/null)" != "$WANT" ]]; then
  echo "installing worker deps (torch with MPS, speciesnet) into $VENV"
  (cd "$ROOT/worker" && uv pip install --python "$PY" -r requirements-native.txt)
  echo "$WANT" > "$STAMP"
fi
[[ "$MODE" == setup ]] && { echo "ready: $VENV"; exit 0; }

# ---------------------------------------------------------------- environment
HOST_SHORT="$(hostname -s)"
export WORKER_STAGE="$MODE"
export MODEL_BACKEND="${MODEL_BACKEND:-speciesnet}"
# MPS for the detector (~5x a container). The classifier is no faster on MPS than on native CPU
# (0.21 s vs 0.23-0.37 s per crop) and would share the GPU with the detector, so it defaults to CPU.
if [[ "$MODE" == classify ]]; then export DEVICE="${DEVICE:-cpu}"; else export DEVICE="${DEVICE:-mps}"; fi
export COORDINATOR_URL="${COORDINATOR_URL:-http://localhost:${COORDINATOR_PORT:-3000}}"
export REDIS_URL="${REDIS_URL:-redis://localhost:${REDIS_HOST_PORT:-16379}}"
export S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:${MINIO_PORT:-9000}}"
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"
export S3_BUCKET="${S3_BUCKET:-wildebeest}"
# Must equal the stack's values (docker-compose.yml x-model-env) or the cache and results diverge.
export DETECTOR_MODEL_VERSION="${DETECTOR_MODEL_VERSION:-speciesnet-md_v5a.0.1-640}"
export CLASSIFIER_MODEL_VERSION="${CLASSIFIER_MODEL_VERSION:-speciesnet-v4.0.3a}"
export DETECTOR_IMG_SIZE="${DETECTOR_IMG_SIZE:-640}"
export SPECIESNET_MODEL="${SPECIESNET_MODEL:-kaggle:google/speciesnet/pyTorch/v4.0.3a/1}"
# CPU threads only serve JPEG decode, letterbox and NMS when the model is on the GPU.
export TORCH_NUM_THREADS="${TORCH_NUM_THREADS:-2}"
# Weights: reuse the host cache that scripts/baseline.py filled (downloaded on first run otherwise).
export KAGGLEHUB_CACHE="${KAGGLEHUB_CACHE:-$ROOT/data/cache/kagglehub}"
# No MegaDetector/SpeciesNet op falls back to CPU on torch 2.14 (measured with fallback off), and
# the fallback costs nothing when unused, so keep it on as a safety net for other torch versions.
export PYTORCH_ENABLE_MPS_FALLBACK="${PYTORCH_ENABLE_MPS_FALLBACK:-1}"
# Read by runtime.py for POST /workers/register (docs/CONTRACTS.md "Native (non-container) workers").
export WORKER_RUNTIME=native
export WORKER_DEVICE="${WORKER_DEVICE:-$DEVICE}"
export WORKER_CONTAINER_ID="${WORKER_CONTAINER_ID:-native-$HOST_SHORT}"
# Set WORKER_HOSTNAME to run a second native worker of the same stage on this host (it becomes the worker ID).
export WORKER_HOSTNAME="${WORKER_HOSTNAME:-}"
export PYTHONPATH="$ROOT/worker${PYTHONPATH:+:$PYTHONPATH}"
export PYTHONUNBUFFERED=1

# ---------------------------------------------------------------- preflight
if [[ "$DEVICE" == auto ]]; then
  WORKER_DEVICE="$("$PY" -W ignore -c 'from wildebeest_worker.tuning import resolve_device; print(resolve_device("auto"))')"
  export WORKER_DEVICE
fi
"$PY" -W ignore -c 'from wildebeest_worker.tuning import resolve_device; resolve_device()' \
  || { echo "DEVICE=$DEVICE is not available on this machine" >&2; exit 1; }
if ! curl -fsS -m 3 "$COORDINATOR_URL/healthz" >/dev/null; then
  echo "coordinator not reachable at $COORDINATOR_URL (is the stack up? set COORDINATOR_PORT)" >&2
  exit 1
fi

# Worker IDs are {stage}-{hostname}. A second native worker of the same stage on this host would
# register under the same ID, and each re-register releases the other's leases. Refuse it.
PIDFILE="$VENV/.native-$WORKER_STAGE.pid"
if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "a native $WORKER_STAGE worker is already running on this host (pid $(cat "$PIDFILE"))" >&2
  exit 1
fi
echo $$ > "$PIDFILE"

echo "native $WORKER_STAGE worker on $WORKER_DEVICE -> $COORDINATOR_URL, redis $REDIS_URL, s3 $S3_ENDPOINT"
echo "  pid=$$ containerId=$WORKER_CONTAINER_ID model=$DETECTOR_MODEL_VERSION / $CLASSIFIER_MODEL_VERSION"
cd "$ROOT/worker"
exec "$PY" -m wildebeest_worker
