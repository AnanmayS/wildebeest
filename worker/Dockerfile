FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    # Model weights are baked into the image here (not the /models volume), so every
    # container starts without a download and scaled-up containers never race on it.
    KAGGLEHUB_CACHE=/opt/models/kagglehub \
    TORCH_HOME=/models/torch

# yolov5 (used by SpeciesNet's MegaDetector) imports OpenCV, which needs these.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgl1 libglib2.0-0 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# CPU-only PyTorch keeps the image ~1 GB smaller than the default CUDA wheels.
RUN pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt .
RUN pip install -r requirements.txt

# Download SpeciesNet (classifier, taxonomy, geofence) + MegaDetector v5a weights at build time.
ARG SPECIESNET_MODEL=kaggle:google/speciesnet/pyTorch/v4.0.3a/1
ENV SPECIESNET_MODEL=${SPECIESNET_MODEL}
RUN python -W ignore -c "import os; from speciesnet.utils import ModelInfo; print(ModelInfo(os.environ['SPECIESNET_MODEL']))"

# OpenTelemetry (tracing is off unless OTEL_EXPORTER_OTLP_ENDPOINT is set). A layer of its own,
# after the model download, so it doesn't invalidate the multi-GB layers above.
COPY requirements-otel.txt .
RUN pip install -r requirements-otel.txt

COPY wildebeest_worker ./wildebeest_worker

CMD ["python", "-m", "wildebeest_worker"]
