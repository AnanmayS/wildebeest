DETECTORS ?= 3
CLASSIFIERS ?= 1
SAMPLE_SIZE ?= 2000
PYTHON ?= .venv/bin/python

.PHONY: demo up down sample venv test test-coordinator test-worker test-integration benchmark benchmark-fake logs clean

## Bring everything up with the sample dataset ready, then open http://localhost:8080
demo: sample up
	@echo ""
	@echo "ForgeGrid is up: open http://localhost:8080 and click 'Load sample dataset'."

up:
	docker compose up -d --build --scale detector=$(DETECTORS) --scale classifier=$(CLASSIFIERS)

down:
	docker compose down

logs:
	docker compose logs -f coordinator

venv:
	test -x $(PYTHON) || (uv venv --python 3.11 .venv && uv pip install --python $(PYTHON) -r scripts/requirements.txt)

## Download the Snapshot Serengeti sample (skipped if data/sample/labels.csv already exists)
sample: venv
	test -f data/sample/labels.csv || $(PYTHON) scripts/download_sample.py --count $(SAMPLE_SIZE)

test: test-coordinator test-worker

test-coordinator:
	docker compose up -d postgres redis minio
	cd coordinator && npm ci && npm test

test-worker:
	cd worker && ../$(PYTHON) -m pytest -q

## Dockerized end-to-end tests (Phase 2-4 acceptance). Takes several minutes on CPU.
test-integration: sample
	cd tests/integration && npm ci && npm test

## Real models: 300 images at 1-4 detectors (~20 min on an M2). Override BENCH_IMAGES / BENCH_DETECTORS.
benchmark: sample
	cd scripts && npm ci && BENCH_IMAGES=$${BENCH_IMAGES:-300} BENCH_RECOVERY_IMAGES=200 npx tsx benchmark.ts

## Fake 300 ms model: orchestration scaling at 1-8 detectors, no ML compute (~15 min).
benchmark-fake: sample
	cd scripts && npm ci && MODEL_BACKEND=fake DETECTOR_MODEL_VERSION=fake-detector-v1 CLASSIFIER_MODEL_VERSION=fake-classifier-v1 \
		BENCH_TAG=fake BENCH_IMAGES=1000 BENCH_DETECTORS=1,2,4,8 BENCH_RECOVERY_IMAGES=500 npx tsx benchmark.ts

clean:
	docker compose down -v
