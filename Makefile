DETECTORS ?= 3
CLASSIFIERS ?= 1
SAMPLE_SIZE ?= 2000
PYTHON ?= .venv/bin/python

.PHONY: demo up down sample venv test test-coordinator test-worker test-integration benchmark logs clean

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

benchmark: sample
	cd scripts && npm ci && npx tsx benchmark.ts

clean:
	docker compose down -v
