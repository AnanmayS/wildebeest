"""Phase 1 baseline: single-process SpeciesNet over data/sample/.

Runs the same two-stage pipeline the distributed workers run, in one process:
  1. MegaDetector on every image;
  2. if an animal box >= ANIMAL_CONF_THRESHOLD, SpeciesNet classifier on the top
     animal crop + the package's ensemble/geofence (country TZA).
It reuses the worker's model wrappers (worker/forgegrid_worker), so the numbers
describe exactly the code that runs in the containers.

Writes benchmarks/baseline_predictions.csv (resumable: rows already there are
skipped) and docs/baseline.md.

Usage:
  python scripts/baseline.py               # all images in labels.csv
  python scripts/baseline.py --limit 300   # fixed, seeded subset
"""

import argparse
import csv
import random
import sys
import time
import warnings
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "worker"))
warnings.filterwarnings("ignore")

from forgegrid_worker.labels import final_category, top_animal  # noqa: E402
from forgegrid_worker.storage import open_rgb  # noqa: E402

SAMPLE_DIR = ROOT / "data" / "sample"
PRED_CSV = ROOT / "benchmarks" / "baseline_predictions.csv"
REPORT = ROOT / "docs" / "baseline.md"
MODEL = "kaggle:google/speciesnet/pyTorch/v4.0.3a/1"
THRESHOLD = 0.2
COUNTRY = "TZA"

FIELDS = [
    "filename", "gt_common_name", "gt_is_empty", "pred_category",
    "top_det_label", "top_det_conf", "species_label", "species_common_name",
    "species_conf", "prediction_source", "detect_ms", "classify_ms", "total_ms",
]


def load_rows(limit: int | None, seed: int) -> list[dict]:
    with open(SAMPLE_DIR / "labels.csv", newline="") as f:
        rows = list(csv.DictReader(f))
    if limit:
        rows = random.Random(seed).sample(rows, limit)
    return rows


def run(rows: list[dict]) -> float:
    """Run the models over rows not yet in PRED_CSV. Returns model load seconds."""
    import torch

    from forgegrid_worker.classifier import SpeciesNetClassifierModel
    from forgegrid_worker.detector import SpeciesNetDetectorModel

    done = set()
    if PRED_CSV.exists():
        with open(PRED_CSV, newline="") as f:
            done = {r["filename"] for r in csv.DictReader(f)}
    todo = [r for r in rows if r["filename"] not in done]
    print(f"{len(rows)} images selected, {len(done)} already predicted, {len(todo)} to run")
    if not todo:
        return 0.0

    t0 = time.perf_counter()
    detector = SpeciesNetDetectorModel(MODEL, device="cpu")
    classifier = SpeciesNetClassifierModel(MODEL, geofence=True, device="cpu")
    load_s = time.perf_counter() - t0
    print(f"models loaded in {load_s:.1f}s, torch threads={torch.get_num_threads()}")

    PRED_CSV.parent.mkdir(parents=True, exist_ok=True)
    new_file = not PRED_CSV.exists()
    with open(PRED_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if new_file:
            writer.writeheader()
        started = time.perf_counter()
        for i, row in enumerate(todo, 1):
            t_start = time.perf_counter()
            img = open_rgb((SAMPLE_DIR / row["filename"]).read_bytes())
            t_det = time.perf_counter()
            detections = detector.detect(img)
            detect_ms = (time.perf_counter() - t_det) * 1000

            category = final_category(detections, THRESHOLD)
            top = detections[0] if detections else None
            out = {
                "filename": row["filename"],
                "gt_common_name": row["common_name"],
                "gt_is_empty": row["is_empty"],
                "pred_category": category,
                "top_det_label": top["label"] if top else "",
                "top_det_conf": top["conf"] if top else "",
                "species_label": "", "species_common_name": "", "species_conf": "",
                "prediction_source": "", "classify_ms": "",
            }
            if category == "animal":
                t_cls = time.perf_counter()
                result = classifier.classify(img, detections, COUNTRY)
                out.update(
                    species_label=result["label"],
                    species_common_name=result["commonName"],
                    species_conf=result["confidence"],
                    prediction_source=result["raw"]["predictionSource"],
                    classify_ms=round((time.perf_counter() - t_cls) * 1000),
                )
            out["detect_ms"] = round(detect_ms)
            out["total_ms"] = round((time.perf_counter() - t_start) * 1000)
            writer.writerow(out)
            f.flush()
            if i % 25 == 0 or i == len(todo):
                rate = i / (time.perf_counter() - started)
                eta = (len(todo) - i) / rate
                print(f"  {i}/{len(todo)}  {rate:.2f} img/s  eta {eta / 60:.1f} min", flush=True)
    return load_s


def pct(n: int, d: int) -> str:
    return f"{100 * n / d:.1f}%" if d else "n/a"


def report(rows: list[dict], load_s: float, subset_note: str) -> str:
    wanted = {r["filename"] for r in rows}
    with open(PRED_CSV, newline="") as f:
        preds = [p for p in csv.DictReader(f) if p["filename"] in wanted]
    missing = len(wanted) - len(preds)

    n = len(preds)
    gt_animal = [p for p in preds if p["gt_is_empty"] == "False"]
    gt_empty = [p for p in preds if p["gt_is_empty"] == "True"]
    pred_animal = lambda p: p["pred_category"] == "animal"  # noqa: E731

    # Variant: trust the stage 2 ensemble when it says the crop is blank.
    ens_animal = lambda p: pred_animal(p) and p["species_common_name"] != "blank"  # noqa: E731
    ens_correct = sum(ens_animal(p) for p in gt_animal) + sum(not ens_animal(p) for p in gt_empty)

    tp = sum(pred_animal(p) for p in gt_animal)
    fn = len(gt_animal) - tp
    fp = sum(pred_animal(p) for p in gt_empty)
    tn = len(gt_empty) - fp
    binary_acc = (tp + tn) / n

    species_correct = sum(p["species_common_name"] == p["gt_common_name"] for p in gt_animal)
    detected = [p for p in gt_animal if pred_animal(p)]
    species_correct_detected = sum(p["species_common_name"] == p["gt_common_name"] for p in detected)

    total_s = sum(float(p["total_ms"]) for p in preds) / 1000
    det_ms = sorted(float(p["detect_ms"]) for p in preds)
    cls_ms = sorted(float(p["classify_ms"]) for p in preds if p["classify_ms"])
    median = lambda xs: xs[len(xs) // 2] if xs else 0  # noqa: E731

    per_species = defaultdict(Counter)
    for p in gt_animal:
        per_species[p["gt_common_name"]]["n"] += 1
        per_species[p["gt_common_name"]]["detected"] += pred_animal(p)
        per_species[p["gt_common_name"]]["correct"] += p["species_common_name"] == p["gt_common_name"]
        if pred_animal(p) and p["species_common_name"] != p["gt_common_name"]:
            per_species[p["gt_common_name"]][f"→{p['species_common_name']}"] += 1

    categories = Counter(p["pred_category"] for p in preds)

    lines = [
        "# Phase 1 baseline: single-process SpeciesNet",
        "",
        f"Generated by `scripts/baseline.py` on {time.strftime('%Y-%m-%d')}. {subset_note}",
        "",
        "Pipeline (identical to the distributed workers): MegaDetector v5a on every image; if any "
        f"animal box has conf >= {THRESHOLD}, the SpeciesNet v4.0.3a classifier runs on the top animal "
        f"crop and the package's ensemble + geofence (country `{COUNTRY}`) picks the label. "
        "Model `kaggle:google/speciesnet/pyTorch/v4.0.3a/1`, speciesnet 5.0.5, PyTorch on CPU "
        "(Apple M2 host, one process, default torch threads). Predictions: `benchmarks/baseline_predictions.csv`.",
        "",
        "## Results",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Images evaluated | {n}{f' ({missing} missing!)' if missing else ''} |",
        f"| Ground truth | {len(gt_empty)} empty, {len(gt_animal)} animal |",
        f"| Predicted categories | {', '.join(f'{k} {v}' for k, v in categories.most_common())} |",
        f"| **Empty-vs-animal accuracy** | **{pct(tp + tn, n)}** |",
        f"| Empty-vs-animal accuracy if stage 2's ensemble `blank` verdict also counts as empty | {pct(ens_correct, n)} |",
        f"| Animal recall (animal images sent to stage 2) | {pct(tp, len(gt_animal))} ({tp}/{len(gt_animal)}) |",
        f"| Animal precision | {pct(tp, tp + fp)} ({tp}/{tp + fp}) |",
        f"| Empty images correctly filtered | {pct(tn, len(gt_empty))} ({tn}/{len(gt_empty)}) |",
        f"| **Top-1 species accuracy** (all animal images; missed = wrong) | **{pct(species_correct, len(gt_animal))}** ({species_correct}/{len(gt_animal)}) |",
        f"| Top-1 species accuracy on images the detector found | {pct(species_correct_detected, len(detected))} ({species_correct_detected}/{len(detected)}) |",
        f"| **Single-process throughput** | **{n / total_s:.2f} images/sec** ({total_s / n:.2f} s/image) |",
        f"| Median detector latency | {median(det_ms):.0f} ms |",
        f"| Median classifier latency (stage 2 images only) | {median(cls_ms):.0f} ms |",
        f"| Model load time | {load_s:.1f} s |" if load_s else "| Model load time | (resumed run; see first run's log) |",
        "",
        "Throughput counts image decode + detector for every image and classifier for the ones",
        "sent to stage 2 (the same work the distributed pipeline does); model load is excluded.",
        "",
        "## Per species",
        "",
        "| Species | Images | Detected | Correct species | Common confusions |",
        "|---|---|---|---|---|",
    ]
    for name in sorted(per_species):
        c = per_species[name]
        confusions = sorted(((k[1:], v) for k, v in c.items() if k.startswith("→")), key=lambda kv: -kv[1])
        conf_txt = ", ".join(f"{k} ({v})" for k, v in confusions[:3])
        lines.append(f"| {name} | {c['n']} | {c['detected']} | {pct(c['correct'], c['n'])} | {conf_txt} |")

    lines += [
        "",
        "## How labels are compared",
        "",
        "Ground truth is the Snapshot Serengeti sequence label (`common_name` in `data/sample/labels.csv`:",
        "`gazellethomsons`/`gazellegrants` → gazelle, `lionfemale`/`lionmale` → lion, `hyenaspotted`/`hyenastriped` → hyena).",
        "SpeciesNet predictions go through `forgegrid_worker.labels.common_name()`, which maps the taxonomy",
        "string by genus/family: any zebra → zebra, *Connochaetes* → wildebeest, *Panthera leo* → lion,",
        "Elephantidae → elephant, Giraffidae → giraffe, *Eudorcas*/*Nanger*/*Gazella* → gazelle, *Syncerus* → buffalo,",
        "*Crocuta*/*Hyaena*/*Parahyaena* (or the Hyaenidae roll-up) → hyena, *Phacochoerus* → warthog, *Aepyceros* → impala.",
        "Anything else keeps SpeciesNet's own common name (e.g. `bovidae family`, `equus`, `animal`, `blank`) and counts as wrong.",
        "",
        "Caveat: Snapshot Serengeti labels are per *sequence* (burst of ~3 frames) and we sampled one random",
        "frame per sequence, so some \"animal\" ground-truth frames genuinely show no animal. That caps the",
        "achievable animal recall below 100% and makes this a conservative estimate.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--limit", type=int, default=None, help="evaluate a seeded random subset")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--report-only", action="store_true")
    args = parser.parse_args()

    rows = load_rows(args.limit, args.seed)
    load_s = 0.0 if args.report_only else run(rows)
    note = (
        f"Evaluated a fixed random subset of {args.limit} images (seed {args.seed}) of the {sum(1 for _ in open(SAMPLE_DIR / 'labels.csv')) - 1}-image sample."
        if args.limit
        else "Evaluated every image in `data/sample/labels.csv`."
    )
    text = report(rows, load_s, note)
    REPORT.write_text(text)
    print("\n" + text)


if __name__ == "__main__":
    main()
