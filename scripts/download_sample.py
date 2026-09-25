"""Download a balanced Snapshot Serengeti sample from LILA BC into data/sample/.

Source: https://lila.science/datasets/snapshot-serengeti
  - per-season COCO Camera Traps metadata: .../snapshotserengeti-v-2-0/SnapshotSerengetiS{NN}.json.zip
  - individual images:                      .../snapshotserengeti-unzipped/{file_name}

Labels in Snapshot Serengeti are sequence-level (every frame of a burst gets the
sequence's label), so we sample one random frame per sequence and only use
sequences that have exactly one label.

Usage:
  python scripts/download_sample.py --count 2000
Re-running skips images that are already on disk.
"""

import argparse
import csv
import io
import json
import random
import sys
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
SAMPLE_DIR = ROOT / "data" / "sample"
METADATA_DIR = ROOT / "data" / "metadata"

# URLs as published on https://lila.science/datasets/snapshot-serengeti (Azure mirror;
# the GCP mirror at storage.googleapis.com/public-datasets-lila/ has the same layout).
METADATA_URL = (
    "https://lilawildlife.blob.core.windows.net/lila-wildlife/"
    "snapshotserengeti-v-2-0/SnapshotSerengeti{season}.json.zip"
)
IMAGE_BASE_URL = (
    "https://lilawildlife.blob.core.windows.net/lila-wildlife/snapshotserengeti-unzipped/"
)

# Our 10 target species -> the Snapshot Serengeti category names that make them up.
SPECIES = {
    "zebra": ["zebra"],
    "wildebeest": ["wildebeest"],
    "lion": ["lionfemale", "lionmale"],
    "elephant": ["elephant"],
    "giraffe": ["giraffe"],
    "gazelle": ["gazellethomsons", "gazellegrants"],
    "buffalo": ["buffalo"],
    "hyena": ["hyenaspotted", "hyenastriped"],
    "warthog": ["warthog"],
    "impala": ["impala"],
}
EMPTY_FRACTION = 0.7


def load_sequences(season: str) -> dict[str, dict]:
    """Return {seq_id: {"labels": set, "files": [file_name]}} for one season.

    The raw season JSON is ~300 MB, so the first run writes a compact CSV index to
    data/metadata/ and later runs read that instead.
    """
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    index_path = METADATA_DIR / f"serengeti_{season}_index.csv"

    if not index_path.exists():
        zip_path = METADATA_DIR / f"SnapshotSerengeti{season}.json.zip"
        if not zip_path.exists():
            url = METADATA_URL.format(season=season)
            print(f"Downloading metadata {url}")
            resp = requests.get(url, timeout=300)
            resp.raise_for_status()
            zip_path.write_bytes(resp.content)
        print(f"Parsing {zip_path.name} (this takes a moment the first time)")
        with zipfile.ZipFile(zip_path) as zf:
            name = next(n for n in zf.namelist() if n.endswith(".json"))
            data = json.load(io.TextIOWrapper(zf.open(name), encoding="utf-8"))

        categories = {c["id"]: c["name"] for c in data["categories"]}
        labels_by_image: dict[str, set] = defaultdict(set)
        for ann in data["annotations"]:
            labels_by_image[ann["image_id"]].add(categories[ann["category_id"]])

        with open(index_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["seq_id", "file_name", "location", "labels"])
            for img in data["images"]:
                if img.get("corrupt"):
                    continue
                labels = labels_by_image.get(img["id"])
                if not labels:
                    continue
                writer.writerow(
                    [img["seq_id"], img["file_name"], img["location"], "|".join(sorted(labels))]
                )
        del data

    sequences: dict[str, dict] = {}
    with open(index_path, newline="") as f:
        for row in csv.DictReader(f):
            seq = sequences.setdefault(row["seq_id"], {"labels": set(), "files": []})
            seq["labels"].update(row["labels"].split("|"))
            seq["files"].append(row["file_name"])
    return sequences


def choose_sample(sequences: dict[str, dict], count: int, rng: random.Random) -> list[dict]:
    """Pick ~70% empty and ~30% animals spread evenly across SPECIES."""
    raw_to_common = {raw: common for common, raws in SPECIES.items() for raw in raws}

    by_class: dict[str, list[tuple[str, str]]] = defaultdict(list)  # class -> [(seq_id, raw)]
    for seq_id in sorted(sequences):  # sorted so the seed alone decides the sample
        labels = sequences[seq_id]["labels"]
        if len(labels) != 1:
            continue  # mixed-species or disputed sequences make poor ground truth
        (raw,) = labels
        if raw == "empty":
            by_class["empty"].append((seq_id, raw))
        elif raw in raw_to_common:
            by_class[raw_to_common[raw]].append((seq_id, raw))

    n_empty = round(count * EMPTY_FRACTION)
    n_animals = count - n_empty
    per_species, extra = divmod(n_animals, len(SPECIES))

    picks: list[tuple[str, str, str]] = []  # (seq_id, raw, common)
    for seq_id, raw in rng.sample(by_class["empty"], n_empty):
        picks.append((seq_id, raw, "empty"))
    for i, common in enumerate(SPECIES):
        want = per_species + (1 if i < extra else 0)
        pool = by_class[common]
        if len(pool) < want:
            sys.exit(f"Only {len(pool)} single-label sequences for {common}, need {want}")
        for seq_id, raw in rng.sample(pool, want):
            picks.append((seq_id, raw, common))

    sample = []
    for seq_id, raw, common in picks:
        file_name = rng.choice(sorted(sequences[seq_id]["files"]))
        sample.append(
            {
                # S1/B04/B04_R1/S1_B04_R1_PICT0001.JPG -> S1_B04_R1_PICT0001.jpg
                "filename": Path(file_name).stem + ".jpg",
                "label": raw,
                "common_name": common,
                "is_empty": common == "empty",
                "seq_id": seq_id,
                "source_url": IMAGE_BASE_URL + file_name,
            }
        )
    rng.shuffle(sample)
    return sample


def download(row: dict, session: requests.Session) -> bool:
    dest = SAMPLE_DIR / row["filename"]
    if dest.exists() and dest.stat().st_size > 0:
        return True
    for _ in range(3):
        try:
            resp = session.get(row["source_url"], timeout=60)
            resp.raise_for_status()
            tmp = dest.with_suffix(".part")
            tmp.write_bytes(resp.content)
            tmp.rename(dest)
            return True
        except requests.RequestException as e:
            err = e
    print(f"  failed {row['filename']}: {err}")
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--season", default="S01", help="Snapshot Serengeti season, e.g. S01")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--workers", type=int, default=16, help="parallel downloads")
    args = parser.parse_args()

    sequences = load_sequences(args.season)
    print(f"{len(sequences):,} sequences in season {args.season}")
    sample = choose_sample(sequences, args.count, random.Random(args.seed))

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    ok_rows = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(download, row, session): row for row in sample}
        for i, fut in enumerate(as_completed(futures), 1):
            if fut.result():
                ok_rows.append(futures[fut])
            if i % 100 == 0 or i == len(sample):
                print(f"  {i}/{len(sample)} images")

    ok_rows.sort(key=lambda r: r["filename"])
    with open(SAMPLE_DIR / "labels.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(sample[0].keys()))
        writer.writeheader()
        writer.writerows(ok_rows)

    counts = Counter(r["common_name"] for r in ok_rows)
    print(f"\nWrote {len(ok_rows)} images to {SAMPLE_DIR} ({len(sample) - len(ok_rows)} failed)")
    for name, n in counts.most_common():
        print(f"  {name:12s} {n}")


if __name__ == "__main__":
    main()
