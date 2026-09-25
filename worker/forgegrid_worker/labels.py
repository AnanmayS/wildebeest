"""Label helpers shared by the workers, the fake backend and scripts/baseline.py.

SpeciesNet labels are 7-field taxonomy strings:
    "<uuid>;<class>;<order>;<family>;<genus>;<species>;<common name>"
e.g. "e2895ed5-...;mammalia;perissodactyla;equidae;equus;quagga;plains zebra".
The full string is stored in the DB; the dashboard shows `common_name(label)`.
"""

ANIMAL, HUMAN, VEHICLE = "animal", "human", "vehicle"

# The 10 species the sample dataset is balanced across (see scripts/download_sample.py).
TARGET_SPECIES = [
    "zebra", "wildebeest", "lion", "elephant", "giraffe",
    "gazelle", "buffalo", "hyena", "warthog", "impala",
]

# (taxonomy field, value, common name). First match wins. These collapse SpeciesNet's
# fine-grained labels ("plains zebra", "thomson's gazelle", "spotted hyaena", ...) and
# its genus/family roll-ups ("equus species", "giraffidae family") into the names
# used by the Snapshot Serengeti ground truth.
_TAXON_RULES = [
    ("genus", "connochaetes", "wildebeest"),
    ("family", "elephantidae", "elephant"),
    ("family", "giraffidae", "giraffe"),
    ("genus", "eudorcas", "gazelle"),
    ("genus", "nanger", "gazelle"),
    ("genus", "gazella", "gazelle"),
    ("genus", "syncerus", "buffalo"),
    ("genus", "crocuta", "hyena"),
    ("genus", "hyaena", "hyena"),
    ("genus", "parahyaena", "hyena"),
    ("genus", "phacochoerus", "warthog"),
    ("genus", "aepyceros", "impala"),
]
_FIELDS = ["uuid", "class", "order", "family", "genus", "species", "common"]

# Labels with a special meaning in SpeciesNet (speciesnet.constants.Classification).
_SPECIAL = {
    "blank": "blank",
    "animal": "animal",
    "human": "human",
    "vehicle": "vehicle",
    "no cv result": "unknown",
}


def common_name(label: str) -> str:
    """Turn a SpeciesNet taxonomy string into a short display name, e.g. 'zebra'."""
    parts = label.strip().lower().split(";")
    if len(parts) != len(_FIELDS):
        return label.strip().lower()
    taxon = dict(zip(_FIELDS, parts))
    common = taxon["common"]

    if common in _SPECIAL:
        return _SPECIAL[common]
    if "zebra" in common:
        return "zebra"
    if taxon["genus"] == "panthera" and taxon["species"] == "leo":
        return "lion"
    if taxon["family"] == "hyaenidae" and not taxon["genus"]:
        return "hyena"  # "hyaenidae family" roll-up
    for field, value, name in _TAXON_RULES:
        if taxon[field] == value:
            return name
    return common.removesuffix(" species")


def sort_detections(detections: list[dict]) -> list[dict]:
    """Highest-confidence first (the order MegaDetector/SpeciesNet use)."""
    return sorted(detections or [], key=lambda d: d["conf"], reverse=True)


def top_animal(detections: list[dict]) -> dict | None:
    """The most confident animal detection, or None."""
    animals = [d for d in detections or [] if d["label"] == ANIMAL]
    return max(animals, key=lambda d: d["conf"]) if animals else None


def final_category(detections: list[dict], threshold: float) -> str:
    """Stage 1 categorisation, same rule as the coordinator (docs/CONTRACTS.md).

    'animal' means "send to the classifier"; otherwise the image is final.
    """
    for label in (ANIMAL, HUMAN, VEHICLE):
        if any(d["label"] == label and d["conf"] >= threshold for d in detections or []):
            return label
    return "empty"
