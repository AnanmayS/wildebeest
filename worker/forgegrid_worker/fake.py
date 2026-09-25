"""MODEL_BACKEND=fake: deterministic stand-ins for the real models.

Output depends only on the image sha256, so reruns and retries agree, and the
coordinator can be tested in seconds without model weights. Same interface as
SpeciesNetDetectorModel / SpeciesNetClassifierModel.
"""

import hashlib
import random
import time

from .labels import ANIMAL, HUMAN, TARGET_SPECIES, VEHICLE, common_name, top_animal

# Real SpeciesNet taxonomy strings, so common_name() and the DB look like the real thing.
FAKE_LABELS = {
    "zebra": "dd39bbd5-077c-482e-9d33-bd176116c870;mammalia;perissodactyla;equidae;equus;quagga;plains zebra",
    "wildebeest": "09fbf931-bbf0-4959-9df6-1082db578281;mammalia;artiodactyla;bovidae;connochaetes;taurinus;common wildebeest",
    "lion": "ddf59264-185a-4d35-b647-2785792bdf54;mammalia;carnivora;felidae;panthera;leo;lion",
    "elephant": "55631055-3e0e-4b7a-9612-dedebe9f78b0;mammalia;proboscidea;elephantidae;loxodonta;africana;african elephant",
    "giraffe": "2dca052b-dff5-4cc9-8072-1282c5713286;mammalia;artiodactyla;giraffidae;giraffa;camelopardalis;giraffe",
    "gazelle": "dc5dbe17-a8ca-40a6-ac6a-3b6b1d63e6d6;mammalia;artiodactyla;bovidae;eudorcas;thomsonii;thomson's gazelle",
    "buffalo": "9732cefb-6a08-49f6-b61e-b9a9054368c4;mammalia;artiodactyla;bovidae;syncerus;caffer;african buffalo",
    "hyena": "dce8d520-a3f6-4ed7-a434-bfe98f81a03d;mammalia;carnivora;hyaenidae;crocuta;crocuta;spotted hyaena",
    "warthog": "ccd7d6d7-8eb2-4fdb-a6d6-f1970847e449;mammalia;artiodactyla;suidae;phacochoerus;africanus;common warthog",
    "impala": "c134e0ab-cf96-45ec-bae6-60b94995f71b;mammalia;artiodactyla;bovidae;aepyceros;melampus;impala",
}


def _rng(sha256: str, salt: str) -> random.Random:
    seed = hashlib.sha256(f"{salt}:{sha256}".encode()).digest()
    return random.Random(int.from_bytes(seed[:8], "big"))


def _bbox(rng: random.Random) -> list[float]:
    w, h = rng.uniform(0.1, 0.4), rng.uniform(0.1, 0.4)
    x, y = rng.uniform(0, 1 - w), rng.uniform(0, 1 - h)
    return [round(v, 4) for v in (x, y, w, h)]


def fake_detections(sha256: str) -> list[dict]:
    """~70% empty, ~27% animal, ~1.5% human, ~1.5% vehicle."""
    rng = _rng(sha256, "detect")
    roll = rng.random()
    if roll < 0.70:
        # Mostly nothing; sometimes a low-confidence box below the 0.2 threshold.
        return [{"label": ANIMAL, "conf": round(rng.uniform(0.1, 0.19), 4), "bbox": _bbox(rng)}] if roll < 0.2 else []
    if roll < 0.97:
        label = ANIMAL
    elif roll < 0.985:
        label = HUMAN
    else:
        label = VEHICLE
    return [{"label": label, "conf": round(rng.uniform(0.5, 0.98), 4), "bbox": _bbox(rng)}]


def fake_classification(sha256: str) -> dict:
    rng = _rng(sha256, "classify")
    label = FAKE_LABELS[rng.choice(TARGET_SPECIES)]
    return {
        "label": label,
        "commonName": common_name(label),
        "confidence": round(rng.uniform(0.6, 0.99), 4),
        "raw": {"predictionSource": "fake"},
    }


class FakeDetector:
    def __init__(self, delay_ms: int) -> None:
        self.delay_s = delay_ms / 1000

    def detect(self, img, sha256: str) -> list[dict]:
        time.sleep(self.delay_s)
        return fake_detections(sha256)


class FakeClassifier:
    def __init__(self, delay_ms: int) -> None:
        self.delay_s = delay_ms / 1000

    def classify(self, img, detections: list[dict], country: str, sha256: str) -> dict:
        time.sleep(self.delay_s)
        if top_animal(detections) is None:
            raise ValueError("classify task without an animal detection")
        return fake_classification(sha256)
