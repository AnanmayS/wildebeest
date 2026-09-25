"""Stage 1: MegaDetector (via SpeciesNet's detector component) on one image.

SpeciesNet API used (speciesnet 5.0.5, speciesnet/detector.py):
    det = SpeciesNetDetector(model_name)          # loads MegaDetector v5a weights
    out = det.predict(filepath, det.preprocess(pil_image))
    out["detections"] = [{"category", "label", "conf", "bbox": [x, y, w, h]}, ...]
bbox is normalised to 0-1 with a top-left origin; detections come sorted by conf.
"""

import os

import PIL.Image

from .labels import sort_detections

# SpeciesNet keeps every box above 0.01. The coordinator only acts on boxes above
# ANIMAL_CONF_THRESHOLD (0.2), so we drop the long tail to keep payloads small.
MIN_DETECTION_CONF = float(os.environ.get("MIN_DETECTION_CONF", "0.1"))

# MegaDetector's native input size is 1280. 640 is ~2.5x faster on CPU and was as accurate
# on our Serengeti sample (docs/DECISIONS.md). If you change it, change
# DETECTOR_MODEL_VERSION too so cached 1280 results are not reused.
DETECTOR_IMG_SIZE = int(os.environ.get("DETECTOR_IMG_SIZE", "1280"))


class SpeciesNetDetectorModel:
    def __init__(self, model_name: str, device: str = "cpu", img_size: int = DETECTOR_IMG_SIZE) -> None:
        from speciesnet import SpeciesNetDetector

        # preprocess() reads this class constant; there is no constructor argument for it.
        SpeciesNetDetector.IMG_SIZE = img_size
        self._det = SpeciesNetDetector(model_name)
        # SpeciesNetDetector picks cuda/mps itself and has no device argument.
        # Force CPU so a Mac host behaves like the (CPU-only) containers.
        if self._det.device != device:
            self._det.model = self._det.model.to(device)
            self._det.device = device

    def detect(self, img: PIL.Image.Image, sha256: str | None = None) -> list[dict]:
        out = self._det.predict("image", self._det.preprocess(img))
        if "failures" in out:
            raise RuntimeError(f"detector failed: {out['failures']}")
        return [
            {
                "label": d["label"],
                "conf": round(float(d["conf"]), 4),
                "bbox": [round(float(v), 4) for v in d["bbox"]],
            }
            for d in out["detections"]
            if d["conf"] >= MIN_DETECTION_CONF
        ]


def make_detect_handler(model, storage, model_version: str):
    """Returns handle(lease) -> DetectResult (runtime adds latencyMs)."""

    def handle(lease: dict) -> dict:
        img = storage.get_image(lease["imageKey"])
        detections = sort_detections(model.detect(img, sha256=lease["sha256"]))
        return {"modelVersion": model_version, "detections": detections}

    return handle
