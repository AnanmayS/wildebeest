"""Stage 2: SpeciesNet species classifier + ensemble/geofence on the top animal crop.

Uses the stage 1 detections from the lease; the detector is never re-run.

SpeciesNet API used (speciesnet 5.0.5):
    cls = SpeciesNetClassifier(model_name, device="cpu")
    pre = cls.preprocess(pil_image, bboxes=[BBox(x, y, w, h)])   # "always_crop" model:
                                                                 # crops to bboxes[0]
    out = cls.predict(filepath, pre)   # {"classifications": {"classes": [5], "scores": [5]}}
    ens = SpeciesNetEnsemble(model_name, geofence=True)
    [pred] = ens.combine(filepaths, classifier_results, detector_results,
                         geolocation_results, partial_predictions)
    pred["prediction"], pred["prediction_score"], pred["prediction_source"]
The ensemble works per image, so every task gets the package's own ensemble +
geofencing (countryCode from the lease, default TZA).
"""

import io
import os

import PIL.Image

from .labels import common_name, sort_detections, top_animal

DEFAULT_COUNTRY = os.environ.get("DEFAULT_COUNTRY", "TZA")
CROP_SIZE = 256


class SpeciesNetClassifierModel:
    def __init__(self, model_name: str, geofence: bool = True, device: str = "cpu") -> None:
        from speciesnet import SpeciesNetClassifier, SpeciesNetEnsemble

        self._cls = SpeciesNetClassifier(model_name, device=device)
        self._ens = SpeciesNetEnsemble(model_name, geofence=geofence)

    def classify(
        self, img: PIL.Image.Image, detections: list[dict], country: str, sha256: str | None = None
    ) -> dict:
        from speciesnet import BBox

        detections = sort_detections(detections)
        animal = top_animal(detections)
        bboxes = [BBox(*animal["bbox"])] if animal else []

        key = "image"
        out = self._cls.predict(key, self._cls.preprocess(img, bboxes=bboxes))
        if "failures" in out:
            raise RuntimeError(f"classifier failed: {out['failures']}")

        [pred] = self._ens.combine(
            filepaths=[key],
            classifier_results={key: out},
            detector_results={key: {"detections": detections}},
            geolocation_results={key: {"country": country}},
            partial_predictions={},
        )
        top5 = out["classifications"]
        label = pred["prediction"]
        return {
            "label": label,
            "commonName": common_name(label),
            "confidence": round(float(pred["prediction_score"]), 4),
            "raw": {
                "predictionSource": pred["prediction_source"],
                "country": country,
                "top5": [
                    {"label": c, "score": round(float(s), 4)}
                    for c, s in zip(top5["classes"], top5["scores"])
                ],
            },
        }


def crop_key_for(sha256: str, model_version: str) -> str:
    return f"crops/{sha256}_{model_version.replace('/', '_')}.jpg"


def make_crop_jpeg(img: PIL.Image.Image, bbox: list[float] | None) -> bytes:
    """A ~256px JPEG thumbnail of the bbox (or the whole image if there is none)."""
    if bbox:
        x, y, w, h = bbox
        W, H = img.size
        box = (int(x * W), int(y * H), int((x + w) * W), int((y + h) * H))
        if box[2] > box[0] and box[3] > box[1]:
            img = img.crop(box)
    img = img.copy()
    img.thumbnail((CROP_SIZE, CROP_SIZE))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def make_classify_handler(model, storage, model_version: str):
    """Returns handle(lease) -> ClassifyResult (runtime adds latencyMs)."""

    def handle(lease: dict) -> dict:
        img = storage.get_image(lease["imageKey"])
        detections = sort_detections(lease.get("detections") or [])
        country = lease.get("countryCode") or DEFAULT_COUNTRY
        result = model.classify(img, detections, country, sha256=lease["sha256"])

        animal = top_animal(detections)
        crop_key = crop_key_for(lease["sha256"], model_version)
        storage.put_jpeg(crop_key, make_crop_jpeg(img, animal["bbox"] if animal else None))

        return {"modelVersion": model_version, **result, "cropKey": crop_key}

    return handle
