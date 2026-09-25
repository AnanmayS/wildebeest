import hashlib
from collections import Counter

import pytest

from wildebeest_worker.fake import FakeClassifier, FakeDetector, fake_classification, fake_detections
from wildebeest_worker.labels import TARGET_SPECIES, final_category


def sha(i: int) -> str:
    return hashlib.sha256(str(i).encode()).hexdigest()


def test_same_sha_gives_same_answer():
    for i in range(50):
        assert fake_detections(sha(i)) == fake_detections(sha(i))
        assert fake_classification(sha(i)) == fake_classification(sha(i))


def test_category_mix_is_roughly_70_percent_empty():
    counts = Counter(final_category(fake_detections(sha(i)), 0.2) for i in range(5000))
    assert 0.66 < counts["empty"] / 5000 < 0.74
    assert 0.23 < counts["animal"] / 5000 < 0.31
    assert counts["human"] > 0 and counts["vehicle"] > 0


def test_detections_are_well_formed():
    for i in range(500):
        for d in fake_detections(sha(i)):
            assert d["label"] in {"animal", "human", "vehicle"}
            assert 0 <= d["conf"] <= 1
            x, y, w, h = d["bbox"]
            assert 0 <= x and 0 <= y and x + w <= 1 and y + h <= 1


def test_classifications_use_the_ten_target_species():
    names = {fake_classification(sha(i))["commonName"] for i in range(500)}
    assert names == set(TARGET_SPECIES)


def test_fake_models_sleep_and_match_pure_functions():
    detector, classifier = FakeDetector(delay_ms=0), FakeClassifier(delay_ms=0)
    s = next(sha(i) for i in range(100) if final_category(fake_detections(sha(i)), 0.2) == "animal")
    dets = detector.detect(None, sha256=s)
    assert dets == fake_detections(s)
    assert classifier.classify(None, dets, "TZA", sha256=s) == fake_classification(s)


def test_fake_classifier_rejects_task_without_animal():
    with pytest.raises(ValueError):
        FakeClassifier(delay_ms=0).classify(None, [], "TZA", sha256=sha(1))
