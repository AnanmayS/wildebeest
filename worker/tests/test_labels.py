import pytest

from wildebeest_worker.fake import FAKE_LABELS
from wildebeest_worker.labels import TARGET_SPECIES, common_name, final_category, top_animal

UUID = "00000000-0000-0000-0000-000000000000"


@pytest.mark.parametrize(
    "taxonomy, expected",
    [
        ("mammalia;perissodactyla;equidae;equus;quagga;plains zebra", "zebra"),
        ("mammalia;perissodactyla;equidae;equus;grevyi;grevy's zebra", "zebra"),
        ("mammalia;artiodactyla;bovidae;connochaetes;taurinus;common wildebeest", "wildebeest"),
        ("mammalia;carnivora;felidae;panthera;leo;lion", "lion"),
        ("mammalia;carnivora;felidae;panthera;pardus;leopard", "leopard"),
        ("mammalia;proboscidea;elephantidae;loxodonta;africana;african elephant", "elephant"),
        ("mammalia;artiodactyla;giraffidae;giraffa;camelopardalis;giraffe", "giraffe"),
        ("mammalia;artiodactyla;giraffidae;;;giraffidae family", "giraffe"),
        ("mammalia;artiodactyla;bovidae;eudorcas;thomsonii;thomson's gazelle", "gazelle"),
        ("mammalia;artiodactyla;bovidae;nanger;granti;grant's gazelle", "gazelle"),
        ("mammalia;artiodactyla;bovidae;syncerus;caffer;african buffalo", "buffalo"),
        ("mammalia;carnivora;hyaenidae;crocuta;crocuta;spotted hyaena", "hyena"),
        ("mammalia;carnivora;hyaenidae;;;hyaenidae family", "hyena"),
        ("mammalia;carnivora;hyaenidae;proteles;cristata;aardwolf", "aardwolf"),
        ("mammalia;artiodactyla;suidae;phacochoerus;africanus;common warthog", "warthog"),
        ("mammalia;artiodactyla;bovidae;aepyceros;melampus;impala", "impala"),
        ("mammalia;primates;cercopithecidae;papio;;baboon species", "baboon"),
        ("mammalia;artiodactyla;bovidae;damaliscus;lunatus;topi", "topi"),
    ],
)
def test_common_name_maps_taxonomy(taxonomy, expected):
    assert common_name(f"{UUID};{taxonomy}") == expected


@pytest.mark.parametrize(
    "label, expected",
    [
        ("f1856211-cfb7-4a5b-9158-c0f72fd09ee6;;;;;;blank", "blank"),
        ("1f689929-883d-4dae-958c-3d57ab5b6c16;;;;;;animal", "animal"),
        ("990ae9dd-7a59-4344-afcb-1b7b21368000;mammalia;primates;hominidae;homo;sapiens;human", "human"),
        ("e2895ed5-780b-48f6-8a11-9e27cb594511;;;;;;vehicle", "vehicle"),
        ("f2efdae9-efb8-48fb-8a91-eccf79ab4ffb;no cv result;no cv result;no cv result;no cv result;no cv result;no cv result", "unknown"),
        ("Not A Taxonomy String", "not a taxonomy string"),
    ],
)
def test_common_name_special_labels(label, expected):
    assert common_name(label) == expected


def test_fake_labels_cover_all_target_species():
    assert sorted(common_name(FAKE_LABELS[s]) for s in TARGET_SPECIES) == sorted(TARGET_SPECIES)


def det(label, conf):
    return {"label": label, "conf": conf, "bbox": [0.1, 0.1, 0.2, 0.2]}


@pytest.mark.parametrize(
    "detections, expected",
    [
        ([], "empty"),
        ([det("animal", 0.19)], "empty"),  # below threshold
        ([det("animal", 0.2)], "animal"),  # threshold is inclusive
        ([det("human", 0.9), det("animal", 0.3)], "animal"),  # any animal wins
        ([det("human", 0.5), det("vehicle", 0.9)], "human"),  # human before vehicle
        ([det("vehicle", 0.5), det("animal", 0.1)], "vehicle"),
        ([det("human", 0.1), det("vehicle", 0.1)], "empty"),
    ],
)
def test_final_category(detections, expected):
    assert final_category(detections, threshold=0.2) == expected


def test_top_animal_ignores_people_and_vehicles():
    dets = [det("human", 0.99), det("animal", 0.4), det("animal", 0.8), det("vehicle", 0.9)]
    assert top_animal(dets)["conf"] == 0.8
    assert top_animal([det("human", 0.9)]) is None
    assert top_animal([]) is None
