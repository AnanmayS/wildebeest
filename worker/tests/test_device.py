"""DEVICE selection and the per-backend channels_last table (no model weights needed)."""

import pytest

torch = pytest.importorskip("torch")

from wildebeest_worker import tuning  # noqa: E402


@pytest.fixture
def gpus(monkeypatch):
    def set_available(cuda: bool, mps: bool) -> None:
        monkeypatch.setattr(torch.cuda, "is_available", lambda: cuda)
        monkeypatch.setattr(tuning, "_mps_available", lambda: mps)

    return set_available


def test_default_is_cpu(monkeypatch, gpus):
    gpus(cuda=True, mps=True)
    monkeypatch.delenv("DEVICE", raising=False)
    assert tuning.resolve_device() == "cpu"


@pytest.mark.parametrize(
    "cuda,mps,expected", [(True, True, "cuda"), (False, True, "mps"), (False, False, "cpu")]
)
def test_auto_prefers_cuda_then_mps(gpus, cuda, mps, expected):
    gpus(cuda=cuda, mps=mps)
    assert tuning.resolve_device("auto") == expected


def test_env_is_read_and_normalised(monkeypatch, gpus):
    gpus(cuda=False, mps=True)
    monkeypatch.setenv("DEVICE", " MPS ")
    assert tuning.resolve_device() == "mps"


@pytest.mark.parametrize("device", ["mps", "cuda"])
def test_explicit_unavailable_device_fails_loudly(gpus, device):
    gpus(cuda=False, mps=False)
    with pytest.raises(RuntimeError):
        tuning.resolve_device(device)


def test_unknown_device_rejected():
    with pytest.raises(ValueError):
        tuning.resolve_device("tpu")


def test_channels_last_table(monkeypatch):
    monkeypatch.delenv("CHANNELS_LAST", raising=False)
    monkeypatch.setattr(tuning.sys, "platform", "linux")  # the containers: unchanged
    assert tuning.use_channels_last("detect", "cpu")
    assert tuning.use_channels_last("classify", "cpu")
    assert not tuning.use_channels_last("detect", "mps")
    assert not tuning.use_channels_last("classify", "mps")
    assert not tuning.use_channels_last("detect", "cuda")
    monkeypatch.setattr(tuning.sys, "platform", "darwin")  # native macOS CPU
    assert tuning.use_channels_last("detect", "cpu")
    assert not tuning.use_channels_last("classify", "cpu")


def test_channels_last_override(monkeypatch):
    monkeypatch.setenv("CHANNELS_LAST", "1")
    assert tuning.use_channels_last("detect", "mps")
    monkeypatch.setenv("CHANNELS_LAST", "0")
    assert not tuning.use_channels_last("detect", "cpu")


def test_tune_converts_only_4d_weights(monkeypatch):
    monkeypatch.setattr(tuning.sys, "platform", "linux")
    monkeypatch.delenv("CHANNELS_LAST", raising=False)
    model = torch.nn.Sequential(torch.nn.Conv2d(3, 4, 3), torch.nn.Flatten(), torch.nn.Linear(4, 2))
    tuning.tune(model, "detect", "cpu")
    assert model[0].weight.is_contiguous(memory_format=torch.channels_last)
    assert model[2].weight.is_contiguous()  # 2-D weights untouched
    assert not model.training


def test_announce_device_does_not_override_launcher(monkeypatch):
    monkeypatch.setenv("WORKER_DEVICE", "mps")
    tuning.announce_device("mps")
    assert tuning.os.environ["WORKER_DEVICE"] == "mps"
    monkeypatch.delenv("WORKER_DEVICE")
    tuning.announce_device("cuda")
    assert tuning.os.environ["WORKER_DEVICE"] == "cuda"
