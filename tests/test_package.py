import numpy as np
import pytest
import torch

from ml.deeplog import DeepLog, session_max_rank
from ml.novelty import CountNovelty
from ml.package import ModelConfig, ModelPackage, load, save


def _package(name="hdfs-test"):
    config = ModelConfig(
        name=name,
        vocab=["PAD", "E1", "E2", "E3"],
        eos=4,
        h=3,
        g=2,
        hidden=8,
        layers=1,
        commit="abc",
    )
    torch.manual_seed(0)
    model = DeepLog(config.vocab_size, hidden=8, layers=1)
    novelty = CountNovelty().fit(np.array([[0, 1, 2, 0, 1], [0, 2, 0, 1, 1]], dtype=np.float32))
    return ModelPackage(config, model, novelty, {"val_f1": 0.75})


def test_roundtrip_preserves_model_novelty_and_config(tmp_path):
    pkg = _package()
    path = save(pkg, tmp_path)
    loaded = load(path)
    assert loaded.config == pkg.config and loaded.metrics == {"val_f1": 0.75}
    seqs = [np.array([1, 2, 3, 4]), np.array([2, 2, 4])]
    assert np.array_equal(
        session_max_rank(pkg.deeplog, seqs, 3), session_max_rank(loaded.deeplog, seqs, 3)
    )
    X = np.array([[0, 1, 2, 0, 1], [9, 9, 9, 9, 9]], dtype=np.float32)
    assert list(loaded.novelty.predict(X)) == [False, True]


def test_versions_are_immutable(tmp_path):
    save(_package(), tmp_path)
    with pytest.raises(FileExistsError):
        save(_package(), tmp_path)
