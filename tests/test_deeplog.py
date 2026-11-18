import numpy as np
import pytest
import torch

from ml.dataset import PAD
from ml.deeplog import DeepLog, detect, train, windows


def test_windows_pad_every_event_as_target_and_dedupe_with_weights():
    X, y, w = windows([np.array([1, 2]), np.array([1, 2])], h=3)
    # her olay hedef; solda h PAD; iki özdeş dizi -> tekilleşir, ağırlık 2
    rows = {(tuple(x), int(t)): float(c) for x, t, c in zip(X, y, w, strict=True)}
    assert rows == {((PAD, PAD, PAD), 1): 2.0, ((PAD, PAD, 1), 2): 2.0}
    assert X.dtype == np.int64 and y.dtype == np.int64 and w.dtype == np.float32
    assert w.sum() == 4  # toplam pencere sayısı korunur


def _trained(seed=0):
    normal = [np.array([1, 2, 3] * 4)] * 5
    torch.manual_seed(seed)  # ilk ağırlıklar
    model = DeepLog(vocab_size=5)
    losses = train(model, *windows(normal, h=3), epochs=60, batch_size=64, lr=2e-2, seed=seed)
    return model, losses


def test_learns_pattern_and_flags_broken_or_unknown():
    model, losses = _trained()
    assert losses[-1] < losses[0]
    result = detect(
        model,
        [np.array([1, 2, 3, 1, 2, 3]), np.array([1, 2, 3, 2, 1, 3]), np.array([1, 2, PAD, 1])],
        h=3,
        g=1,
    )
    assert list(result) == [False, True, True]


def test_training_is_reproducible_with_seed():
    a, la = _trained(seed=7)
    b, lb = _trained(seed=7)
    assert la == lb
    assert all(torch.equal(p, q) for p, q in zip(a.parameters(), b.parameters(), strict=True))


def test_detect_rejects_empty_session():
    with pytest.raises(ValueError):
        detect(DeepLog(vocab_size=5), [np.array([], dtype=np.int64)])
