import numpy as np
import pytest
import torch

from ml.dataset import PAD
from ml.deeplog import (
    DeepLog,
    detect,
    session_max_rank,
    train,
    unique_sequences,
    windows,
    with_eos,
)


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


def test_session_weights_equal_repeating_sequences():
    a, b = np.array([1, 2, 3, 1]), np.array([2, 2])
    X1, y1, w1 = windows([a, a, a, b], h=2)
    uniques, counts, inverse = unique_sequences([a, a, a, b])
    X2, y2, w2 = windows(uniques, h=2, seq_weights=counts)
    assert np.array_equal(X1, X2) and np.array_equal(y1, y2) and np.array_equal(w1, w2)
    assert list(counts) == [3, 1] and list(inverse) == [0, 0, 0, 1]


def test_detect_equals_max_rank_threshold():
    torch.manual_seed(0)
    model = DeepLog(vocab_size=5)
    seqs = [np.array([1, 2, 3, 4, 1]), np.array([4, 4]), np.array([1, 0, 2])]  # 0: bilinmeyen
    ranks = session_max_rank(model, seqs, h=3)
    assert ranks[2] == 5  # bilinmeyen olay en kötü sıra
    for g in range(1, 6):
        assert np.array_equal(detect(model, seqs, h=3, g=g), ranks >= g)


def test_with_eos_appends_end_marker():
    out = with_eos([np.array([1, 2], dtype=np.int8), np.array([3])], eos=9)
    assert [list(s) for s in out] == [[1, 2, 9], [3, 9]]
