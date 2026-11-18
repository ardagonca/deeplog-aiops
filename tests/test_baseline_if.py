import numpy as np

from ml.baseline_if import count_matrix, fit, scores
from ml.dataset import Dataset


def _dataset(sequences):
    lengths = [len(s) for s in sequences]
    return Dataset(
        blocks=np.array([f"blk_{i}" for i in range(len(sequences))]),
        labels=np.zeros(len(sequences), dtype=np.int8),
        split=np.zeros(len(sequences), dtype=np.int8),
        events=np.array([e for s in sequences for e in s], dtype=np.int8),
        offsets=np.concatenate([[0], np.cumsum(lengths)]),
        vocab=np.array(["PAD", "E1", "E2", "E3"]),
    )


def test_count_matrix_counts_events_per_session():
    ds = _dataset([[1, 2, 2], [3], [], [0, 1, 1, 1]])
    X = count_matrix(ds, np.array([3, 0, 2]))
    assert X.dtype == np.float32
    assert X.tolist() == [[1, 3, 0, 0], [0, 1, 2, 0], [0, 0, 0, 0]]


def test_outlier_scores_higher_and_seed_is_deterministic():
    rng = np.random.default_rng(0)
    normal = rng.poisson([0, 3, 3, 1], size=(500, 4)).astype(np.float32)
    outlier = np.array([[5, 0, 0, 20]], dtype=np.float32)
    model = fit(normal, seed=7)
    s = scores(model, np.vstack([normal[:50], outlier]))
    assert s[-1] > s[:-1].max()
    assert np.array_equal(s, scores(fit(normal, seed=7), np.vstack([normal[:50], outlier])))
