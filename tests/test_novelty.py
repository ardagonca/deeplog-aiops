import numpy as np

from ml.novelty import CountNovelty


def test_unseen_count_vectors_are_anomalous():
    model = CountNovelty().fit(np.array([[1, 2, 0], [1, 2, 0], [0, 1, 1]], dtype=np.float32))
    X = np.array([[1, 2, 0], [0, 1, 1], [1, 3, 0], [0, 0, 0]], dtype=np.float32)
    assert list(model.predict(X)) == [False, False, True, True]
    assert model.predict(np.zeros((0, 3))).shape == (0,)
