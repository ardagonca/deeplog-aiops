"""ML-03: Isolation Forest karşılaştırma modeli (Liu vd., 2008).

Kabul edilen TÜBİTAK önerisinde taahhüt edilen yöntemdir; DeepLog ile aynı bölmelerde
eğitilir ve değerlendirilir. Girdi, oturum başına olay sayım vektörüdür (sıra bilgisi yok).
"""

import numpy as np
from sklearn.ensemble import IsolationForest

from ml.dataset import Dataset


def count_matrix(ds: Dataset, idx: np.ndarray) -> np.ndarray:
    """(len(idx), vocab_size) oturum başına olay sayıları (float32)."""
    idx = np.asarray(idx)
    starts = ds.offsets[idx]
    lengths = ds.offsets[idx + 1] - starts
    rows = np.repeat(np.arange(len(idx)), lengths)
    # her oturumun olay konumları: başlangıç + 0..uzunluk-1
    within = np.arange(lengths.sum()) - np.repeat(np.cumsum(lengths) - lengths, lengths)
    events = ds.events[np.repeat(starts, lengths) + within].astype(np.int64)
    flat = np.bincount(rows * ds.vocab_size + events, minlength=len(idx) * ds.vocab_size)
    return flat.reshape(len(idx), ds.vocab_size).astype(np.float32)


def fit(
    X_train: np.ndarray,
    seed: int,
    n_estimators: int = 100,
    max_samples: int | str = "auto",
    n_jobs: int = -1,
) -> IsolationForest:
    """max_samples: her ağacın örneklem büyüklüğü. Varsayılan ("auto" = 256) HDFS'te zayıf;
    EXP-02 değeri doğrulama setinde ızgara ile seçer."""
    model = IsolationForest(
        n_estimators=n_estimators,
        max_samples=min(max_samples, len(X_train)) if isinstance(max_samples, int) else max_samples,
        contamination="auto",
        n_jobs=n_jobs,
        random_state=seed,
    )
    return model.fit(X_train)


def scores(model: IsolationForest, X: np.ndarray) -> np.ndarray:
    """Anomali skoru: büyük değer daha anormal."""
    return -model.score_samples(X)
