"""RT-02/03: Canlı akışta oturum bazında skorlama (ADR-0009). Spark'a bağlı değildir; tespit
işi (jobs/detect.py) bunu applyInPandasWithState içinde çağırır.

Bir oturumun olayları hangi parçalara bölünerek gelirse gelsin sonuç, offline değerlendirmeyle
(ml.deeplog.session_max_rank + ml.novelty) aynıdır: aynı başlangıç dolgusu, aynı pencereler,
aynı sayım vektörü (tests/test_stream_score.py, RT-05).
"""

from dataclasses import dataclass, field

import numpy as np

from ml.dataset import PAD
from ml.deeplog import window_ranks
from ml.package import ModelPackage


@dataclass
class Session:
    history: list[int]  # son h olay kodu; yeni oturum h tane PAD ile başlar
    counts: list[int]  # olay kodu başına sayım (sözlük boyu)
    alerted: bool = False  # bu oturum için DeepLog alarmı verildi mi
    ranks: list[int] = field(default_factory=list)  # yalnızca son advance() çağrısının sıraları

    @classmethod
    def new(cls, pkg: ModelPackage) -> "Session":
        return cls([PAD] * pkg.config.h, [0] * len(pkg.config.vocab))


def advance(pkg: ModelPackage, s: Session, codes: list[int]) -> Session:
    """Yeni olayları işler; her olayın tahmin sırası s.ranks'e yazılır."""
    h = pkg.config.h
    stream = s.history + list(codes)
    rows = np.lib.stride_tricks.sliding_window_view(np.array(stream), h + 1) if codes else []
    s.ranks = window_ranks(pkg.deeplog, rows).tolist() if len(rows) else []
    for c in codes:
        s.counts[c] += 1
    s.history = stream[-h:]
    return s


def close(pkg: ModelPackage, s: Session) -> tuple[int, bool]:
    """Oturum kapanırken: (oturum sonu olayının tahmin sırası, sayım vektörü görülmemiş mi)."""
    eos_rank = int(window_ranks(pkg.deeplog, [s.history + [pkg.config.eos]])[0])
    novel = bool(pkg.novelty.predict(np.array([s.counts]))[0])
    return eos_rank, novel
