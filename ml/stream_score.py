"""RT-02/03: Canlı akışta oturum bazında skorlama (ADR-0009). Spark'a bağlı değildir; tespit
işi (jobs/detect.py) bunu ml.detector üzerinden, her micro-batch için toplu çağırır (ADR-0010).

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


def advance_many(pkg: ModelPackage, batch: list[tuple[Session, list[int]]]) -> None:
    """advance() ile aynı sonuç, ama tüm oturumların pencereleri tek ileri geçişte skorlanır.
    Canlı akışta bir micro-batch binlerce oturum içerir; oturum başına çağrı darboğazdı (RT-06)."""
    h = pkg.config.h
    streams = [s.history + list(codes) for s, codes in batch]
    rows = [np.lib.stride_tricks.sliding_window_view(np.array(st), h + 1) for st in streams]
    ranks = window_ranks(pkg.deeplog, np.concatenate(rows)) if rows else np.zeros(0, np.int64)
    start = 0
    for (s, codes), st in zip(batch, streams, strict=True):
        s.ranks = ranks[start : start + len(codes)].tolist()
        start += len(codes)
        for c in codes:
            s.counts[c] += 1
        s.history = st[-h:]


def close_many(pkg: ModelPackage, sessions: list[Session]) -> list[tuple[int, bool]]:
    """close() ile aynı sonuç, toplu."""
    if not sessions:
        return []
    eos = window_ranks(pkg.deeplog, [s.history + [pkg.config.eos] for s in sessions])
    novel = pkg.novelty.predict(np.array([s.counts for s in sessions]))
    return [(int(r), bool(n)) for r, n in zip(eos, novel, strict=True)]


def close(pkg: ModelPackage, s: Session) -> tuple[int, bool]:
    """Oturum kapanırken: (oturum sonu olayının tahmin sırası, sayım vektörü görülmemiş mi)."""
    eos_rank = int(window_ranks(pkg.deeplog, [s.history + [pkg.config.eos]])[0])
    novel = bool(pkg.novelty.predict(np.array([s.counts]))[0])
    return eos_rank, novel
