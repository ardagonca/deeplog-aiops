"""RT-05: canlı skorlama, oturum hangi parçalara bölünürse bölünsün offline skorla aynıdır."""

import numpy as np
import torch

from ml.deeplog import DeepLog, session_max_rank, with_eos
from ml.novelty import CountNovelty
from ml.package import ModelConfig, ModelPackage
from ml.stream_score import Session, advance, advance_many, close, close_many

VOCAB = ["PAD", "E1", "E2", "E3", "E4"]


def _pkg():
    cfg = ModelConfig("t", VOCAB, eos=len(VOCAB), h=3, g=2, hidden=8, layers=1, commit="x")
    torch.manual_seed(0)
    known = np.array([[0, 1, 1, 1, 0], [0, 2, 0, 1, 0]])
    return ModelPackage(cfg, DeepLog(cfg.vocab_size, 8, 1), CountNovelty().fit(known), {})


def test_streaming_equals_offline_for_any_chunking():
    pkg = _pkg()
    rng = np.random.default_rng(1)
    for _ in range(30):
        seq = rng.integers(0, len(VOCAB), size=rng.integers(1, 15)).tolist()  # 0: bilinmeyen
        offline_rank = session_max_rank(pkg.deeplog, with_eos([np.array(seq)], pkg.config.eos), 3)
        counts = np.bincount(seq, minlength=len(VOCAB))
        offline_novel = pkg.novelty.predict(counts[None])[0]

        s, ranks, i = Session.new(pkg), [], 0
        while i < len(seq):  # rastgele parçalar: micro-batch sınırları
            step = int(rng.integers(1, 5))
            ranks += advance(pkg, s, seq[i : i + step]).ranks
            i += step
        eos_rank, novel = close(pkg, s)
        assert max([*ranks, eos_rank]) == offline_rank[0]
        assert novel == offline_novel and s.counts == counts.tolist()


def test_novelty_on_known_pattern_and_unknown_event_rank():
    pkg = _pkg()
    s = advance(pkg, Session.new(pkg), [1, 2, 3])
    assert close(pkg, s)[1] is False  # [0,1,1,1,0] biliniyor
    s = advance(pkg, s, [0])  # bilinmeyen olay
    assert s.ranks == [pkg.config.vocab_size] and close(pkg, s)[1] is True


def test_batched_equals_one_by_one():
    pkg = _pkg()
    rng = np.random.default_rng(2)
    chunks = [
        [rng.integers(0, len(VOCAB), size=rng.integers(1, 6)).tolist() for _ in range(4)]
        for _ in range(12)
    ]  # 12 oturum x 4 micro-batch
    single = [Session.new(pkg) for _ in chunks]
    batched = [Session.new(pkg) for _ in chunks]
    for step in range(4):
        expected = [advance(pkg, s, c[step]).ranks for s, c in zip(single, chunks, strict=True)]
        advance_many(pkg, [(s, c[step]) for s, c in zip(batched, chunks, strict=True)])
        assert [s.ranks for s in batched] == expected
    assert close_many(pkg, batched) == [close(pkg, s) for s in single]
    assert [s.counts for s in batched] == [s.counts for s in single]
