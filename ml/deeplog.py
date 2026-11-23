"""ML-04: DeepLog (Du vd., 2017). Önceki h olaydan sıradaki olayı tahmin eden LSTM; gerçekleşen
olay ilk g tahmin arasında değilse oturum anormaldir. Olay kodları ml/dataset.py ile aynıdır
(0 = PAD/bilinmeyen).
"""

from collections.abc import Sequence

import numpy as np
import torch
from torch import nn
from torch.nn import functional as F

from ml.dataset import PAD


def _all_windows(seq: np.ndarray, h: int) -> np.ndarray:
    """(len(seq), h+1): her satır önceki h olay + hedef. Diziye solundan h PAD eklenir (uzunluk
    en az h+1 olur); böylece oturumun ilk h olayı da hedef olur ve bilinmeyen olay kaçmaz."""
    padded = np.concatenate([np.full(h, PAD, dtype=np.int64), np.asarray(seq, dtype=np.int64)])
    return np.lib.stride_tricks.sliding_window_view(padded, h + 1)


def windows(sequences: Sequence[np.ndarray], h: int = 10, seq_weights=None):
    """Eğitim örnekleri: aynı (pencere, hedef) çiftleri tekilleşir, weights tekrar sayısıdır.

    seq_weights verilirse her dizi o kadar kez tekrarlanmış sayılır; böylece önce aynı oturumlar
    birleştirilip (unique_sequences) sonuç tam veriyle birebir aynı kalacak şekilde hızlanır."""
    per_seq = [_all_windows(s, h) for s in sequences]
    rows = np.concatenate(per_seq)
    row_weights = np.repeat(
        np.ones(len(per_seq)) if seq_weights is None else np.asarray(seq_weights, dtype=float),
        [len(w) for w in per_seq],
    )
    unique, inverse = np.unique(rows, axis=0, return_inverse=True)
    weights = np.bincount(inverse.ravel(), weights=row_weights, minlength=len(unique))
    return unique[:, :h].copy(), unique[:, h].copy(), weights.astype(np.float32)


def with_eos(sequences: Sequence[np.ndarray], eos: int) -> list[np.ndarray]:
    """Her oturumun sonuna "oturum sonu" olayı ekler (eos genellikle vocab_size; model bir büyük
    sözlükle kurulur). Model böylece oturumun ne zaman bitmesi gerektiğini de öğrenir: erken
    biten bir oturumda (ör. beklenen kayıt olayları gelmeden) sonu tahmin etmek bir sapma olur.
    Makaledeki DeepLog'a eklenen bir uzantıdır; EXP-02 iki hali ayrı raporlar."""
    return [np.append(np.asarray(s, dtype=np.int64), eos) for s in sequences]


def unique_sequences(sequences: Sequence[np.ndarray]):
    """Aynı oturumları birleştirir: (benzersiz diziler, her birinin sayısı, geri eşleme).
    HDFS oturumları çok tekrarlıdır; eğitim ve değerlendirme benzersiz diziler üzerinde yapılır."""
    index: dict[bytes, int] = {}
    inverse = np.empty(len(sequences), dtype=np.int64)
    uniques: list[np.ndarray] = []
    for i, s in enumerate(sequences):
        key = np.asarray(s, dtype=np.int8).tobytes()
        j = index.setdefault(key, len(uniques))
        if j == len(uniques):
            uniques.append(np.asarray(s))
        inverse[i] = j
    return uniques, np.bincount(inverse, minlength=len(uniques)), inverse


class DeepLog(nn.Module):
    """One-hot girdi, 2 katmanlı LSTM (gizli 64), son adımdan olay logit'leri."""

    def __init__(self, vocab_size: int, hidden: int = 64, layers: int = 2):
        super().__init__()
        self.vocab_size = vocab_size
        self.lstm = nn.LSTM(vocab_size, hidden, num_layers=layers, batch_first=True)
        self.out = nn.Linear(hidden, vocab_size)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(F.one_hot(x, self.vocab_size).float())
        return self.out(out[:, -1, :])


def train(model, X, y, weights, epochs, batch_size=2048, lr=1e-3, seed=0) -> list[float]:
    """Adam ile eğitir; epoch başına ortalama kaybı döner.

    Her mini-batch, tekilleştirilmiş pencerelerden ağırlıklarıyla orantılı olasılıkla (yerine
    koyarak) örneklenir. Bu, tüm (tekrarlı) pencerelerle yapılan SGD'ye eşdeğerdir. Ağırlıklı
    kayıpla (sum(w*ce)/sum(w)) karşılaştırıldığında, ağırlıklar çok çarpık olduğunda
    (HDFS'te tek pencere yüz binlerce kez tekrarlanır) gradyan gürültüsü daha azdır; EXP-02
    tanısında doğrulama F1'i arttı ve tohumlar arası fark azaldı. Bir epoch,
    ceil(len(y) / batch_size) adımdır (hesap bütçesi eski yöntemle aynı).

    seed örneklemeyi belirler. İlk ağırlıklar için modeli kurmadan önce torch.manual_seed
    çağrılmalıdır."""
    torch.manual_seed(seed)
    rng = np.random.default_rng(seed)
    X, y = torch.as_tensor(X), torch.as_tensor(y)
    p = np.asarray(weights, dtype=np.float64)
    p = p / p.sum()
    steps = max(1, -(-len(y) // batch_size))
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    model.train()
    losses = []
    for _ in range(epochs):
        total = 0.0
        for _ in range(steps):
            idx = torch.as_tensor(rng.choice(len(y), size=batch_size, p=p))
            loss = F.cross_entropy(model(X[idx]), y[idx])
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            total += loss.item()
        losses.append(total / steps)
    return losses


def session_max_rank(model, sequences, h=10, batch_size=8192) -> np.ndarray:
    """Oturum skoru: her konumda gerçek olayın tahmin sıralamasındaki yeri (0 = en olası;
    kendisinden kesin büyük logit sayısı), oturum boyunca en kötüsü. Bilinmeyen (PAD) olay
    vocab_size alır.
    DeepLog kuralı "gerçek olay ilk g tahminde değilse anormal", skor >= g demektir; tek ileri
    geçişle tüm g değerleri değerlendirilebilir ve g doğrulama setinde seçilebilir."""
    if not len(sequences):
        return np.zeros(0, dtype=np.int64)
    if any(len(s) == 0 for s in sequences):
        raise ValueError("boş oturum değerlendirilemez")
    per_seq = [_all_windows(s, h) for s in sequences]
    rows = np.concatenate(per_seq)
    starts = np.concatenate([[0], np.cumsum([len(w) for w in per_seq])[:-1]])
    return np.maximum.reduceat(window_ranks(model, rows, batch_size), starts)


def window_ranks(model, rows: np.ndarray, batch_size=8192) -> np.ndarray:
    """rows: (n, h+1) önceki h olay + gerçek olay. Gerçek olayın tahmin sırası (0 = en olası);
    bilinmeyen (PAD) olay vocab_size alır. Offline ve canlı skorlama (ml.stream_score) ortak."""
    rows = np.array(rows, dtype=np.int64)  # kopya: sliding_window_view salt okunurdur
    rank = np.empty(len(rows), dtype=np.int64)
    model.eval()
    with torch.no_grad():
        for start in range(0, len(rows), batch_size):
            chunk = torch.as_tensor(rows[start : start + batch_size])
            logits, target = model(chunk[:, :-1]), chunk[:, -1]
            r = (logits > logits.gather(1, target[:, None])).sum(dim=1)
            r[target == PAD] = model.vocab_size
            rank[start : start + len(chunk)] = r.numpy()
    return rank


def detect(model, sequences, h=10, g=9, batch_size=8192) -> np.ndarray:
    """Oturum başına anormal mi: herhangi bir konumda gerçek olay ilk g tahminde değilse ya da
    bilinmeyen (PAD) ise True."""
    return session_max_rank(model, sequences, h, batch_size) >= g
