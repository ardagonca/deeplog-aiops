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


def windows(sequences: Sequence[np.ndarray], h: int = 10):
    """Eğitim örnekleri: aynı (pencere, hedef) çiftleri tekilleşir, weights tekrar sayısıdır."""
    rows = np.concatenate([_all_windows(s, h) for s in sequences])
    unique, counts = np.unique(rows, axis=0, return_counts=True)
    return unique[:, :h].copy(), unique[:, h].copy(), counts.astype(np.float32)


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
    """Ağırlıklı CrossEntropy (sum(w*ce)/sum(w)) ile Adam; epoch kayıplarını döner.

    seed karıştırma sırasını belirler. İlk ağırlıklar için modeli kurmadan önce
    torch.manual_seed çağrılmalıdır."""
    torch.manual_seed(seed)
    rng = np.random.default_rng(seed)
    X, y, w = torch.as_tensor(X), torch.as_tensor(y), torch.as_tensor(weights)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    model.train()
    losses = []
    for _ in range(epochs):
        total, weight_sum = 0.0, 0.0
        for idx in np.array_split(rng.permutation(len(y)), max(1, -(-len(y) // batch_size))):
            idx = torch.as_tensor(idx)
            ce = F.cross_entropy(model(X[idx]), y[idx], reduction="none")
            loss = (w[idx] * ce).sum() / w[idx].sum()
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            total += float((w[idx] * ce.detach()).sum())
            weight_sum += float(w[idx].sum())
        losses.append(total / weight_sum)
    return losses


def detect(model, sequences, h=10, g=9, batch_size=8192) -> np.ndarray:
    """Oturum başına anormal mi: herhangi bir konumda gerçek olay ilk g tahminde değilse ya da
    bilinmeyen (PAD) ise True. Tüm pencereler toplu değerlendirilir, sonuç oturuma toplanır."""
    if not len(sequences):
        return np.zeros(0, dtype=bool)
    if any(len(s) == 0 for s in sequences):
        raise ValueError("boş oturum değerlendirilemez")
    per_seq = [_all_windows(s, h) for s in sequences]
    rows = np.concatenate(per_seq)
    starts = np.concatenate([[0], np.cumsum([len(w) for w in per_seq])[:-1]])
    miss = np.zeros(len(rows), dtype=bool)
    model.eval()
    with torch.no_grad():
        for start in range(0, len(rows), batch_size):
            chunk = torch.as_tensor(rows[start : start + batch_size])
            top = model(chunk[:, :h]).topk(min(g, model.vocab_size), dim=1).indices
            target = chunk[:, h]
            hit = (top == target[:, None]).any(dim=1)
            miss[start : start + len(chunk)] = (~hit | (target == PAD)).numpy()
    return np.maximum.reduceat(miss, starts)
