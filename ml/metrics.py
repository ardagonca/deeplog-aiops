"""ML-05: Değerlendirme ölçütleri ve istatistiksel testler (docs/04-test-stratejisi.md §2, §4)."""

import math

import numpy as np
from scipy.stats import binomtest, chi2


def prf(y_true, y_pred) -> dict:
    """Precision, Recall, F1 ve karışıklık matrisi; 0'a bölmede 0.0."""
    t = np.asarray(y_true, dtype=bool)
    p = np.asarray(y_pred, dtype=bool)
    tp, fp = int((t & p).sum()), int((~t & p).sum())
    fn, tn = int((t & ~p).sum()), int((~t & ~p).sum())
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
    }


def best_threshold(scores, y_true) -> tuple[float, float]:
    """F1'i en yüksek yapan eşik (skor büyük = daha anormal, tahmin = skor >= eşik)."""
    s = np.asarray(scores, dtype=float)
    t = np.asarray(y_true, dtype=bool)
    order = np.argsort(-s, kind="stable")
    s_sorted, t_sorted = s[order], t[order]
    tp = np.cumsum(t_sorted)
    predicted = np.arange(1, len(s) + 1)
    # Eşik v, skoru >= v olan herkesi seçer: eşit skor grubunun son elemanı aday olur.
    last = np.r_[s_sorted[1:] != s_sorted[:-1], True]
    tp, predicted, candidates = tp[last], predicted[last], s_sorted[last]
    fp, fn = predicted - tp, t.sum() - tp
    f1 = 2 * tp / (2 * tp + fp + fn)  # payda >= 1, çünkü predicted >= 1
    i = int(np.argmax(f1))
    return float(candidates[i]), float(f1[i])


def mcnemar(y_true, pred_a, pred_b) -> dict:
    """İki sınıflandırıcıyı aynı örneklerde karşılaştırır. b: a doğru b yanlış, c: tersi.
    b + c < 25 ise kesin binom testi, değilse süreklilik düzeltmeli ki-kare (df=1)."""
    t = np.asarray(y_true, dtype=bool)
    a_ok = np.asarray(pred_a, dtype=bool) == t
    b_ok = np.asarray(pred_b, dtype=bool) == t
    b, c = int((a_ok & ~b_ok).sum()), int((~a_ok & b_ok).sum())
    if b + c < 25:
        statistic = float(min(b, c))
        p_value = binomtest(min(b, c), b + c, 0.5).pvalue if b + c else 1.0
    else:
        statistic = (abs(b - c) - 1) ** 2 / (b + c)
        p_value = float(chi2.sf(statistic, 1))
    return {"b": b, "c": c, "statistic": statistic, "p_value": float(p_value)}


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Başarı oranı k/n için Wilson güven aralığı; n=0 ise (0.0, 1.0)."""
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (center - half, center + half)
