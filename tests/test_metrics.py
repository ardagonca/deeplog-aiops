import time

import numpy as np
import pytest

from ml.metrics import best_threshold, mcnemar, prf, wilson


def test_prf_known_values():
    r = prf([1, 1, 0, 0, 1], [1, 0, 1, 0, 1])
    assert (r["tp"], r["fp"], r["fn"], r["tn"]) == (2, 1, 1, 1)
    assert r["precision"] == pytest.approx(2 / 3)
    assert r["recall"] == pytest.approx(2 / 3)
    assert r["f1"] == pytest.approx(2 / 3)


def test_prf_zero_division_gives_zero():
    r = prf([1, 0, 1], [0, 0, 0])
    assert (r["precision"], r["recall"], r["f1"]) == (0.0, 0.0, 0.0)


def test_best_threshold_separable():
    assert best_threshold([0.9, 0.8, 0.7, 0.1], [1, 1, 0, 0]) == (0.8, 1.0)


def test_best_threshold_handles_ties():
    # Eşik 0,5 eşit skorlu ikisini birlikte seçer: tp=1 fp=1 -> F1=2/3; 0,1 eşiği -> F1=0,5
    thr, f1 = best_threshold([0.5, 0.5, 0.1], [1, 0, 0])
    assert thr == 0.5 and f1 == pytest.approx(2 / 3)


def test_best_threshold_matches_brute_force_and_is_fast():
    rng = np.random.default_rng(0)
    scores = rng.integers(0, 50, 2_000).astype(float)
    y = rng.random(2_000) < 0.1
    best = max((prf(y, scores >= v)["f1"], v) for v in np.unique(scores))
    thr, f1 = best_threshold(scores, y)
    assert f1 == pytest.approx(best[0]) and prf(y, scores >= thr)["f1"] == pytest.approx(f1)
    big = rng.random(500_000)
    started = time.perf_counter()
    best_threshold(big, big > 0.99)
    assert time.perf_counter() - started < 1.0


def test_mcnemar_exact_path():
    y = np.ones(10)
    r = mcnemar(y, [1] * 8 + [0] * 2, [1] * 2 + [0] * 8)
    assert (r["b"], r["c"]) == (6, 0)
    assert r["p_value"] == pytest.approx(2 * 0.5**6)  # 0,03125


def test_mcnemar_chi_square_path():
    y = np.ones(40)
    r = mcnemar(y, [1] * 30 + [0] * 10, [0] * 30 + [1] * 10)
    assert (r["b"], r["c"]) == (30, 10)
    assert r["statistic"] == pytest.approx(361 / 40)  # (|30-10| - 1)^2 / 40
    assert r["p_value"] == pytest.approx(0.002663, abs=1e-5)


def test_mcnemar_identical_predictions():
    r = mcnemar([1, 0, 1], [1, 0, 0], [1, 0, 0])
    assert (r["b"], r["c"], r["p_value"]) == (0, 0, 1.0)


def test_wilson():
    low, high = wilson(18, 20)
    assert low == pytest.approx(0.699, abs=1e-3) and high == pytest.approx(0.972, abs=1e-3)
    assert wilson(0, 0) == (0.0, 1.0)
