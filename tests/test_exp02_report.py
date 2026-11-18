from ml.exp02 import IF_GRID, METHODS, report


def _r(p, r, tp=90, fp=10, **extra):
    return {"precision": p, "recall": r, "f1": 2 * p * r / (p + r), "tp": tp, "fp": fp, **extra}


def test_report_renders_verdicts_and_tables():
    paper = _r(0.7, 0.5)
    runs = {
        "deeplog": [_r(0.8, 0.6, g=8, paper_g9=paper), _r(0.82, 0.6, g=7, paper_g9=paper)],
        "deeplog_eos": [_r(0.76, 0.78, g=6), _r(0.77, 0.78, g=6)],
        "iforest": [_r(0.77, 0.69, threshold=0.5), _r(0.75, 0.7, threshold=0.5)],
        "novelty": [_r(0.999, 1.0), _r(0.999, 1.0)],
    }
    mc = {"b": 10, "c": 2, "statistic": 4.08, "p_value": 0.04}
    res = {
        "seeds": [0, 1],
        "runs": runs,
        "if_grid": [{"max_samples": m, "n_estimators": n, **_r(0.5, 0.5)} for m, n in IF_GRID],
        "if_choice": (65536, 300),
        "mcnemar": {"deeplog_eos vs iforest": mc, "novelty vs deeplog_eos": mc},
        "config": "commit: abc\n",
    }
    text = report(res)
    for name in METHODS.values():
        assert name in text
    assert "| Olay sayımı yeniliği | **SAĞLANDI** | **SAĞLANDI** |" in text
    assert "| Isolation Forest (ızgarada seçilen ayar) | **SAĞLANMADI** | **SAĞLANMADI** |" in text
    assert "**K4** (DeepLog F1 > Isolation Forest F1, McNemar p < 0,05): **SAĞLANDI**" in text
    assert "makaledeki sabit g=9" in text and "max_samples=65536, n=300" in text
