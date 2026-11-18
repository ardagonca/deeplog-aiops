from experiments.exp01_summary import summarize, to_markdown


def run(rate, ingest, lost=0, dup=0, keeps_up=True):
    return {
        "rate_target": rate,
        "sent": rate * 660,
        "lost": lost,
        "duplicates": dup,
        "ingest_rate": ingest,
        "input_rate": rate,
        "lag_p95": 31.0,
        "keeps_up": keeps_up,
    }


def test_all_good_runs_pass_k1_and_k2():
    s = summarize([run(5000, 4990), run(5000, 5010), run(1000, 1000)])
    assert s["k1"] and s["k2"]
    row = next(r for r in s["rows"] if r["rate"] == 5000)
    assert (row["runs"], row["ingest_mean"], row["ingest_std"]) == (2, 5000, 14.142135623730951)


def test_k1_fails_if_any_target_run_falls_behind_or_loses_data():
    assert not summarize([run(5000, 5000), run(5000, 3000, keeps_up=False)])["k1"]
    assert not summarize([run(5000, 5000, lost=1)])["k1"]


def test_k1_not_measured_without_target_rate_runs():
    s = summarize([run(1000, 1000)])
    assert s["k1"] is None
    assert "**ÖLÇÜLMEDİ**" in to_markdown(s, "")


def test_k2_fails_on_loss_or_duplicates_at_any_rate():
    assert not summarize([run(5000, 5000), run(10000, 9000, dup=3)])["k2"]
    assert not summarize([run(1000, 1000, lost=2)])["k2"]


def test_markdown_contains_verdicts_and_rows():
    md = to_markdown(summarize([run(5000, 5000), run(10000, 7000, keeps_up=False)]), "rates: x")
    assert "**K1** (≥ 5,000 log/sn, 10 dk kesintisiz): **SAĞLANDI**" in md
    assert "| 10,000 | 1 |" in md and "| hayır |" in md
