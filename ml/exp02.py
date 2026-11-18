"""EXP-02 (K3, K4 ve formdaki "Precision > %85"): HDFS_v1 üzerinde dört yöntem.

Kullanım: uv run python -m ml.exp02 [--seeds 5] [--threads 3]
Protokol (docs/04-test-stratejisi.md §3–4): eğitimde yalnızca normal oturumlar; tüm seçimler
(IF ızgarası, IF skor eşiği, DeepLog g) YALNIZCA doğrulama setinde F1'e göre yapılır; test seti
bir kez değerlendirilir. Rastgelelik içeren yöntemler --seeds tohumla çalışır; McNemar
karşılaştırmaları medyan-F1 tohumunda yapılır.

Yöntemler:
- DeepLog (makaledeki hali; h=10, LSTM 2x64; g=9 ve doğrulamada seçilen g)
- DeepLog + oturum sonu (EOS) olayı (ml.deeplog.with_eos)
- Isolation Forest (oturum başına olay sayımı; kabul edilen öneride taahhüt edilen yöntem)
- Olay sayımı yeniliği (ml.novelty; basit referans yöntem)
Çıktı: experiments/EXP-02/<tarih>/{results.json, ozet.md} (betik üretir).
"""

import argparse
import json
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import torch

from ml import dataset
from ml.baseline_if import count_matrix, fit, scores
from ml.deeplog import (
    DeepLog,
    session_max_rank,
    train,
    unique_sequences,
    windows,
    with_eos,
)
from ml.metrics import best_threshold, mcnemar, prf, wilson
from ml.novelty import CountNovelty

ROOT = Path(__file__).resolve().parents[1]
H, G_PAPER = 10, 9  # Du vd. (2017)
EPOCHS, BATCH, LR = 50, 256, 1e-3
IF_GRID = [(ms, n) for ms in (256, 1024, 4096, 16384, 65536, "tümü") for n in (100, 300)]
FORM_PRECISION = 0.85  # kabul edilen önerideki bağlayıcı ölçüt
K3 = 0.90  # tüzük K3: Precision ve Recall >= 0,90
METHODS = {
    "deeplog": "DeepLog (makaledeki hali, g doğrulamada seçildi)",
    "deeplog_eos": "DeepLog + oturum sonu (EOS)",
    "iforest": "Isolation Forest (ızgarada seçilen ayar)",
    "novelty": "Olay sayımı yeniliği",
}


class Data:
    """Bölmelere göre etiketler, sayım matrisleri ve benzersiz diziler (bir kez hazırlanır)."""

    def __init__(self, ds):
        self.ds = ds
        self.idx = {n: ds.indices(n) for n in dataset.SPLITS}
        self.y = {n: ds.labels[i] for n, i in self.idx.items()}
        self.counts = {n: count_matrix(ds, i) for n, i in self.idx.items()}
        self.seqs = {}
        for eos in (False, True):
            for n, i in self.idx.items():
                s = [ds.sequence(j) for j in i]
                self.seqs[eos, n] = unique_sequences(with_eos(s, ds.vocab_size) if eos else s)


def run_deeplog(data: Data, seed: int, eos: bool):
    vocab = data.ds.vocab_size + (1 if eos else 0)
    uniques, counts, _ = data.seqs[eos, "train"]
    X, y, w = windows(uniques, H, seq_weights=counts)
    torch.manual_seed(seed)
    model = DeepLog(vocab)
    losses = train(model, X, y, w, EPOCHS, batch_size=BATCH, lr=LR, seed=seed)

    def ranks(split):
        u, _, inverse = data.seqs[eos, split]
        return session_max_rank(model, u, H)[inverse]

    test_rank = ranks("test")
    g, _ = best_threshold(ranks("val"), data.y["val"])
    pred = test_rank >= g
    out = {"g": int(g), "loss_last": losses[-1], **prf(data.y["test"], pred)}
    if not eos:
        out["paper_g9"] = prf(data.y["test"], test_rank >= G_PAPER)
    return out, pred


def if_grid(data: Data, seeds: list[int], n_jobs: int) -> list[dict]:
    """Her ayar tüm tohumlarla denenir; seçim ortalama doğrulama F1'iyle yapılır. Tek tohumla
    seçilen ayar diğer tohumlara genellenmiyordu (ilk koşu: 5 tohumun 3'ünde test F1 0,27)."""
    rows = []
    for ms, n in IF_GRID:
        size = len(data.counts["train"]) if ms == "tümü" else ms
        f1s = []
        for seed in seeds:
            model = fit(data.counts["train"], seed, n_estimators=n, max_samples=size, n_jobs=n_jobs)
            s = scores(model, data.counts["val"])
            thr, _ = best_threshold(s, data.y["val"])
            f1s.append(prf(data.y["val"], s >= thr)["f1"])
        std = statistics.stdev(f1s) if len(f1s) > 1 else 0.0
        rows.append(
            {"max_samples": ms, "n_estimators": n, "f1": statistics.mean(f1s), "f1_std": std}
        )
    return rows


def run_iforest(data: Data, seed: int, max_samples, n_estimators: int, n_jobs: int):
    size = len(data.counts["train"]) if max_samples == "tümü" else max_samples
    model = fit(data.counts["train"], seed, n_estimators, max_samples=size, n_jobs=n_jobs)
    thr, _ = best_threshold(scores(model, data.counts["val"]), data.y["val"])
    pred = scores(model, data.counts["test"]) >= thr
    return {"threshold": thr, **prf(data.y["test"], pred)}, pred


def run_novelty(data: Data):
    pred = CountNovelty().fit(data.counts["train"]).predict(data.counts["test"])
    return prf(data.y["test"], pred), pred


def mean_std(runs: list[dict], key: str) -> str:
    values = [r[key] for r in runs]
    std = statistics.stdev(values) if len(values) > 1 else 0.0
    return f"{statistics.mean(values):.4f} ± {std:.4f}"


def report(res: dict) -> str:
    ok = {True: "SAĞLANDI", False: "SAĞLANMADI"}
    runs = res["runs"]
    n_seeds = len(res["seeds"])
    lines = [
        "# EXP-02 Özeti: HDFS_v1 üzerinde anomali tespiti",
        "",
        "`ml.exp02` üretir; elle düzenlenmez. Tüm seçimler (IF ızgarası, eşikler, DeepLog g) "
        "yalnızca doğrulama setinde yapıldı; test seti bir kez değerlendirildi.",
        "",
        "## Sonuç (test seti)",
        "",
        "| Yöntem | Formdaki ölçüt: Precision > %85 (her tohumda) | K3: P ≥ 0,90 ve R ≥ 0,90 |",
        "|---|---|---|",
    ]
    for key, name in METHODS.items():
        r = runs[key]
        form = all(x["precision"] > FORM_PRECISION for x in r)
        k3 = all(x["precision"] >= K3 and x["recall"] >= K3 for x in r)
        lines.append(f"| {name} | **{ok[form]}** | **{ok[k3]}** |")
    mc = res["mcnemar"]["deeplog_eos vs iforest"]
    best_dl = max(
        ("deeplog", "deeplog_eos"), key=lambda k: statistics.mean(x["f1"] for x in runs[k])
    )
    k4 = (
        statistics.mean(x["f1"] for x in runs[best_dl])
        > statistics.mean(x["f1"] for x in runs["iforest"])
        and mc["p_value"] < 0.05
    )
    lines += [
        "",
        f"**K4** (DeepLog F1 > Isolation Forest F1, McNemar p < 0,05): **{ok[k4]}**",
        "",
        f"## Test seti ({n_seeds} tohum, ortalama ± std; sayım yeniliği deterministik)",
        "",
        "| Yöntem | Precision | Recall | F1 | Precision %95 GA (medyan tohum) | Seçilen eşik |",
        "|---|---|---|---|---|---|",
    ]
    for key, name in METHODS.items():
        r = runs[key]
        mid = sorted(r, key=lambda x: x["f1"])[len(r) // 2]
        lo, hi = wilson(mid["tp"], mid["tp"] + mid["fp"])
        if key.startswith("deeplog"):
            chosen = f"g ∈ {sorted({x['g'] for x in r})}"
        elif key == "iforest":
            chosen = f"max_samples={res['if_choice'][0]}, n={res['if_choice'][1]}; skor eşiği"
        else:
            chosen = "–"
        lines.append(
            f"| {name} | {mean_std(r, 'precision')} | {mean_std(r, 'recall')} "
            f"| {mean_std(r, 'f1')} | [{lo:.3f}, {hi:.3f}] | {chosen} |"
        )
    paper = [x["paper_g9"] for x in runs["deeplog"]]
    lines += [
        f"| DeepLog (makaledeki sabit g={G_PAPER}, karşılaştırma için) "
        f"| {mean_std(paper, 'precision')} | {mean_std(paper, 'recall')} "
        f"| {mean_std(paper, 'f1')} | – | g = {G_PAPER} |",
        "",
        "## McNemar karşılaştırmaları (test, medyan-F1 tohumu)",
        "",
        "b: ilk yöntem doğru, ikinci yanlış; c: tersi.",
        "",
        "| Karşılaştırma | b | c | p |",
        "|---|---|---|---|",
    ]
    for pair, m in res["mcnemar"].items():
        lines.append(f"| {pair} | {m['b']:,} | {m['c']:,} | {m['p_value']:.3g} |")
    lines += [
        "",
        "## Isolation Forest ızgarası (doğrulama seti; tüm tohumların ortalaması ± std)",
        "",
        "| max_samples | n_estimators | Doğrulama F1 |",
        "|---|---|---|",
    ]
    for g in res["if_grid"]:
        lines.append(
            f"| {g['max_samples']} | {g['n_estimators']} | {g['f1']:.4f} ± {g['f1_std']:.4f} |"
        )
    lines += ["", "## Ayarlar", "", "```", res["config"].strip(), "```", ""]
    return "\n".join(lines)


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seeds", type=int, default=5)
    parser.add_argument("--threads", type=int, default=3, help="torch ve IF iş parçacığı sınırı")
    args = parser.parse_args()
    torch.set_num_threads(args.threads)
    seeds = list(range(args.seeds))
    git = ["git", "-C", str(ROOT)]
    commit = subprocess.run([*git, "rev-parse", "--short", "HEAD"], capture_output=True, text=True)
    dirty = subprocess.run([*git, "diff", "--quiet"]).returncode != 0
    started = time.time()

    data = Data(dataset.load())
    grid = if_grid(data, seeds, args.threads)
    best = max(grid, key=lambda g: g["f1"])
    if_choice = (best["max_samples"], best["n_estimators"])
    print(f"IF ızgarası: seçilen {if_choice} (doğrulama F1 {best['f1']:.4f})", flush=True)

    runs = {k: [] for k in METHODS}
    preds = {k: [] for k in METHODS}
    novelty, novelty_pred = run_novelty(data)
    for seed in seeds:
        for key, (r, p) in {
            "deeplog": run_deeplog(data, seed, eos=False),
            "deeplog_eos": run_deeplog(data, seed, eos=True),
            "iforest": run_iforest(data, seed, *if_choice, args.threads),
        }.items():
            runs[key].append(r)
            preds[key].append(p)
        runs["novelty"].append(novelty)
        preds["novelty"].append(novelty_pred)
        print(
            f"tohum {seed}: " + ", ".join(f"{k} F1 {runs[k][-1]['f1']:.4f}" for k in METHODS),
            flush=True,
        )

    def median_pred(key):
        order = np.argsort([r["f1"] for r in runs[key]])
        return preds[key][int(order[len(order) // 2])]

    y = data.y["test"]
    pairs = [("deeplog_eos", "iforest"), ("novelty", "deeplog_eos"), ("novelty", "iforest")]
    result = {
        "seeds": seeds,
        "runs": runs,
        "if_grid": grid,
        "if_choice": if_choice,
        "mcnemar": {f"{a} vs {b}": mcnemar(y, median_pred(a), median_pred(b)) for a, b in pairs},
        "config": (
            f"commit: {commit.stdout.strip()}{' (+commit edilmemiş değişiklik)' if dirty else ''}\n"
            f"tohumlar: {seeds}\n"
            f"DeepLog: h={H}, LSTM 2x64, epoch={EPOCHS}, batch={BATCH}, lr={LR}\n"
            f"iş parçacığı: {args.threads}\nsüre: {time.time() - started:.0f} sn\n"
        ),
    }
    out = ROOT / "experiments" / "EXP-02" / datetime.now().strftime("%Y-%m-%d_%H%M")
    out.mkdir(parents=True, exist_ok=True)
    (out / "results.json").write_text(
        json.dumps(result, indent=2, default=float) + "\n", encoding="utf-8", newline="\n"
    )
    text = report(result)
    (out / "ozet.md").write_text(text, encoding="utf-8", newline="\n")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
