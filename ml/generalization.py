"""Genelleme (overfitting) denetimi: bir model paketinin eğitim, doğrulama ve test bölmelerindeki
yanlış alarm oranı ve recall'u. Eğitimde belirgin şekilde düşük yanlış alarm, ezberleme demektir.

Kullanım: uv run python -m ml.generalization [--model data/models/hdfs-v1] [--threads 6]
Çıktı: experiments/EXP-02/genelleme-<paket>.md (betik üretir). Paketlenen modelin test sonucu da
burada bir kez raporlanır; paket test setiyle seçilmemiştir (ml.train_final).
"""

import argparse
import sys
from pathlib import Path

import torch

from ml import dataset
from ml.baseline_if import count_matrix
from ml.deeplog import session_max_rank, unique_sequences, with_eos
from ml.metrics import prf
from ml.package import load

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--model", type=Path, default=ROOT / "data" / "models" / "hdfs-v1")
    parser.add_argument("--threads", type=int, default=6)
    args = parser.parse_args()
    torch.set_num_threads(args.threads)
    pkg, ds = load(args.model), dataset.load()
    c = pkg.config
    lines = [
        f"# Genelleme denetimi: `{c.name}`",
        "",
        "`ml.generalization` üretir; elle düzenlenmez. Yanlış alarm = normal oturumların "
        "anormal sayılma oranı. Eğitimde belirgin şekilde düşük olması ezberleme belirtisidir.",
        "",
        f"DeepLog + EOS (g = {c.g}, paket commit'i {c.commit}); sayım yeniliği "
        f"{len(pkg.novelty.known)} bilinen desen.",
        "",
        "| Bölme | Normal oturum | DeepLog yanlış alarm | DeepLog recall "
        "| Sayım yanlış alarm | Sayım recall |",
        "|---|---|---|---|---|---|",
    ]
    for name in dataset.SPLITS:
        idx = ds.indices(name)
        y = ds.labels[idx]
        u, _, inv = unique_sequences(with_eos([ds.sequence(i) for i in idx], c.eos))
        deeplog = session_max_rank(pkg.deeplog, u, c.h)[inv] >= c.g
        novelty = pkg.novelty.predict(count_matrix(ds, idx))
        normal, anomaly = y == 0, y == 1

        def recall(pred, anomaly=anomaly):
            return f"{pred[anomaly].mean():.4f}" if anomaly.any() else "–"

        lines.append(
            f"| {name} | {int(normal.sum()):,} | {deeplog[normal].mean():.4%} | {recall(deeplog)} "
            f"| {novelty[normal].mean():.4%} | {recall(novelty)} |"
        )
        if name == "test":
            test = {"DeepLog + EOS": prf(y, deeplog), "Sayım yeniliği": prf(y, novelty)}
    lines += ["", "## Paketin test sonucu (bir kez)", "", "| Model | Precision | Recall | F1 |"]
    lines.append("|---|---|---|---|")
    for model, r in test.items():
        lines.append(f"| {model} | {r['precision']:.4f} | {r['recall']:.4f} | {r['f1']:.4f} |")
    out = ROOT / "experiments" / "EXP-02" / f"genelleme-{c.name}.md"
    text = "\n".join(lines) + "\n"
    out.write_text(text, encoding="utf-8", newline="\n")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
