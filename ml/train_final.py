"""ML-07: Sistemde kullanılacak model paketini üretir (ADR-0009: DeepLog + EOS ve sayım yeniliği).

Kullanım: uv run python -m ml.train_final [--name hdfs-v1] [--seeds 5] [--threads 3]
- DeepLog + EOS, EXP-02 ayarlarıyla --seeds farklı tohumla eğitilir; doğrulama F1'i en yüksek
  tohum seçilir (seçim yalnızca doğrulama setinde; test setine bakılmaz).
- Sayım yeniliği eğitimdeki normal oturumların sayım vektörleriyle kurulur.
- Paket data/models/<ad>/ altına yazılır; HDFS'e jobs/publish_model.py ile yüklenir.
"""

import argparse
import subprocess
import sys
from pathlib import Path

import torch

from ml import dataset
from ml.baseline_if import count_matrix
from ml.deeplog import DeepLog, session_max_rank, train, unique_sequences, windows, with_eos
from ml.exp02 import BATCH, EPOCHS, LR, H
from ml.metrics import best_threshold, prf
from ml.novelty import CountNovelty
from ml.package import ModelConfig, ModelPackage, save

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--name", default="hdfs-v1")
    parser.add_argument("--seeds", type=int, default=5)
    parser.add_argument("--threads", type=int, default=3)
    args = parser.parse_args()
    torch.set_num_threads(args.threads)

    ds = dataset.load()
    eos = ds.vocab_size
    train_idx, val_idx = ds.indices("train"), ds.indices("val")
    y_val = ds.labels[val_idx]
    u, c, _ = unique_sequences(with_eos([ds.sequence(i) for i in train_idx], eos))
    X, y, w = windows(u, H, seq_weights=c)
    vu, _, vinv = unique_sequences(with_eos([ds.sequence(i) for i in val_idx], eos))

    best = None
    for seed in range(args.seeds):
        torch.manual_seed(seed)
        model = DeepLog(ds.vocab_size + 1)
        train(model, X, y, w, EPOCHS, batch_size=BATCH, lr=LR, seed=seed)
        rank = session_max_rank(model, vu, H)[vinv]
        g, f1 = best_threshold(rank, y_val)
        print(f"tohum {seed}: doğrulama F1 {f1:.4f} (g={g:.0f})", flush=True)
        if best is None or f1 > best[0]:
            best = (f1, seed, int(g), model, prf(y_val, rank >= g))

    f1, seed, g, model, val_metrics = best
    novelty = CountNovelty().fit(count_matrix(ds, train_idx))
    novelty_val = prf(y_val, novelty.predict(count_matrix(ds, val_idx)))
    commit = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"], capture_output=True, text=True
    ).stdout.strip()
    config = ModelConfig(
        name=args.name,
        vocab=[str(v) for v in ds.vocab],
        eos=eos,
        h=H,
        g=g,
        hidden=64,
        layers=2,
        commit=commit,
    )
    metrics = {
        "secilen_tohum": seed,
        # Aynı tohum, farklı iş parçacığı sayısında farklı model verir (kayan nokta toplam sırası).
        "egitim": {
            "epoch": EPOCHS,
            "batch": BATCH,
            "lr": LR,
            "torch_is_parcacigi": args.threads,
            "torch": torch.__version__,
        },
        "dogrulama": {"deeplog_eos": val_metrics, "sayim_yeniligi": novelty_val},
        "test_sonuclari": "experiments/EXP-02 (bu paket test setiyle seçilmedi)",
    }
    out = save(ModelPackage(config, model, novelty, metrics), ROOT / "data" / "models")
    print(f"Paket: {out} (tohum {seed}, g={g}, doğrulama F1 {f1:.4f})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
