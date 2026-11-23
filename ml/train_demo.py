"""REM-03: Demo izi için model paketi (deeplog-demo-vN): Drain şablonları, DeepLog + EOS, sayım
yeniliği.

Kullanım:
  uv run --group ml python -m ml.train_demo --data data/exports/<ad>.jsonl.gz --name deeplog-demo-v1

- Veri yalnızca normal yüktür (arıza enjekte edilmemiş pencere, jobs/export_demo.py). Oturumlar
  kronolojik olarak 60/10/30 bölünür.
- Şablonlar yalnızca eğitim bölümünden Drain ile çıkarılır. Canlıda kullanılacak eşleştirici
  (paketteki templates.csv) ile Drain kümelerinin uyumu eğitim verisinde denetlenir.
- Etiketli anomali yoktur; eşik g, doğrulama setinde yanlış alarm oranını (FPR) --max-fpr altında
  tutan en küçük değerdir (en duyarlı). Test FPR'si bir kez raporlanır. Arızaları yakalama
  başarısı EXP-04'te enjeksiyon kayıtlarıyla ölçülür.
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

import numpy as np
import torch

from ml.deeplog import DeepLog, session_max_rank, train, unique_sequences, windows, with_eos
from ml.demo_data import chronological_split, load_sessions
from ml.exp02 import BATCH, LR, H
from ml.novelty import CountNovelty
from ml.official_templates import compile_templates, matcher_from
from ml.package import ModelConfig, ModelPackage, save
from ml.parsing import new_miner, templates

ROOT = Path(__file__).resolve().parents[1]
MASK = re.compile(r"<[A-Z]+>|<\*>")
MIN_AGREEMENT = 0.999


def drain_templates(contents) -> tuple[dict[str, str], object]:
    """Eğitim içeriklerinden şablonlar: {"D<küme>": şablon (maskeler <*>)} ve madenci."""
    miner = new_miner()
    for c in contents:
        miner.add_log_message(c)
    return {f"D{cid}": MASK.sub("<*>", t) for cid, t in sorted(templates(miner).items())}, miner


def encode(sessions, matcher, code) -> list[np.ndarray]:
    return [np.array([code.get(matcher(c), 0) for c in s.contents], np.int64) for s in sessions]


def counts(seqs, size: int) -> np.ndarray:
    return np.stack([np.bincount(s, minlength=size) for s in seqs]) if seqs else np.zeros((0, size))


def smallest_g(ranks: np.ndarray, max_fpr: float, vocab_size: int) -> int:
    """FPR(g) = P(max sıra >= g), g arttıkça azalır; FPR <= max_fpr olan en küçük g."""
    for g in range(1, vocab_size + 1):
        if (ranks >= g).mean() <= max_fpr:
            return g
    return vocab_size + 1  # hiçbir g yetmiyor: bilinmeyen olaylar normal veride bile çok


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--max-fpr", type=float, default=0.01, help="REM-03 kabul ölçütü")
    parser.add_argument("--steps", type=int, default=3000, help="toplam eğitim adımı")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()
    torch.set_num_threads(args.threads)

    sessions = load_sessions(args.data)
    tr, va, te = chronological_split(sessions)
    tmpl, miner = drain_templates(c for s in tr for c in s.contents)
    matcher = matcher_from(compile_templates(list(tmpl.items())))
    train_contents = [c for s in tr for c in s.contents]
    agreement = float(
        np.mean([matcher(c) == f"D{miner.match(c).cluster_id}" for c in train_contents])
    )
    if agreement < MIN_AGREEMENT:
        print(f"eşleştirici Drain ile uyumsuz: {agreement:.4%}", file=sys.stderr)
        return 1
    vocab = ["PAD", *tmpl]
    code = {e: i for i, e in enumerate(vocab)}
    eos = len(vocab)
    seq = {name: encode(part, matcher, code) for name, part in (("train", tr), ("val", va),
                                                                ("test", te))}  # fmt: skip

    u, c, _ = unique_sequences(with_eos(seq["train"], eos))
    X, y, w = windows(u, H, seq_weights=c)
    epochs = max(1, -(-args.steps // max(1, -(-len(y) // BATCH))))
    torch.manual_seed(args.seed)
    model = DeepLog(eos + 1)
    losses = train(model, X, y, w, epochs, batch_size=BATCH, lr=LR, seed=args.seed)

    def ranks(name):
        uu, _, inv = unique_sequences(with_eos(seq[name], eos))
        return session_max_rank(model, uu, H)[inv]

    g = smallest_g(ranks("val"), args.max_fpr, eos + 1)
    novelty = CountNovelty().fit(counts(seq["train"], len(vocab)))
    fpr = {
        name: {
            "deeplog_eos": float((ranks(name) >= g).mean()),
            "sayim_yeniligi": float(novelty.predict(counts(seq[name], len(vocab))).mean()),
            "bilinmeyen_olay": float(np.mean([(s == 0).any() for s in seq[name]])),
        }
        for name in ("val", "test")
    }
    commit = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"], capture_output=True, text=True
    ).stdout.strip()
    config = ModelConfig(args.name, vocab, eos, H, g, hidden=64, layers=2, commit=commit)
    metrics = {
        "veri": {
            "dosya": args.data.name,
            "oturum": {"train": len(tr), "val": len(va), "test": len(te)},
            "aralik": [sessions[0].start, sessions[-1].start],
        },
        "sablon": {"sayi": len(tmpl), "eslestirici_drain_uyumu": agreement},
        "egitim": {
            "adim": epochs * max(1, -(-len(y) // BATCH)),
            "epoch": epochs,
            "son_kayip": losses[-1],
            "tohum": args.seed,
            "torch": torch.__version__,
            "torch_is_parcacigi": args.threads,
        },  # fmt: skip
        "esik": {"g": g, "hedef_fpr": args.max_fpr},
        "yanlis_alarm_orani": fpr,
    }
    out = save(ModelPackage(config, model, novelty, metrics, tmpl), ROOT / "data" / "models")
    print(f"Paket: {out}")
    print(f"şablon {len(tmpl)}, uyum {agreement:.4%}, g={g}, son kayıp {losses[-1]:.4f}")
    for name, v in fpr.items():
        print(
            f"{name}: FPR DeepLog {v['deeplog_eos']:.4%}, sayım yeniliği {v['sayim_yeniligi']:.4%}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
