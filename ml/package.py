"""ML-07: Model paketi. Hız katmanının (M3) yükleyeceği her şey tek dizinde, sürümlü:

  <dizin>/<ad>/   ör. data/models/hdfs-v1/  ->  HDFS: /models/deeplog/hdfs-v1/
    deeplog.pt         DeepLog ağırlıkları (state_dict)
    known_counts.npy   sayım yeniliğinin bildiği normal olay sayım vektörleri (ADR-0009)
    config.json        h, g, sözlük, oturum sonu (EOS) kodu, model boyutları, kaynak commit
    metrics.json       paketi üreten değerlendirmenin doğrulama metrikleri
    templates.csv      (isteğe bağlı) EventId,EventTemplate: paketin kendi şablonları (demo izi,
                       Drain). Yoksa tüketici kendi şablon kaynağını kullanır (HDFS_v1: resmi
                       Loghub şablonları, ADR-0008).

Yükleme, kaydedilen yapılandırmayla modeli yeniden kurar; böylece eğitimdeki ve canlıdaki
model ayarları ayrışamaz (ADR-0001).
"""

import csv
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

import numpy as np
import torch

from ml.deeplog import DeepLog
from ml.novelty import CountNovelty

TEMPLATES = "templates.csv"


@dataclass(frozen=True)
class ModelConfig:
    name: str  # ör. hdfs-v1
    vocab: list[str]  # olay kodu -> EventId; vocab[0] = "PAD"
    eos: int | None  # oturum sonu olay kodu; None = EOS'suz model
    h: int  # pencere boyu
    g: int  # aday sayısı: gerçek olay ilk g tahminde değilse anormal (doğrulamada seçildi)
    hidden: int
    layers: int
    commit: str

    @property
    def vocab_size(self) -> int:
        """Modelin çıkış boyutu: sözlük + varsa EOS."""
        return len(self.vocab) + (1 if self.eos is not None else 0)


@dataclass
class ModelPackage:
    config: ModelConfig
    deeplog: DeepLog
    novelty: CountNovelty
    metrics: dict
    templates: dict[str, str] | None = field(default=None)  # EventId -> şablon (<*> joker)


def save(pkg: ModelPackage, root: Path) -> Path:
    out = Path(root) / pkg.config.name
    if out.exists() and any(out.iterdir()):
        raise FileExistsError(f"{out} zaten var; sürümler değiştirilmez, yeni ad verin")
    out.mkdir(parents=True, exist_ok=True)
    torch.save(pkg.deeplog.state_dict(), out / "deeplog.pt")
    vectors = [np.frombuffer(b, dtype=np.int32) for b in sorted(pkg.novelty.known)]
    known = np.stack(vectors) if vectors else np.zeros((0, 0), np.int32)
    np.save(out / "known_counts.npy", known)
    _write_json(out / "config.json", asdict(pkg.config))
    _write_json(out / "metrics.json", pkg.metrics)
    if pkg.templates is not None:
        with (out / TEMPLATES).open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerow(["EventId", "EventTemplate"])
            writer.writerows(pkg.templates.items())
    return out


def load(path: Path) -> ModelPackage:
    path = Path(path)
    config = ModelConfig(**json.loads((path / "config.json").read_text(encoding="utf-8")))
    model = DeepLog(config.vocab_size, hidden=config.hidden, layers=config.layers)
    model.load_state_dict(torch.load(path / "deeplog.pt", weights_only=True))
    model.eval()
    novelty = CountNovelty().fit(np.load(path / "known_counts.npy"))
    metrics = json.loads((path / "metrics.json").read_text(encoding="utf-8"))
    templates = None
    if (path / TEMPLATES).exists():
        with (path / TEMPLATES).open(newline="", encoding="utf-8") as f:
            templates = {r["EventId"]: r["EventTemplate"] for r in csv.DictReader(f)}
    return ModelPackage(config, model, novelty, metrics, templates)


def _write_json(path: Path, data) -> None:
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False, default=float) + "\n",
        encoding="utf-8",
        newline="\n",
    )
