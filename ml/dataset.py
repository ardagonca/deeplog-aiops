"""ML-02: İz A veri seti. HDFS_v1 oturumları (resmi şablonlarla, ADR-0008), etiketler ve
kronolojik bölme (docs/04-test-stratejisi.md §3).

Kullanım: uv run python -m ml.dataset [--data-dir data]   (ilk kez ~5 dk, sonra önbellekten)
Çıktı: data/processed/hdfs_v1.npz. Girdi dosyalarının SHA-256'sı dosyaya yazılır; önbellek,
girdiler değişmedikçe yeniden kullanılır.

Olay kodları: E1..E29 -> 1..29. 0, dolgu (padding) ve bilinmeyen olay içindir.
"""

import argparse
import csv
import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from ingestion.replayer import load_labels
from ml.official_templates import load_matcher
from ml.sessions import build_sessions, chronological_split

ROOT = Path(__file__).resolve().parents[1]
PAD = 0
SPLITS = {"train": 0, "val": 1, "test": 2}
EXCLUDED = -1  # eğitim dilimindeki anormal oturumlar: hiçbir bölmede kullanılmaz


@dataclass(frozen=True)
class Dataset:
    blocks: np.ndarray  # (n,) blok kimlikleri, ilk görülme sırasıyla
    labels: np.ndarray  # (n,) 1 anormal, 0 normal
    split: np.ndarray  # (n,) SPLITS değerleri ya da EXCLUDED
    events: np.ndarray  # tüm oturumların olay kodları art arda
    offsets: np.ndarray  # (n + 1,) oturum i = events[offsets[i]:offsets[i + 1]]
    vocab: np.ndarray  # olay kodu -> EventId; vocab[0] = "PAD"

    def sequence(self, i: int) -> np.ndarray:
        return self.events[self.offsets[i] : self.offsets[i + 1]]

    def indices(self, name: str) -> np.ndarray:
        return np.flatnonzero(self.split == SPLITS[name])

    @property
    def vocab_size(self) -> int:
        return len(self.vocab)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _vocab(templates_csv: Path) -> list[str]:
    with templates_csv.open(newline="", encoding="utf-8") as f:
        ids = [row["EventId"] for row in csv.DictReader(f)]
    return ["PAD", *sorted(ids, key=lambda e: int(e[1:]))]  # E1, E2, ..., E29


def build(data_dir: Path) -> Dataset:
    hdfs = data_dir / "HDFS_v1"
    templates_csv = hdfs / "preprocessed" / "HDFS.log_templates.csv"
    vocab = _vocab(templates_csv)
    code = {event_id: i for i, event_id in enumerate(vocab)}
    matcher = load_matcher(templates_csv)
    with (hdfs / "HDFS.log").open(encoding="utf-8", errors="replace") as lines:
        sequences, first_seen = build_sessions(lines, lambda c: code.get(matcher(c), PAD))
    labels = load_labels(hdfs / "preprocessed" / "anomaly_label.csv")
    parts = chronological_split(first_seen, labels)

    split = np.full(len(first_seen), EXCLUDED, dtype=np.int8)
    position = {block: i for i, block in enumerate(first_seen)}
    for name, blocks in parts.items():
        split[[position[b] for b in blocks]] = SPLITS[name]
    lengths = np.fromiter((len(sequences[b]) for b in first_seen), dtype=np.int64)
    return Dataset(
        blocks=np.array(first_seen),
        labels=np.fromiter((labels[b] for b in first_seen), dtype=np.int8),
        split=split,
        events=np.fromiter(
            (e for b in first_seen for e in sequences[b]), dtype=np.int8, count=int(lengths.sum())
        ),
        offsets=np.concatenate([[0], np.cumsum(lengths)]),
        vocab=np.array(vocab),
    )


def load(data_dir: Path = ROOT / "data") -> Dataset:
    """Önbellekten yükler; yoksa ya da girdiler değiştiyse yeniden üretir."""
    hdfs = data_dir / "HDFS_v1"
    inputs = [hdfs / "HDFS.log", hdfs / "preprocessed" / "anomaly_label.csv"]
    fingerprint = np.array([f"{p.name}:{_sha256(p)}" for p in inputs])
    cache = data_dir / "processed" / "hdfs_v1.npz"
    if cache.exists():
        with np.load(cache) as z:
            if np.array_equal(z["fingerprint"], fingerprint):
                return Dataset(**{k: z[k] for k in Dataset.__dataclass_fields__})
    ds = build(data_dir)
    cache.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(cache, fingerprint=fingerprint, **ds.__dict__)
    return ds


def summary(ds: Dataset) -> str:
    rows = ["| Bölme | Oturum | Anormal | Olay |", "|---|---|---|---|"]
    for name in SPLITS:
        idx = ds.indices(name)
        n_events = int((ds.offsets[idx + 1] - ds.offsets[idx]).sum())
        rows.append(f"| {name} | {len(idx):,} | {int(ds.labels[idx].sum()):,} | {n_events:,} |")
    excluded = int((ds.split == EXCLUDED).sum())
    rows.append(f"| dışarıda (eğitim dilimindeki anormal) | {excluded:,} | {excluded:,} | – |")
    unknown = int((ds.events == PAD).sum())
    return "\n".join(rows) + f"\n\nSözlük: {ds.vocab_size - 1} olay. Bilinmeyen olay: {unknown:,}."


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    print(summary(load(parser.parse_args().data_dir)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
