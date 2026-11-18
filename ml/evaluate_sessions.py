"""ML-02 doğrulaması: tam HDFS_v1'de oturumları kurar, Loghub'ın resmi Event_traces.csv'siyle
karşılaştırır ve kronolojik bölmeyi raporlar.

Kullanım: uv run python -m ml.evaluate_sessions [--data-dir data]   (~4-5 dk)
Çıktı: experiments/ML-02/sessions.md (betik üretir, elle düzenlenmez)
"""

import argparse
import csv
import subprocess
import sys
from pathlib import Path

from ingestion.replayer import load_labels
from ml.parsing import new_miner
from ml.sessions import build_sessions, chronological_split

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "experiments" / "ML-02" / "sessions.md"


def official_traces(path: Path) -> dict[str, tuple[int, int]]:
    """BlockId -> (resmi dizi uzunluğu, etiket 1/0). Features biçimi: "[E5,E22,...]"."""
    csv.field_size_limit(2**31 - 1)  # uzun diziler varsayılan alan sınırını aşıyor
    with path.open(newline="", encoding="utf-8") as f:
        return {
            r["BlockId"]: (r["Features"].count(",") + 1, int(r["Label"] == "Fail"))
            for r in csv.DictReader(f)
        }


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    hdfs = parser.parse_args().data_dir / "HDFS_v1"

    miner = new_miner(sim_th=0.5)
    with (hdfs / "HDFS.log").open(encoding="utf-8", errors="replace") as f:
        sequences, first_seen = build_sessions(f, lambda c: miner.add_log_message(c)["cluster_id"])
    labels = load_labels(hdfs / "preprocessed" / "anomaly_label.csv")
    official = official_traces(hdfs / "preprocessed" / "Event_traces.csv")

    ours, theirs = set(sequences), set(official)
    common = ours & theirs
    mismatched = [b for b in first_seen if b in common and len(sequences[b]) != official[b][0]]
    label_mismatch = sum(1 for b in common if labels.get(b) != official[b][1])
    split = chronological_split(first_seen, labels)

    commit = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, cwd=ROOT
    ).stdout.strip()
    lines = [
        "# ML-02: HDFS_v1 oturumları ve kronolojik bölme",
        "",
        "`ml.evaluate_sessions` üretir; elle düzenlenmez. Olay kimliği: Drain (sim_th=0,5). "
        f"Commit: {commit}",
        "",
        "## Loghub resmi Event_traces.csv ile karşılaştırma",
        "",
        "| Ölçüt | Değer |",
        "|---|---|",
        f"| Bizim blok sayımız | {len(ours):,} |",
        f"| Resmi blok sayısı | {len(theirs):,} |",
        f"| Yalnızca bizde / yalnızca resmide | {len(ours - theirs):,} / {len(theirs - ours):,} |",
        f"| Dizi uzunluğu uyumu | {1 - len(mismatched) / len(common):.4%} "
        f"({len(common) - len(mismatched):,} / {len(common):,}) |",
        f"| Etiket uyuşmazlığı (anomaly_label.csv ↔ Event_traces) | {label_mismatch:,} |",
        "",
    ]
    if mismatched:
        lines += ["Uzunluğu uyuşmayan ilk 5 blok:", "", "| Blok | Bizde | Resmi |", "|---|---|---|"]
        lines += [f"| {b} | {len(sequences[b])} | {official[b][0]} |" for b in mismatched[:5]]
        lines.append("")
    lines += [
        "## Kronolojik bölme (60/10/30, eğitimde yalnızca normal)",
        "",
        "| Bölme | Oturum | Anormal | Anormal oranı |",
        "|---|---|---|---|",
    ]
    for name, blocks in split.items():
        anomalies = sum(labels[b] for b in blocks)
        lines.append(
            f"| {name} | {len(blocks):,} | {anomalies:,} | {anomalies / len(blocks):.2%} |"
        )
    report = "\n".join(lines) + "\n"
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(report, encoding="utf-8", newline="\n")
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
