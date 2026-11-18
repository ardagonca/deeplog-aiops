"""ML-01 kabul ölçümü: Drain şablonlamasının Loghub HDFS 2k etiketli örneğindeki doğruluğu.

Kullanım: uv run python -m ml.evaluate_parsing [--full]   (--full: tam HDFS_v1 da, ~3 dk)
Önce: uv run python -m ingestion.download_dataset HDFS_2k
Çıktı: experiments/ML-01/parsing.md (betik üretir, elle düzenlenmez)
"""

import csv
import subprocess
import sys
from pathlib import Path

from ml.parsing import content_of, grouping_accuracy, mine, new_miner, templates

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "data" / "HDFS_2k" / "HDFS_2k.log_structured.csv"
OUT = ROOT / "experiments" / "ML-01" / "parsing.md"
SIM_THRESHOLDS = (0.3, 0.4, 0.5, 0.6, 0.7)
CHOSEN = 0.5  # ml.parsing.new_miner varsayılanı


def full_dataset_section() -> list[str]:
    """Tam HDFS_v1 (11,2 M satır, ~3 dk): bulunan şablon sayısı resmi listeyle karşılaştırılır."""
    log = ROOT / "data" / "HDFS_v1" / "HDFS.log"
    official = ROOT / "data" / "HDFS_v1" / "preprocessed" / "HDFS.log_templates.csv"
    miner = new_miner(sim_th=CHOSEN)
    with log.open(encoding="utf-8", errors="replace") as f:
        n = sum(1 for line in f if miner.add_log_message(content_of(line)))
    with official.open(newline="", encoding="utf-8") as f:
        official_count = sum(1 for _ in csv.DictReader(f))
    clusters = sorted(miner.drain.clusters, key=lambda c: c.size, reverse=True)
    lines = [
        "",
        "## Tam HDFS_v1",
        "",
        f"{n:,} satırda **{len(clusters)}** şablon bulundu; Loghub'ın resmi listesinde "
        f"**{official_count}** şablon var. Fark çoğunlukla seyrek istisna mesajlarından gelir: "
        "Drain istisna türüne göre ayrı şablon açar, resmi liste bunları tek `<*>` altında toplar.",
        "",
        "| Satır sayısı | Şablon |",
        "|---|---|",
    ]
    return lines + [f"| {c.size:,} | `{c.get_template()[:140]}` |" for c in clusters]


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    with SAMPLE.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    contents = [r["Content"] for r in rows]
    truth = [r["EventId"] for r in rows]

    lines = [
        "# ML-01: Şablonlama doğruluğu (Loghub HDFS 2k)",
        "",
        "`ml.evaluate_parsing` üretir. Ölçüt: Grouping Accuracy (Zhu vd., 2019).",
        f"Örnek: {len(rows):,} satır, {len(set(truth))} gerçek şablon. "
        f"Commit: {subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], capture_output=True, text=True, cwd=ROOT).stdout.strip()}",  # noqa: E501
        "",
        "| Benzerlik eşiği (sim_th) | Bulunan şablon | Gruplama doğruluğu | Seçilen |",
        "|---|---|---|---|",
    ]
    chosen = None
    for sim_th in SIM_THRESHOLDS:
        miner = new_miner(sim_th=sim_th)
        predicted = mine(miner, contents)
        ga = grouping_accuracy(truth, predicted)
        lines.append(
            f"| {sim_th} | {len(templates(miner))} | {ga:.4f} | {'✔' if sim_th == CHOSEN else ''} |"
        )
        if sim_th == CHOSEN:
            chosen = (ga, miner)
    ga, miner = chosen
    lines += ["", f"## Seçilen ayarla (sim_th={CHOSEN}) bulunan şablonlar", ""]
    lines += [f"- `{t}`" for _, t in sorted(templates(miner).items())]
    if "--full" in sys.argv:
        lines += full_dataset_section()
    report = "\n".join(lines) + "\n"
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(report, encoding="utf-8", newline="\n")  # Windows'ta da LF
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
