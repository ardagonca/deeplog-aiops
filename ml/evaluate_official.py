"""ADR-0008 kanıtı: resmi HDFS_v1 şablonlarının tam veride kapsaması ve blok dizisi uyumu.

Kullanım: uv run python -m ml.evaluate_official [--data-dir data]   (~3 dk)
Çıktı: experiments/ML-01/official_templates.md (betik üretir, elle düzenlenmez)
"""

import argparse
import csv
import random
import re
import sys
from pathlib import Path

from ml.official_templates import load_templates
from ml.parsing import content_of

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "experiments" / "ML-01" / "official_templates.md"
BLOCK = re.compile(r"blk_-?\d+")
SAMPLE_BLOCKS, SEED = 2000, 42


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    hdfs = parser.parse_args().data_dir / "HDFS_v1"
    templates = load_templates(hdfs / "preprocessed" / "HDFS.log_templates.csv")

    csv.field_size_limit(1 << 30)  # Features sütunu çok uzun olabilir
    with (hdfs / "preprocessed" / "Event_traces.csv").open(newline="", encoding="utf-8") as f:
        official = {r["BlockId"]: r["Features"].strip("[]").split(",") for r in csv.DictReader(f)}
    sample = set(random.Random(SEED).sample(sorted(official), SAMPLE_BLOCKS))
    ours: dict[str, list[str]] = {b: [] for b in sample}

    total = matched = multi = 0
    unmatched: list[str] = []
    with (hdfs / "HDFS.log").open(encoding="utf-8", errors="replace") as f:
        for line in f:
            content = content_of(line)
            hits = [eid for eid, match in templates if match(content)]
            total += 1
            if hits:
                matched += 1
                multi += len(hits) > 1
            elif len(unmatched) < 10:
                unmatched.append(content)
            for block in dict.fromkeys(BLOCK.findall(content)):  # satır içi tekrar bir kez
                if block in ours and hits:
                    ours[block].append(hits[0])

    agree = sum(ours[b] == official[b] for b in sample)
    lines = [
        "# ADR-0008 kanıtı: resmi HDFS_v1 şablonları",
        "",
        "`ml.evaluate_official` üretir; elle düzenlenmez.",
        "",
        f"- Satır: {total:,}",
        f"- Resmi şablonlardan birine uyan: {matched:,} (**%{100 * matched / total:.4f}**)",
        f"- Birden çok şablona uyan (en özgülü seçildi): {multi:,}",
        f"- Loghub `Event_traces.csv` ile uyum ({SAMPLE_BLOCKS:,} rastgele blok, seed {SEED}): "
        f"{agree:,} blokta olay dizisi birebir aynı (**%{100 * agree / SAMPLE_BLOCKS:.2f}**)",
        "",
        "## Eşleşmeyen ilk satırlar",
        "",
    ]
    lines += [f"- `{c[:140]}`" for c in unmatched] or ["- yok"]
    report = "\n".join(lines) + "\n"
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(report, encoding="utf-8", newline="\n")
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
