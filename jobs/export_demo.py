"""REM-03: Demo izinin loglarını veri gölünden (raw bölge) dışa aktarır; model eğitimi buradan
beslenir (Lambda batch katmanı, ADR-0001).

Kullanım (infra/ dizininde):
  docker compose --profile jobs run --rm spark-job /opt/deeplog/jobs/export_demo.py \\
      --since 2026-11-23T17:54:00Z --until 2026-11-23T18:54:00Z --out demo-normal-1
Çıktı: data/exports/<out>.jsonl.gz (satır başına bir log; source, session_key, event_time,
message, level, kafka_partition, kafka_offset). Var olan dosyanın üzerine yazılmaz.
"""

import argparse
import gzip
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW = "/datalake/raw/logs"
SOURCES = ("demo-app", "demo-client")
COLUMNS = ("source", "session_key", "event_time", "message", "level", "kafka_partition",
           "kafka_offset")  # fmt: skip


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--since", required=True, help="UTC ISO, dahil")
    parser.add_argument("--until", required=True, help="UTC ISO, hariç")
    parser.add_argument("--out", required=True, help="dosya adı (uzantısız)")
    args = parser.parse_args()
    out = Path("/exports") / f"{args.out}.jsonl.gz"
    if out.exists():
        raise SystemExit(f"{out} zaten var; dışa aktarımlar değiştirilmez")

    spark = SparkSession.builder.appName("export-demo").getOrCreate()
    since, until = F.to_timestamp(F.lit(args.since)), F.to_timestamp(F.lit(args.until))
    rows = (
        spark.read.parquet(RAW)
        .where(F.col("date").between(args.since[:10], args.until[:10]))  # bölüm budama
        .where(F.col("source").isin(*SOURCES))
        .where((F.col("event_time") >= since) & (F.col("event_time") < until))
        .select(
            *COLUMNS[:2],
            F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX").alias("event_time"),
            *COLUMNS[3:],
        )
    )
    n = 0
    tmp = out.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8", newline="\n") as f:
        for row in rows.toLocalIterator():  # sürücü belleği: satırlar akış halinde gelir
            f.write(json.dumps(row.asDict(), ensure_ascii=False) + "\n")
            n += 1
    tmp.rename(out)
    print(f"{n:,} satır -> {out}")
    spark.stop()


if __name__ == "__main__":
    main()
