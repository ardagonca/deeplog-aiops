"""ING-06: logs.raw ile HDFS (raw + karantina) arasında kayıp ve tekrar denetimi.

Kafka'da şu an bulunan her (bölüm, offset) HDFS'te tam olarak bir kez yer almalı.
Ingest geride olabileceği için kayıp sıfırlanana kadar --timeout saniye boyunca yeniden dener.
Kullanım: docker compose --profile jobs run --rm spark-job /opt/deeplog/jobs/verify_ingest.py

Not: logs.raw silinip yeniden oluşturulursa offset'ler sıfırlanır; raw/karantina bölgeleri ve ingest
checkpoint'i de silinmelidir (docs/runbooks/altyapi.md).
"""

import argparse
import time

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from jobs.ingest_raw import KAFKA, QUARANTINE_PATH, RAW_PATH

KEYS = ["partition", "offset"]


def _hdfs_positions(spark: SparkSession, path: str) -> DataFrame:
    try:
        return spark.read.parquet(path).selectExpr(
            "kafka_partition AS partition", "kafka_offset AS offset"
        )
    except AnalysisException:  # ingest henüz hiç yazmadıysa dizin yoktur
        return spark.createDataFrame([], "partition INT, offset LONG")


def check(spark: SparkSession) -> dict[str, int]:
    kafka = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", "logs.raw")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(*KEYS)
    )
    raw = _hdfs_positions(spark, RAW_PATH)
    quarantine = _hdfs_positions(spark, QUARANTINE_PATH)
    hdfs = raw.unionByName(quarantine)
    return {
        "kafka": kafka.count(),
        "raw": raw.count(),
        "quarantine": quarantine.count(),
        "missing": kafka.join(hdfs, KEYS, "left_anti").count(),
        "duplicates": hdfs.groupBy(*KEYS).count().where("count > 1").count(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("verify-ingest").getOrCreate()
    deadline = time.monotonic() + args.timeout
    while True:
        result = check(spark)
        print(f"[denetim] {result}", flush=True)
        if result["missing"] == 0 or time.monotonic() > deadline:
            break
        time.sleep(15)
    spark.stop()
    assert result["missing"] == 0, f"KAYIP: Kafka'daki {result['missing']} kayıt HDFS'te yok"
    assert result["duplicates"] == 0, (
        f"TEKRAR: {result['duplicates']} Kafka konumu birden çok kez yazılmış"
    )
    print(
        f"INGEST OK: Kafka'daki {result['kafka']:,} kaydın tamamı HDFS'te tam bir kez var "
        f"(raw {result['raw']:,}, karantina {result['quarantine']:,})"
    )


if __name__ == "__main__":
    main()
