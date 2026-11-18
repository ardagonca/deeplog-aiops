"""ING-06: Kafka ile HDFS (raw + karantina) arasında kayıp ve tekrar denetimi, her akış için.

Denetimin başında Kafka'daki konumların (bölüm, offset) anlık görüntüsü alınır; bu görüntüdeki
her konum HDFS'te tam olarak bir kez yer almalı. Anlık görüntü, sürekli yazılan topic'lerde
(metrics.raw) denetimin kayan bir hedefi kovalamasını önler. Ingest geride olabileceği için
kayıp sıfırlanana kadar --timeout saniye boyunca yeniden dener.
Kullanım: docker compose --profile jobs run --rm spark-job /opt/deeplog/jobs/verify_ingest.py

Not: Bir topic silinip yeniden oluşturulursa offset'ler sıfırlanır; ilgili raw/karantina
dizinleri ve ingest checkpoint'i de silinmelidir (docs/runbooks/altyapi.md).
"""

import argparse
import time

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from jobs.ingest_raw import KAFKA, STREAMS, Stream

KEYS = ["partition", "offset"]


def _hdfs_positions(spark: SparkSession, path: str) -> DataFrame:
    try:
        return spark.read.parquet(path).selectExpr(
            "kafka_partition AS partition", "kafka_offset AS offset"
        )
    except AnalysisException:  # ingest henüz hiç yazmadıysa dizin yoktur
        return spark.createDataFrame([], "partition INT, offset LONG")


def kafka_snapshot(spark: SparkSession, topic: str) -> DataFrame:
    positions = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(*KEYS)
    )
    return positions.localCheckpoint(eager=True)  # sonraki okumalarda Kafka'ya tekrar gidilmez


def check(spark: SparkSession, s: Stream, kafka: DataFrame) -> dict[str, int]:
    raw = _hdfs_positions(spark, s.raw_path)
    quarantine = _hdfs_positions(spark, s.quarantine_path)
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
    parser.add_argument("--timeout", type=int, default=420, help="metrikler 5 dk'da bir yazılır")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("verify-ingest").getOrCreate()
    snapshots = {s.topic: kafka_snapshot(spark, s.topic) for s in STREAMS}
    deadline = time.monotonic() + args.timeout
    while True:
        results = {s.topic: check(spark, s, snapshots[s.topic]) for s in STREAMS}
        print(f"[denetim] {results}", flush=True)
        if all(r["missing"] == 0 for r in results.values()) or time.monotonic() > deadline:
            break
        time.sleep(15)
    spark.stop()
    for topic, r in results.items():
        assert r["missing"] == 0, f"KAYIP: {topic} için Kafka'daki {r['missing']} kayıt HDFS'te yok"
        assert r["duplicates"] == 0, f"TEKRAR: {topic} için {r['duplicates']} konum birden çok kez"
    for topic, r in results.items():
        print(
            f"INGEST OK [{topic}]: {r['kafka']:,} kaydın tamamı HDFS'te tam bir kez var "
            f"(raw {r['raw']:,}, karantina {r['quarantine']:,})"
        )


if __name__ == "__main__":
    main()
