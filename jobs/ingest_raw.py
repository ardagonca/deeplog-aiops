"""ING-04: logs.raw -> HDFS raw bölgesi (Parquet), Spark Structured Streaming.

- Sözleşmeye uyan kayıtlar /datalake/raw/logs/date=YYYY-MM-DD/hour=HH/ altına yazılır.
- Uymayanlar ham haliyle ve nedeniyle /datalake/quarantine/logs/ altına yazılır;
  hiçbir kayıt düşmez.
- İki sorgu da dosya sink'i ve kendi checkpoint'iyle çalışır. Spark yalnızca _spark_metadata'ya
  işlenmiş dosyaları "var" sayar; bu yüzden yeniden başlatmada kayıp ya da tekrar oluşmaz.
- Her satır Kafka konumunu (bölüm, offset) taşır; jobs/verify_ingest.py bununla denetler.

Çalıştırma: compose'taki spark-ingest servisi (docker compose up -d).
"""

import os

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from ingestion.schemas import LOG_EVENT_DDL, SCHEMA_VERSION

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TRIGGER = os.environ.get("INGEST_TRIGGER", "30 seconds")
RAW_PATH = "/datalake/raw/logs"
QUARANTINE_PATH = "/datalake/quarantine/logs"
CHECKPOINTS = "/checkpoints/ingest_raw"


def _blank(col: Column) -> Column:
    return col.isNull() | (F.trim(col) == "")


def problem(raw: Column, e: Column) -> Column:
    """Kaydın sözleşmeye (ingestion.schemas.LogEvent) neden uymadığı; uyuyorsa null."""
    return (
        # Spark 4'te from_json bozuk girdide null değil, alanları boş bir yapı döner. Bu yüzden
        # geçerlilik ham metinden denetlenir: json_object_keys, geçersiz veya nesne olmayan
        # JSON'da null döner.
        F.when(F.json_object_keys(raw).isNull(), F.lit("JSON ayrıştırılamadı"))
        .when(_blank(e.id) | _blank(e.source) | _blank(e.message), F.lit("zorunlu alan boş"))
        .when(e.event_time.isNull(), F.lit("event_time eksik veya geçersiz"))
        .when(F.coalesce(e.schema_version != SCHEMA_VERSION, F.lit(True)), F.lit("schema_version"))
        .when(e.label.isNotNull() & ~e.label.isin(0, 1), F.lit("label geçersiz"))
    )


def main() -> None:
    spark = SparkSession.builder.appName("ingest-raw").getOrCreate()
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", "logs.raw")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 500_000)
        .load()
    )
    staged = source.select(
        F.col("value").cast("string").alias("raw"),
        F.from_json(F.col("value").cast("string"), LOG_EVENT_DDL).alias("e"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingest_time"),
    ).withColumn("problem", problem(F.col("raw"), F.col("e")))

    valid = (
        staged.where(F.col("problem").isNull())
        .select("e.*", "kafka_partition", "kafka_offset", "kafka_timestamp", "ingest_time")
        .withColumn("date", F.date_format("event_time", "yyyy-MM-dd"))
        .withColumn("hour", F.hour("event_time"))
    )
    rejected = (
        staged.where(F.col("problem").isNotNull())
        .select(
            "raw", "problem", "kafka_partition", "kafka_offset", "kafka_timestamp", "ingest_time"
        )
        .withColumn("date", F.date_format("ingest_time", "yyyy-MM-dd"))
    )

    def start(df, name: str, path: str, partitions: list[str]):
        # coalesce(1): her micro-batch her bölüm dizinine tek dosya yazar (az sayıda küçük dosya).
        return (
            df.coalesce(1)
            .writeStream.queryName(name)
            .format("parquet")
            .option("path", path)
            .option("checkpointLocation", f"{CHECKPOINTS}/{name}")
            .partitionBy(*partitions)
            .trigger(processingTime=TRIGGER)
            .start()
        )

    start(valid, "logs", RAW_PATH, ["date", "hour"])
    start(rejected, "quarantine", QUARANTINE_PATH, ["date"])
    # Bir sorgu düşerse süreç hatayla çıkar ve Docker yeniden başlatır (restart: on-failure).
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
