"""ING-04/05: Kafka topic'leri -> HDFS raw bölgesi (Parquet), Spark Structured Streaming.

Her akış (STREAMS) için iki sorgu çalışır:
- Sözleşmeye uyan kayıtlar raw bölgesine (logs: date/hour, metrics: date bölümlü) yazılır.
- Uymayanlar ham haliyle ve nedeniyle karantinaya yazılır; hiçbir kayıt düşmez.
Sorgular dosya sink'i ve kendi checkpoint'leriyle çalışır. Spark yalnızca _spark_metadata'ya
işlenmiş dosyaları "var" sayar; bu yüzden yeniden başlatmada kayıp ya da tekrar oluşmaz.
Her satır Kafka konumunu (bölüm, offset) taşır; jobs/verify_ingest.py bununla denetler.

Çalıştırma: compose'taki spark-ingest servisi (docker compose up -d).
"""

import os
from dataclasses import dataclass

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from ingestion.schemas import SCHEMA_VERSION, LogEvent, MetricEvent, required_fields, spark_ddl

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
CHECKPOINTS = "/checkpoints/ingest_raw"


@dataclass(frozen=True)
class Stream:
    topic: str
    contract: type
    raw_path: str
    quarantine_path: str
    # Sorgu adları checkpoint dizinini belirler. DEĞİŞTİRİLMEZ: değişirse Kafka baştan okunur
    # ve HDFS'te her kayıt ikinci kez yazılır.
    query: str
    quarantine_query: str
    partitions: tuple[str, ...]
    trigger: str
    # Kafka'da saklama süresi (24 sa) dolduğu için silinmiş offset'lerde: True = sorgu durur.
    # Yığın 24 saatten uzun kapalı kalırsa tüketilmemiş veri silinir; bu durumda False olan akış
    # uyarıyla en eski mevcut offset'ten devam eder, True olan durur ve elle karar beklenir.
    fail_on_data_loss: bool


STREAMS = (
    Stream(
        "logs.raw", LogEvent, "/datalake/raw/logs", "/datalake/quarantine/logs",
        "logs", "quarantine", ("date", "hour"), os.environ.get("INGEST_TRIGGER", "30 seconds"),
        fail_on_data_loss=True,  # ana veri: kayıp sessizce geçilmez (K2)
    ),
    # Metrikler gerçek zamanlı olarak Kafka'dan okunur (Remediator); HDFS kopyası toplu analiz
    # içindir. 5 dk'lık tetikleyici, 5 sn'lik ölçümlerin binlerce küçük dosyaya bölünmesini önler.
    Stream(
        "metrics.raw", MetricEvent, "/datalake/raw/metrics", "/datalake/quarantine/metrics",
        "metrics", "metrics_quarantine", ("date",), os.environ.get("METRICS_TRIGGER", "5 minutes"),
        # telemetri: silinmiş ölçüm geri gelmez; 20.11.2026'da bu yüzden sorgu 3 gün çöküp
        # yeniden başladı ve aynı süreçteki log sorgusunu da her seferinde düşürdü
        fail_on_data_loss=False,
    ),
)  # fmt: skip


def _extra_rules(contract: type, e: Column) -> list[tuple[Column, str]]:
    """Sözleşme sınıflarındaki _check() kurallarının Spark karşılığı."""
    if contract is LogEvent:
        return [(e.label.isNotNull() & ~e.label.isin(0, 1), "label geçersiz")]
    if contract is MetricEvent:
        counters = ["mem_bytes", "mem_limit_bytes", *MetricEvent._COUNTERS]
        negative = F.lit(False)
        for name in counters:
            negative = negative | (e[name] < 0)
        return [
            (negative | (e.cpu_pct < 0), "negatif değer"),
            (~e.disk_used_pct.between(0, 100), "disk_used_pct aralık dışı"),
        ]
    return []


def problem(raw: Column, e: Column, contract: type) -> Column:
    """Kaydın sözleşmeye neden uymadığı; uyuyorsa null."""
    # Spark 4'te from_json bozuk girdide null değil, alanları boş bir yapı döner. Bu yüzden
    # geçerlilik ham metinden denetlenir: json_object_keys, geçersiz veya nesne olmayan
    # JSON'da null döner.
    expr = F.when(F.json_object_keys(raw).isNull(), F.lit("JSON ayrıştırılamadı"))
    for name in required_fields(contract):  # tipi uymayan alan da from_json'da null olur
        missing = e[name].isNull()
        if contract.SQL[name] == "STRING":
            missing = missing | (F.trim(e[name]) == "")
        expr = expr.when(missing, F.lit(f"{name} eksik, boş veya geçersiz"))
    expr = expr.when(e.schema_version != SCHEMA_VERSION, F.lit("schema_version desteklenmiyor"))
    for condition, reason in _extra_rules(contract, e):
        expr = expr.when(F.coalesce(condition, F.lit(False)), F.lit(reason))
    return expr


def start_stream(spark: SparkSession, s: Stream) -> None:
    staged = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", s.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 500_000)
        .option("failOnDataLoss", str(s.fail_on_data_loss).lower())
        .load()
        .select(
            F.col("value").cast("string").alias("raw"),
            F.from_json(F.col("value").cast("string"), spark_ddl(s.contract)).alias("e"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingest_time"),
        )
        .withColumn("problem", problem(F.col("raw"), F.col("e"), s.contract))
    )
    meta = ["kafka_partition", "kafka_offset", "kafka_timestamp", "ingest_time"]
    valid = staged.where(F.col("problem").isNull()).select("e.*", *meta)
    valid = valid.withColumn("date", F.date_format("event_time", "yyyy-MM-dd"))
    if "hour" in s.partitions:
        valid = valid.withColumn("hour", F.hour("event_time"))
    rejected = (
        staged.where(F.col("problem").isNotNull())
        .select("raw", "problem", *meta)
        .withColumn("date", F.date_format("ingest_time", "yyyy-MM-dd"))
    )
    for df, name, path, partitions in (
        (valid, s.query, s.raw_path, s.partitions),
        (rejected, s.quarantine_query, s.quarantine_path, ("date",)),
    ):
        # coalesce(1): her micro-batch her bölüm dizinine tek dosya yazar (az sayıda küçük dosya).
        (
            df.coalesce(1)
            .writeStream.queryName(name)
            .format("parquet")
            .option("path", path)
            .option("checkpointLocation", f"{CHECKPOINTS}/{name}")
            .partitionBy(*partitions)
            .trigger(processingTime=s.trigger)
            .start()
        )


def main() -> None:
    spark = SparkSession.builder.appName("ingest-raw").getOrCreate()
    for s in STREAMS:
        start_stream(spark, s)
    # Bir sorgu düşerse süreç hatayla çıkar ve Docker yeniden başlatır (restart: on-failure).
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
