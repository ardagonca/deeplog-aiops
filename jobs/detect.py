"""RT-01..03: Hız katmanı. logs.raw -> oturum bazında skorlama -> alerts (ADR-0004, ADR-0009).

- Kafka okuma, offset/checkpoint ve satırı bloklarına ayırma Spark'ta; karar mantığı
  ml.detector.Detector'da (Spark'tan bağımsız, birim testli; skorlama offline ile ortak, RT-05).
- Her micro-batch foreachBatch ile sürücüde TOPLU skorlanır: tüm oturumların pencereleri tek ileri
  geçişte. Oturum başına çağrı yapan applyInPandasWithState ~1.100 log/sn'de kalıyordu (RT-06).
- Veri gelmediğinde Spark batch çalıştırmaz; zaman aşımı kararlarını ayrı bir zamanlayıcı verir.
- Etiket alanı okunmaz (tests/test_detect_contract.py).

Çalıştırma: compose'taki spark-detect servisi.
"""

import os
import threading
import time
from datetime import datetime, timezone

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingestion.schemas import LOG_EVENT_DDL
from jobs.ingest_raw import KAFKA

MODEL = os.environ.get("DETECT_MODEL", "hdfs-v1")
TEMPLATES = "/opt/deeplog/data/HDFS_v1/preprocessed/HDFS.log_templates.csv"
TRIGGER = os.environ.get("DETECT_TRIGGER", "2 seconds")
TIMEOUT_S = float(os.environ.get("DETECT_SESSION_TIMEOUT_SEC", "120"))
EXPIRE_EVERY_S = 5


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def main() -> None:
    import torch

    from ml.detector import Detector
    from ml.official_templates import load_matcher
    from ml.package import load

    spark = SparkSession.builder.appName("detect").getOrCreate()
    spark.sparkContext.addFile(f"hdfs:///models/deeplog/{MODEL}", recursive=True)
    torch.set_num_threads(2)
    detector = Detector(load(SparkFiles.get(MODEL)), load_matcher(TEMPLATES), TIMEOUT_S)
    lock = threading.Lock()

    def publish(alerts) -> None:
        if alerts:
            rows = [(a.session_key, a.to_json().decode()) for a in alerts]
            (
                spark.createDataFrame(rows, "key STRING, value STRING")
                .write.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA)
                .option("topic", "alerts")
                .save()
            )

    def on_batch(batch, _batch_id) -> None:
        rows = batch.orderBy("event_time", "kafka_partition", "kafka_offset").collect()
        with lock:
            alerts = detector.process(
                ((r.session_key, r.source, r.message, r.event_time) for r in rows),
                time.monotonic(),
                _now_iso(),
            )
        publish(alerts)  # en az bir kez; alert_id deterministik olduğu için tekrarlar ayıklanabilir

    def expire_loop() -> None:
        while True:
            time.sleep(EXPIRE_EVERY_S)
            with lock:
                alerts = detector.expire(time.monotonic(), _now_iso())
            publish(alerts)

    threading.Thread(target=expire_loop, daemon=True).start()
    (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", "logs.raw")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 200_000)
        .load()
        .select(
            F.from_json(F.col("value").cast("string"), LOG_EVENT_DDL).alias("e"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .where("e.message IS NOT NULL AND e.event_time IS NOT NULL")
        # Satırdaki her blok kendi oturumuna (offline ml.sessions.build_sessions ile aynı kural:
        # satır içi tekrar bir kez; ör. çoklu "delete" satırı listedeki her bloğa gider).
        .select(
            F.explode(
                F.array_distinct(F.regexp_extract_all("e.message", F.lit(r"(blk_-?\d+)"), 1))
            ).alias("session_key"),
            "e.source",
            "e.message",
            F.date_format("e.event_time", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("event_time"),
            "kafka_partition",
            "kafka_offset",
        )
        .writeStream.foreachBatch(on_batch)
        .option("checkpointLocation", "/checkpoints/detect")
        .trigger(processingTime=TRIGGER)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
