"""RT-01..03: Hız katmanı. logs.raw -> oturum bazında skorlama -> alerts (ADR-0004, ADR-0009).

- Kafka okuma, offset/checkpoint ve satırı bloklarına ayırma Spark'ta; karar mantığı
  ml.detector.Detector'da (Spark'tan bağımsız, birim testli; skorlama offline ile ortak, RT-05).
- Her micro-batch foreachBatch ile sürücüde TOPLU skorlanır: tüm oturumların pencereleri tek ileri
  geçişte. Oturum başına çağrı yapan applyInPandasWithState ~1.100 log/sn'de kalıyordu (RT-06).
- Veri gelmediğinde Spark batch çalıştırmaz; zaman aşımı kararlarını ayrı bir zamanlayıcı verir.
- Etiket alanı okunmaz (tests/test_detect_contract.py).
- Durum (RT-08): her batch'ten sonra açık oturumların anlık görüntüsü checkpoint dizinine yazılır;
  yeniden başlamada yüklenir. Spark yarım kalan batch'i yeniden oynatırsa ve batch zaten anlık
  görüntüdeyse atlanır. Görüntü checkpoint'in içinde durduğu için ikisi birlikte silinir.
- İş geride kaldığında (batch maxOffsetsPerTrigger ile doluysa) zaman aşımı işletilmez: aksi halde
  Kafka'da bekleyen olayları olan oturumlar erken kapanırdı (ör. yeniden başlamadan sonra).

Çalıştırma: compose'taki spark-detect servisi.
"""

import os
import threading
import time
import traceback
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
MAX_OFFSETS = 200_000
CHECKPOINT = "/checkpoints/detect"
STATE_DIR = f"{CHECKPOINT}/detector_state"
KEEP_STATES = 2


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class _StateStore:
    """Sürücüden HDFS'e (Hadoop FileSystem, py4j). Yazım: geçici dosya + atomik rename."""

    def __init__(self, spark):
        self.jvm = spark._jvm
        self.dir = self._path(STATE_DIR)
        self.fs = self.dir.getFileSystem(spark._jsc.hadoopConfiguration())

    def _path(self, p):
        return self.jvm.org.apache.hadoop.fs.Path(p)

    def _names(self) -> list[str]:
        if not self.fs.exists(self.dir):
            return []
        return sorted(s.getPath().getName() for s in self.fs.listStatus(self.dir))

    def latest(self) -> bytes | None:
        names = [n for n in self._names() if n.endswith(".json.gz")]
        if not names:
            return None
        stream = self.fs.open(self._path(f"{STATE_DIR}/{names[-1]}"))
        try:
            return bytes(self.jvm.org.apache.commons.io.IOUtils.toByteArray(stream))
        finally:
            stream.close()

    def save(self, batch_id: int, data: bytes) -> None:
        # Ad sıralaması = yazım sırası: aynı batch'ten sonra zaman aşımıyla da kaydedilebilir.
        name = f"{batch_id:012d}-{time.time_ns()}.json.gz"
        tmp = self._path(f"{STATE_DIR}/{name}.tmp")
        out = self.fs.create(tmp, True)
        try:
            out.write(bytearray(data))
        finally:
            out.close()
        if not self.fs.rename(tmp, self._path(f"{STATE_DIR}/{name}")):
            raise OSError(f"durum yazılamadı: {name}")
        for name in [n for n in self._names() if n.endswith(".json.gz")][:-KEEP_STATES]:
            self.fs.delete(self._path(f"{STATE_DIR}/{name}"), False)


def main() -> None:
    import torch

    from ml.detector import Detector
    from ml.official_templates import load_matcher
    from ml.package import load

    spark = SparkSession.builder.appName("detect").getOrCreate()
    spark.sparkContext.addFile(f"hdfs:///models/deeplog/{MODEL}", recursive=True)
    torch.set_num_threads(2)
    detector = Detector(load(SparkFiles.get(MODEL)), load_matcher(TEMPLATES), TIMEOUT_S)
    store = _StateStore(spark)
    if (saved := store.latest()) is not None:
        detector.restore(saved, time.time())
        print(
            f"durum yüklendi: batch {detector.last_batch}, {len(detector.open):,} açık oturum",
            flush=True,
        )
    lock = threading.Lock()
    # Son batch Kafka'daki birikimi bitirdi mi. Başta açık: veri hiç gelmezse de yüklenen oturumlar
    # kapanabilsin; yüklenen oturumların zaman aşımı sayacı zaten yeniden başladı.
    caught_up = threading.Event()
    caught_up.set()

    # Alarmlar sürücüden Kafka Java istemcisiyle (imajdaki kafka-clients) yazılır: Spark ile yazmak
    # her batch'e ayrı bir iş ekliyor ve 0,3-9 sn sürüyordu (RT-08 ölçümü).
    kafka = spark._jvm.org.apache.kafka.clients.producer
    props = spark._jvm.java.util.Properties()
    for k, v in {
        "bootstrap.servers": KAFKA,
        "acks": "all",
        "enable.idempotence": "true",
        "key.serializer": "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer": "org.apache.kafka.common.serialization.StringSerializer",
    }.items():
        props.setProperty(k, v)
    producer = kafka.KafkaProducer(props)

    def publish(alerts) -> None:
        sent = [
            producer.send(kafka.ProducerRecord("alerts", a.session_key, a.to_json().decode()))
            for a in alerts
        ]
        producer.flush()
        for future in sent:
            future.get()  # teslim edilemeyen varsa hata: durum kaydedilmez, alarm yeniden üretilir

    def on_batch(batch, batch_id) -> None:
        if batch_id <= detector.last_batch:  # yeniden oynatılan batch zaten durumda
            return
        t0 = time.perf_counter()
        # Sıralama sürücüde: Spark'ta orderBy, aralık bölümleme örneklemesi için kaynağı 2 kez okur.
        rows = sorted(
            batch.collect(), key=lambda r: (r.event_time, r.kafka_partition, r.kafka_offset)
        )
        offsets = len({(r.kafka_partition, r.kafka_offset) for r in rows})
        t1 = time.perf_counter()
        with lock:
            alerts = detector.process(
                ((r.session_key, r.source, r.message, r.event_time) for r in rows),
                time.time(),
                _now_iso(),
            )
        t2 = time.perf_counter()
        publish(alerts)  # en az bir kez; alert_id deterministik olduğu için tekrarlar ayıklanabilir
        t3 = time.perf_counter()
        with lock:  # Spark bu batch'i commit etmeden önce: kayıp batch olmaz
            detector.last_batch = batch_id
            store.save(batch_id, detector.dump())
            n_open = len(detector.open)
        print(
            f"batch {batch_id}: {len(rows):,} satır, {len(alerts)} alarm, {n_open:,} açık oturum; "
            f"okuma {t1 - t0:.1f} sn, skor {t2 - t1:.1f} sn, yayın {t3 - t2:.1f} sn, "
            f"durum {time.perf_counter() - t3:.1f} sn",
            flush=True,
        )
        # shortcut: bloksuz satırlar explode'da düştüğü için sayım eksik olabilir (geride kalmışken
        # "yetişti" sanılır); bloksuz kaynak (demo sistem, M4) gelince Kafka kaydı başına sayılmalı.
        if offsets < MAX_OFFSETS:
            caught_up.set()
        else:
            caught_up.clear()

    def expire_loop() -> None:
        try:
            _expire_forever()
        except Exception:  # iş parçacığı sessizce ölmesin; durum son kayda döner, Docker başlatır
            traceback.print_exc()
            os._exit(1)

    def _expire_forever() -> None:
        while True:
            time.sleep(EXPIRE_EVERY_S)
            if not caught_up.is_set():
                continue
            with lock:
                before = len(detector.open)
                alerts = detector.expire(time.time(), _now_iso())
            publish(alerts)
            if len(detector.open) != before:  # kapanan oturumlar yeniden başlamada geri gelmesin
                with lock:
                    store.save(detector.last_batch, detector.dump())

    threading.Thread(target=expire_loop, daemon=True).start()
    (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", "logs.raw")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
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
            F.date_format("e.event_time", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX").alias("event_time"),
            "kafka_partition",
            "kafka_offset",
        )
        .writeStream.foreachBatch(on_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
