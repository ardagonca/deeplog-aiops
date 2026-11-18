"""ING-03: HDFS_v1 loglarını ayarlanabilir hızla Kafka'ya (logs.raw) basar.

Kullanım:
  uv run python -m ingestion.replayer --rate 5000 --limit 300000   # 60 sn, 5.000 log/sn
  uv run python -m ingestion.replayer --rate 0                     # hız sınırı yok, tüm dosya

- event_time gönderim anıdır (UTC). Orijinal satır, kendi zaman damgasıyla
  message alanında aynen kalır.
- session_key satırdaki ilk blok kimliğidir ve Kafka anahtarı olarak kullanılır; böylece
  bir oturumun olayları aynı bölümde ve sırayla kalır. Birden çok blok geçen satırlar
  ML-02'de yeniden ayrıştırılır.
- label oturum (blok) etiketidir (1 anormal, 0 normal); yalnızca değerlendirme içindir.
"""

import argparse
import csv
import re
import sys
import time
import uuid
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from pathlib import Path

from ingestion.schemas import LogEvent

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "HDFS_v1"
SOURCE = "hdfs-replay"
BLOCK_ID = re.compile(r"blk_-?\d+")
PROGRESS_EVERY = 100_000


def load_labels(path: Path) -> dict[str, int]:
    with path.open(newline="", encoding="utf-8") as f:
        return {row["BlockId"]: int(row["Label"] == "Anomaly") for row in csv.DictReader(f)}


def to_event(line: str, labels: dict[str, int], now: datetime, source: str = SOURCE) -> LogEvent:
    line = line.rstrip("\r\n")
    parts = line.split(" ", 4)  # tarih saat pid seviye "bileşen: içerik"
    block = match.group() if (match := BLOCK_ID.search(line)) else None
    return LogEvent(
        id=str(uuid.uuid4()),
        source=source,
        event_time=now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        message=line,
        session_key=block,
        level=parts[3] if len(parts) == 5 else None,
        label=labels.get(block) if block else None,
    )


def replay(
    lines: Iterable[str],
    send: Callable[[bytes, bytes], None],
    labels: dict[str, int],
    rate: float,
    limit: int | None = None,
    clock: Callable[[], float] = time.perf_counter,
    sleep: Callable[[float], None] = time.sleep,
    source: str = SOURCE,
) -> int:
    """Satırları LogEvent'e çevirip gönderir; gönderilen mesaj sayısını döner.

    rate > 0 ise i. mesaj başlangıçtan i/rate sn sonra gönderilir. Mutlak takvim kullanıldığı için
    sleep'in kaba çözünürlüğü (Windows'ta ~15 ms) birikmez; ortalama hız korunur.
    """
    start = clock()
    sent = 0
    for line in lines:
        if limit is not None and sent >= limit:
            break
        if not line.strip():
            continue
        if rate > 0 and (delay := start + sent / rate - clock()) > 0:
            sleep(delay)
        event = to_event(line, labels, datetime.now(timezone.utc), source)
        send(event.to_json(), (event.session_key or source).encode())
        sent += 1
        if sent % PROGRESS_EVERY == 0:
            elapsed = clock() - start
            print(f"  {sent:,} mesaj, {elapsed:.0f} sn, {sent / elapsed:,.0f} log/sn", flush=True)
    return sent


def main(argv: list[str] | None = None) -> int:
    from confluent_kafka import KafkaException, Producer  # testler Kafka'sız çalışabilsin

    # Windows'ta yönlendirilen çıktı cp1252 olur ve Türkçe karakterler hata verir.
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rate", type=float, default=5000, help="log/sn; 0 = sınırsız")
    parser.add_argument("--limit", type=int, help="en fazla bu kadar mesaj gönder")
    parser.add_argument("--topic", default="logs.raw")
    parser.add_argument("--bootstrap", default="127.0.0.1:9092")
    parser.add_argument(
        "--source",
        default=SOURCE,
        help=f"varsayılan '{SOURCE}' yalnızca ML'in kullanacağı tek kanonik oynatma içindir; "
        "testler ve deneyler kendi adını vermelidir (ör. exp01-r5000-1)",
    )
    parser.add_argument("--log", type=Path, default=DATA_DIR / "HDFS.log")
    parser.add_argument(
        "--labels", type=Path, default=DATA_DIR / "preprocessed" / "anomaly_label.csv"
    )
    args = parser.parse_args(argv)

    for path in (args.log, args.labels):
        if not path.exists():
            parser.error(f"{path} yok; önce: uv run python -m ingestion.download_dataset")

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "acks": "all",
            "enable.idempotence": True,  # yeniden denemede tekrar mesaj oluşmaz
            "linger.ms": 20,
            "compression.type": "lz4",
            "message.timeout.ms": 30_000,  # Kafka erişilemezse 30 sn içinde hata görülsün
        }
    )
    try:  # konu yoksa ya da Kafka kapalıysa hemen dur (auto.create kapalı)
        topic_meta = producer.list_topics(args.topic, timeout=10).topics[args.topic]
    except KafkaException as e:
        print(f"Kafka'ya bağlanılamadı ({args.bootstrap}): {e}", file=sys.stderr)
        return 1
    if topic_meta.error is not None:
        print(f"'{args.topic}' topic'i kullanılamıyor: {topic_meta.error}", file=sys.stderr)
        return 1

    failures: list[str] = []

    def on_delivery(err, _msg) -> None:
        if err is not None:
            failures.append(str(err))

    def send(value: bytes, key: bytes) -> None:
        while True:
            try:
                producer.produce(args.topic, value=value, key=key, on_delivery=on_delivery)
                break
            except BufferError:  # yerel kuyruk dolu: teslimatların ilerlemesini bekle
                producer.poll(0.1)
        producer.poll(0)

    labels = load_labels(args.labels)
    print(f"{args.log.name} -> {args.topic} @ {args.bootstrap}, hedef {args.rate:,.0f} log/sn")
    start = time.perf_counter()
    with args.log.open(encoding="utf-8", errors="replace") as lines:
        sent = replay(lines, send, labels, args.rate, args.limit, source=args.source)
    send_secs = time.perf_counter() - start
    undelivered = producer.flush(60)
    total_secs = time.perf_counter() - start

    print(
        f"Gönderilen: {sent:,} mesaj, {send_secs:.1f} sn, {sent / send_secs:,.0f} log/sn "
        f"(teslimat dahil {total_secs:.1f} sn)"
    )
    if failures or undelivered:
        print(
            f"TESLİM EDİLEMEYEN: {len(failures) + undelivered:,} ({failures[:1]})", file=sys.stderr
        )
        return 1
    print("Tüm mesajlar Kafka tarafından onaylandı (acks=all).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
