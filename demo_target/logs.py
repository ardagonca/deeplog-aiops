"""Demo sistemin logları: HDFS_v1 ile aynı satır biçimi ve logs.raw'a LogEvent olarak gönderim.

event_time mikrosaniye hassasiyetindedir: istekler birkaç ms sürer; ms'de aynı ana düşen istemci
ve uygulama olayları Kafka sırasına kalır, o da iki üretici arasında rastgeledir.

Satır biçimi "YYMMDD HHMMSS PID SEVİYE bileşen: içerik" (ml.parsing.HDFS_LINE ile aynı); şablonlama
ve oturum kodu iki izde ortak kalır. Oturum = istek (ADR-0011): session_key, isteğin kimliğidir.
"""

import contextvars
import logging
import os
import sys
import uuid
from collections.abc import Callable
from datetime import datetime, timezone

from ingestion.schemas import LogEvent

request_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("request_id", default=None)


class LineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        t = datetime.fromtimestamp(record.created)
        head = f"{t:%y%m%d %H%M%S} {os.getpid()} {record.levelname} {record.name}"
        return f"{head}: {record.getMessage()}"


class KafkaLogHandler(logging.Handler):
    """Her kaydı LogEvent olarak send(value, key)'e verir. Gönderim hatası uygulamayı durdurmaz;
    kaybolan kayıt sayısı dropped'da tutulur."""

    def __init__(self, send: Callable[[bytes, bytes], None], source: str):
        super().__init__()
        self.send, self.source, self.dropped = send, source, 0
        self.setFormatter(LineFormatter())

    def emit(self, record: logging.LogRecord) -> None:
        try:
            key = request_id.get()
            event = LogEvent(
                id=str(uuid.uuid4()),
                source=self.source,
                event_time=datetime.fromtimestamp(record.created, timezone.utc)
                .isoformat(timespec="microseconds")
                .replace("+00:00", "Z"),
                message=self.format(record),
                session_key=key,
                level=record.levelname,
                label=None,  # İz B'de etiket logdan değil enjeksiyon kayıtlarından gelir (ADR-0011)
            )
            self.send(event.to_json(), (key or self.source).encode())
        except Exception:
            self.dropped += 1


def kafka_sender(bootstrap: str, topic: str = "logs.raw") -> Callable[[bytes, bytes], None]:
    from confluent_kafka import Producer

    producer = Producer(
        {"bootstrap.servers": bootstrap, "linger.ms": 20, "enable.idempotence": True}
    )

    def send(value: bytes, key: bytes) -> None:
        producer.produce(topic, value=value, key=key)  # kuyruk doluysa BufferError: kayıt düşer
        producer.poll(0)

    return send


def setup(source: str, bootstrap: str | None) -> KafkaLogHandler | None:
    """'demo' kaydedicisini stdout'a ve (bootstrap verilmişse) Kafka'ya bağlar."""
    logger = logging.getLogger("demo")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(LineFormatter())
    logger.handlers = [console]
    if not bootstrap:
        return None
    handler = KafkaLogHandler(kafka_sender(bootstrap), source)
    logger.addHandler(handler)
    return handler
