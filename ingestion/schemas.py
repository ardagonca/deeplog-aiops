"""Kafka mesaj sözleşmeleri (docs/01-mimari.md §5): logs.raw (ING-02) ve metrics.raw (ING-05).

alerts ve actions ilgili iş kalemleriyle eklenir. Üretici tarafı sözleşme sınıfıyla doğrular
ve to_json() ile yazar. Tüketici tarafı from_json() (Python) ya da spark_ddl() (Spark) ile okur.
"""

import json
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from typing import ClassVar

SCHEMA_VERSION = 1
NONE = type(None)


class _Contract:
    """Ortak doğrulama ve JSON dönüşümü. Alt sınıflar frozen dataclass'tır ve geçersiz değerle
    oluşturulamaz (ValueError)."""

    TYPES: ClassVar[dict[str, tuple[type, ...]]]  # alan -> izin verilen tipler; NONE = boş olabilir
    SQL: ClassVar[dict[str, str]]  # alan -> Spark tipi

    def __post_init__(self) -> None:
        for f in fields(self):
            value, allowed = getattr(self, f.name), self.TYPES[f.name]
            if (isinstance(value, bool) and bool not in allowed) or not isinstance(value, allowed):
                raise ValueError(f"{f.name}: geçersiz tip {type(value).__name__}")
            if isinstance(value, str) and NONE not in allowed and not value.strip():
                raise ValueError(f"{f.name}: boş olamaz")
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"schema_version: {self.schema_version} desteklenmiyor")
        try:
            parsed = datetime.fromisoformat(self.event_time.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"event_time: ISO-8601 değil: {self.event_time!r}") from None
        if parsed.tzinfo is None:
            raise ValueError(f"event_time: saat dilimi yok: {self.event_time!r}")
        self._check()

    def _check(self) -> None:
        """Sözleşmeye özgü ek kurallar."""

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, raw: bytes | str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON değil: {e}") from None
        if not isinstance(data, dict):
            raise ValueError("JSON nesnesi bekleniyordu")
        expected = {f.name for f in fields(cls)}
        if missing := expected - data.keys():
            raise ValueError(f"eksik alan: {', '.join(sorted(missing))}")
        if unknown := data.keys() - expected:
            raise ValueError(f"bilinmeyen alan: {', '.join(sorted(unknown))}")
        return cls(**data)


@dataclass(frozen=True)
class LogEvent(_Contract):
    """logs.raw mesajı: tek bir log satırı."""

    TYPES: ClassVar = {
        "id": (str,),
        "source": (str,),
        "event_time": (str,),
        "message": (str,),
        "session_key": (str, NONE),
        "level": (str, NONE),
        "label": (int, NONE),
        "schema_version": (int,),
    }
    SQL: ClassVar = {
        "id": "STRING",
        "source": "STRING",
        "event_time": "TIMESTAMP",
        "message": "STRING",
        "session_key": "STRING",
        "level": "STRING",
        "label": "INT",
        "schema_version": "INT",
    }

    id: str
    source: str
    event_time: str  # ISO-8601, saat dilimli; üreticiler UTC ve "Z" yazar
    message: str  # ham log satırı, olduğu gibi
    session_key: str | None = None  # HDFS_v1'de blok kimliği; Kafka mesaj anahtarı
    level: str | None = None
    # Yalnızca replay: oturum etiketi (1 anormal, 0 normal). Tespit job'u bu alanı okumaz.
    label: int | None = None
    schema_version: int = SCHEMA_VERSION

    def _check(self) -> None:
        if self.label not in (None, 0, 1):
            raise ValueError(f"label: 0, 1 veya null olmalı, {self.label} geldi")


@dataclass(frozen=True)
class MetricEvent(_Contract):
    """metrics.raw mesajı: bir konteynerin bir andaki kaynak kullanımı."""

    _COUNTERS: ClassVar = ("blk_read_bytes", "blk_write_bytes", "net_rx_bytes", "net_tx_bytes")
    TYPES: ClassVar = {
        "source": (str,),
        "event_time": (str,),
        "mem_bytes": (int,),
        "mem_limit_bytes": (int,),
        **dict.fromkeys(_COUNTERS, (int,)),
        "cpu_pct": (int, float, NONE),
        "healthy": (bool, NONE),
        "disk_used_pct": (int, float, NONE),
        "schema_version": (int,),
    }
    SQL: ClassVar = {
        "source": "STRING",
        "event_time": "TIMESTAMP",
        "mem_bytes": "BIGINT",
        "mem_limit_bytes": "BIGINT",
        **dict.fromkeys(_COUNTERS, "BIGINT"),
        "cpu_pct": "DOUBLE",
        "healthy": "BOOLEAN",
        "disk_used_pct": "DOUBLE",
        "schema_version": "INT",
    }

    source: str  # compose servis adı
    event_time: str
    mem_bytes: int  # sayfa önbelleği hariç (docker stats ile aynı)
    mem_limit_bytes: int
    blk_read_bytes: int  # sayaçlar: konteyner başladığından beri toplam
    blk_write_bytes: int
    net_rx_bytes: int
    net_tx_bytes: int
    cpu_pct: float | None = None  # 100 = bir çekirdek tam dolu; ilk ölçümde önceki örnek yok
    healthy: bool | None = None  # Docker healthcheck sonucu; healthcheck yoksa boş
    disk_used_pct: float | None = None  # yalnızca /health ile disk bildiren servisler (REM-01)
    schema_version: int = SCHEMA_VERSION

    def _check(self) -> None:
        for name in ("mem_bytes", "mem_limit_bytes", *self._COUNTERS):
            if getattr(self, name) < 0:
                raise ValueError(f"{name}: negatif olamaz")
        if self.cpu_pct is not None and self.cpu_pct < 0:
            raise ValueError("cpu_pct: negatif olamaz")
        if self.disk_used_pct is not None and not 0 <= self.disk_used_pct <= 100:
            raise ValueError("disk_used_pct: 0-100 aralığında olmalı")


def spark_ddl(cls: type[_Contract]) -> str:
    """Spark from_json şeması; alan sırası ve listesi dataclass'tan gelir, iki taraf kopamaz."""
    return ", ".join(f"{f.name} {cls.SQL[f.name]}" for f in fields(cls))


def required_fields(cls: type[_Contract]) -> list[str]:
    """Boş olamayan alanlar; Spark tarafındaki karantina kuralları bunlardan üretilir."""
    return [f.name for f in fields(cls) if NONE not in cls.TYPES[f.name]]


LOG_EVENT_DDL = spark_ddl(LogEvent)
METRIC_EVENT_DDL = spark_ddl(MetricEvent)
