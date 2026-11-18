"""ING-02: Kafka mesaj sözleşmeleri (docs/01-mimari.md §5).

Şimdilik yalnızca logs.raw tanımlı; metrics.raw, alerts ve actions ilgili iş kalemleriyle eklenir.
Üretici tarafı LogEvent(...) ile doğrular, to_json() ile yazar. Tüketici tarafı from_json() ile
(Python) ya da LOG_EVENT_DDL ile (Spark from_json) okur.
"""

import json
from dataclasses import asdict, dataclass, fields
from datetime import datetime

SCHEMA_VERSION = 1

_TYPES = {  # alan -> izin verilen Python tipleri (None = boş olabilir)
    "id": (str,),
    "source": (str,),
    "event_time": (str,),
    "message": (str,),
    "session_key": (str, type(None)),
    "level": (str, type(None)),
    "label": (int, type(None)),
    "schema_version": (int,),
}


@dataclass(frozen=True)
class LogEvent:
    """logs.raw mesajı. Geçersiz değerle oluşturulamaz (ValueError)."""

    id: str
    source: str
    event_time: str  # ISO-8601, saat dilimli; üreticiler UTC ve "Z" yazar
    message: str  # ham log satırı, olduğu gibi
    session_key: str | None = None  # HDFS_v1'de blok kimliği; Kafka mesaj anahtarı
    level: str | None = None
    # Yalnızca replay: oturum etiketi (1 anormal, 0 normal). Tespit job'u bu alanı okumaz.
    label: int | None = None
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        for f in fields(self):
            value = getattr(self, f.name)
            if isinstance(value, bool) or not isinstance(value, _TYPES[f.name]):
                raise ValueError(f"{f.name}: geçersiz tip {type(value).__name__}")
        for name in ("id", "source", "message"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name}: boş olamaz")
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"schema_version: {self.schema_version} desteklenmiyor")
        if self.label not in (None, 0, 1):
            raise ValueError(f"label: 0, 1 veya null olmalı, {self.label} geldi")
        try:
            parsed = datetime.fromisoformat(self.event_time.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"event_time: ISO-8601 değil: {self.event_time!r}") from None
        if parsed.tzinfo is None:
            raise ValueError(f"event_time: saat dilimi yok: {self.event_time!r}")

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, raw: bytes | str) -> "LogEvent":
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


def spark_ddl(cls: type, timestamps: tuple[str, ...] = ("event_time",)) -> str:
    """Dataclass alanlarından Spark from_json şeması üretir; iki taraf aynı listeden beslenir."""

    def sql_type(name: str) -> str:
        if name in timestamps:
            return "TIMESTAMP"
        return "INT" if int in _TYPES[name] else "STRING"

    return ", ".join(f"{f.name} {sql_type(f.name)}" for f in fields(cls))


LOG_EVENT_DDL = spark_ddl(LogEvent)
