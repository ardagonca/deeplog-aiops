"""Kafka mesaj sözleşmeleri (docs/01-mimari.md §5): logs.raw (ING-02), metrics.raw (ING-05),
alerts (RT-03) ve actions (REM-06).

Üretici tarafı sözleşme sınıfıyla doğrular ve to_json() ile yazar. Tüketici tarafı from_json()
(Python) ya da spark_ddl() (Spark) ile okur.
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
    TIME_FIELDS: ClassVar[tuple[str, ...]] = ("event_time",)  # saat dilimli ISO-8601 olmalı

    def __post_init__(self) -> None:
        for f in fields(self):
            value, allowed = getattr(self, f.name), self.TYPES[f.name]
            if (isinstance(value, bool) and bool not in allowed) or not isinstance(value, allowed):
                raise ValueError(f"{f.name}: geçersiz tip {type(value).__name__}")
            if isinstance(value, str) and NONE not in allowed and not value.strip():
                raise ValueError(f"{f.name}: boş olamaz")
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"schema_version: {self.schema_version} desteklenmiyor")
        for name in self.TIME_FIELDS:
            value = getattr(self, name)
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                raise ValueError(f"{name}: ISO-8601 değil: {value!r}") from None
            if parsed.tzinfo is None:
                raise ValueError(f"{name}: saat dilimi yok: {value!r}")
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


@dataclass(frozen=True)
class AlertEvent(_Contract):
    """alerts mesajı (RT-03, ADR-0009). İki tür:
    - deeplog: oturum sürerken gerçek olay ilk g tahminde değil (erken uyarı) ya da oturum
      kapanırken oturum sonu (EOS) beklenmiyordu;
    - novelty: oturum kapandı ve olay sayım vektörü normal desenlerde hiç görülmemiş
      (oturum kararı).
    event_time, alarmı tetikleyen olayın zamanıdır; K5 = alert_time - event_time."""

    KINDS: ClassVar = ("deeplog", "novelty")
    TYPES: ClassVar = {
        "alert_id": (str,),
        "source": (str,),
        "session_key": (str,),
        "kind": (str,),
        "risk_score": (int, float),
        "threshold": (int, float),
        "event_time": (str,),
        "alert_time": (str,),
        "model_version": (str,),
        "event_id": (str, NONE),
        "schema_version": (int,),
    }
    SQL: ClassVar = {
        "alert_id": "STRING",
        "source": "STRING",
        "session_key": "STRING",
        "kind": "STRING",
        "risk_score": "DOUBLE",
        "threshold": "DOUBLE",
        "event_time": "TIMESTAMP",
        "alert_time": "TIMESTAMP",
        "model_version": "STRING",
        "event_id": "STRING",
        "schema_version": "INT",
    }

    alert_id: str
    source: str
    session_key: str
    kind: str
    risk_score: float  # 0..1; deeplog: gerçek olayın tahmin sırası / sözlük boyu
    threshold: float  # aynı ölçekte eşik (g / sözlük boyu); novelty için 1
    event_time: str
    alert_time: str
    model_version: str
    event_id: str | None = None  # tetikleyen olayın şablonu (E5, EOS, UNKNOWN); novelty'de boş
    schema_version: int = SCHEMA_VERSION

    def _check(self) -> None:
        if self.kind not in self.KINDS:
            raise ValueError(f"kind: {self.KINDS} dışında: {self.kind!r}")
        for name in ("risk_score", "threshold"):
            if not 0 <= getattr(self, name) <= 1:
                raise ValueError(f"{name}: 0-1 aralığında olmalı")
        try:
            datetime.fromisoformat(self.alert_time.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"alert_time: ISO-8601 değil: {self.alert_time!r}") from None


@dataclass(frozen=True)
class ActionEvent(_Contract):
    """actions mesajı (REM-06, ADR-0005, ADR-0011): Remediator'ın her alarm için verdiği karar.
    Aksiyon alınmasa da (koruma, bilinmeyen arıza tipi) kayıt yazılır; denetim izi budur.
    K6 = finished_at - alert_time (yalnızca status = success ve action != none)."""

    TIME_FIELDS: ClassVar = ("alert_time", "started_at", "finished_at")
    FAULT_TYPES: ClassVar = ("disk_full", "memory_exhaustion", "service_unresponsive", "unknown")
    ACTIONS: ClassVar = ("ansible:disk_cleanup", "docker:restart", "none")
    STATUSES: ClassVar = (
        "success",
        "failed",
        "skipped_unknown",  # arıza tipi belirlenemedi: aksiyon yok, yalnızca kayıt
        "skipped_duplicate",
        "skipped_disabled",
        "skipped_not_allowlisted",
        "skipped_cooldown",
        "skipped_rate_limit",
    )
    TYPES: ClassVar = {
        "action_id": (str,),
        "alert_id": (str,),
        "source": (str,),
        "target": (str, NONE),
        "fault_type": (str,),
        "action": (str,),
        "dry_run": (bool,),
        "status": (str,),
        "alert_time": (str,),
        "started_at": (str,),
        "finished_at": (str,),
        "detail": (str, NONE),
        "schema_version": (int,),
    }
    SQL: ClassVar = {
        "action_id": "STRING",
        "alert_id": "STRING",
        "source": "STRING",
        "target": "STRING",
        "fault_type": "STRING",
        "action": "STRING",
        "dry_run": "BOOLEAN",
        "status": "STRING",
        "alert_time": "TIMESTAMP",
        "started_at": "TIMESTAMP",
        "finished_at": "TIMESTAMP",
        "detail": "STRING",
        "schema_version": "INT",
    }

    action_id: str
    alert_id: str
    source: str  # alarmın kaynağı (ör. demo-client)
    target: str | None  # aksiyonun hedef servisi (ör. demo-app); eşlenemezse boş
    fault_type: str
    action: str
    dry_run: bool
    status: str
    alert_time: str
    started_at: str
    finished_at: str
    detail: str | None = None
    schema_version: int = SCHEMA_VERSION

    def _check(self) -> None:
        for name, allowed in (
            ("fault_type", self.FAULT_TYPES),
            ("action", self.ACTIONS),
            ("status", self.STATUSES),
        ):
            if getattr(self, name) not in allowed:
                raise ValueError(f"{name}: {allowed} dışında: {getattr(self, name)!r}")


def spark_ddl(cls: type[_Contract]) -> str:
    """Spark from_json şeması; alan sırası ve listesi dataclass'tan gelir, iki taraf kopamaz."""
    return ", ".join(f"{f.name} {cls.SQL[f.name]}" for f in fields(cls))


def required_fields(cls: type[_Contract]) -> list[str]:
    """Boş olamayan alanlar; Spark tarafındaki karantina kuralları bunlardan üretilir."""
    return [f.name for f in fields(cls) if NONE not in cls.TYPES[f.name]]


LOG_EVENT_DDL = spark_ddl(LogEvent)
METRIC_EVENT_DDL = spark_ddl(MetricEvent)
ALERT_EVENT_DDL = spark_ddl(AlertEvent)
ACTION_EVENT_DDL = spark_ddl(ActionEvent)
