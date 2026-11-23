"""RT-02/03: Canlı tespitin karar mantığı (ADR-0009). Spark'tan bağımsızdır; jobs/detect.py her
micro-batch'i process(), zaman aşımlarını expire() ile çağırır.

Kurallar:
- deeplog alarmı: oturumda gerçek olayın ilk g tahminde olmadığı ilk olay (oturum başına bir kez).
- Oturum timeout_s boyunca olay almazsa kapanır. Oturum sonu (EOS) beklenmiyorsa ve daha önce
  deeplog alarmı verilmemişse deeplog alarmı; sayım vektörü görülmemişse novelty alarmı.
- alert_id (model, tür, oturum)'dan türetilir: aynı batch yeniden işlenirse aynı kimlik üretilir.
- Durum dump()/restore() ile kalıcıdır (RT-08); last_batch, anlık görüntünün hangi micro-batch'e
  kadar işlendiğini tutar ve yeniden oynatılan batch'lerin ikinci kez işlenmesini önler.
"""

import gzip
import json
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from ingestion.schemas import AlertEvent
from ml.package import ModelPackage
from ml.parsing import content_of
from ml.stream_score import Session, advance_many, close_many


@dataclass
class _Open:
    session: Session
    source: str
    last_time: str  # son olayın event_time'ı (ISO)
    last_seen: float  # son olayın işlendiği an (saniye; zaman aşımı için)


class Detector:
    def __init__(self, pkg: ModelPackage, matcher: Callable[[str], str | None], timeout_s: float):
        self.pkg, self.matcher, self.timeout_s = pkg, matcher, timeout_s
        self.code = {e: i for i, e in enumerate(pkg.config.vocab)}
        self.open: dict[str, _Open] = {}
        self.last_batch = -1  # durumun içerdiği son micro-batch kimliği

    def process(
        self, events: Iterable[tuple[str, str, str, str]], now: float, alert_time: str
    ) -> list[AlertEvent]:
        """events: (oturum, kaynak, mesaj, event_time) olay sırasıyla."""
        by_session: dict[str, list[tuple[int, str]]] = {}
        for key, source, message, event_time in events:
            code = self.code.get(self.matcher(content_of(message)), 0)
            by_session.setdefault(key, []).append((code, event_time))
            if key not in self.open:
                self.open[key] = _Open(Session.new(self.pkg), source, event_time, now)
        advance_many(
            self.pkg, [(self.open[k].session, [c for c, _ in ev]) for k, ev in by_session.items()]
        )
        alerts = []
        for key, ev in by_session.items():
            o = self.open[key]
            o.last_time, o.last_seen = ev[-1][1], now
            if o.session.alerted:
                continue
            for (code, event_time), rank in zip(ev, o.session.ranks, strict=True):
                if rank >= self.pkg.config.g:
                    event_id = self.pkg.config.vocab[code] if code else "UNKNOWN"
                    alerts.append(
                        self._alert(key, o, "deeplog", rank, event_time, event_id, alert_time)
                    )
                    o.session.alerted = True
                    break
        return alerts

    def expire(self, now: float, alert_time: str) -> list[AlertEvent]:
        """timeout_s'dir olay almayan oturumları kapatır ve son kararlarını üretir."""
        done = [k for k, o in self.open.items() if now - o.last_seen >= self.timeout_s]
        closed = close_many(self.pkg, [self.open[k].session for k in done])
        alerts = []
        for key, (eos_rank, novel) in zip(done, closed, strict=True):
            o = self.open.pop(key)
            if not o.session.alerted and eos_rank >= self.pkg.config.g:
                alerts.append(
                    self._alert(key, o, "deeplog", eos_rank, o.last_time, "EOS", alert_time)
                )
            if novel:
                alerts.append(self._alert(key, o, "novelty", None, o.last_time, None, alert_time))
        return alerts

    def dump(self) -> bytes:
        """Açık oturumların anlık görüntüsü (gzip'li JSON; pickle değil: dosya dışarıdan okunur)."""
        rows = [
            [k, o.source, o.last_time, o.session.history, o.session.counts, o.session.alerted]
            for k, o in self.open.items()
        ]
        doc = {"version": 1, "model": self.pkg.config.name, "batch": self.last_batch, "open": rows}
        return gzip.compress(json.dumps(doc, separators=(",", ":")).encode(), compresslevel=1)

    def restore(self, data: bytes, now: float) -> None:
        """dump() çıktısını yükler. Kesinti süresi oturumların aleyhine sayılmasın diye zaman aşımı
        sayacı now'dan yeniden başlar."""
        doc = json.loads(gzip.decompress(data))
        if doc["version"] != 1 or doc["model"] != self.pkg.config.name:
            raise ValueError(f"durum başka bir model/sürüme ait: {doc['model']} v{doc['version']}")
        self.last_batch = doc["batch"]
        self.open = {
            k: _Open(Session(history, counts, alerted), source, last_time, now)
            for k, source, last_time, history, counts, alerted in doc["open"]
        }

    def _alert(self, key, o: _Open, kind, rank, event_time, event_id, alert_time) -> AlertEvent:
        cfg = self.pkg.config
        size = cfg.vocab_size
        return AlertEvent(
            alert_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"deeplog/{cfg.name}/{kind}/{key}")),
            source=o.source,
            session_key=key,
            kind=kind,
            risk_score=1.0 if rank is None else min(1.0, rank / size),
            threshold=1.0 if rank is None else cfg.g / size,
            event_time=event_time,
            alert_time=alert_time,
            model_version=cfg.name,
            event_id=event_id,
        )
