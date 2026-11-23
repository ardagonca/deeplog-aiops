"""REM-03: Demo izinin dışa aktarılmış loglarından (jobs/export_demo.py) oturumlar (ADR-0011).

Oturum = istek: aynı session_key'li satırlar (istemci + uygulama), olay zamanına ve Kafka
konumuna göre sıralı. Kimliği olmayan satırlar (ör. servis başlangıcı) oturuma girmez.
"""

import gzip
import json
from dataclasses import dataclass
from pathlib import Path

from ml.parsing import content_of


@dataclass(frozen=True)
class Session:
    key: str
    start: str  # ilk olayın zamanı (ISO); kronolojik bölme bununla yapılır
    contents: tuple[str, ...]


def load_sessions(path: Path) -> list[Session]:
    rows: dict[str, list[tuple[str, int, int, str]]] = {}
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if r["session_key"]:
                rows.setdefault(r["session_key"], []).append(
                    (r["event_time"], r["kafka_partition"], r["kafka_offset"], r["message"])
                )
    sessions = []
    for key, events in rows.items():
        events.sort()
        sessions.append(Session(key, events[0][0], tuple(content_of(e[3]) for e in events)))
    sessions.sort(key=lambda s: (s.start, s.key))
    return sessions


def chronological_split(sessions: list, fractions=(0.6, 0.1, 0.3)) -> tuple[list, list, list]:
    """ML-02 ile aynı ilke: oturumlar başlangıç zamanına göre sıralı, gelecek eğitime sızmaz."""
    n = len(sessions)
    a = int(n * fractions[0])
    b = a + int(n * fractions[1])
    return sessions[:a], sessions[a:b], sessions[b:]
