"""ADR-0008: Loghub'ın HDFS_v1 için yayımladığı resmi şablonlarla deterministik olay eşleştirme.

Şablon dosyası (preprocessed/HDFS.log_templates.csv) `EventId,EventTemplate` sütunlarına sahiptir;
joker `[*]` (ya da `<*>`) boşluk dahil her şeyle eşleşir. Bir içerik, şablonun sabit parçalarını
sırasıyla içeriyorsa o şablona uyar. Birden çok şablon uyarsa en özgül olan (en çok sabit karakter)
seçilir. Girdi, ml.parsing.content_of çıktısıdır.
"""

import csv
import re
from collections.abc import Callable
from pathlib import Path

WILDCARD = re.compile(r"\[\*\]|<\*>")


def _compile(template: str) -> Callable[[str], bool]:
    parts = WILDCARD.split(template)  # baştaki/sondaki joker, '' parça bırakır

    def match(text: str) -> bool:
        if len(parts) == 1:
            return text == parts[0]
        if not text.startswith(parts[0]):
            return False
        pos = len(parts[0])
        for fragment in parts[1:-1]:
            found = text.find(fragment, pos)
            if found < 0:
                return False
            pos = found + len(fragment)
        return text.endswith(parts[-1]) and len(text) - len(parts[-1]) >= pos

    return match


def compile_templates(rows: list[tuple[str, str]]) -> list[tuple[str, Callable[[str], bool]]]:
    """(EventId, eşleştirici) listesi; en özgül şablon önce gelecek şekilde sıralı."""
    rows = sorted(rows, key=lambda r: -len(WILDCARD.sub("", r[1])))  # kararlı: eşitlikte sıra
    return [(event_id, _compile(template)) for event_id, template in rows]


def load_templates(path: Path) -> list[tuple[str, Callable[[str], bool]]]:
    with Path(path).open(newline="", encoding="utf-8") as f:
        return compile_templates([(r["EventId"], r["EventTemplate"]) for r in csv.DictReader(f)])


def load_matcher(path: Path) -> Callable[[str], str | None]:
    return matcher_from(load_templates(path))


def matcher_from(templates: list[tuple[str, Callable[[str], bool]]]) -> Callable[[str], str | None]:
    """Model paketindeki şablonlar için de kullanılır (demo izi, Drain; ml.train_demo)."""

    def event_id(content: str) -> str | None:
        return next((eid for eid, match in templates if match(content)), None)

    return event_id
