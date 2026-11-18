"""ML-02: HDFS_v1 oturumları (blok bazında olay dizileri) ve kronolojik bölme.

Olay kimliği fonksiyonu dışarıdan verilir; şablon kaynağı kararı (Drain ya da resmi şablonlar)
ADR-0008'dedir. Bölme kuralı: docs/04-test-stratejisi.md §3 ve ADR-0006.
"""

import re
from collections.abc import Callable, Hashable, Iterable

from ml.parsing import content_of

BLOCK_ID = re.compile(r"blk_-?\d+")


def build_sessions(
    lines: Iterable[str], event_id: Callable[[str], Hashable]
) -> tuple[dict[str, list[Hashable]], list[str]]:
    """Blok başına olay dizisi ve blokların ilk görülme sırası.

    Bir satırda geçen her blok (satır içinde tekrar edenler bir kez) o satırın olayını alır;
    ör. çoklu "delete" satırı listedeki her bloğun dizisine eklenir.
    """
    sequences: dict[str, list[Hashable]] = {}
    for line in lines:
        blocks = dict.fromkeys(BLOCK_ID.findall(line))  # sırayı koruyan tekilleştirme
        if not blocks:
            continue
        event = event_id(content_of(line))
        for block in blocks:
            sequences.setdefault(block, []).append(event)
    return sequences, list(sequences)  # dict ekleme sırası = ilk görülme sırası


def chronological_split(
    first_seen: list[str],
    labels: dict[str, int],
    ratios: tuple[float, float, float] = (0.6, 0.1, 0.3),
) -> dict[str, list[str]]:
    """İlk görülme sırasına göre train/val/test. Eğitimde yalnızca normal (0) oturumlar kalır."""
    if abs(sum(ratios) - 1) > 1e-9:
        raise ValueError("oranların toplamı 1 olmalı")
    n_train = int(len(first_seen) * ratios[0])
    n_val = int(len(first_seen) * ratios[1])
    train = first_seen[:n_train]
    return {
        "train": [b for b in train if labels[b] == 0],
        "val": first_seen[n_train : n_train + n_val],
        "test": first_seen[n_train + n_val :],
    }
