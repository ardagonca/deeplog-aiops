"""ML-01: Drain ile log şablonlama (He vd., 2017; Drain3 uygulaması).

Batch katmanı (şablon sözlüğünü çıkaran Spark işi) ve hız katmanı (canlı eşleştirme) bu modülü
ortak kullanır; böylece eğitimdeki şablon kimlikleriyle canlıdaki kimlikler ayrışmaz (ADR-0001).
"""

import re
from collections import defaultdict
from collections.abc import Hashable, Iterable, Sequence

from drain3 import TemplateMiner
from drain3.masking import MaskingInstruction
from drain3.template_miner_config import TemplateMinerConfig

# HDFS_v1 satır biçimi: <Tarih> <Saat> <Pid> <Seviye> <Bileşen>: <İçerik>
HDFS_LINE = re.compile(r"^\d{6} \d{6} \d+ \w+ [^:]+: (?P<content>.*)$")

# Değişken alanlar şablonlamadan önce maskelenir. Sıra önemlidir: blok kimliği ve IP:port,
# genel sayı kuralından önce gelmelidir; yoksa parçalanırlar.
MASKS = (
    # Ardışık blok listesi ("delete blk_1 blk_2 ...") tek belirteç: Drain belirteç sayısına göre
    # gruplar; liste uzunluğu değişince aynı olay ayrı şablonlara bölünürdü. Blok kimlikleri
    # oturum ayrıştırmada ham mesajdan ayrıca çıkarılır, bu maskeleme onları kaybettirmez.
    MaskingInstruction(r"blk_-?\d+(?:\s+blk_-?\d+)*", "BLK"),
    MaskingInstruction(r"/?\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?", "IP"),
    MaskingInstruction(r"(?<![A-Za-z0-9_])-?\d+(?![A-Za-z0-9_])", "NUM"),
)


def content_of(line: str) -> str:
    """Ham HDFS satırından içerik kısmı; biçim tutmazsa satırın kendisi."""
    match = HDFS_LINE.match(line.rstrip("\r\n"))
    return match.group("content") if match else line.rstrip("\r\n")


def new_miner(sim_th: float = 0.5, depth: int = 4) -> TemplateMiner:
    config = TemplateMinerConfig()
    config.drain_sim_th = sim_th
    config.drain_depth = depth
    config.masking_instructions = list(MASKS)
    config.profiling_enabled = False
    return TemplateMiner(config=config)


def mine(miner: TemplateMiner, contents: Iterable[str]) -> list[int]:
    """İçerikleri şablonlara ayırır (sözlüğü büyüterek); her içerik için şablon kimliği döner."""
    return [miner.add_log_message(c)["cluster_id"] for c in contents]


def templates(miner: TemplateMiner) -> dict[int, str]:
    return {c.cluster_id: c.get_template() for c in miner.drain.clusters}


def grouping_accuracy(truth: Sequence[Hashable], predicted: Sequence[Hashable]) -> float:
    """Zhu vd. (2019) Grouping Accuracy: bir satır, tahmin edilen grubu gerçek grubuyla birebir
    aynı satırları içeriyorsa doğru sayılır. Doğru satırların oranını döner."""
    if len(truth) != len(predicted):
        raise ValueError("truth ve predicted aynı uzunlukta olmalı")
    by_truth: dict[Hashable, set[int]] = defaultdict(set)
    by_pred: dict[Hashable, set[int]] = defaultdict(set)
    for i, (t, p) in enumerate(zip(truth, predicted, strict=True)):
        by_truth[t].add(i)
        by_pred[p].add(i)
    correct = sum(
        len(members)
        for members in by_pred.values()
        if members == by_truth[truth[next(iter(members))]]
    )
    return correct / len(truth) if truth else 1.0
