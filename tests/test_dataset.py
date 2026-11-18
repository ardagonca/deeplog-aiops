import numpy as np

from ml import dataset
from ml.dataset import EXCLUDED, PAD, SPLITS

TEMPLATES = (
    "EventId,EventTemplate\n"
    "E2,Receiving block [*] src: [*] dest: [*]\n"
    "E10,PacketResponder [*] for block [*] terminating\n"
    "E1,[*]Served block [*] to [*]\n"
)


def _line(content: str) -> str:
    return f"081109 203518 143 INFO dfs.DataNode: {content}\n"


def _write(tmp_path, n_blocks: int, anomalies: set[int]):
    hdfs = tmp_path / "HDFS_v1"
    (hdfs / "preprocessed").mkdir(parents=True)
    (hdfs / "preprocessed" / "HDFS.log_templates.csv").write_text(TEMPLATES, encoding="utf-8")
    lines, labels = [], ["BlockId,Label"]
    for b in range(n_blocks):
        lines += [
            _line(f"Receiving block blk_{b} src: /10.0.0.1:1 dest: /10.0.0.2:2"),
            _line(f"PacketResponder 1 for block blk_{b} terminating"),
        ]
        labels.append(f"blk_{b},{'Anomaly' if b in anomalies else 'Normal'}")
    lines.append(_line("bilinmeyen olay blk_0"))  # hiçbir şablona uymaz -> PAD
    (hdfs / "HDFS.log").write_text("".join(lines), encoding="utf-8")
    (hdfs / "preprocessed" / "anomaly_label.csv").write_text("\n".join(labels), encoding="utf-8")


def test_build_encodes_events_and_splits_chronologically(tmp_path):
    _write(tmp_path, n_blocks=10, anomalies={1, 8})
    ds = dataset.load(tmp_path)
    assert list(ds.vocab) == ["PAD", "E1", "E2", "E10"]  # sayısal sıra
    assert list(ds.sequence(0)) == [2, 3, PAD]  # E2, E10, bilinmeyen
    assert list(ds.sequence(5)) == [2, 3]
    # 10 blok: ilk 6 eğitim dilimi (blk_1 anormal -> dışarıda), 1 doğrulama, 3 test
    assert ds.split[1] == EXCLUDED
    assert list(ds.indices("train")) == [0, 2, 3, 4, 5]
    assert list(ds.indices("val")) == [6] and list(ds.indices("test")) == [7, 8, 9]
    assert ds.labels[8] == 1 and ds.split[8] == SPLITS["test"]
    assert not ds.labels[ds.indices("train")].any()


def test_cache_reused_and_invalidated_when_input_changes(tmp_path, monkeypatch):
    _write(tmp_path, n_blocks=10, anomalies=set())
    first = dataset.load(tmp_path)
    calls = []
    monkeypatch.setattr(dataset, "build", lambda d: calls.append(d) or first)
    dataset.load(tmp_path)
    assert calls == []  # önbellekten
    log = tmp_path / "HDFS_v1" / "HDFS.log"
    log.write_text(log.read_text(encoding="utf-8") + _line("ek satır blk_99"), encoding="utf-8")
    dataset.load(tmp_path)
    assert len(calls) == 1  # girdi değişti -> yeniden üretildi
    assert np.array_equal(first.offsets, dataset.load(tmp_path).offsets)
