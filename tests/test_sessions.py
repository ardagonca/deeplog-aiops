import pytest

from ml.sessions import build_sessions, chronological_split

H = "081109 203518 143 INFO dfs.X: "
LINES = [
    H + "Receiving block blk_1 src: /1.1.1.1:1",
    H + "Receiving block blk_-2 src: /1.1.1.1:1",
    H + "BLOCK* ask 1.1.1.1:1 to delete  blk_1 blk_-2 blk_1",  # iki blok, blk_1 tekrar
    H + "satırda blok yok",
    H + "PacketResponder 0 for block blk_1 terminating",
]


def _event(content: str) -> str:
    return content.split()[0]


def test_multi_block_line_goes_to_each_block_once_in_order():
    sequences, first_seen = build_sessions(LINES, _event)
    assert sequences == {
        "blk_1": ["Receiving", "BLOCK*", "PacketResponder"],
        "blk_-2": ["Receiving", "BLOCK*"],
    }
    assert first_seen == ["blk_1", "blk_-2"]


def test_split_is_chronological_and_train_has_no_anomalies():
    blocks = [f"blk_{i}" for i in range(10)]
    labels = {b: int(b in {"blk_1", "blk_8"}) for b in blocks}
    split = chronological_split(blocks, labels)
    assert split["train"] == ["blk_0", "blk_2", "blk_3", "blk_4", "blk_5"]  # blk_1 anormal: çıkar
    assert split["val"] == ["blk_6"]
    assert split["test"] == ["blk_7", "blk_8", "blk_9"]  # testte anormal kalır
    assert chronological_split(blocks, labels) == split  # deterministik


def test_split_rejects_bad_ratios():
    with pytest.raises(ValueError):
        chronological_split(["blk_1"], {"blk_1": 0}, ratios=(0.5, 0.1, 0.1))
