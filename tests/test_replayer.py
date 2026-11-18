from datetime import datetime, timezone

import pytest

from ingestion.replayer import load_labels, replay, to_event
from ingestion.schemas import LogEvent

NOW = datetime(2026, 11, 23, 10, 15, 30, 123456, tzinfo=timezone.utc)
LINE = (
    "081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 "
    "src: /10.250.19.102:54106 dest: /10.250.19.102:50010\n"
)


def test_to_event_parses_hdfs_line():
    event = to_event(LINE, {"blk_-1608999687919862906": 1}, NOW)
    assert event.message == LINE.rstrip("\n")
    assert event.session_key == "blk_-1608999687919862906"
    assert event.level == "INFO"
    assert event.label == 1
    assert event.event_time == "2026-11-23T10:15:30.123Z"
    assert LogEvent.from_json(event.to_json()) == event


def test_to_event_without_block_or_label():
    event = to_event("081109 203518 143 WARN dfs.FSNamesystem: blok yok\r\n", {}, NOW)
    assert (event.session_key, event.label, event.level) == (None, None, "WARN")
    assert not event.message.endswith(("\r", "\n"))


def test_load_labels(tmp_path):
    path = tmp_path / "anomaly_label.csv"
    path.write_text("BlockId,Label\nblk_1,Normal\nblk_-2,Anomaly\n", encoding="utf-8")
    assert load_labels(path) == {"blk_1": 0, "blk_-2": 1}


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.slept = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.slept += seconds
        self.now += seconds


def test_replay_paces_to_rate_and_keys_by_session():
    sent = []
    clock = FakeClock()
    count = replay(
        [LINE] * 10, lambda v, k: sent.append(k), {}, rate=100, clock=clock, sleep=clock.sleep
    )
    assert count == 10
    assert clock.now == pytest.approx(9 / 100)  # 10. mesaj 0,09 sn'de: ilk mesaj beklemeden gider
    assert set(sent) == {b"blk_-1608999687919862906"}


def test_replay_respects_limit_skips_blank_lines_and_unlimited_rate():
    sent = []
    clock = FakeClock()
    lines = [LINE, "\n", "   \n", LINE, LINE, LINE]
    count = replay(
        lines, lambda v, k: sent.append(v), {}, rate=0, limit=3, clock=clock, sleep=clock.sleep
    )
    assert count == 3 == len(sent)
    assert clock.slept == 0
