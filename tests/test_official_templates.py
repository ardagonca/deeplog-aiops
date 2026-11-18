import pytest

from ml.official_templates import load_matcher

# Loghub HDFS.log_templates.csv'den gerçek satırlar (çakışan PacketResponder şablonları dahil).
TEMPLATES = """EventId,EventTemplate
E3,[*]Served block[*]to[*]
E5,[*]Receiving block[*]src:[*]dest:[*]
E6,[*]Received block[*]src:[*]dest:[*]of size[*]
E8,[*]PacketResponder[*]for block[*]Interrupted[*]
E10,[*]PacketResponder[*]Exception[*]
E11,[*]PacketResponder[*]for block[*]terminating[*]
E22,[*]BLOCK* NameSystem[*]allocateBlock:[*]
X1,tam <*> eşleşme
"""


@pytest.fixture
def event_id(tmp_path):
    path = tmp_path / "templates.csv"
    path.write_text(TEMPLATES, encoding="utf-8")
    return load_matcher(path)


@pytest.mark.parametrize(
    "content, expected",
    [
        ("10.250.14.38:50010 Served block blk_-1 to /10.250.14.38", "E3"),
        ("Receiving block blk_1 src: /10.0.0.1:54106 dest: /10.0.0.1:50010", "E5"),
        # E5'in parçaları da geçiyor; daha özgül E6 seçilmeli
        ("Received block blk_1 src: /10.0.0.1:1 dest: /10.0.0.1:2 of size 67108864", "E6"),
        ("PacketResponder 1 for block blk_38865049064139660 terminating", "E11"),
        ("PacketResponder 0 for block blk_-1 Interrupted.", "E8"),
        (
            "PacketResponder blk_-1 1 Exception java.io.InterruptedIOException: Interruped while "
            "waiting for IO on channel",
            "E10",
        ),
        ("BLOCK* NameSystem.allocateBlock: /mnt/hadoop/job.jar. blk_-1", "E22"),
        ("tam her şey eşleşme", "X1"),
        ("hiçbir şablona uymayan satır", None),
        ("öncesi tam x eşleşme", None),  # baş jokersiz şablon başa sabitlenir
    ],
)
def test_event_id(event_id, content, expected):
    assert event_id(content) == expected
