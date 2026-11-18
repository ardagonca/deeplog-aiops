import pytest

from ml.parsing import content_of, grouping_accuracy, mine, new_miner, templates

LINE = (
    "081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 "
    "src: /10.250.19.102:54106 dest: /10.250.19.102:50010"
)


def test_content_of_strips_hdfs_header():
    assert content_of(LINE + "\r\n").startswith("Receiving block blk_-1608999687919862906 src:")
    assert content_of("biçimsiz satır\n") == "biçimsiz satır"


def test_variable_fields_share_one_template():
    miner = new_miner()
    ids = mine(
        miner,
        [
            "Receiving block blk_-160 src: /10.250.19.102:54106 dest: /10.250.19.102:50010",
            "Receiving block blk_738 src: /10.251.71.16:41224 dest: /10.251.71.16:50010",
            "PacketResponder 1 for block blk_38865049064139660 terminating",
            "PacketResponder 2 for block blk_-6952295868487656571 terminating",
        ],
    )
    assert ids[0] == ids[1] and ids[2] == ids[3] and ids[0] != ids[2]
    assert "<BLK>" in templates(miner)[ids[0]] and "<IP>" in templates(miner)[ids[0]]


def test_block_lists_of_any_length_share_one_template():
    miner = new_miner()
    ids = mine(
        miner,
        [
            "BLOCK* ask 10.250.11.53:50010 to delete  blk_-6060213036935086178",
            "BLOCK* ask 10.250.11.53:50010 to delete  blk_1 blk_2 blk_3",
            "BLOCK* ask 10.251.43.21:50010 to delete  " + " ".join(f"blk_{i}" for i in range(100)),
        ],
    )
    assert len(set(ids)) == 1


@pytest.mark.parametrize(
    "truth, predicted, expected",
    [
        (["A", "A", "B"], [1, 1, 2], 1.0),  # birebir
        (["A", "A", "B"], [1, 1, 1], 0.0),  # iki grup birleşti: hiçbiri doğru değil
        (["A", "A", "B"], [1, 2, 3], 1 / 3),  # A bölündü: yalnızca B doğru
        (["A", "B", "B", "C"], [7, 8, 8, 9], 1.0),  # etiket adları önemsiz
    ],
)
def test_grouping_accuracy(truth, predicted, expected):
    assert grouping_accuracy(truth, predicted) == pytest.approx(expected)


def test_grouping_accuracy_requires_equal_lengths():
    with pytest.raises(ValueError):
        grouping_accuracy(["A"], [1, 2])
