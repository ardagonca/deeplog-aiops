import json

import pytest

from ingestion.schemas import LOG_EVENT_DDL, LogEvent

VALID = {
    "id": "6f1c0d2e-0000-4000-8000-000000000001",
    "source": "hdfs-replay",
    "event_time": "2026-11-23T10:15:30.123Z",
    "message": "081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_1",
    "session_key": "blk_1",
    "level": "INFO",
    "label": 0,
    "schema_version": 1,
}


def test_roundtrip():
    event = LogEvent(**VALID)
    assert LogEvent.from_json(event.to_json()) == event
    assert json.loads(event.to_json()) == VALID


def test_optional_fields_may_be_null():
    event = LogEvent(**{**VALID, "session_key": None, "level": None, "label": None})
    assert LogEvent.from_json(event.to_json()) == event


@pytest.mark.parametrize(
    "change, error",
    [
        ({"id": ""}, "id: boş"),
        ({"message": "   "}, "message: boş"),
        ({"label": 2}, "label"),
        ({"label": True}, "label: geçersiz tip"),
        ({"source": 5}, "source: geçersiz tip"),
        ({"schema_version": 2}, "schema_version"),
        ({"event_time": "dün"}, "ISO-8601"),
        ({"event_time": "2026-11-23T10:15:30"}, "saat dilimi"),
    ],
)
def test_invalid_values_rejected(change, error):
    with pytest.raises(ValueError, match=error):
        LogEvent(**{**VALID, **change})


@pytest.mark.parametrize(
    "raw, error",
    [
        ("{bozuk", "JSON değil"),
        ("[1, 2]", "JSON nesnesi"),
        (json.dumps({k: v for k, v in VALID.items() if k != "level"}), "eksik alan: level"),
        (json.dumps({**VALID, "lable": 1}), "bilinmeyen alan: lable"),
    ],
)
def test_malformed_json_rejected(raw, error):
    with pytest.raises(ValueError, match=error):
        LogEvent.from_json(raw)


def test_spark_ddl_matches_contract():
    assert LOG_EVENT_DDL == (
        "id STRING, source STRING, event_time TIMESTAMP, message STRING, "
        "session_key STRING, level STRING, label INT, schema_version INT"
    )
