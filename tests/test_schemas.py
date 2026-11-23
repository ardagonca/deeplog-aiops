import json

import pytest

from ingestion.schemas import (
    LOG_EVENT_DDL,
    METRIC_EVENT_DDL,
    AlertEvent,
    LogEvent,
    MetricEvent,
)

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


METRIC = {
    "source": "kafka",
    "event_time": "2026-11-18T12:00:00.000Z",
    "mem_bytes": 465_309_696,
    "mem_limit_bytes": 1_073_741_824,
    "blk_read_bytes": 0,
    "blk_write_bytes": 17_850_368,
    "net_rx_bytes": 377_278,
    "net_tx_bytes": 31_344_713,
    "cpu_pct": 12.5,
    "healthy": True,
    "disk_used_pct": None,
    "schema_version": 1,
}


def test_metric_roundtrip_and_int_accepted_for_float():
    event = MetricEvent(**{**METRIC, "cpu_pct": 0})
    assert MetricEvent.from_json(event.to_json()) == event


@pytest.mark.parametrize(
    "change, error",
    [
        ({"mem_bytes": -1}, "mem_bytes: negatif"),
        ({"cpu_pct": -0.5}, "cpu_pct: negatif"),
        ({"disk_used_pct": 101}, "disk_used_pct"),
        ({"healthy": "evet"}, "healthy: geçersiz tip"),
        ({"healthy": 1}, "healthy: geçersiz tip"),
        ({"mem_bytes": True}, "mem_bytes: geçersiz tip"),
        ({"source": " "}, "source: boş"),
    ],
)
def test_invalid_metric_rejected(change, error):
    with pytest.raises(ValueError, match=error):
        MetricEvent(**{**METRIC, **change})


def test_metric_spark_ddl_uses_bigint_for_byte_counters():
    assert METRIC_EVENT_DDL == (
        "source STRING, event_time TIMESTAMP, mem_bytes BIGINT, mem_limit_bytes BIGINT, "
        "blk_read_bytes BIGINT, blk_write_bytes BIGINT, net_rx_bytes BIGINT, net_tx_bytes BIGINT, "
        "cpu_pct DOUBLE, healthy BOOLEAN, disk_used_pct DOUBLE, schema_version INT"
    )


ALERT = {
    "alert_id": "a1",
    "source": "hdfs-replay",
    "session_key": "blk_1",
    "kind": "deeplog",
    "risk_score": 0.4,
    "threshold": 0.2,
    "event_time": "2026-11-23T10:00:00.000Z",
    "alert_time": "2026-11-23T10:00:02.500Z",
    "model_version": "hdfs-v1",
    "event_id": "E7",
    "schema_version": 1,
}


def test_alert_roundtrip():
    event = AlertEvent(**ALERT)
    assert AlertEvent.from_json(event.to_json()) == event
    assert AlertEvent(**{**ALERT, "kind": "novelty", "event_id": None, "risk_score": 1})


@pytest.mark.parametrize(
    "change, error",
    [
        ({"kind": "baska"}, "kind"),
        ({"risk_score": 1.5}, "risk_score"),
        ({"alert_time": "dün"}, "alert_time"),
        ({"session_key": ""}, "session_key: boş"),
    ],
)
def test_invalid_alert_rejected(change, error):
    with pytest.raises(ValueError, match=error):
        AlertEvent(**{**ALERT, **change})
