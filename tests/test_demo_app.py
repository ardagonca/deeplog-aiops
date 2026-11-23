"""REM-01: demo sistem (ADR-0011): oturum = istek, sessiz uçlar, ENOSPC yolu, log biçimi."""

import errno
import importlib
import logging

import pytest
from fastapi.testclient import TestClient

from ingestion.schemas import LogEvent
from ml.parsing import content_of


@pytest.fixture
def demo(tmp_path, monkeypatch):
    monkeypatch.setenv("DEMO_VAR_DIR", str(tmp_path))
    monkeypatch.setenv("FAULTS_ENABLED", "true")
    monkeypatch.delenv("KAFKA_BOOTSTRAP", raising=False)
    from demo_target import app as module
    from demo_target import logs

    module = importlib.reload(module)  # ortam değişkenleri içe aktarmada okunur
    sent: list[tuple[bytes, bytes]] = []
    with TestClient(module.app) as client:
        logging.getLogger("demo").addHandler(
            logs.KafkaLogHandler(lambda v, k: sent.append((v, k)), "demo-app")
        )
        yield module, client, sent
    module.faults.clear()


def _events(sent):
    return [LogEvent.from_json(v) for v, _ in sent]


def test_request_logs_share_client_request_id_and_parse_like_hdfs(demo):
    _, client, sent = demo
    r = client.post("/api/orders/7", content=b'{"x":1}', headers={"X-Request-ID": "r-1"})
    assert r.is_success
    events = _events(sent)
    assert [content_of(e.message) for e in events] == [
        "request start POST /api/orders/7",
        "order 7 saved (7 bytes)",
        events[-1].message.split(": ", 1)[1],  # süre değişken
    ]
    assert content_of(events[-1].message).startswith("request done 200 in ")
    assert {e.session_key for e in events} == {"r-1"} and {k for _, k in sent} == {b"r-1"}
    assert all(e.source == "demo-app" and e.label is None for e in events)


def test_invalid_request_id_is_replaced(demo):
    _, client, sent = demo
    client.get("/api/orders/1", headers={"X-Request-ID": "bad id; DROP"})
    keys = {e.session_key for e in _events(sent)}
    assert len(keys) == 1 and keys.pop() != "bad id; DROP"


def test_health_and_fault_endpoints_are_silent(demo):
    _, client, sent = demo
    body = client.get("/health").json()
    assert body["status"] == "ok" and 0 <= body["disk_used_pct"] <= 100
    assert client.post("/fault/clear").is_success
    assert sent == []  # arıza enjeksiyonu logdan anlaşılamaz


def test_no_space_is_logged_as_error_and_returns_507(demo, monkeypatch):
    module, client, sent = demo

    def full(self, data):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(module.Path, "write_bytes", full)
    assert client.post("/api/orders/3", content=b"{}").status_code == 507
    errors = [e for e in _events(sent) if e.level == "ERROR"]
    assert [content_of(e.message) for e in errors] == [
        "order 3 save failed: No space left on device"
    ]


def test_resource_warning_precedes_handler_logs(demo, monkeypatch):
    module, client, sent = demo
    monkeypatch.setattr(module, "disk_used_pct", lambda: 85.0)
    client.get("/api/report")
    contents = [content_of(e.message) for e in _events(sent)]
    assert contents[1] == "disk usage high: 85.0%"
    assert client.get("/health").json()["status"] == "ok"  # %90 altında henüz bozulmadı


def test_fill_fault_writes_junk_until_stopped(demo):
    module, client, _ = demo
    client.post("/fault/fill", params={"mb_per_s": 50})
    deadline = __import__("time").monotonic() + 5
    while not list(module.tmp_dir().glob("junk-*.bin")):
        assert __import__("time").monotonic() < deadline
    client.post("/fault/clear")
    assert list(module.tmp_dir().glob("junk-*.bin"))  # temizlik Remediator'ın işi
