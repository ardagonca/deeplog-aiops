"""REM-03: istemci gözlemi; yanıt, zaman aşımı ve bağlantı reddi ayrı şablonlarla loglanır."""

import logging
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from demo_target import loadgen, logs
from ingestion.schemas import LogEvent
from ml.parsing import content_of


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/slow"):
            time.sleep(1.5)
        self.send_response(507 if self.path.startswith("/full") else 200)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def server():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


@pytest.fixture
def sent():
    logs.setup("demo-client", None)
    out: list[bytes] = []
    handler = logs.KafkaLogHandler(lambda v, k: out.append(v), "demo-client")
    logging.getLogger("demo").addHandler(handler)
    yield out
    logging.getLogger("demo").removeHandler(handler)


def _contents(sent):
    events = [LogEvent.from_json(v) for v in sent]
    assert len({e.session_key for e in events}) == 1  # tek istek = tek oturum
    return [content_of(e.message).rsplit(" in ", 1)[0] for e in events]


def test_success_and_server_error(server, sent):
    loadgen.call(server, "GET", "/ok?x=1", None, timeout=1)
    assert _contents(sent) == ["client sent GET /ok", "client received 200"]
    sent.clear()
    loadgen.call(server, "GET", "/full", None, timeout=1)
    assert _contents(sent) == ["client sent GET /full", "client received 507"]


def test_timeout_and_refused_have_own_templates(server, sent):
    loadgen.call(server, "GET", "/slow", None, timeout=0.3)
    assert _contents(sent) == ["client sent GET /slow", "client timeout after 0 s"]
    sent.clear()
    with socket.socket() as s:  # boş port: bağlantı reddedilir
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    # Windows reddetmeden önce ~2 sn yeniden dener; Linux konteynerde ret anında gelir.
    loadgen.call(f"http://127.0.0.1:{port}", "GET", "/x", None, timeout=5)
    assert _contents(sent) == ["client sent GET /x", "client connection refused"]


def test_client_sends_its_session_key_as_request_id(server, sent):
    seen = []
    _Handler.do_POST = lambda self: (
        seen.append(self.headers["X-Request-ID"]),
        _Handler.do_GET(self),
    )
    loadgen.call(server, "POST", "/ok", b"{}", timeout=1)
    assert seen == [LogEvent.from_json(sent[0]).session_key]  # uygulama aynı oturuma yazar
