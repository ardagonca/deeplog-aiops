"""REM-01: Demo hedef sistem (ADR-0011). Otonom döngünün denendiği küçük bir sipariş servisi.

Çalıştırma: compose'taki demo-app servisi (profil: demo). Yerelde:
  DEMO_VAR_DIR=/tmp/demo uv run uvicorn demo_target.app:app --port 8000

- Oturum = istek: X-Request-ID (yoksa üretilir) logların session_key'idir.
- Veri ve geçici dosyalar DEMO_VAR_DIR'dedir (compose'ta 64 MB tmpfs: gerçek ENOSPC, host güvende).
- Kaynak uyarıları (disk/bellek ≥ %80) istek içinde loglanır: kademeli arızaların erken sinyali.
- /health ve /fault/* log üretmez; arıza enjeksiyonu logdan anlaşılmasın (etiket sızıntısı).
- /fault/* yalnızca FAULTS_ENABLED=true iken vardır (REM-02 enjektörü kullanır).
"""

import asyncio
import contextlib
import errno
import logging
import os
import random
import re
import shutil
import threading
import time
import uuid
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from demo_target import ORDER_IDS, logs

VAR = Path(os.environ.get("DEMO_VAR_DIR", "/app/var"))
FAULTS_ENABLED = os.environ.get("FAULTS_ENABLED") == "true"
WARN_PCT = 80.0
DEGRADED_PCT = 90.0
SILENT = ("/health", "/fault")
REQUEST_ID = re.compile(r"[A-Za-z0-9-]{1,64}")
CHUNK = 1 << 20

http = logging.getLogger("demo.http")
orders = logging.getLogger("demo.orders")
report = logging.getLogger("demo.report")
resources = logging.getLogger("demo.resources")


def data_dir() -> Path:
    return VAR / "data"


def tmp_dir() -> Path:
    return VAR / "tmp"


def disk_used_pct() -> float:
    usage = shutil.disk_usage(VAR)
    return round(100 * usage.used / usage.total, 1)


def _read_int(path: str) -> int | None:
    try:
        text = Path(path).read_text().split()[0]
        return None if text == "max" else int(text)
    except (OSError, ValueError, IndexError):
        return None  # cgroup v2 dışında (ör. Windows'ta testler)


def mem_bytes() -> int | None:
    """Konteynerin bellek kullanımı (cgroup; OOM'u belirleyen değer, tmpfs dahil)."""
    return _read_int("/sys/fs/cgroup/memory.current")


def mem_limit_bytes() -> int | None:
    return _read_int("/sys/fs/cgroup/memory.max")


def mem_used_pct() -> float | None:
    used, limit = mem_bytes(), mem_limit_bytes()
    return round(100 * used / limit, 1) if used is not None and limit else None


class _Faults:
    """Enjekte edilen arızaların durumu (REM-02). Süreç yeniden başlayınca hepsi sıfırlanır."""

    def __init__(self):
        self.hang = False
        self.leak: list[bytes] = []
        self.stop = threading.Event()
        self.threads: list[threading.Thread] = []

    def start(self, target, *args) -> None:
        t = threading.Thread(target=target, args=args, daemon=True)
        self.threads.append(t)
        t.start()

    def clear(self) -> None:
        self.stop.set()
        for t in self.threads:
            t.join(timeout=2)
        self.threads, self.leak, self.hang = [], [], False
        self.stop = threading.Event()


faults = _Faults()


def _leak(mb_per_s: float, stop: threading.Event) -> None:
    while not stop.wait(1.0):
        faults.leak.append(
            b"\x01" * int(mb_per_s * CHUNK)
        )  # sıfır olmayan bayt: sayfalar gerçekten ayrılır


def _fill(mb_per_s: float, stop: threading.Event) -> None:
    n = 0
    while not stop.wait(1.0 / mb_per_s):
        try:
            (tmp_dir() / f"junk-{n:06d}.bin").write_bytes(b"\x00" * CHUNK)
        except OSError:
            return  # disk doldu; enjeksiyon tamam
        n += 1


@contextlib.asynccontextmanager
async def _lifespan(_app):
    logs.setup("demo-app", os.environ.get("KAFKA_BOOTSTRAP"))
    data_dir().mkdir(parents=True, exist_ok=True)
    tmp_dir().mkdir(parents=True, exist_ok=True)
    yield


app = FastAPI(title="demo-app", lifespan=_lifespan)


@app.middleware("http")
async def _request_context(request: Request, call_next):
    path = request.url.path
    if faults.hang and not path.startswith("/fault"):
        await asyncio.sleep(3600)  # yanıt vermeme: bağlantı açık kalır, istemci zaman aşımına düşer
    rid = request.headers.get("x-request-id", "")
    token = logs.request_id.set(rid if REQUEST_ID.fullmatch(rid) else uuid.uuid4().hex)
    silent = path.startswith(SILENT)
    start = time.perf_counter()
    try:
        if not silent:
            http.info("request start %s %s", request.method, path)
            _warn_resources()
        try:
            response = await call_next(request)
        except Exception as e:
            http.error("request failed: %s", type(e).__name__)
            raise
        if not silent:
            ms = (time.perf_counter() - start) * 1000
            http.info("request done %d in %d ms", response.status_code, ms)
        return response
    finally:
        logs.request_id.reset(token)


def _warn_resources() -> None:
    if (disk := disk_used_pct()) >= WARN_PCT:
        resources.warning("disk usage high: %.1f%%", disk)
    if (mem := mem_used_pct()) is not None and mem >= WARN_PCT:
        resources.warning("memory usage high: %.1f%%", mem)


def _no_space(e: OSError) -> bool:
    return e.errno == errno.ENOSPC


@app.get("/api/orders/{oid}")
def get_order(oid: int):
    path = data_dir() / f"order-{oid}.json"
    if not 1 <= oid <= ORDER_IDS or not path.exists():
        orders.info("order %d not found", oid)
        return JSONResponse({"error": "not found"}, status_code=404)
    body = path.read_bytes()
    orders.info("loaded order %d (%d bytes)", oid, len(body))
    return JSONResponse(content={"id": oid, "size": len(body)})


@app.post("/api/orders/{oid}")
async def put_order(oid: int, request: Request):
    if not 1 <= oid <= ORDER_IDS:
        orders.info("order %d rejected: invalid id", oid)
        return JSONResponse({"error": "invalid id"}, status_code=400)
    body = await request.body()
    try:
        (data_dir() / f"order-{oid}.json").write_bytes(body)
    except OSError as e:
        if not _no_space(e):
            raise
        orders.error("failed to save order %d: %s", oid, e.strerror)
        return JSONResponse({"error": "no space"}, status_code=507)
    orders.info("saved order %d (%d bytes)", oid, len(body))
    return JSONResponse({"id": oid, "saved": len(body)})


@app.get("/api/report")
def get_report(rows: int = 200):
    rows = max(1, min(rows, 5000))
    temp = tmp_dir() / f"report-{uuid.uuid4().hex}.tmp"
    try:
        values = [random.random() for _ in range(rows)]
        temp.write_text("\n".join(f"{v:.6f}" for v in values))
        report.info("report temp file written (%d bytes)", temp.stat().st_size)
    except OSError as e:
        if not _no_space(e):
            raise
        report.error("report temp file write failed: %s", e.strerror)
        return JSONResponse({"error": "no space"}, status_code=507)
    finally:
        temp.unlink(missing_ok=True)
    report.info("report generated (%d rows)", rows)
    return {"rows": rows, "mean": sum(values) / rows}


@app.get("/health")
def health():
    disk, mem = disk_used_pct(), mem_used_pct()
    degraded = disk >= DEGRADED_PCT or (mem is not None and mem >= DEGRADED_PCT)
    return {
        "status": "degraded" if degraded else "ok",
        "disk_used_pct": disk,
        "mem_bytes": mem_bytes(),
        "mem_limit_bytes": mem_limit_bytes(),
    }


if FAULTS_ENABLED:

    @app.post("/fault/leak")
    def fault_leak(mb_per_s: float = 4.0):
        faults.start(_leak, mb_per_s, faults.stop)
        return {"fault": "leak", "mb_per_s": mb_per_s}

    @app.post("/fault/fill")
    def fault_fill(mb_per_s: float = 1.0):
        faults.start(_fill, mb_per_s, faults.stop)
        return {"fault": "fill", "mb_per_s": mb_per_s}

    @app.post("/fault/hang")
    def fault_hang():
        faults.hang = True
        return {"fault": "hang"}

    @app.post("/fault/clear")
    def fault_clear():
        faults.clear()  # çöp dosyalar silinmez: onları temizlemek Remediator'ın işi
        return {"fault": None}
