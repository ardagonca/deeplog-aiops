"""REM-03: Demo sisteme normal yük üretir ve istemci (ağ geçidi) gözlemini loglar (ADR-0011).

Kullanım: compose'taki demo-loadgen servisi (profil: demo) ya da
  uv run python -m demo_target.loadgen --url http://127.0.0.1:8100 --kafka 127.0.0.1:9092

Her istek bir X-Request-ID taşır; istemcinin logları (source = demo-client) ve uygulamanın logları
aynı oturumda birleşir. Uygulama durduğunda ya da kilitlendiğinde anomaliyi bu loglar taşır:
zaman aşımı, bağlantı reddi.
Karışım: %50 sipariş okuma, %30 sipariş yazma, %20 rapor. Sipariş kimlikleri 1..ORDER_IDS.
"""

import argparse
import json
import logging
import random
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from demo_target import ORDER_IDS, logs

HEARTBEAT = Path("/tmp/heartbeat")  # healthcheck bu dosyanın tazeliğine bakar
client = logging.getLogger("demo.client")


def pick(rng: random.Random) -> tuple[str, str, bytes | None]:
    """(yöntem, yol, gövde): normal trafiğin karışımı."""
    oid = rng.randint(1, ORDER_IDS)
    r = rng.random()
    if r < 0.5:
        return "GET", f"/api/orders/{oid}", None
    if r < 0.8:
        body = json.dumps({"id": oid, "qty": rng.randint(1, 9), "note": "x" * rng.randint(0, 200)})
        return "POST", f"/api/orders/{oid}", body.encode()
    return "GET", f"/api/report?rows={rng.randint(50, 500)}", None


def call(base: str, method: str, path: str, body: bytes | None, timeout: float) -> None:
    token = logs.request_id.set(uuid.uuid4().hex)
    try:
        route = path.split("?")[0]
        client.info("request sent %s %s", method, route)
        start = time.perf_counter()
        req = urllib.request.Request(base + path, data=body, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                status = response.status
        except urllib.error.HTTPError as e:  # 4xx/5xx yanıt da bir yanıttır
            status = e.code
        except (TimeoutError, urllib.error.URLError, OSError) as e:
            reason = getattr(e, "reason", e)
            if isinstance(reason, TimeoutError) or "timed out" in str(reason):
                client.error("request timeout after %.0f s", timeout)
            elif isinstance(reason, ConnectionRefusedError):
                client.error("connection refused")
            else:
                client.error("connection error: %s", type(reason).__name__)
            return
        ms = (time.perf_counter() - start) * 1000
        level = logging.WARNING if status >= 500 else logging.INFO
        client.log(level, "response %d in %d ms", status, ms)
    finally:
        logs.request_id.reset(token)


def main(argv: list[str] | None = None) -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default="http://demo-app:8000")
    parser.add_argument("--rate", type=float, default=20, help="istek/sn")
    parser.add_argument("--timeout", type=float, default=2.0, help="istek zaman aşımı (sn)")
    parser.add_argument("--kafka", default="kafka:29092", help="boş: yalnızca stdout")
    parser.add_argument("--duration", type=float, help="sn; verilmezse durdurulana kadar")
    parser.add_argument("--seed", type=int)
    args = parser.parse_args(argv)

    logs.setup("demo-client", args.kafka or None)
    rng = random.Random(args.seed)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    # Kilitlenmede istekler zaman aşımına kadar bekler: hız × zaman aşımı kadar eşzamanlı istek.
    workers = max(8, int(args.rate * args.timeout * 2))
    start, sent = time.monotonic(), 0
    with ThreadPoolExecutor(workers) as pool:
        while not stop.is_set():
            if args.duration is not None and time.monotonic() - start >= args.duration:
                break
            pool.submit(call, args.url, *pick(rng), args.timeout)
            sent += 1
            if sent % max(1, int(args.rate)) == 0:
                HEARTBEAT.touch()
            stop.wait(max(0.0, start + sent / args.rate - time.monotonic()))
    print(json.dumps({"sent": sent, "seconds": round(time.monotonic() - start, 1)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
