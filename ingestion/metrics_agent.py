"""ING-05: Projedeki konteynerlerin kaynak kullanımını toplayıp metrics.raw'a gönderir.

Docker API'ye yalnızca okuma izni olan socket proxy üzerinden erişir (docs/01-mimari.md §9);
ajan hiçbir konteyneri başlatamaz, durduramaz, içinde komut çalıştıramaz.
Compose'taki metrics-agent servisi çalıştırır:
  python -m ingestion.metrics_agent --docker http://socket-proxy:2375 --bootstrap kafka:29092
--health servis=url: o servisin /health çıktısındaki disk_used_pct ölçüme eklenir (REM-01; Docker
API konteyner içi disk doluluğunu vermez). Yanıt gelmezse alan boş kalır.
"""

import argparse
import json
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ingestion.schemas import MetricEvent

PROJECT = "deeplog"
HEARTBEAT = Path("/tmp/heartbeat")  # healthcheck bu dosyanın tazeliğine bakar


def log(level: str, msg: str, **fields) -> None:
    print(json.dumps({"level": level, "msg": msg, **fields}, ensure_ascii=False), flush=True)


def health(status: str) -> bool | None:
    """Docker'ın 'Up 3 minutes (healthy)' metninden healthcheck sonucu; yoksa/başlıyorsa None."""
    if "(healthy)" in status:
        return True
    if "(unhealthy)" in status:
        return False
    return None


def cpu_pct(prev: dict | None, cur: dict) -> float | None:
    """docker stats formülü: (Δkonteyner CPU / Δsistem CPU) × çekirdek sayısı × 100."""
    if prev is None:
        return None
    used = cur["cpu_usage"]["total_usage"] - prev["cpu_usage"]["total_usage"]
    total = cur.get("system_cpu_usage", 0) - prev.get("system_cpu_usage", 0)
    if total <= 0 or used < 0:  # konteyner yeniden başladıysa sayaç sıfırlanır
        return None
    return round(used / total * cur.get("online_cpus", 1) * 100, 2)


def disk_from_health(url: str, timeout: float = 2.0) -> float | None:
    """Servisin /health JSON'undaki disk_used_pct; ulaşılamazsa ya da alan yoksa None."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            value = json.load(response).get("disk_used_pct")
    except (OSError, ValueError, AttributeError):  # yanıt vermeme senaryosunda zaman aşımı
        return None
    return float(value) if isinstance(value, (int, float)) and 0 <= value <= 100 else None


def to_metric(
    service: str,
    status: str,
    stats: dict,
    prev_cpu: dict | None,
    now: datetime,
    disk_used_pct: float | None = None,
):
    memory = stats.get("memory_stats") or {}
    page_cache = (memory.get("stats") or {}).get("inactive_file", 0)
    blkio = (stats.get("blkio_stats") or {}).get("io_service_bytes_recursive") or []
    networks = (stats.get("networks") or {}).values()

    def blk(op: str) -> int:
        return sum(
            e["value"] for e in blkio if e["op"].lower() == op
        )  # cgroup v1: "Read", v2: "read"

    return MetricEvent(
        source=service,
        event_time=now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        mem_bytes=max(memory.get("usage", 0) - page_cache, 0),
        mem_limit_bytes=memory.get("limit", 0),
        blk_read_bytes=blk("read"),
        blk_write_bytes=blk("write"),
        net_rx_bytes=sum(n["rx_bytes"] for n in networks),
        net_tx_bytes=sum(n["tx_bytes"] for n in networks),
        cpu_pct=cpu_pct(prev_cpu, stats["cpu_stats"]),
        healthy=health(status),
        disk_used_pct=disk_used_pct,
    )


def main(argv: list[str] | None = None) -> int:
    from confluent_kafka import Producer

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--docker", default="http://socket-proxy:2375")
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="metrics.raw")
    parser.add_argument("--interval", type=float, default=5.0, help="saniye")
    parser.add_argument("--health", action="append", default=[], metavar="SERVİS=URL")
    args = parser.parse_args(argv)
    try:
        health_urls = dict(item.split("=", 1) for item in args.health)
    except ValueError:
        parser.error("--health servis=url biçiminde olmalı")

    def get_json(path: str):
        with urllib.request.urlopen(args.docker + path, timeout=10) as response:
            return json.load(response)

    def on_delivery(err, msg) -> None:
        if err is not None:
            log("ERROR", "teslim edilemedi", key=(msg.key() or b"").decode(), error=str(err))

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            "message.timeout.ms": 30_000,
        }
    )
    # SIGTERM'de (docker stop) kuyruktaki ölçümler gönderilmeden çıkılmasın.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    project_filter = urllib.parse.quote(
        json.dumps({"label": [f"com.docker.compose.project={PROJECT}"]})
    )
    previous: dict[str, dict] = {}  # konteyner id -> önceki cpu_stats
    log("INFO", "başladı", docker=args.docker, topic=args.topic, interval=args.interval)
    try:
        while True:
            started = time.monotonic()
            try:
                containers = get_json(f"/containers/json?filters={project_filter}")
            except (OSError, ValueError) as e:  # proxy geçici olarak yoksa bir sonraki turda dene
                log("WARN", "konteyner listesi alınamadı", error=str(e))
                containers = None
            if containers is not None:
                now = datetime.now(timezone.utc)
                current: dict[str, dict] = {}
                for c in containers:
                    service = c["Labels"].get("com.docker.compose.service", c["Names"][0])
                    try:
                        stats = get_json(f"/containers/{c['Id']}/stats?stream=false&one-shot=true")
                        disk = (
                            disk_from_health(health_urls[service])
                            if service in health_urls
                            else None
                        )
                        event = to_metric(
                            service, c["Status"], stats, previous.get(c["Id"]), now, disk
                        )
                    except (OSError, ValueError, KeyError) as e:  # bu arada kapanmış olabilir
                        log("WARN", "ölçüm atlandı", service=service, error=str(e))
                        continue
                    current[c["Id"]] = stats["cpu_stats"]
                    producer.produce(
                        args.topic, value=event.to_json(), key=service, on_delivery=on_delivery
                    )
                previous = current
                producer.poll(0)
                HEARTBEAT.touch()
            time.sleep(max(0.0, args.interval - (time.monotonic() - started)))
    finally:
        remaining = producer.flush(10)
        log("INFO", "durdu", undelivered=remaining)
    return 0


if __name__ == "__main__":
    sys.exit(main())
