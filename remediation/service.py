"""REM-04..06: Remediator servisi (ADR-0005, ADR-0011). Alarmı alır, arıza tipini belirler,
korumaları uygular, aksiyonu çalıştırır ve her kararı actions topic'ine yazar.

Compose'taki remediator servisi çalıştırır (profil: demo). Ortam değişkenleri:
  AUTOREMEDIATION_ENABLED  false ise hiçbir aksiyon alınmaz (kill switch); varsayılan true
  DRY_RUN                  true ise aksiyon çalıştırılmaz, yalnızca kaydedilir; varsayılan true
  TARGETS                  hedef=konteyner, ör. demo-app=deeplog-demo-app-1 (allowlist budur)
  ALERT_SOURCES            alarm kaynağı=hedef, ör. demo-app=demo-app,demo-client=demo-app
  HEALTH_URLS              hedef=url; Remediator her saniye yoklar (service_unresponsive kuralı)
  DOCKER_HOST              yalnızca Remediator'ın ağındaki Docker API vekili
Yalnızca ALERT_SOURCES'taki kaynakların alarmları işlenir; diğer izlerin (HDFS_v1) alarmları bu
servisin sorumluluğunda değildir ve kayıt da üretmez.
"""

import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from ingestion.schemas import ActionEvent, AlertEvent, MetricEvent
from remediation.decide import Guards, TargetState, decide

PLAYBOOKS = Path(__file__).parent / "playbooks"
HEARTBEAT = Path("/tmp/heartbeat")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def pairs(text: str) -> dict[str, str]:
    """'a=b,c=d' -> {'a': 'b', 'c': 'd'}"""
    return dict(item.split("=", 1) for item in text.split(",") if item.strip())


def log(level: str, msg: str, **fields) -> None:
    print(json.dumps({"level": level, "msg": msg, **fields}, ensure_ascii=False), flush=True)


class Remediator:
    """Kafka'dan bağımsız çekirdek: handle() bir alarm için kararı verir ve aksiyonu çalıştırır."""

    def __init__(
        self,
        guards: Guards,
        alert_sources: dict[str, str],
        executors: dict[str, Callable[[str], str | None]],
        dry_run: bool,
        clock: Callable[[], float] = time.time,
    ):
        self.guards, self.alert_sources = guards, alert_sources
        self.executors, self.dry_run, self.clock = executors, dry_run, clock
        self.states: dict[str, TargetState] = {}
        self.lock = threading.Lock()

    def responsible_for(self, alert: AlertEvent) -> bool:
        return alert.source in self.alert_sources

    def handle(self, alert: AlertEvent) -> ActionEvent:
        target = self.alert_sources.get(alert.source)
        started = now_iso()
        with self.lock:
            state = self.states.get(target)
            snapshot = TargetState(**vars(state)) if state else None
        d = decide(self.guards, alert.alert_id, target, snapshot, self.clock())
        status, detail = d.status, None
        if status is None:
            if self.dry_run:
                status, detail = "success", "dry-run: çalıştırılmadı"
            else:
                try:
                    detail = self.executors[d.action](target)
                    status = "success"
                except Exception as e:  # aksiyon hatası servisi durdurmaz; kayda geçer
                    status, detail = "failed", f"{type(e).__name__}: {e}"[:500]
            if status == "success" and d.action == "docker:restart":
                with self.lock:  # eski ölçümler yeniden tetiklemesin; taze ölçüm gelene kadar
                    self.states[target] = TargetState()
        return ActionEvent(
            action_id=str(uuid.uuid4()),
            alert_id=alert.alert_id,
            source=alert.source,
            target=target,
            fault_type=d.fault_type,
            action=d.action,
            dry_run=self.dry_run,
            status=status,
            alert_time=alert.alert_time,
            started_at=started,
            finished_at=now_iso(),
            detail=detail,
        )

    def on_metric(self, m: MetricEvent) -> None:
        with self.lock:
            s = self.states.setdefault(m.source, TargetState())
            s.disk_used_pct, s.mem_bytes, s.mem_limit_bytes = (
                m.disk_used_pct,
                m.mem_bytes,
                m.mem_limit_bytes,
            )

    def on_health(self, target: str, body: dict | None) -> None:
        """Yoklama sonucu: yanıt yoksa ardışık başarısızlık artar. Yanıttaki disk ve bellek
        değerleri metrics.raw'dan (5 sn) tazedir (1 sn): disk %90'dan %100'e ~6 sn'de dolar."""
        with self.lock:
            s = self.states.setdefault(target, TargetState())
            if body is None:
                s.health_failures += 1
                return
            s.health_failures = 0
            for name in ("disk_used_pct", "mem_bytes", "mem_limit_bytes"):
                if body.get(name) is not None:
                    setattr(s, name, body[name])


def docker_restart(docker: str, containers: dict[str, str]) -> Callable[[str], str]:
    def run(target: str) -> str:
        name = containers[target]  # allowlist dışı hedef buraya hiç gelmez (Guards)
        base = docker.replace("tcp://", "http://", 1)  # DOCKER_HOST biçimi (Ansible ile ortak)
        req = urllib.request.Request(f"{base}/containers/{name}/restart?t=5", method="POST")
        with urllib.request.urlopen(req, timeout=60) as r:
            return f"docker restart {name}: HTTP {r.status}"

    return run


def ansible_cleanup(containers: dict[str, str]) -> Callable[[str], str]:
    def run(target: str) -> str:
        name = containers[target]
        result = subprocess.run(
            ["ansible-playbook", "-i", f"{name},", str(PLAYBOOKS / "disk_cleanup.yml")],
            capture_output=True, text=True, timeout=120,
        )  # fmt: skip
        if result.returncode != 0:
            raise RuntimeError(result.stdout[-400:] + result.stderr[-400:])
        return result.stdout.strip().splitlines()[-1] if result.stdout.strip() else "ok"

    return run


def probe(url: str) -> dict | None:
    """/health JSON'u; yanıt yoksa ya da 200 değilse None."""
    try:
        with urllib.request.urlopen(url, timeout=1) as r:
            return json.load(r) if r.status == 200 else None
    except (urllib.error.URLError, OSError, ValueError):
        return None


def main() -> int:
    from confluent_kafka import Consumer, Producer

    env = os.environ
    containers = pairs(env.get("TARGETS", "demo-app=deeplog-demo-app-1"))
    health_urls = pairs(env.get("HEALTH_URLS", "demo-app=http://demo-app:8000/health"))
    rem = Remediator(
        Guards(env.get("AUTOREMEDIATION_ENABLED", "true") == "true", set(containers)),
        pairs(env.get("ALERT_SOURCES", "demo-app=demo-app,demo-client=demo-app")),
        {
            "docker:restart": docker_restart(env["DOCKER_HOST"], containers),
            "ansible:disk_cleanup": ansible_cleanup(containers),
        },
        dry_run=env.get("DRY_RUN", "true") != "false",
    )
    bootstrap = env.get("KAFKA_BOOTSTRAP", "kafka:29092")
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})

    def consumer(topic: str, group: str) -> Consumer:
        c = Consumer(
            {"bootstrap.servers": bootstrap, "group.id": group, "auto.offset.reset": "latest"}
        )
        c.subscribe([topic])
        return c

    def metrics_loop() -> None:
        c = consumer("metrics.raw", f"remediator-metrics-{uuid.uuid4()}")  # hep en güncel
        while True:
            msg = c.poll(1.0)
            if msg is not None and not msg.error():
                try:
                    rem.on_metric(MetricEvent.from_json(msg.value()))
                except ValueError as e:
                    log("WARN", "geçersiz metrik", error=str(e))

    def health_loop() -> None:
        while True:
            started = time.monotonic()
            for target, url in health_urls.items():
                rem.on_health(target, probe(url))
            HEARTBEAT.touch()
            time.sleep(max(0.0, 1.0 - (time.monotonic() - started)))

    for loop in (metrics_loop, health_loop):
        threading.Thread(target=loop, daemon=True).start()
    alerts = consumer("alerts", "remediator")
    log("INFO", "başladı", dry_run=rem.dry_run, enabled=rem.guards.enabled, targets=containers)
    while True:
        msg = alerts.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            alert = AlertEvent.from_json(msg.value())
        except ValueError as e:
            log("WARN", "geçersiz alarm", error=str(e))
            continue
        if not rem.responsible_for(alert):
            continue
        action = rem.handle(alert)
        producer.produce("actions", value=action.to_json(), key=(action.target or "").encode())
        producer.poll(0)
        if action.action != "none":
            log("INFO", "karar", **json.loads(action.to_json()))


if __name__ == "__main__":
    sys.exit(main())
