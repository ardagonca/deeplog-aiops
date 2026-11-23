"""EXP-04 (K6, K7, K9) ve EXP-05 (K8) koşucusu: demo sistemde otonom döngü (ADR-0011).

Kullanım (önce: docker compose --profile demo up -d --wait; model deeplog-demo-v1 yayımlanmış):
  uv run --group ml python -m remediation.exp04 remediate --runs 20     # K6, K7
  uv run --group ml python -m remediation.exp04 observe --runs 20       # K9 (Remediator kapalı)
  uv run --group ml python -m remediation.exp04 normal --minutes 120    # EXP-05, K8

remediate: Her koşuda arıza enjekte edilir; Remediator (DRY_RUN=false) alarmlara göre aksiyon alır.
  Başarı (K7): enjeksiyondan sonra --timeout içinde, insan müdahalesi olmadan servis yeniden
  sağlıklı (sentetik istek 3 kez üst üste başarılı) ve en az bir aksiyon başarılı.
  K6 = aksiyonun bitişi - aksiyonu tetikleyen alarmın zamanı. Formdaki ölçüt: ilk alarm ->
  aksiyon komutunun gönderilmesi < 1 dk.
observe: Kademeli senaryolarda Remediator kapalıyken (AUTOREMEDIATION_ENABLED=false) ilk alarm ile
  arıza anı arasındaki süre ölçülür: K9 = failure_time - ilk alarm (H2: medyan > 0).
normal: Arıza enjekte edilmeden yalnızca normal yük; alarm ve aksiyon sayılır (K8: 0 aksiyon).

Saatler: arıza anı host saatiyle ölçülür. Alarmlar ve aksiyonlar için host'a ulaştıkları an
kullanılır (Docker sanal makinesinin saati kayabilir). Bu, öngörü süresini Kafka gecikmesi kadar
eksik gösterir, asla fazla göstermez.
"""

import argparse
import json
import statistics
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from demo_target.inject import SCENARIOS, inject, probe
from ingestion.schemas import ActionEvent, AlertEvent
from ml.metrics import wilson

ROOT = Path(__file__).resolve().parents[1]
URL = "http://127.0.0.1:8100"
DEMO_SOURCES = ("demo-app", "demo-client")
EXPECTED = {  # senaryo -> doğru kabul edilen arıza tipleri
    "disk": {"disk_full"},
    "leak": {"memory_exhaustion", "service_unresponsive"},  # OOM'dan önce ya da sonra
    "kill": {"service_unresponsive"},
    "hang": {"service_unresponsive"},
}


def iso(t: float | None) -> str | None:
    if t is None:
        return None
    return (
        datetime.fromtimestamp(t, timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class Watch:
    """alerts ve actions topic'lerini arka planda okur; her mesajı host'a ulaştığı anla tutar."""

    def __init__(self, bootstrap: str = "127.0.0.1:9092"):
        from confluent_kafka import Consumer

        self.alerts: list[tuple[float, AlertEvent]] = []
        self.actions: list[tuple[float, ActionEvent]] = []
        self.lock = threading.Lock()
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"exp04-{uuid.uuid4()}",
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            }
        )
        self.consumer.subscribe(["alerts", "actions"])
        threading.Thread(target=self._run, daemon=True).start()
        deadline = time.time() + 30
        while not self.consumer.assignment() and time.time() < deadline:
            time.sleep(0.2)  # atama gelmeden başlanırsa ilk mesajlar kaçabilir

    def _run(self) -> None:
        while True:
            msg = self.consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            now = time.time()
            with self.lock:
                if msg.topic() == "alerts":
                    a = AlertEvent.from_json(msg.value())
                    if a.source in DEMO_SOURCES:
                        self.alerts.append((now, a))
                else:
                    self.actions.append((now, ActionEvent.from_json(msg.value())))

    def since(self, t: float):
        with self.lock:
            return [x for x in self.alerts if x[0] >= t], [x for x in self.actions if x[0] >= t]


def healthy(n: int = 3, timeout: float = 120) -> bool:
    """Sentetik istek n kez üst üste başarılı mı (timeout içinde)?"""
    ok, deadline = 0, time.time() + timeout
    while time.time() < deadline:
        ok = ok + 1 if probe(URL) else 0
        if ok >= n:
            return True
        time.sleep(0.5)
    return False


def reset() -> None:
    """Enjeksiyonu durdurur ve çöpü temizler; servis düşmüşse ayağa kaldırır (deney altyapısı)."""
    try:
        urllib_post(f"{URL}/fault/clear")
    except OSError:
        pass  # kilitli ya da düşmüş servis: yeniden başlatma enjeksiyonu zaten sıfırlar
    subprocess.run(["docker", "start", "deeplog-demo-app-1"], capture_output=True)
    subprocess.run(
        ["docker", "exec", "deeplog-demo-app-1", "sh", "-c", "rm -f /app/var/tmp/junk-*.bin"],
        capture_output=True,
    )


def urllib_post(url: str) -> None:
    import urllib.request

    urllib.request.urlopen(urllib.request.Request(url, data=b"", method="POST"), timeout=3).close()


def run_once(watch: Watch, scenario: str, timeout: float, mode: str) -> dict:
    kind, rate = SCENARIOS[scenario]
    t0 = time.time()
    inject(scenario, URL, rate)
    failure = recovered = None
    streak = 0
    while time.time() - t0 < timeout:
        t = time.time()
        ok = probe(URL)
        if not ok and failure is None:
            failure = t
        alerts, actions = watch.since(t0)
        done = [a for _, a in actions if a.status == "success" and a.action != "none"]
        if mode == "remediate" and done:
            streak = streak + 1 if ok else 0
            if streak >= 3:
                recovered = t
                break
        if mode == "observe" and failure is not None and alerts:
            break
        time.sleep(max(0.0, 0.5 - (time.time() - t)))
    alerts, actions = watch.since(t0)
    first_alert = alerts[0][0] if alerts else None
    acted = [(t, a) for t, a in actions if a.status == "success" and a.action != "none"]
    record = {
        "scenario": scenario,
        "kind": kind,
        "mode": mode,
        "inject_time": iso(t0),
        "first_alert_seen": iso(first_alert),
        "alerts": len(alerts),
        "failure_time": iso(failure),
        "lead_time_s": round(failure - first_alert, 3) if failure and first_alert else None,
        "decisions": dict(sorted(_count(a.status for _, a in actions).items())),
    }
    if mode == "remediate":
        first = acted[0][1] if acted else None
        record |= {
            "recovered_time": iso(recovered),
            "success": recovered is not None,
            "action": first.action if first else None,
            "fault_type": first.fault_type if first else None,
            "fault_type_ok": bool(first and first.fault_type in EXPECTED[scenario]),
            "k6_s": _seconds(first.alert_time, first.finished_at) if first else None,
            "alert_to_command_s": round(acted[0][0] - first_alert, 3)
            if acted and first_alert
            else None,
            "ttr_s": round(recovered - t0, 3) if recovered else None,
            "prevented_failure": failure is None and recovered is not None,
        }
    return record


def _count(items) -> dict:
    out: dict = {}
    for i in items:
        out[i] = out.get(i, 0) + 1
    return out


def _seconds(a: str, b: str) -> float:
    t = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))  # noqa: E731
    return round((t(b) - t(a)).total_seconds(), 3)


def pct(values, q):
    v = sorted(x for x in values if x is not None)
    return v[min(len(v) - 1, int(q * (len(v) - 1)))] if v else None


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("mode", choices=["remediate", "observe", "normal"])
    parser.add_argument("--scenarios", default=None, help="virgüllü; varsayılan: moda göre")
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=240, help="koşu başına sn")
    parser.add_argument("--gap", type=float, default=75, help="koşular arası en az sn")
    parser.add_argument("--quiet", type=float, default=20, help="koşu öncesi alarmsız sn")
    parser.add_argument("--minutes", type=float, default=120, help="normal mod süresi")
    args = parser.parse_args()

    default = {"remediate": "disk,leak,kill,hang", "observe": "disk,leak", "normal": ""}
    scenarios = (args.scenarios or default[args.mode]).split(",") if args.mode != "normal" else []
    exp = "EXP-05" if args.mode == "normal" else "EXP-04"
    out = ROOT / "experiments" / exp / f"{datetime.now():%Y-%m-%d_%H%M}-{args.mode}"
    out.mkdir(parents=True, exist_ok=True)
    watch = Watch()
    if not healthy():
        print("demo-app sağlıklı değil", file=sys.stderr)
        return 1

    if args.mode == "normal":
        t0 = time.time()
        print(f"{args.minutes:.0f} dk normal yük (arıza yok)", flush=True)
        while time.time() - t0 < args.minutes * 60:
            time.sleep(10)
        alerts, actions = watch.since(t0)
        acted = [a for _, a in actions if a.action != "none" and a.status == "success"]
        result = {
            "minutes": args.minutes,
            "alerts": len(alerts),
            "decisions": _count(a.status for _, a in actions),
            "actions_taken": len(acted),
            "k8_ok": len(acted) == 0,
        }
        (out / "results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False))
        return 0

    records = []
    order = [
        s for _ in range(args.runs) for s in scenarios
    ]  # senaryolar iç içe: zaman etkisi dağılır
    for i, scenario in enumerate(order, 1):
        started = time.time()
        reset()
        if not healthy():
            print(f"[{i}] servis toparlanmadı, koşu atlandı", flush=True)
            continue
        quiet_deadline = time.time() + 60  # yanlış alarm varsa sonsuza dek beklemesin
        while time.time() < quiet_deadline:  # önceki koşunun alarmları bitsin
            alerts, _ = watch.since(time.time() - args.quiet)
            if not alerts:
                break
            time.sleep(2)
        rec = run_once(watch, scenario, args.timeout, args.mode)
        rec["run"] = i
        records.append(rec)
        with (out / "runs.jsonl").open("a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print(f"[{i}/{len(order)}] {json.dumps(rec, ensure_ascii=False)}", flush=True)
        reset()
        time.sleep(max(0.0, args.gap - (time.time() - started)))

    summary = {}
    for s in scenarios:
        rs = [r for r in records if r["scenario"] == s]
        if args.mode == "remediate":
            k = sum(r["success"] for r in rs)
            summary[s] = {
                "runs": len(rs),
                "success": k,
                "success_wilson95": wilson(k, len(rs)),
                "fault_type_ok": sum(r["fault_type_ok"] for r in rs),
                "prevented_failure": sum(r["prevented_failure"] for r in rs),
                "k6_p50": pct([r["k6_s"] for r in rs], 0.5),
                "k6_p95": pct([r["k6_s"] for r in rs], 0.95),
                "alert_to_command_max": max(
                    (r["alert_to_command_s"] for r in rs if r["alert_to_command_s"] is not None),
                    default=None,
                ),
                "ttr_p50": pct([r["ttr_s"] for r in rs], 0.5),
                "ttr_p95": pct([r["ttr_s"] for r in rs], 0.95),
            }
        else:
            leads = [r["lead_time_s"] for r in rs if r["lead_time_s"] is not None]
            summary[s] = {
                "runs": len(rs),
                "with_lead": len(leads),
                "lead_median_s": statistics.median(leads) if leads else None,
                "lead_min_s": min(leads, default=None),
                "wilcoxon_p_greater_0": _wilcoxon(leads),
            }
    (out / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n"
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


def _wilcoxon(values) -> float | None:
    """H2: tek örneklem Wilcoxon, H1: medyan > 0."""
    if len(values) < 6:
        return None
    from scipy.stats import wilcoxon

    return float(wilcoxon(values, alternative="greater").pvalue)


if __name__ == "__main__":
    sys.exit(main())
