"""EXP-03 (K5 tespit gecikmesi + canlı/offline tutarlılık): spark-detect uçtan uca.

Kullanım: uv run python -m ml.exp03 [--sessions 20000] [--rate 5000]
Önce: altyapı açık, spark-detect dahil (docker compose --profile hdfs up -d --wait); model paketi
data/models/<model> yerelde de var.
1. Test bölmesinden --sessions oturum seçilir (sabit tohum) ve satırları EKSİKSİZ, orijinal sırayla
   --rate hızında oynatılır (replayer --blocks); dosya başını oynatmak oturumları keserdi.
2. alerts topic'i, bu koşunun kaynağından --wait sn boyunca yeni alarm gelmeyene kadar okunur
   (oturumların zaman aşımıyla kapanması dahil; sabit bekleme yeniden başlatmada yetmiyordu).
3. Raporlanan: erken DeepLog alarmlarının gecikmesi (alert_time - event_time), canlı kararların
   P/R/F1'i ve aynı oturumlarda offline kararla uyum oranı (RT-05).
Çıktı: experiments/EXP-03/<tarih>/{blocks.txt, results.json, ozet.md}.

--restart-after N: oynatma başladıktan N sn sonra spark-detect yeniden başlatılır (RT-08 sınaması:
durum korunuyorsa kararlar yine offline ile aynı olmalı; kesinti süresince gecikme doğal olarak
artar).
"""

import argparse
import json
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

import numpy as np

from ingestion.schemas import AlertEvent
from ml import dataset
from ml.baseline_if import count_matrix
from ml.deeplog import session_max_rank, unique_sequences, with_eos
from ml.metrics import prf
from ml.package import load

ROOT = Path(__file__).resolve().parents[1]
K5_P95 = 10.0  # tüzük K5: log -> alarm p95 <= 10 sn


def read_alerts(source: str, idle_s: float = 15) -> list[AlertEvent]:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": f"exp03-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(["alerts"])
    alerts, last = [], time.monotonic()
    while time.monotonic() - last < idle_s:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        alert = AlertEvent.from_json(msg.value())
        if alert.source == source:  # başka koşuların alarmları bekleme süresini uzatmasın
            alerts.append(alert)
            last = time.monotonic()
    consumer.close()
    return alerts


def seconds(a: AlertEvent) -> float:
    t = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))  # noqa: E731
    return (t(a.alert_time) - t(a.event_time)).total_seconds()


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sessions", type=int, default=20_000)
    parser.add_argument("--rate", type=int, default=5000)
    parser.add_argument("--model", default="hdfs-v1")
    parser.add_argument(
        "--wait",
        type=int,
        default=180,
        help="bu kadar sn yeni alarm gelmezse bitti say (> zaman aşımı)",
    )
    parser.add_argument(
        "--restart-after", type=float, help="spark-detect'i bu kadar sn sonra yeniden başlat"
    )
    args = parser.parse_args()

    ds, pkg = dataset.load(), load(ROOT / "data" / "models" / args.model)
    rng = np.random.default_rng(0)
    idx = np.sort(rng.choice(ds.indices("test"), size=args.sessions, replace=False))
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    out = ROOT / "experiments" / "EXP-03" / (stamp + ("-restart" if args.restart_after else ""))
    out.mkdir(parents=True, exist_ok=True)
    (out / "blocks.txt").write_text("\n".join(ds.blocks[idx]) + "\n", encoding="utf-8")
    source = f"exp03-{int(time.time())}"

    print(f"{args.sessions:,} oturum oynatılıyor ({source}, {args.rate} log/sn)", flush=True)
    if args.restart_after:
        restart = ["docker", "compose", "--profile", "hdfs", "restart", "spark-detect"]
        threading.Timer(
            args.restart_after, subprocess.run, [restart], {"cwd": ROOT / "infra", "check": True}
        ).start()
    subprocess.run(
        [sys.executable, "-m", "ingestion.replayer", "--rate", str(args.rate), "--source", source,
         "--blocks", str(out / "blocks.txt")],
        check=True, cwd=ROOT,
    )  # fmt: skip
    print(f"alarmlar okunuyor ({args.wait} sn sessizliğe kadar)", flush=True)
    wanted = {b: i for i, b in enumerate(ds.blocks[idx])}
    # çok bloklu satırlar seçilmemiş bloklara da oturum açar; onların alarmları sayılmaz
    alerts = [a for a in read_alerts(source, args.wait) if a.session_key in wanted]
    duplicates = len(alerts) - len({a.alert_id for a in alerts})  # en az bir kez yazımın tekrarları

    # Canlı kararlar (oturum bazında) ve aynı oturumlarda offline kararlar
    live_dl = np.zeros(len(idx), bool)
    live_nv = np.zeros(len(idx), bool)
    for a in alerts:
        (live_dl if a.kind == "deeplog" else live_nv)[wanted[a.session_key]] = True
    u, _, inv = unique_sequences(with_eos([ds.sequence(i) for i in idx], pkg.config.eos))
    off_dl = session_max_rank(pkg.deeplog, u, pkg.config.h)[inv] >= pkg.config.g
    off_nv = pkg.novelty.predict(count_matrix(ds, idx))
    y = ds.labels[idx]
    early = sorted(seconds(a) for a in alerts if a.kind == "deeplog" and a.event_id != "EOS")

    def pct(q):
        return early[min(len(early) - 1, int(q * (len(early) - 1)))] if early else float("nan")

    result = {
        "source": source,
        "sessions": args.sessions,
        "anomalous": int(y.sum()),
        "rate": args.rate,
        "restart_after_s": args.restart_after,
        "alerts": len(alerts),
        "duplicate_alerts": duplicates,
        "early_latency_s": {
            "n": len(early),
            "p50": pct(0.5),
            "p95": pct(0.95),
            "max": early[-1] if early else None,
        },  # fmt: skip
        "live": {"deeplog": prf(y, live_dl), "novelty": prf(y, live_nv)},
        "offline": {"deeplog": prf(y, off_dl), "novelty": prf(y, off_nv)},
        "agreement": {
            "deeplog": float((live_dl == off_dl).mean()),
            "novelty": float((live_nv == off_nv).mean()),
        },  # fmt: skip
    }
    (out / "results.json").write_text(
        json.dumps(result, indent=2, default=float) + "\n", encoding="utf-8", newline="\n"
    )
    ok = {True: "SAĞLANDI", False: "SAĞLANMADI"}
    lat = result["early_latency_s"]
    lines = [
        "# EXP-03 Özeti: Canlı tespit (spark-detect)",
        "",
        "`ml.exp03` üretir; elle düzenlenmez.",
        "",
        f"- Test bölmesinden {args.sessions:,} oturum ({int(y.sum()):,} anormal) eksiksiz olarak "
        f"{args.rate:,} log/sn ile oynatıldı; {len(alerts):,} alarm üretildi "
        f"(aynı alert_id ile tekrar: {duplicates}).",
        *(
            [
                f"- Oynatma başladıktan {args.restart_after:.0f} sn sonra spark-detect yeniden "
                "başlatıldı (RT-08); kesinti süresindeki alarmların gecikmesi bu yüzden yüksektir."
            ]
            if args.restart_after
            else []
        ),  # fmt: skip
        (
            "- Erken DeepLog alarmı gecikmesi (kesinti dahil; K5 ölçümü değildir): "
            if args.restart_after
            else f"- **K5** (erken DeepLog alarmı gecikmesi p95 ≤ {K5_P95:.0f} sn): "
            f"**{ok[lat['p95'] <= K5_P95]}** "
        )
        + f"(p50 {lat['p50']:.1f} sn, p95 {lat['p95']:.1f} sn, "
        f"max {lat['max']:.1f} sn, n = {lat['n']:,})",
        "",
        "| Karar | Canlı P / R / F1 | Offline P / R / F1 | Canlı = offline (oturum oranı) |",
        "|---|---|---|---|",
    ]
    for kind, name in (("deeplog", "DeepLog + EOS"), ("novelty", "Sayım yeniliği")):
        lv, of = result["live"][kind], result["offline"][kind]
        lines.append(
            f"| {name} | {lv['precision']:.4f} / {lv['recall']:.4f} / {lv['f1']:.4f} "
            f"| {of['precision']:.4f} / {of['recall']:.4f} / {of['f1']:.4f} "
            f"| {result['agreement'][kind]:.4%} |"
        )
    text = "\n".join(lines) + "\n"
    (out / "ozet.md").write_text(text, encoding="utf-8", newline="\n")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
