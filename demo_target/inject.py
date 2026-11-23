"""REM-02: Demo sisteme arıza enjeksiyonu ve arıza anının (failure_time) kaydı (ADR-0011).

Kullanım: uv run python -m demo_target.inject disk|leak|kill|hang [--out kayit.jsonl]

Arıza anı, sentetik bir isteğin (probe: GET /api/report, saniyede bir) ilk başarısız olduğu andır:
- disk: yazma hatası (507), leak: OOM sonrası bağlantı reddi, kill: bağlantı reddi,
  hang: zaman aşımı. Kademeli senaryolarda enjeksiyon ile arıza arasında zaman geçer; öngörü süresi
  (K9) = failure_time - ilk alarm zamanı (EXP-04).
Kayıt bir JSON satırıdır: senaryo, tür, enjeksiyon ve arıza zamanı (UTC ISO), parametreler.
Probe istekleri normal trafikteki rapor istekleriyle aynıdır; kimlikleri "probe-" ile başlar.
"""

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

CONTAINER = "deeplog-demo-app-1"
SCENARIOS = {
    # ad: (tür, varsayılan hız)
    "disk": ("gradual", 1.0),  # MB/sn; 64 MB tmpfs ~1 dk'da dolar, %80 uyarısı ~13 sn önce
    "leak": ("gradual", 4.0),  # MB/sn; 384 MB sınırına ~1,5 dk
    "kill": ("sudden", None),
    "hang": ("sudden", None),
}


def iso(t: float) -> str:
    return (
        datetime.fromtimestamp(t, timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _post(url: str) -> None:
    urllib.request.urlopen(urllib.request.Request(url, data=b"", method="POST"), timeout=5).close()


def probe(base: str, timeout: float = 2.0) -> bool:
    """Servis gerçek bir isteği yanıtlayabiliyor mu (2xx)?"""
    rid = f"probe-{uuid.uuid4().hex[:12]}"
    req = urllib.request.Request(f"{base}/api/report?rows=10", headers={"X-Request-ID": rid})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, OSError):  # HTTPError (507) de URLError'dır
        return False


def wait_failure(
    check: Callable[[], bool],
    timeout: float,
    interval: float = 1.0,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> float | None:
    """check() ilk kez False döndüğünde o anın zamanı; timeout içinde olmazsa None."""
    deadline = clock() + timeout
    while clock() < deadline:
        started = clock()
        if not check():
            return started  # isteğin gönderildiği an: zaman aşımı süresi arızaya eklenmez
        sleep(max(0.0, interval - (clock() - started)))
    return None


def inject(scenario: str, base: str, rate: float | None) -> None:
    if scenario == "disk":
        _post(f"{base}/fault/fill?mb_per_s={rate}")
    elif scenario == "leak":
        _post(f"{base}/fault/leak?mb_per_s={rate}")
    elif scenario == "hang":
        _post(f"{base}/fault/hang")
    elif scenario == "kill":
        subprocess.run(["docker", "kill", "--signal", "KILL", CONTAINER], check=True,
                       capture_output=True)  # fmt: skip
    else:
        raise ValueError(scenario)


def main(argv: list[str] | None = None) -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--url", default="http://127.0.0.1:8100")
    parser.add_argument("--rate", type=float, help="kademeli senaryolarda MB/sn")
    parser.add_argument("--timeout", type=float, default=300, help="arıza bu sürede olmazsa dur")
    parser.add_argument("--out", type=Path, help="kaydın ekleneceği JSONL dosyası")
    args = parser.parse_args(argv)

    kind, default_rate = SCENARIOS[args.scenario]
    rate = args.rate if args.rate is not None else default_rate
    if not probe(args.url):
        print(f"{args.url} sağlıklı değil; enjeksiyon yapılmadı", file=sys.stderr)
        return 1
    injected = time.time()
    inject(args.scenario, args.url, rate)
    failed = wait_failure(lambda: probe(args.url), args.timeout)
    record = {
        "scenario": args.scenario,
        "kind": kind,
        "inject_time": iso(injected),
        "failure_time": iso(failed) if failed is not None else None,
        "rate_mb_per_s": rate,
    }
    line = json.dumps(record, ensure_ascii=False)
    print(line)
    if args.out:
        with args.out.open("a", encoding="utf-8", newline="\n") as f:
            f.write(line + "\n")
    return 0 if failed is not None else 2


if __name__ == "__main__":
    sys.exit(main())
