"""EXP-01 özeti: results.jsonl -> ozet.md (K1, K2 kararıyla birlikte).

Kullanım: uv run python -m experiments.exp01_summary experiments/EXP-01/<tarih>
Hedefler docs/00-proje-tuzugu.md §4'ten: K1 >= 5.000 log/sn 10 dk kesintisiz, K2 = %0 kayıp.
"""

import json
import statistics
import sys
from pathlib import Path

K1_RATE = 5000


def summarize(results: list[dict]) -> dict:
    by_rate: dict[int, list[dict]] = {}
    for r in results:
        by_rate.setdefault(r["rate_target"], []).append(r)
    rows = []
    for rate, runs in sorted(by_rate.items()):
        ingest = [r["ingest_rate"] for r in runs]
        rows.append(
            {
                "rate": rate,
                "runs": len(runs),
                "ingest_mean": statistics.mean(ingest),
                "ingest_std": statistics.stdev(ingest) if len(ingest) > 1 else 0.0,
                "input_mean": statistics.mean(r["input_rate"] for r in runs),
                "lag_p95_max": max(r["lag_p95"] for r in runs),
                "sent": sum(r["sent"] for r in runs),
                "lost": sum(r["lost"] for r in runs),
                "duplicates": sum(r["duplicates"] for r in runs),
                "keeps_up": all(r["keeps_up"] for r in runs),
            }
        )
    k1_runs = by_rate.get(K1_RATE, [])
    # K1: hedef hızdaki her koşuda ingest yetişti ve kayıpsız yazdı. O hızda koşu yoksa None.
    k1 = all(r["keeps_up"] and r["lost"] == 0 for r in k1_runs) if k1_runs else None
    k2 = bool(results) and all(r["lost"] == 0 and r["duplicates"] == 0 for r in results)
    return {"rows": rows, "k1": k1, "k2": k2}


def to_markdown(summary: dict, config: str) -> str:
    verdict = {True: "SAĞLANDI", False: "SAĞLANMADI", None: "ÖLÇÜLMEDİ"}
    lines = [
        "# EXP-01 Özeti: Alım hızı ve veri kaybı",
        "",
        "Bu dosya `experiments.exp01_summary` tarafından üretilir; elle düzenlenmez.",
        "",
        "## Sonuç",
        "",
        f"- **K1** (≥ {K1_RATE:,} log/sn, 10 dk kesintisiz): **{verdict[summary['k1']]}**",
        f"- **K2** (%0 veri kaybı, tekrar yok): **{verdict[summary['k2']]}**",
        "",
        "## Hız başına sonuçlar",
        "",
        "| Hedef (log/sn) | Koşu | Gönderim hızı | Ingest hızı (ort. ± std) "
        "| p95 gecikme, en kötü (sn) | Gönderilen | Kayıp | Tekrar | Yetişti mi |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in summary["rows"]:
        lines.append(
            f"| {r['rate']:,} | {r['runs']} | {r['input_mean']:,.0f} "
            f"| {r['ingest_mean']:,.0f} ± {r['ingest_std']:,.0f} | {r['lag_p95_max']:.1f} "
            f"| {r['sent']:,} | {r['lost']:,} | {r['duplicates']:,} "
            f"| {'evet' if r['keeps_up'] else 'hayır'} |"
        )
    lines += ["", "## Koşu ayarları", "", "```", config.strip(), "```", ""]
    return "\n".join(lines)


def main(run_dir: str) -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    path = Path(run_dir)
    lines = (path / "results.jsonl").read_text(encoding="utf-8").splitlines()
    results = [json.loads(line) for line in lines if line.strip()]
    summary = summarize(results)
    report = to_markdown(summary, (path / "config.txt").read_text(encoding="utf-8"))
    (path / "ozet.md").write_text(report, encoding="utf-8")
    print(report)
    return 0 if summary["k1"] is True and summary["k2"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
