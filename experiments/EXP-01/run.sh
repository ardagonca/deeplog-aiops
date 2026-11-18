#!/usr/bin/env bash
# EXP-01: alım hızı (K1) ve veri kaybı (K2). Protokol: docs/04-test-stratejisi.md §4.
# Kullanım: bash experiments/EXP-01/run.sh [hızlar] [tekrar] [süre_sn] [başlangıç_tekrarı]
#   varsayılan: "1000 2500 5000 10000" 3 660 1  -> 12 koşu, her biri 11 dk (ilk 1 dk ısınma)
# Yarıda kalan bir deneyi sürdürmek için: EXP01_OUT=<klasör> ile mevcut klasöre yazılır ve
# başlangıç tekrarı verilir; ör. EXP01_OUT=experiments/EXP-01/2026-11-18_1605 bash ... 3 660 3
# Önce altyapı açık olmalı (docker compose up -d --wait). Her koşu kendi kaynak adını kullanır
# (exp01-r<hız>-<tekrar>-<zaman>); ML'in kullanacağı "hdfs-replay" verisine karışmaz.
# Çıktı: <klasör>/{config.txt, results.jsonl, ozet.md, logs/<koşu>.log}
set -euo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash'in /opt/... yollarını Windows yoluna çevirmesini engeller

# pwd -W: Git Bash'te C:/... biçimi; Windows'taki git ve python /c/... yolunu anlamaz.
ROOT=$(cd "$(dirname "$0")/../.." && (pwd -W 2>/dev/null || pwd))
RATES=${1:-"1000 2500 5000 10000"}
REPS=${2:-3}
SECS=${3:-660}
REP_START=${4:-1}
OUT=${EXP01_OUT:-"$ROOT/experiments/EXP-01/$(date +%Y-%m-%d_%H%M)"}
mkdir -p "$OUT/logs"
{
  [ -s "$OUT/config.txt" ] && echo "--- devam: tekrar $REP_START..$REPS ---"
  echo "commit: $(git -C "$ROOT" rev-parse --short HEAD)$(git -C "$ROOT" diff --quiet || echo ' (+commit edilmemiş değişiklik)')"
  echo "hızlar (log/sn): $RATES"
  echo "tekrar: $REP_START..$REPS"
  echo "koşu süresi (sn): $SECS (ilk 60 sn ısınma)"
  echo "ingest tetikleyici: 30 sn, uygulama başına 1 çekirdek / 1 GB executor"
  echo "başlangıç (UTC): $(date -u +%FT%TZ)"
} >>"$OUT/config.txt"

total=$(($(wc -w <<<"$RATES") * (REPS - REP_START + 1)))
i=0
for rep in $(seq "$REP_START" "$REPS"); do # tekrar dışta: zamana bağlı etkiler hızlara eşit dağılsın
  for rate in $RATES; do
    i=$((i + 1))
    src="exp01-r$rate-$rep-$(date +%s)"
    log="$OUT/logs/$src.log"
    echo "[$i/$total] $src: $((rate * SECS)) mesaj, $rate log/sn"
    if ! (cd "$ROOT" && uv run python -m ingestion.replayer \
      --rate "$rate" --limit "$((rate * SECS))" --source "$src") >"$log" 2>&1; then
      echo "HATA: replayer başarısız ($log):"
      tail -20 "$log"
      exit 1
    fi
    sent=$(sed -nE 's/^Gönderilen: ([0-9,]+) mesaj.*/\1/p' "$log" | tr -d ,)
    [ -n "$sent" ] || { echo "HATA: replayer çıktısı okunamadı ($log)"; exit 1; }
    if ! (cd "$ROOT/infra" && docker compose --profile jobs run --rm -T spark-job \
      /opt/deeplog/jobs/exp01_analyze.py --source "$src" --sent "$sent" --rate "$rate") \
      >>"$log" 2>&1; then
      echo "HATA: analiz işi başarısız ($log):"
      grep -E "Error|Exception|Traceback" "$log" | tail -10
      exit 1
    fi
    sed -n 's/^EXP01_RESULT //p' "$log" | tee -a "$OUT/results.jsonl"
  done
done

echo "bitiş (UTC): $(date -u +%FT%TZ)" >>"$OUT/config.txt"
cd "$ROOT" && uv run python -m experiments.exp01_summary "$OUT"
