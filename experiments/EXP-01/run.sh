#!/usr/bin/env bash
# EXP-01: alım hızı (K1) ve veri kaybı (K2). Protokol: docs/04-test-stratejisi.md §4.
# Kullanım: bash experiments/EXP-01/run.sh [hızlar] [tekrar] [süre_sn]
#   varsayılan: "1000 2500 5000 10000" 3 660  -> 12 koşu, her biri 11 dk (ilk 1 dk ısınma)
# Önce altyapı açık olmalı (docker compose up -d --wait). Her koşu kendi kaynak adını kullanır
# (exp01-r<hız>-<tekrar>-<zaman>); ML'in kullanacağı "hdfs-replay" verisine karışmaz.
# Çıktı: experiments/EXP-01/<tarih>/{config.txt, results.jsonl, ozet.md}
set -euo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash'in /opt/... yollarını Windows yoluna çevirmesini engeller

# pwd -W: Git Bash'te C:/... biçimi; Windows'taki git ve python /c/... yolunu anlamaz.
ROOT=$(cd "$(dirname "$0")/../.." && (pwd -W 2>/dev/null || pwd))
RATES=${1:-"1000 2500 5000 10000"}
REPS=${2:-3}
SECS=${3:-660}
OUT="$ROOT/experiments/EXP-01/$(date +%Y-%m-%d_%H%M)"
mkdir -p "$OUT"
{
  echo "commit: $(git -C "$ROOT" rev-parse --short HEAD)$(git -C "$ROOT" diff --quiet || echo ' (+commit edilmemiş değişiklik)')"
  echo "hızlar (log/sn): $RATES"
  echo "tekrar: $REPS"
  echo "koşu süresi (sn): $SECS (ilk 60 sn ısınma)"
  echo "ingest tetikleyici: 30 sn, uygulama başına 1 çekirdek / 1 GB executor"
  echo "başlangıç (UTC): $(date -u +%FT%TZ)"
} >"$OUT/config.txt"

total=$(($(wc -w <<<"$RATES") * REPS))
i=0
for rep in $(seq 1 "$REPS"); do # tekrar dışta: zamana bağlı etkiler hızlara eşit dağılsın
  for rate in $RATES; do
    i=$((i + 1))
    src="exp01-r$rate-$rep-$(date +%s)"
    echo "[$i/$total] $src: $((rate * SECS)) mesaj, $rate log/sn"
    replay=$(cd "$ROOT" && uv run python -m ingestion.replayer \
      --rate "$rate" --limit "$((rate * SECS))" --source "$src" 2>&1)
    sent=$(sed -nE 's/^Gönderilen: ([0-9,]+) mesaj.*/\1/p' <<<"$replay" | tr -d ,)
    [ -n "$sent" ] || { echo "replayer çıktısı okunamadı:"; echo "$replay"; exit 1; }
    (cd "$ROOT/infra" && docker compose --profile jobs run --rm -T spark-job \
      /opt/deeplog/jobs/exp01_analyze.py --source "$src" --sent "$sent" --rate "$rate" 2>&1) |
      sed -n 's/^EXP01_RESULT //p' | tee -a "$OUT/results.jsonl"
  done
done

echo "bitiş (UTC): $(date -u +%FT%TZ)" >>"$OUT/config.txt"
cd "$ROOT" && uv run python -m experiments.exp01_summary "$OUT"
