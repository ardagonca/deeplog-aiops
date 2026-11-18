#!/usr/bin/env bash
# ING-04 kabul testi: yazma sürerken ingest job'u çökertilir; sonra kayıp ve tekrar denetlenir.
#   1. Replayer, N mesajı logs.raw'a basar (varsayılan 150.000; 5.000 log/sn ile 30 sn)
#   2. Aynı anda sözleşmeye uymayan 3 mesaj gönderilir (karantinaya düşmeli)
#   3. Akış sürerken spark-ingest'in sürücüsü SIGKILL ile öldürülür; Docker yeniden başlatır
#   4. jobs/verify_ingest.py: Kafka'daki her kayıt HDFS'te (raw + karantina) tam bir kez olmalı
# Önce altyapı açık olmalı: docker compose up -d --wait
set -euo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash'in /opt/... yollarını Windows yoluna çevirmesini engeller
cd "$(dirname "$0")"

N=${1:-150000}
CID=$(docker compose ps -q spark-ingest)
restarts() { docker inspect "$CID" --format '{{.RestartCount}}'; }
BEFORE=$(restarts)
START=$SECONDS

echo "1) $N mesaj logs.raw'a gönderiliyor (arka planda)"
(cd .. && uv run python -m ingestion.replayer --rate 5000 --limit "$N" --source ingest-test >/dev/null) &
REPLAYER=$!

echo "2) Sözleşmeye uymayan 3 mesaj gönderiliyor"
printf '%s\n' 'bu JSON degil' '{"id": "eksik-alanlar"}' \
  '{"id":"x","source":"s","event_time":"2026-11-18T10:00:00Z","message":"m","session_key":null,"level":null,"label":7,"schema_version":1}' |
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic logs.raw

echo "3) 40 sn sonra ingest sürücüsü SIGKILL ile öldürülecek"
sleep 40
docker compose exec -T spark-ingest pkill -9 -o java
wait "$REPLAYER"
for _ in $(seq 1 90); do
  [ "$(restarts)" -gt "$BEFORE" ] && [ "$(docker inspect "$CID" --format '{{.State.Health.Status}}')" = healthy ] && break
  sleep 2
done
[ "$(restarts)" -gt "$BEFORE" ] || { echo "HATA: spark-ingest yeniden başlamadı"; exit 1; }
echo "   spark-ingest yeniden başladı (restart #$(restarts)), checkpoint'ten devam ediyor"

echo "4) Kayıp/tekrar denetimi (ingest yetişene kadar bekler)"
docker compose --profile jobs run --rm -T spark-job /opt/deeplog/jobs/verify_ingest.py --timeout 300 2>&1 |
  grep -E "^\[denetim\]|INGEST OK|AssertionError" || { echo "INGEST TESTİ BAŞARISIZ"; exit 1; }
echo "Ingest testi geçti: $((SECONDS - START)) sn"
