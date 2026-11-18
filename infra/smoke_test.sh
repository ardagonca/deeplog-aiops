#!/usr/bin/env bash
# INF-06 uçtan uca duman testi: Kafka -> Spark -> HDFS.
# Önce altyapı açık olmalı: docker compose up -d --wait
# Geçici bir topic ve HDFS dizini kullanır, sonunda (hata olsa da) ikisini de siler.
set -euo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash'in /opt/... yollarını Windows yoluna çevirmesini engeller
cd "$(dirname "$0")"

RUN="smoke-$(date +%s)"
N=1000
TOPICS=/opt/kafka/bin/kafka-topics.sh
START=$(date +%s)

cleanup() {
  docker compose exec -T kafka "$TOPICS" --bootstrap-server kafka:29092 --delete --topic "$RUN" >/dev/null 2>&1 || true
  docker compose exec -T namenode hdfs dfs -rm -r -f -skipTrash "/checkpoints/$RUN" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose exec -T kafka "$TOPICS" --bootstrap-server kafka:29092 --create --topic "$RUN" \
  --partitions 3 --replication-factor 1 >/dev/null
seq 1 "$N" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:29092 --topic "$RUN"
docker compose --profile jobs run --rm -T spark-job /opt/deeplog/jobs/smoke_test.py "$RUN" "$N" "/checkpoints/$RUN" \
  2>&1 | grep -E "SMOKE OK|Error|Exception|AssertionError" || { echo "DUMAN TESTİ BAŞARISIZ"; exit 1; }

echo "Duman testi geçti: $(($(date +%s) - START)) sn"
