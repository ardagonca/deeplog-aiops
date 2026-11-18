#!/usr/bin/env bash
# INF-09 altyapı arıza testleri. Önce altyapı açık olmalı: docker compose up -d --wait
# Senaryolar (docs/04-test-stratejisi.md §1):
#   T1  Süreç çökmesi (SIGKILL) -> Docker otomatik yeniden başlatır, servis sağlıklı döner
#   T2  Tüm servisler aynı anda SIGKILL (ani kapanma) -> veri kaybı yok, sistem geri gelir
#   T3  DataNode yokken HDFS yazma takılmadan hata verir; DataNode dönünce yazma çalışır
#   T4  Spark worker kapalıyken gönderilen iş worker'ı otomatik başlatır ve tamamlanır
#   T5  Son duman testi
# Her adımdan sonra işaret verisi (Kafka'da N mesaj, HDFS'te bir dosya) kontrol edilir.
# Süre ~8 dk. Geçici topic ve HDFS dizini kullanır, sonunda (hata olsa da) siler.
set -uo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash'in /opt/... yollarını Windows yoluna çevirmesini engeller
cd "$(dirname "$0")"

RUN="failtest-$(date +%s)"
N=500
MARKER="/checkpoints/$RUN/marker.txt"
TOPICS=/opt/kafka/bin/kafka-topics.sh
RESULTS=()
FAILED=0

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }

record() { # record <id> <PASS|FAIL> <açıklama>
  RESULTS+=("$(printf '%-7s %-4s  %s' "$1" "$2" "$3")")
  [ "$2" = PASS ] || FAILED=1
  log "$1 $2: $3"
}

cid() { docker compose ps -aq "$1"; }
state() { docker inspect "$(cid "$1")" --format '{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{end}}'; }
restarts() { docker inspect "$(cid "$1")" --format '{{.RestartCount}}'; }

wait_healthy() { # wait_healthy <servis> <saniye>
  local t0=$SECONDS
  while ((SECONDS - t0 < $2)); do
    [ "$(state "$1")" = running/healthy ] && return 0
    sleep 2
  done
  return 1
}

hdfs_ready() { # safe mode kapalı ve en az bir canlı DataNode var
  local t0=$SECONDS
  while ((SECONDS - t0 < $1)); do
    docker compose exec -T namenode bash -c \
      'hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF &&
       hdfs dfsadmin -report -live 2>/dev/null | grep -q "Live datanodes ([1-9]"' && return 0
    sleep 3
  done
  return 1
}

worker_registered() { # Spark master en az bir canlı worker görüyor
  local t0=$SECONDS
  while ((SECONDS - t0 < $1)); do
    curl -s http://localhost:8080/json/ | grep -Eq '"aliveworkers" *: *[1-9]' && return 0
    sleep 2
  done
  return 1
}

verify_data() { # verify_data <id>: işaret verisi kayıpsız mı?
  local count marker
  count=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:29092 --topic "$RUN" 2>/dev/null | awk -F: '{s += $3} END {print s + 0}')
  marker=$(docker compose exec -T namenode hdfs dfs -cat "$MARKER" 2>/dev/null)
  if [ "$count" = "$N" ] && [ "$marker" = "$RUN" ]; then
    record "$1" PASS "veri sağlam: Kafka $count/$N mesaj, HDFS işaret dosyası okunuyor"
  else
    record "$1" FAIL "veri kaybı: Kafka $count/$N mesaj, HDFS işaret='${marker:-okunamadı}'"
  fi
}

crash_and_recover() { # crash_and_recover <servis>: ana Java sürecini SIGKILL ile öldürür
  local svc=$1 before t0
  before=$(restarts "$svc")
  t0=$SECONDS
  docker compose exec -T "$svc" pkill -9 -o java >/dev/null 2>&1
  while ((SECONDS - t0 < 120)) && [ "$(restarts "$svc")" -le "$before" ]; do sleep 1; done
  if [ "$(restarts "$svc")" -le "$before" ]; then
    record T1 FAIL "$svc çöktü ama Docker yeniden başlatmadı ($(state "$svc"))"
    return
  fi
  if ! wait_healthy "$svc" 120; then
    record T1 FAIL "$svc yeniden başladı ama 120 sn içinde sağlıklı olmadı ($(state "$svc"))"
    return
  fi
  case "$svc" in
    namenode | datanode) hdfs_ready 120 || { record T1 FAIL "$svc döndü ama HDFS hazır değil"; return; } ;;
    spark-master | spark-worker) worker_registered 90 || { record T1 FAIL "$svc döndü ama worker kayıtlı değil"; return; } ;;
  esac
  record T1 PASS "$svc çöktü (SIGKILL) -> otomatik yeniden başladı, $((SECONDS - t0)) sn'de tekrar hizmette"
}

cleanup() {
  docker compose exec -T kafka "$TOPICS" --bootstrap-server kafka:29092 --delete --topic "$RUN" >/dev/null 2>&1 || true
  docker compose exec -T namenode hdfs dfs -rm -r -f -skipTrash "/checkpoints/$RUN" >/dev/null 2>&1 || true
}
trap cleanup EXIT

START=$SECONDS
log "Hazırlık: $N mesaj -> Kafka '$RUN', işaret dosyası -> HDFS $MARKER"
docker compose exec -T kafka "$TOPICS" --bootstrap-server kafka:29092 --create --topic "$RUN" \
  --partitions 1 --replication-factor 1 >/dev/null
seq 1 "$N" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:29092 --topic "$RUN" --command-property acks=all
docker compose exec -T namenode bash -c \
  "hdfs dfs -mkdir -p /checkpoints/$RUN && echo $RUN | hdfs dfs -put -f - $MARKER" 2>/dev/null
verify_data HAZIRLIK

log "T1: her servis tek tek çökertiliyor"
for svc in kafka namenode datanode spark-master spark-worker spark-ingest; do crash_and_recover "$svc"; done
verify_data T1-veri

log "T2: tüm servisler aynı anda SIGKILL (ani kapanma)"
docker compose kill >/dev/null 2>&1
t0=$SECONDS
if ! up_out=$(docker compose up -d --wait 2>&1); then
  record T2 FAIL "docker compose up --wait başarısız: $(echo "$up_out" | grep -viE 'running|started|healthy|waiting|created' | tail -1)"
elif ! hdfs_ready 120; then
  record T2 FAIL "sistem açıldı ama HDFS 120 sn içinde hazır olmadı"
elif ! worker_registered 90; then
  record T2 FAIL "sistem açıldı ama Spark worker 90 sn içinde kayıt olmadı"
else
  record T2 PASS "tüm sistem ani kapanmadan $((SECONDS - t0)) sn'de geri geldi"
fi
verify_data T2-veri

log "T3: DataNode durduruldu, HDFS'e yazma deneniyor"
docker compose stop datanode >/dev/null 2>&1
t0=$SECONDS
if docker compose exec -T namenode bash -c \
  "echo x | timeout 150 hdfs dfs -put -f - /checkpoints/$RUN/t3.txt" >/dev/null 2>&1; then
  record T3 FAIL "DataNode yokken yazma başarılı göründü (beklenmiyordu)"
elif ((SECONDS - t0 < 150)); then
  record T3 PASS "DataNode yokken yazma $((SECONDS - t0)) sn'de hata verdi (takılmadı)"
else
  record T3 FAIL "DataNode yokken yazma 150 sn zaman aşımına kadar takıldı"
fi
t0=$SECONDS
if docker compose start datanode >/dev/null 2>&1 && wait_healthy datanode 120 && hdfs_ready 120 &&
  docker compose exec -T namenode bash -c \
    "echo y | hdfs dfs -put -f - /checkpoints/$RUN/t3.txt" >/dev/null 2>&1; then
  record T3 PASS "DataNode geri geldi, $((SECONDS - t0)) sn sonra yazma yeniden çalışıyor"
else
  record T3 FAIL "DataNode geri gelince yazma çalışmadı"
fi
verify_data T3-veri

log "T4: Spark worker durduruldu, iş gönderiliyor"
docker compose stop spark-worker >/dev/null 2>&1
t0=$SECONDS
if bash smoke_test.sh >/dev/null 2>&1 && [ "$(state spark-worker)" = running/healthy ]; then
  record T4 PASS "iş, kapalı worker'ı otomatik başlattı ve $((SECONDS - t0)) sn'de tamamlandı"
else
  record T4 FAIL "worker kapalıyken gönderilen iş tamamlanmadı"
fi

log "T5: son duman testi"
if bash smoke_test.sh >/dev/null 2>&1; then record T5 PASS "duman testi geçti"; else record T5 FAIL "duman testi başarısız"; fi

echo
echo "=================== ARIZA TESTİ ÖZETİ ($((SECONDS - START)) sn) ==================="
printf '%s\n' "${RESULTS[@]}"
echo "=========================================================================="
[ "$FAILED" -eq 0 ] && echo "SONUÇ: TÜM SENARYOLAR GEÇTİ" || echo "SONUÇ: BAŞARISIZ SENARYO VAR"
exit "$FAILED"
