# Runbook: Altyapı

Bütün komutlar `infra/` dizininde çalıştırılır. Git Bash kullanılıyorsa `/opt/...` yollarının bozulmaması için önce `export MSYS_NO_PATHCONV=1` komutu verilmelidir.

## Başlatma ve durdurma

| İş | Komut |
|---|---|
| Çekirdeği başlat ve sağlıklı olmalarını bekle | `docker compose up -d --wait` (Kafka, HDFS, Spark, ingest, metrik ajanı) |
| İz A'yı (HDFS_v1 tespiti) aç | `docker compose --profile hdfs up -d --wait` |
| İz B'yi (demo sistem) aç | `docker compose stop spark-detect` (açıksa), ardından `docker compose --profile demo up -d --wait` |
| Durumu gör | `docker compose ps -a` |
| Durdur (veri kalır) | `docker compose down` |
| Uçtan uca doğrula | `bash smoke_test.sh` (yaklaşık 1 dakika) |
| Ingest kabul testi | `bash ingest_test.sh` (yaklaşık 3 dakika; 150.000 mesaj, yazarken ingest'i çökertir, kayıp/tekrar denetler) |
| Metrik akışını izle | `docker compose logs -f metrics-agent` ve `docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic metrics.raw` |
| Ingest kayıp/tekrar denetimi | `docker compose --profile jobs run --rm spark-job /opt/deeplog/jobs/verify_ingest.py` |
| EXP-01 deneyi (K1, K2) | Depo kökünde `bash experiments/EXP-01/run.sh` (yaklaşık 2,5–3 saat; 12 koşu). Kısa deneme için: `bash experiments/EXP-01/run.sh "2000" 1 150`. Sonuç `experiments/EXP-01/<tarih>/ozet.md` dosyasına yazılır |
| Arıza testleri | `bash failure_test.sh` (yaklaşık 8 dakika; servisleri bilerek çökertir, çalışan işler varken çalıştırma) |
| Bir servisin loglarını izle | `docker compose logs -f <servis>` |

**Profiller** bellek bütçesi için ayrıdır (Mimari §7): `hdfs` (İz A tespiti), `demo` (demo sistem, yük üreteci, İz B tespiti), `jobs` (tek seferlik Spark işleri). `hdfs` ve `demo` aynı anda açılmaz.

Tek seferlik servisler (`kafka-init`, `hdfs-volume-init`, `hdfs-init`) her açılışta çalışıp `Exited (0)` durumunda kalır; bu normaldir. Bu servislerin işi var olan veriye dokunmadan eksik topic'leri ve dizinleri oluşturmaktır.

## Arayüzler

| Arayüz | Adres |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| Spark master | http://localhost:8080 |
| Spark worker | http://localhost:8081 |
| Ingest işi (Spark UI, Structured Streaming sekmesi) | http://localhost:4040 |
| Kafka (makineden erişim) | `127.0.0.1:9092` |
| Kafka (konteynerler arası) | `kafka:29092` |

## Spark işi çalıştırma

```bash
docker compose --profile jobs run --rm spark-job /opt/deeplog/jobs/<betik>.py <argümanlar>
```

Depo kökü konteynerin `/opt/deeplog` dizinine salt okunur olarak bağlanır ve `PYTHONPATH` bu dizini gösterir. Bu yüzden `jobs/` ve `ingestion/` altındaki değişiklikler için imajı yeniden build etmek gerekmez. Sürekli çalışan `spark-ingest` ise kod değiştiğinde `docker compose restart spark-ingest` ile yeniden başlatılmalıdır; kaldığı yerden checkpoint ile devam eder.

## Sıfırlama

> ⚠️ Aşağıdaki işlemler veri siler ve geri alınamaz.

| İş | Komut |
|---|---|
| Tek bir topic'i boşalt | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --delete --topic <topic>` ve ardından `docker compose up kafka-init` |
| `logs.raw`'ı sıfırla | Önce `docker compose stop spark-ingest`. Sonra topic'i sil ve yeniden oluştur (yukarıdaki satır). Ardından `/checkpoints/ingest_raw/logs`, `/checkpoints/ingest_raw/quarantine`, `/datalake/raw/logs` ve `/datalake/quarantine/logs` dizinlerini sil (`metrics.raw` için karşılıkları: `metrics`, `metrics_quarantine`, `/datalake/raw/metrics`, `/datalake/quarantine/metrics`) ve `docker compose up -d --wait` çalıştır. **Checkpoint silinmezse ingest, olmayan offset'leri bekleyip hata verir (failOnDataLoss).** |
| HDFS'te bir dizini sil | `docker compose exec namenode hdfs dfs -rm -r -skipTrash <yol>` |
| Her şeyi sıfırla (Kafka + HDFS) | `docker compose down -v` ve ardından `docker compose up -d --wait` |

`down -v` komutu yalnızca bu projenin volume'larını siler (`deeplog_kafka_data`, `deeplog_hdfs_namenode`, `deeplog_hdfs_datanode`). Eski prototipten kalan `deeplog_namenode_data` ve `deeplog_datanode_data` volume'ları bu compose dosyasına ait olmadığı için etkilenmez.

## Spark imajını güncelleme

1. Python paketleri değişecekse önce `infra/images/spark/requirements.in` dosyasını düzenle, ardından depo kökünde şunu çalıştır:
   ```bash
   uv pip compile infra/images/spark/requirements.in -o infra/images/spark/requirements.txt \
     --python-version 3.10 --python-platform x86_64-manylinux_2_28 --torch-backend cpu --no-header
   ```
2. Spark sürümü değişecekse:
   - Dockerfile'daki `FROM` satırını, Kafka jar'larının sürümlerini ve `docker-compose.yml`'deki `image:` etiketini birlikte güncelle.
   - Jar sürümlerini şu POM dosyasındaki `kafka.version` ve `commons-pool2.version` alanlarından al: `https://repo1.maven.org/maven2/org/apache/spark/spark-parent_2.13/<sürüm>/spark-parent_2.13-<sürüm>.pom`
3. Son olarak imajı build et ve duman testini çalıştır: `docker compose build spark-master && docker compose up -d --wait && bash smoke_test.sh`

## Tespit job'u (`spark-detect`)

`spark-detect`, `logs.raw`'ı okuyup `alerts` topic'ine alarm yazar ([ADR-0009](../adr/0009-hibrit-tespit.md), [ADR-0010](../adr/0010-tespit-foreachbatch.md)). Arayüzü http://127.0.0.1:4041 adresindedir. Profil `hdfs`; bu bölümdeki `docker compose` komutlarına `--profile hdfs` eklenir. İz B'nin eşi `spark-detect-demo`dur (profil `demo`, arayüz 4042, checkpoint `/checkpoints/detect-demo`, model `deeplog-demo-v1`, kaynak `^demo-`, oturum zaman aşımı 10 sn).

| Ortam değişkeni | Varsayılan | Anlamı |
|---|---|---|
| `DETECT_MODEL` | `hdfs-v1` | HDFS'teki `/models/deeplog/<ad>` paketi |
| `DETECT_TRIGGER` | `2 seconds` | Micro-batch aralığı |
| `DETECT_SESSION_TIMEOUT_SEC` | `120` | Bu süre olay almayan oturum kapanır ve son kararı (EOS, sayım yeniliği) verilir |

- **Alarmları izlemek:** `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alerts`
- **Model sürümünü değiştirmek:** Yeni paket `jobs/publish_model.py` ile HDFS'e yazılır (sürümler değişmez, üzerine yazılmaz). Eski modelin oturum durumu yeni modelle kullanılamaz: önce checkpoint sıfırlanır (aşağıda). Ardından `infra/` dizininde `DETECT_MODEL=<yeni> docker compose up -d spark-detect` çalıştırılır (değişken `.env` dosyasına da yazılabilir). Çalışırken model değiştirme yoktur.
- **Kod değişikliği:** Depo kökü bu servise bağlı olduğu için `docker compose restart spark-detect` yeterlidir. İlk çalıştırmada `latest` offset'ten başlar; sonraki başlatmalarda checkpoint'teki offset'ten devam eder. Açık oturumların durumu her batch'ten sonra `/checkpoints/detect/detector_state/` altına yazılır ve başlarken yüklenir (RT-08; günlükte `durum yüklendi: batch N, M açık oturum`). Durum başka bir model sürümüne aitse iş başlamaz; model değiştirirken checkpoint sıfırlanmalıdır.
- **Checkpoint'i sıfırlamak:** `docker compose stop spark-detect`, ardından `docker compose exec namenode hdfs dfs -rm -r -f /checkpoints/detect` ve `docker compose up -d spark-detect`. Oturum durumu checkpoint'in içinde olduğu için onunla birlikte silinir; ikisi ayrı ayrı silinmemelidir.
- **Gecikme ve tutarlılık ölçümü:** `uv run --group ml python -m ml.exp03` (EXP-03). Önce yığın açık ve model paketi `data/models/<ad>` altında yerelde de bulunmalıdır.

## Demo hedef sistem (`demo-app`, İz B)

[ADR-0011](../adr/0011-demo-sistem.md). Profil `demo` ile açılır; adres http://127.0.0.1:8100 (`DEMO_PORT` ile değişir).

```bash
docker compose --profile demo up -d --wait demo-app
curl -s http://127.0.0.1:8100/health          # status, disk_used_pct, mem_bytes, mem_limit_bytes
curl -s -X POST -H "X-Request-ID: deneme-1" -d '{"a":1}' http://127.0.0.1:8100/api/orders/5
```

- Loglar `logs.raw`'a `source = demo-app`, `session_key = X-Request-ID` ile gider. `/health` ve `/fault/*` log üretmez.
- Veri ve geçici dizinler 64 MB'lık tmpfs'tedir (`/app/var`); konteyner silinince içerik de gider.
- **Arıza uçları** (`FAULTS_ENABLED=true` iken; normalde enjektör kullanır, REM-02): `POST /fault/fill?mb_per_s=1` (disk doldurma), `POST /fault/leak?mb_per_s=4` (bellek sızıntısı), `POST /fault/hang` (yanıt vermeme), `POST /fault/clear` (enjeksiyonu durdurur; çöp dosyaları silmez).
- **Arıza enjektörü** (REM-02): `uv run python -m demo_target.inject disk|leak|kill|hang [--out kayit.jsonl]`. Önce servisin sağlıklı olduğunu doğrular, arızayı enjekte eder ve sentetik isteğin (probe) ilk başarısız olduğu anı `failure_time` olarak yazar. `kill` için Docker CLI gerekir.
- Elle temizlik: `docker compose exec demo-app sh -c 'rm -f /app/var/tmp/junk-*.bin'`.
- `restart: "no"` bilinçlidir: çöken servisi Remediator kaldırır. Elle kaldırmak için `docker compose --profile demo up -d demo-app`.

## Çökme davranışı

Uzun çalışan servisler `restart: on-failure` ayarıyla çalışır:

- Bir servisin süreci çökerse (sıfır olmayan çıkış kodu, örneğin OOM veya SIGKILL) Docker o servisi kendiliğinden yeniden başlatır.
- `docker compose stop`, `kill` veya `down` ile elle durdurulan servisler ise yeniden başlatılmaz.

Kafka ve Spark konteynerleri `init: true` ile çalışır. Bu ayarla PID 1 Java değil, küçük bir init sürecidir (tini). Böylece sinyaller doğru iletilir ve zombi süreçler toplanır.

Bir servisin kaç kez yeniden başladığını görmek için:

```bash
docker inspect deeplog-<servis>-1 --format '{{.RestartCount}}'
```

## Sık görülen sorunlar

| Belirti | Neden | Çözüm |
|---|---|---|
| `spark-ingest` sürekli yeniden başlıyor; günlükte "Some data may have been lost because they are not available in Kafka any more" | Yığın 24 saatten uzun kapalı kaldı; tüketilmemiş kayıtlar Kafka'nın saklama süresi dolunca silindi | Metrik akışı bu durumda uyarıyla devam eder (`failOnDataLoss=false`). Durursa log akışıdır (risk R-14): kayıp aralığı `verify_ingest` ile raporla, sonra karar ver. Önlem: yığını kapatmadan önce ingest'in birikimi bitirmesini bekle |
| NameNode `healthy` olmuyor | Volume izinleri | `docker compose up hdfs-volume-init` çıktısına bak |
| `spark-job` HDFS'e yazamıyor, `Connection refused` | DataNode henüz kayıt olmamış | `docker compose exec namenode hdfs dfsadmin -report` ile canlı DataNode sayısına bak |
| Executor'lar sürücüye bağlanamıyor | `spark.driver.host` yanlış | `spark-job` servisinin entrypoint'i IP'yi `hostname -i` ile verir; servis tanımı değiştiyse kontrol et |
| Docker belleği yetmiyor | WSL2 sınırı | `%USERPROFILE%\.wslconfig` dosyasında `memory=12GB` olmalı; ardından `wsl --shutdown` |
