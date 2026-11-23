# Runbook: Altyapı

Bütün komutlar `infra/` dizininde çalıştırılır. Git Bash kullanılıyorsa `/opt/...` yollarının bozulmaması için önce `export MSYS_NO_PATHCONV=1` komutu verilmelidir.

## Başlatma ve durdurma

| İş | Komut |
|---|---|
| Hepsini başlat ve sağlıklı olmalarını bekle | `docker compose up -d --wait` |
| Durumu gör | `docker compose ps -a` |
| Durdur (veri kalır) | `docker compose down` |
| Uçtan uca doğrula | `bash smoke_test.sh` (yaklaşık 1 dakika) |
| Ingest kabul testi | `bash ingest_test.sh` (yaklaşık 3 dakika; 150.000 mesaj, yazarken ingest'i çökertir, kayıp/tekrar denetler) |
| Metrik akışını izle | `docker compose logs -f metrics-agent` ve `docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic metrics.raw` |
| Ingest kayıp/tekrar denetimi | `docker compose --profile jobs run --rm spark-job /opt/deeplog/jobs/verify_ingest.py` |
| EXP-01 deneyi (K1, K2) | Depo kökünde `bash experiments/EXP-01/run.sh` (yaklaşık 2,5–3 saat; 12 koşu). Kısa deneme için: `bash experiments/EXP-01/run.sh "2000" 1 150`. Sonuç `experiments/EXP-01/<tarih>/ozet.md` dosyasına yazılır |
| Arıza testleri | `bash failure_test.sh` (yaklaşık 8 dakika; servisleri bilerek çökertir, çalışan işler varken çalıştırma) |
| Bir servisin loglarını izle | `docker compose logs -f <servis>` |

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

`spark-detect`, `logs.raw`'ı okuyup `alerts` topic'ine alarm yazar ([ADR-0009](../adr/0009-hibrit-tespit.md), [ADR-0010](../adr/0010-tespit-foreachbatch.md)). Arayüzü http://127.0.0.1:4041 adresindedir.

| Ortam değişkeni | Varsayılan | Anlamı |
|---|---|---|
| `DETECT_MODEL` | `hdfs-v1` | HDFS'teki `/models/deeplog/<ad>` paketi |
| `DETECT_TRIGGER` | `2 seconds` | Micro-batch aralığı |
| `DETECT_SESSION_TIMEOUT_SEC` | `120` | Bu süre olay almayan oturum kapanır ve son kararı (EOS, sayım yeniliği) verilir |

- **Alarmları izlemek:** `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alerts`
- **Model sürümünü değiştirmek:** Yeni paket `jobs/publish_model.py` ile HDFS'e yazılır (sürümler değişmez, üzerine yazılmaz). Ardından `infra/` dizininde `DETECT_MODEL=<yeni> docker compose up -d spark-detect` çalıştırılır (değişken `.env` dosyasına da yazılabilir). Çalışırken model değiştirme yoktur.
- **Kod değişikliği:** Depo kökü bu servise bağlı olduğu için `docker compose restart spark-detect` yeterlidir. Job `latest` offset'ten başlar ve checkpoint'ten devam eder; açık oturumların durumu bellekte olduğundan yeniden başlatmada kaybolur (RT-08).
- **Checkpoint'i sıfırlamak:** `docker compose stop spark-detect`, ardından `docker compose exec namenode hdfs dfs -rm -r -f /checkpoints/detect` ve `docker compose up -d spark-detect`.
- **Gecikme ve tutarlılık ölçümü:** `uv run --group ml python -m ml.exp03` (EXP-03). Önce yığın açık ve model paketi `data/models/<ad>` altında yerelde de bulunmalıdır.

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
| NameNode `healthy` olmuyor | Volume izinleri | `docker compose up hdfs-volume-init` çıktısına bak |
| `spark-job` HDFS'e yazamıyor, `Connection refused` | DataNode henüz kayıt olmamış | `docker compose exec namenode hdfs dfsadmin -report` ile canlı DataNode sayısına bak |
| Executor'lar sürücüye bağlanamıyor | `spark.driver.host` yanlış | `spark-job` servisinin entrypoint'i IP'yi `hostname -i` ile verir; servis tanımı değiştiyse kontrol et |
| Docker belleği yetmiyor | WSL2 sınırı | `%USERPROFILE%\.wslconfig` dosyasında `memory=12GB` olmalı; ardından `wsl --shutdown` |
