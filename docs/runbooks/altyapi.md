# Runbook: Altyapı

Bütün komutlar `infra/` dizininde çalıştırılır. Git Bash kullanılıyorsa `/opt/...` yollarının bozulmaması için önce `export MSYS_NO_PATHCONV=1` komutu verilmelidir.

## Başlatma ve durdurma

| İş | Komut |
|---|---|
| Hepsini başlat ve sağlıklı olmalarını bekle | `docker compose up -d --wait` |
| Durumu gör | `docker compose ps -a` |
| Durdur (veri kalır) | `docker compose down` |
| Uçtan uca doğrula | `bash smoke_test.sh` (yaklaşık 1 dakika) |
| Arıza testleri | `bash failure_test.sh` (yaklaşık 8 dakika; servisleri bilerek çökertir, çalışan işler varken çalıştırma) |
| Bir servisin loglarını izle | `docker compose logs -f <servis>` |

Tek seferlik servisler (`kafka-init`, `hdfs-volume-init`, `hdfs-init`) her açılışta çalışıp `Exited (0)` durumunda kalır; bu normaldir. Bu servislerin işi var olan veriye dokunmadan eksik topic'leri ve dizinleri oluşturmaktır.

## Arayüzler

| Arayüz | Adres |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| Spark master | http://localhost:8080 |
| Spark worker | http://localhost:8081 |
| Kafka (makineden erişim) | `127.0.0.1:9092` |
| Kafka (konteynerler arası) | `kafka:29092` |

## Spark işi çalıştırma

```bash
docker compose --profile jobs run --rm spark-job /opt/deeplog/jobs/<betik>.py <argümanlar>
```

Depodaki `jobs/` klasörü konteynere salt okunur olarak bağlanır, bu yüzden betikteki değişiklik için imajı yeniden build etmek gerekmez.

## Sıfırlama

> ⚠️ Aşağıdaki işlemler veri siler ve geri alınamaz.

| İş | Komut |
|---|---|
| Tek bir topic'i boşalt | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --delete --topic <topic>` ve ardından `docker compose up kafka-init` |
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
