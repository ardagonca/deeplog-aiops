# Backlog

Bu dosya projenin backlog'udur (18.11.2026'da resmi takvime göre yeniden planlandı; sprint tarihleri [Yol Haritası §3](02-yol-haritasi.md#3-sprint-takvimi)'te). İş takibi GitHub Issues ve Projects üzerinden yapılır (GOV-03); her iş kalemi bir issue'ya dönüşür ve ilgili kilometre taşına bağlanır.

- **Tahmin:** saat cinsinden. 12 saati aşan iş kalemi bölünür.
- **Öncelik (MoSCoW):** Must = olmazsa KPI veya teslimat düşer. Should = önemli ama gerekirse ertelenir. Could = zaman sıkışırsa ilk kesilecekler.
- Bir iş kalemi ancak [Çalışma Kuralları](06-calisma-kurallari.md#definition-of-done)'ndaki Definition of Done sağlandığında biter.

## EP-0 GOV — Proje temeli (M0)

| ID | İş | Kabul kriteri | Sa | Öncelik | Bağımlılık | Sprint |
|---|---|---|---|---|---|---|
| GOV-01 | Git deposu, `.gitignore`, `.gitattributes` (LF), LICENSE (Apache-2.0) | Depo GitHub'da; `data/`, `.env`, `*.pt` ve checkpoint'ler git dışında | 2 | Must | – | ✅ S0 |
| GOV-02 | Python iskeleti: `pyproject.toml`, ruff, pytest, pre-commit, `uv.lock` | `ruff check` ve `pytest` temiz geçiyor; temiz klonda `uv sync` çalışıyor | 3 | Must | GOV-01 | ✅ S0 |
| GOV-03 | GitHub Projects panosu; epic etiketleri; backlog'un issue'lara aktarılması | Her iş kalemi bir issue ve bir milestone'a bağlı | 2 | Should | GOV-01 | S1 |
| GOV-04 | CI: GitHub Actions ile lint ve birim testleri | `main`'e her push'ta yeşil | 2 | Must | GOV-02 | ✅ S0 |
| GOV-05 | Kabul edilen öneriden sapmaların kaydı ve sonuç raporu (`docs/tubitak/`) | Her sapma (yöntem, teknoloji, takvim) gerekçesi ve kanıtıyla kayıtlı; formdaki 5 başarı ölçütünün her birinin ölçülmüş karşılığı raporda var; kaynakça doğrulanmış | 8 | Must | – | S12 |

## EP-1 INF — Altyapı (M0)

| ID | İş | Kabul kriteri | Sa | Öncelik | Bağımlılık | Sprint |
|---|---|---|---|---|---|---|
| INF-01 | Docker Desktop ve WSL2 bellek sınırı (`.wslconfig`) | `docker info` çalışıyor; WSL2 sınırı [Mimari §7](01-mimari.md#7-servisler-portlar-ve-kaynak-bütçesi)'deki bütçeye uygun | 1 | Must | – | ✅ S0 |
| INF-02 | Kafka (KRaft, tek broker) ve topic oluşturma betiği | Dört topic [Mimari §4](01-mimari.md#4-kafka-topicleri)'teki ayarlarla oluşuyor; produce/consume smoke testi geçiyor | 4 | Must | INF-01 | ✅ S0 |
| INF-03 | HDFS (NameNode + DataNode) | NameNode arayüzü açılıyor; dosya yazma/okuma testi geçiyor; veri volume'da kalıcı | 6 | Must | INF-01 | ✅ S0 |
| INF-04 | Özel Spark imajı (Spark 4.2.1, Kafka bağlayıcısı, PyTorch CPU, drain3, pyarrow) | Master ve worker ayakta; worker'da `import torch, drain3` çalışıyor; sürücü ve worker'da Python minör sürümü aynı | 6 | Must | INF-01 | ✅ S0 |
| INF-05 | Tüm servislerde healthcheck, RAM limitleri ve compose profilleri | `docker compose ps` her servisi `healthy` gösteriyor; limitler Mimari §7 ile birebir | 3 | Must | INF-02..04 | ✅ S0 |
| INF-06 | Uçtan uca smoke test betiği | Tek komutla Kafka'ya yazılan veri Spark üzerinden HDFS'e ulaşıyor ve doğrulanıyor; toplam süre 2 dakikanın altında | 4 | Must | INF-05 | ✅ S0 |
| INF-07 | `.env.example` ve README hızlı başlangıç bölümü | Temiz klonda README adımları izlenerek sistem ayağa kalkıyor | 2 | Must | INF-06 | ✅ S0 |
| INF-08 | Runbook: altyapıyı başlatma, durdurma ve sıfırlama (`docs/runbooks/altyapi.md`) | HDFS formatlama ve topic sıfırlama adımları dahil | 2 | Should | INF-06 | ✅ S0 |
| INF-09 | Altyapı arıza testleri (`infra/failure_test.sh`) ve çökmede otomatik yeniden başlatma (`restart: on-failure`, `init: true`) | Süreç çökmesi, ani kapanma, DataNode yokluğu ve worker yokluğu senaryolarının hepsi geçiyor; hiçbir senaryoda veri kaybı yok | 6 | Should | INF-06 | ✅ S0 |

## EP-2 ING — Veri hattı (M1)

| ID | İş | Kabul kriteri | Sa | Öncelik | Bağımlılık | Sprint |
|---|---|---|---|---|---|---|
| ING-01 | Veri seti indirme betiği (Loghub HDFS_v1 ve `anomaly_label.csv`) | SHA-256 doğrulaması yapılıyor; yeniden çalıştırmada tekrar indirmiyor | 3 | Must | GOV-02 | ✅ S0 |
| ING-02 | Mesaj şemaları ve doğrulama ([Mimari §5](01-mimari.md#5-veri-sözleşmeleri)) | Geçersiz mesaj için birim testleri var | 3 | Must | GOV-02 | ✅ S0 |
| ING-03 | Replayer: hız ayarı (log/sn), zaman damgası yeniden yazma, `session_key` = `block_id`, `label` alanı | 5.000 log/sn hedefini ±%5 sapmayla tutturuyor | 8 | Must | ING-01, ING-02, INF-02 | ✅ S0 |
| ING-04 | Ingest job: Kafka'dan HDFS raw bölgesine Parquet (tarih/saat bölümlü, checkpoint'li) | Job yeniden başlatıldığında kayıt kaybı ya da tekrar oluşmuyor | 8 | Must | ING-02, INF-06 | ✅ S0 |
| ING-05 | Metrik ajanı (Docker stats) → `metrics.raw`; ingest job'a eklenmesi | Tüm konteynerlerin metrikleri 5 sn aralıkla HDFS'e düşüyor | 6 | Must | ING-04 | ✅ S0 |
| ING-06 | Kayıp ve tekrar doğrulama aracı | Üretilen ve HDFS'e yazılan `id` kümelerini karşılaştırıp kayıp ve tekrar oranını raporluyor | 4 | Must | ING-04 | ✅ S0 |
| ING-07 | Performans ayarı: partition sayısı, trigger süresi, Parquet dosya boyutu | Ayar kararları gerekçeleriyle birlikte kayıtlı | 10 | Should | ING-06 | S1 |
| ING-08 | EXP-01: alım hızı ve kayıp deneyi ve raporu | K1 ve K2 sonuçları `experiments/EXP-01/` altında | 8 | Must | ING-06 | ✅ S0 |
| ING-09 | Runbook: veri hattı (job'u yeniden başlatma, checkpoint sıfırlama) | – | 2 | Should | ING-04 | S1 |
| ING-10 | Kanonik HDFS_v1 oynatması: `logs.raw`, ham bölge ve checkpoint'ler sıfırlanır (runbook), HDFS_v1'in tamamı **bir kez** `--source hdfs-replay` ile oynatılır | Ham bölgede `source = hdfs-replay` olan 11.175.629 kayıt var; tekrar yok (`verify_ingest`). Test ve deney verileri ayrı kaynak adlarıyla tutulur | 3 | Must | ING-08 | S1 |

## EP-3 ML — Model (M2)

| ID | İş | Kabul kriteri | Sa | Öncelik | Bağımlılık | Sprint |
|---|---|---|---|---|---|---|
| ML-01 | Şablonlama: HDFS_v1 için resmi şablon eşleştirici, demo sistem için Drain3 ([ADR-0008](adr/0008-sablon-kaynagi.md)); Spark işi (`mapInPandas`) `curated/events` üretir | Loghub'ın etiketli 2k örneğinde parsing doğruluğu raporlanıyor; yalnızca `source = hdfs-replay` kayıtları okunuyor | 10 | Must | ING-10 | ✅ S1 |
| ML-02 | Oturum oluşturma (`block_id`), etiket birleştirme, kronolojik train/val/test bölmesi | Bölme sabit seed ile tekrarlanabilir; eğitim setinde yalnızca normal oturumlar var | 6 | Must | ML-01 | ✅ S1 |
| ML-03 | Isolation Forest baseline (oturum başına olay sayım vektörü) | Val ve test metrikleri `metrics.json`'a yazılıyor | 4 | Must | ML-02 | ✅ S1 |
| ML-04 | DeepLog (LSTM, h=10, g=9, makaleye uygun) ve `config.yaml` ile eğitim | Eğitim tek komutla çalışıyor; sabit seed'le kayıp eğrisi tekrarlanabilir | 12 | Must | ML-02 | ✅ S1 |
| ML-05 | Ortak skor modülü ve değerlendirme: P/R/F1, karışıklık matrisi, McNemar | Skor fonksiyonunun birim testleri var; eşik yalnızca val setinde seçiliyor | 6 | Must | ML-03, ML-04 | ✅ S1 |
| ML-06 | Hiperparametre taraması (h, g, gizli boyut, katman sayısı); yalnızca val seti | Sonuç tablosu ve seçilen konfigürasyon kayıtlı | 8 | Should | ML-05 | S2 |
| ML-07 | Model paketi: `models/deeplog/<iz>-vN/` ve `load_model(version)` | Paketten yüklenen model aynı test skorunu veriyor | 4 | Must | ML-05 | S2 |
| ML-08 | EXP-02 raporu; sonuçların TÜBİTAK formunun 5. bölümüne tablo olarak eklenmesi | K3 ve K4 sonuçları; test seti yalnızca bir kez kullanıldı | 6 | Must | ML-07 | ✅ S2 |
| ML-09 | Tek komutla veriden modele ve rapora uzanan pipeline betiği | Temiz HDFS üzerinde EXP-02 sonuçlarını yeniden üretiyor | 6 | Should | ML-08 | S2 |

## EP-4 RT — Gerçek zamanlı tespit (M3)

| ID | İş | Kabul kriteri | Sa | Öncelik | Bağımlılık | Sprint |
|---|---|---|---|---|---|---|
| RT-01 | Tespit job'u: `logs.raw` okuma, Drain3 yalnızca eşleştirme modunda (bilinmeyen şablon → `UNKNOWN`) | Şablon sözlüğü modelle aynı sürümden yükleniyor | 8 | Must | ML-07 | S3 |
| RT-02 | Oturum durumu: `applyInPandasWithState` ile son h olay; durum zaman aşımı | 1 saat çalışmada durum boyutu sınırlı kalıyor | 8 | Must | RT-01 | S3 |
| RT-03 | Skorlama (model worker başına bir kez yükleniyor), eşik kontrolü ve `alerts` topic'ine yazma | Alarmlar şemaya uygun | 6 | Must | RT-02 | S3 |
| RT-04 | Gecikme ölçümü (`event_time`, `alert_time`) ve p50/p95 hesaplama betiği | – | 4 | Must | RT-03 | S4 |
| RT-05 | Tutarlılık testleri: aynı oturumlar için offline ve streaming skorları aynı; tespit job'u `label` alanını okumuyor | Testler CI'da çalışıyor | 4 | Must | RT-03 | S4 |
| RT-06 | EXP-03: gecikme deneyi ve trigger süresinin ayarlanması | K5 sonucu | 8 | Must | RT-04 | S4 |
| RT-07 | Runbook: tespit job'u ve model sürümü değiştirme | – | 2 | Should | RT-03 | S4 |

## EP-5 REM — Otonom iyileştirme (M4)

| ID | İş | Kabul kriteri | Sa | Öncelik | Bağımlılık | Sprint |
|---|---|---|---|---|---|---|
| REM-01 | Demo hedef sistem: FastAPI uygulaması; logları `logs.raw`'a gönderiyor; `/health` disk doluluğu ve sağlık durumunu dönüyor | Compose profiliyle ayağa kalkıyor | 6 | Must | INF-05 | S5 |
| REM-02 | Arıza enjektörü: disk doldurma (kademeli), bellek sızıntısı (kademeli), süreç öldürme (ani), yanıt vermeme (ani) | Her senaryo tek komutla tetikleniyor; arıza anı (`failure_time`) kaydediliyor | 6 | Must | REM-01 | S5 |
| REM-03 | Yük üreteci, normal log toplama ve demo iz için DeepLog eğitimi (`deeplog-demo-v1`) | Normal yükte val setinde FPR ≤ %1 | 8 | Must | REM-01, ML-09 | S5 |
| REM-04 | Remediator: `alerts` ve `metrics.raw` tüketimi, kurallarla arıza tipi belirleme ([ADR-0005](adr/0005-remediator-ayri-servis.md)) | Kurallar için birim testleri var | 8 | Must | REM-03, RT-03 | S6 |
| REM-05 | Aksiyonlar: Docker SDK ile restart; Ansible `disk_cleanup` playbook'u (yalnızca izinli dizinler) | Her aksiyon dry-run ve gerçek modda test edildi | 8 | Must | REM-04 | S6 |
| REM-06 | Korumalar: allowlist, `alert_id` ile tekrar engelleme, cooldown, saatlik üst sınır, kill switch, dry-run varsayılan; her karar `actions` topic'ine yazılıyor | Her koruma için birim testi var | 6 | Must | REM-04 | S6 |
| REM-07 | Bildirim (Slack webhook; tanımlı değilse yalnızca log) | – | 2 | Could | REM-05 | S7 |
| REM-08 | EXP-04: 4 senaryo × 20 koşu | K6, K7 ve K9 sonuçları | 10 | Must | REM-06 | S7 |
| REM-09 | EXP-05: 2 saatlik normal yük koşusu | K8 sonucu | 3 | Must | REM-06 | S7 |
| REM-10 | Runbook: otonom aksiyonları durdurma ve aksiyon geçmişini inceleme | – | 2 | Should | REM-06 | S7 |

## EP-6 UI — Panel ve entegrasyon (M5)

| ID | İş | Kabul kriteri | Sa | Öncelik | Bağımlılık | Sprint |
|---|---|---|---|---|---|---|
| UI-01 | Panel API: FastAPI, Kafka tüketimi, WebSocket yayını (alerts, actions, son loglar) | Bağlantı koparsa istemci yeniden bağlanıyor | 6 | Must | RT-03, REM-06 | S9 |
| UI-02 | React panel: canlı log akışı, alarm listesi, aksiyon geçmişi, KPI kartları | Klavyeyle gezilebiliyor; renk kontrastı WCAG AA | 12 | Must | UI-01 | S9 |
| UI-03 | Batch görünümü: günlük özetin Spark ile üretilmesi ve panelde gösterilmesi | – | 4 | Could | UI-02 | S9 |
| UI-04 | E2E test: compose up → arıza enjeksiyonu → alarm → aksiyon → panelde görünür | Tek komutla çalışıyor | 6 | Must | UI-02 | S10 |
| UI-05 | EXP-06: panel gecikmesi | K10 sonucu | 2 | Must | UI-02 | S10 |
| UI-06 | v1.0.0: tüm KPI'ların tek koşuda yeniden üretilmesi | Sonuç tablosu betikle üretiliyor | 4 | Must | UI-04 | S10 |

## EP-7 DOC — Raporlama (M6)

| ID | İş | Kabul kriteri | Sa | Öncelik | Bağımlılık | Sprint |
|---|---|---|---|---|---|---|
| DOC-01 | Deney raporu (yöntem, K1–K10, istatistik, sınırlılıklar) | Tüm sayılar `experiments/` altındaki çıktılardan alınıyor | 10 | Must | UI-06 | S12 |
| DOC-02 | Lisans bitirme tezi taslağı | Danışman incelemesine hazır | 16 | Must | DOC-01 | S12 |
| DOC-03 | Bildiri taslağı | Hedef kongrenin şablonunda | 10 | Should | DOC-01 | S12 |
| DOC-04 | Açık kaynak hazırlığı: README, mimari görselleri, CONTRIBUTING | Yabancı bir geliştirici README ile sistemi kurabiliyor | 4 | Should | UI-06 | S12 |
