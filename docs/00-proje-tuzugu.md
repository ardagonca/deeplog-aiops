# Proje Tüzüğü (Project Charter)

| | |
|---|---|
| Proje | Dağıtık Sistemlerde Hata Öngörüsü ve Otonom İyileştirme Sağlayan Büyük Veri Tabanlı AIOps Platformu |
| Program | TÜBİTAK 2209-A Üniversite Öğrencileri Araştırma Projeleri |
| Durum | **Kabul edildi, destekleniyor.** Resmi dönem: 12.05.2026 – 12.05.2027 |
| Kurum | Bartın Üniversitesi |
| Proje yürütücüsü | Arda Gonca |
| Danışman | Bartın Üniversitesi öğretim üyesi |
| Hedef olgunluk | TRL-4 (laboratuvar ortamında doğrulanmış prototip) |
| Son güncelleme | 2026-11-18 |

## 1. Problem

Geleneksel izleme araçları reaktiftir: alarm, kesinti yaşandıktan sonra gelir. Log ve metrik hacmi insanların elle izleyebileceği düzeyi çoktan aşmıştır. Ticari AIOps ürünleri ise pahalıdır ve kapalı kutu olarak çalışır.

## 2. Vizyon

Log dizilerindeki anormallikleri arıza gerçekleşmeden yakalayan, tanımlı senaryolarda insan müdahalesi olmadan sistemi toparlayan ve tamamı açık kaynak bileşenlerden oluşan, yeniden üretilebilir bir AIOps referans platformu.

## 3. Kapsam

**Kapsam içi**

- Log ve metrik toplama (Kafka), HDFS üzerinde Parquet veri gölü
- Batch katmanı: Drain ile log şablonlama, DeepLog eğitimi, Isolation Forest baseline
- Hız katmanı: Spark Structured Streaming ile gerçek zamanlı risk skoru ve alarm
- Otonom iyileştirme: iki arıza tipi (disk doluluğu, servisin yanıt vermemesi) için Docker ve Ansible aksiyonları, güvenlik korumalarıyla birlikte
- Web paneli (FastAPI + React)
- Deneyler: HDFS_v1 benchmark'ı ve demo sistemde arıza enjeksiyonu

**Kapsam dışı**

- Kubernetes ve çok sunuculu gerçek küme (tek makinede Docker Compose kullanılır)
- Üretim sertleştirmesi: kimlik doğrulama, çok kullanıcı, yüksek erişilebilirlik
- Denetimli (etiket gerektiren) modeller
- HDFS_v1 ve demo uygulama dışındaki log formatları
- Yukarıdaki iki arıza tipi dışındaki otonom aksiyonlar
- Apache Flume ve ZooKeeper (bkz. [ADR-0002](adr/0002-veri-alimi.md))

## 4. Başarı ölçütleri (KPI)

**Bağlayıcı ölçütler**, kabul edilen önerinin çalışma takvimindekilerdir: 1.000 log/sn kayıpsız aktarım, test verisinde Precision > %85, tespitten iyileştirme komutuna < 1 dk ve canlı akışın arayüzde gecikmesiz görüntülenmesi ([Yol Haritası §1](02-yol-haritasi.md#1-resmi-çalışma-takvimi-kabul-edilen-öneri)). Aşağıdaki K1–K10 bu ölçütlerin hepsini kapsar ve onlardan daha sıkıdır. Bu tablo iç hedeflerin **tek doğru kaynağıdır**; raporlardaki tüm sayılar buradan alınır. Bir hedef değişecekse önce bu tablo güncellenir ve değişiklik danışmanın onayından geçer. Ölçümlerin nasıl yapıldığı [Test Stratejisi §2](04-test-stratejisi.md#2-metrik-tanımları) bölümünde tanımlıdır.

| ID | Ölçüt | Hedef | Deney | Kilometre taşı |
|---|---|---|---|---|
| K1 | Alım hızı (Kafka → HDFS) | ≥ 5.000 log/sn, 10 dk kesintisiz | EXP-01 | M1 |
| K2 | Veri kaybı | %0 | EXP-01 | M1 |
| K3 | Tespit başarısı (HDFS_v1 test seti) | Precision ≥ 0,90 ve Recall ≥ 0,90 | EXP-02 | M2 |
| K4 | Baseline'a üstünlük | DeepLog F1 > Isolation Forest F1, McNemar p < 0,05 | EXP-02 | M2 |
| K5 | Tespit gecikmesi (log → alarm) | p95 ≤ 10 sn | EXP-03 | M3 |
| K6 | İyileştirme süresi (alarm → aksiyon tamam) | p95 ≤ 30 sn | EXP-04 | M4 |
| K7 | Otonom iyileştirme başarısı | Enjekte edilen arızaların ≥ %90'ında servis, insan müdahalesi olmadan sağlıklı duruma döner | EXP-04 | M4 |
| K8 | Gereksiz aksiyon | 2 saatlik normal yük koşusunda 0 aksiyon | EXP-05 | M4 |
| K9 | Öngörü süresi (lead time) | Hedef yok. Araştırma sorusunun cevabıdır; kademeli arızalarda ölçülür ve raporlanır | EXP-04 | M4 |
| K10 | Panel gecikmesi (alarm → ekranda) | p95 ≤ 2 sn | EXP-06 | M5 |

## 5. Araştırma sorusu ve hipotezler

**Araştırma sorusu:** Dağıtık sistemlerden akan heterojen log verisi büyük veri teknolojileriyle gerçek zamanlı işlendiğinde, arızalar gerçekleşmeden ne kadar önce tespit edilebilir? Bu tespitlere dayanan otonom bir döngü, servisi hangi başarı oranı ve sürede toparlayabilir?

- **H1:** DeepLog, HDFS_v1 test setinde K3'ü sağlar ve Isolation Forest baseline'ından istatistiksel olarak anlamlı ölçüde daha iyidir (K4).
- **H2:** Kademeli arızalarda (disk dolması, bellek sızıntısı) alarm, servis arızası gerçekleşmeden önce üretilir. Başka bir deyişle medyan öngörü süresi 0'dan büyüktür (K9, tek örneklem Wilcoxon testi).
- **H3:** Otonom döngü K6, K7 ve K8'i sağlar.

## 6. Teslimatlar

| Teslimat | Kilometre taşı | TÜBİTAK çıktı kategorisi |
|---|---|---|
| Çalışan prototip (TRL-4) ve açık kaynak GitHub deposu | M5 | Ekonomik/Ticari/Sosyal |
| Deney raporu (K1–K10 sonuçları) | M6 | Bilimsel/Akademik |
| Lisans bitirme tezi taslağı | M6 | Bilimsel/Akademik |
| TÜBİTAK sonuç raporu (sapma kaydı dahil, GOV-05) | M6 | – |
| Ulusal kongre bildirisi taslağı (UYMS veya IEEE öğrenci konferansı) | M6 | Bilimsel/Akademik |

## 7. Paydaşlar ve sorumluluklar (RACI)

R: yapan, A: onaylayan, C: danışılan, I: bilgilendirilen

| İş | Arda Gonca | Danışman | TÜBİTAK |
|---|---|---|---|
| Geliştirme ve deneyler | R/A | C | – |
| Mimari kararlar (ADR) | R/A | C | – |
| KPI veya kapsam değişikliği | R | A | I |
| Ara ve sonuç raporları | R | A | I |

## 8. Yönetişim ve iletişim

- İki haftalık sprintler yapılır; her sprint sonunda danışmanla 30 dakikalık demo ve değerlendirme toplantısı olur.
- Her ay sonunda bir sayfalık durum raporu yazılır (tamamlananlar, KPI durumu, açık riskler). Raporlar `docs/raporlar/YYYY-MM.md` yoluna konur.
- Bir kilometre taşı, [yol haritasındaki](02-yol-haritasi.md) çıkış kriterleri sağlandığında kapanır ve sürüm etiketi atılır.

## 9. Kısıtlar ve varsayımlar

**Kısıtlar**

- Resmi bitiş tarihi 12.05.2027'dir ve kaydırılamaz.
- Tek geliştirici çalışıyor. Kapasite dönem içinde haftada yaklaşık 12 saat, sınav dönemlerinde bunun yarısı, tatillerde yaklaşık 20 saat.
- Tek dizüstü bilgisayar kullanılıyor (Windows 11, Docker Desktop, WSL2). RAM bütçesi için bkz. [Mimari §7](01-mimari.md#7-servisler-portlar-ve-kaynak-bütçesi).
- Ücretli yazılım kullanılmayacak. Bulut (AWS) yalnızca yerel makineyi aşan deneyler (büyük ölçekli veri seti, çok düğümlü küme) için ve en fazla 9.000 TL'lik proje bütçesiyle kullanılır; plan ve harcama denetimi: [Bulut Bütçe Planı](07-bulut-butcesi.md).

**Varsayımlar**

- Loghub HDFS_v1 veri seti erişilebilir ve araştırma amaçlı kullanıma açıktır.
- Demo sistemde enjekte edilen arızalar gerçek arızaların makul bir temsilidir (bkz. risk R-07).

## 10. Lisans

Apache-2.0. Kafka, Spark ve Hadoop ile aynı lisanstır ve GOV-01 kapsamında depoya eklenir.
