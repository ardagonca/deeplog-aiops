# Test ve Deney Stratejisi

## 1. Test seviyeleri

| Seviye | Kapsam | Araç | Ne zaman |
|---|---|---|---|
| Birim | Şemalar, skor fonksiyonu, arıza tipi kuralları, korumalar | pytest | Her commit (CI) |
| Entegrasyon | Kafka, Spark ve HDFS arasındaki bağlantılar | Smoke test (INF-06) | Compose dosyası her değiştiğinde ve kilometre taşı kapanışında |
| Altyapı arıza | Servis çökmesi, ani kapanma, DataNode ve worker yokluğu; her adımda veri kaybı kontrolü | `infra/failure_test.sh` (INF-09) | Compose dosyası veya imaj sürümü değiştiğinde ve kilometre taşı kapanışında |
| Uçtan uca | Kaynak → alarm → aksiyon → panel | E2E betiği (UI-04) | M5'ten itibaren her sürümde |
| Performans | Alım hızı ve gecikme | EXP-01, EXP-03, EXP-06 | Kilometre taşı kapanışında |
| Model değerlendirme | P/R/F1 ve baseline karşılaştırması | EXP-02 | Model her değiştiğinde |
| Arıza/kaos | Otonom döngü | EXP-04, EXP-05 | M4'te ve her sürümden önce |

**Testi zorunlu kritik mantık:** skor fonksiyonu, Remediator korumaları (allowlist, cooldown, tekrar engelleme, kill switch), arıza tipi kuralları, şema doğrulama ve etiket sızıntısı testi (tespit job'u `label` alanını okumamalı).

## 2. Metrik tanımları

| Metrik | Tanım |
|---|---|
| Precision, Recall, F1 | Değerlendirme birimi oturumdur (HDFS_v1'de `block_id`). Precision = TP / (TP + FP), Recall = TP / (TP + FN), F1 bu ikisinin harmonik ortalamasıdır |
| FPR | FP / (FP + TN) |
| Alım hızı | HDFS'e yazılan kayıt sayısının süreye bölümü. Ölçüm 10 dakikalık kararlı bölümde yapılır; ilk 1 dakika ısınma olarak hariç tutulur |
| Veri kaybı | 1 − (HDFS'teki benzersiz `id` sayısı / üretilen `id` sayısı). Tekrar oranı ayrıca raporlanır |
| Tespit gecikmesi | `alert_time` − `trigger_event_time` |
| İyileştirme süresi (TTR) | `actions.finished_at` − `alerts.alert_time`; yalnızca `status = success` olan aksiyonlar |
| Otonom iyileştirme başarısı | Başarılı koşu sayısı / toplam koşu sayısı. Bir koşu, aksiyondan sonraki 60 saniye içinde `/health` ardışık 3 kez sağlıklı dönerse başarılı sayılır |
| Öngörü süresi (lead time) | `failure_time` − `alert_time`. `failure_time`, `/health`'in ilk kez başarısız döndüğü andır. Pozitif değer, alarmın arızadan önce geldiğini gösterir. Yalnızca kademeli senaryolarda ölçülür |

## 3. Veri setleri ve bölme ([ADR-0006](adr/0006-veri-ve-degerlendirme.md))

**İz A, HDFS_v1 (Loghub):** 11.175.629 log satırı ve 575.061 oturum içerir; oturumların 16.838'i (%2,93) anormaldir (ING-01 ile indirilen dosyada sayıldı). Etiketler oturum bazlıdır ve `anomaly_label.csv` dosyasında bulunur.

- Oturumlar başlangıç zamanına göre sıralanıp kronolojik olarak bölünür: %60 eğitim (yalnızca normal oturumlar), %10 doğrulama (val), %30 test.
- Eşik ve hiperparametreler yalnızca val setinde seçilir.
- Test seti nihai raporda **yalnızca bir kez** kullanılır.

**İz B, demo sistem:** Model, normal yük altında toplanan loglarla eğitilir. Test verisi arıza enjeksiyonu koşularından gelir. Bu izdeki etiketler sentetiktir; bu nedenle tespit başarısı (K3) bu izde değil, yalnızca İz A'da raporlanır.

## 4. Deney protokolleri

Her deney koşusu `experiments/EXP-XX/<tarih>/` klasörüne şunları yazar: kullanılan config, ham sonuçlar, özet (`ozet.md`) ve commit hash'i. Raporlardaki sayılar elle kopyalanmaz, betik tarafından üretilir.

| ID | Ne ölçer | KPI | Kurulum | Tekrar | Raporlanan |
|---|---|---|---|---|---|
| EXP-01 | Alım hızı ve kayıp | K1, K2 | Replayer sabit hızlarda: 1.000, 2.500, 5.000 ve 10.000 log/sn; her koşu 11 dakika | Her hız için 3 | Ortalama ± std, kayıp ve tekrar oranı |
| EXP-02 | Model başarısı | K3, K4 | HDFS_v1 test seti; DeepLog ve Isolation Forest karşılaştırması | 5 farklı seed | P/R/F1 ortalama ± std; McNemar testi medyan F1'e sahip seed üzerinde yapılır |
| EXP-03 | Tespit gecikmesi | K5 | HDFS_v1 test oturumları 5.000 log/sn hızla oynatılır | 3 koşu, her biri en az 100 alarm | p50 ve p95 |
| EXP-04 | Otonom döngü | K6, K7, K9 | 4 arıza senaryosu (2 kademeli, 2 ani) | Senaryo başına 20 | Başarı oranı (%95 Wilson güven aralığıyla), TTR p50/p95, lead time medyanı; H2 için tek örneklem Wilcoxon testi |
| EXP-05 | Gereksiz aksiyon | K8 | 2 saatlik normal yük, hiç arıza enjekte edilmez | 1 | Aksiyon sayısı ve alarm sayısı |
| EXP-06 | Panel gecikmesi | K10 | `alerts` topic'ine zaman damgalı test alarmı yazılır ve tarayıcıda görünene kadar geçen süre ölçülür | 50 | p95 |

## 5. Değişkenler (TÜBİTAK formu için)

- **Bağımsız değişkenler:** log hızı, pencere boyu (h), aday sayısı (top-g), eşik, arıza tipi, micro-batch trigger süresi
- **Bağımlı değişkenler:** Precision, Recall, F1, tespit gecikmesi, TTR, otonom iyileştirme başarısı, öngörü süresi
