# ADR-0010: Tespit job'unda `foreachBatch` ile sürücüde toplu skorlama

- Durum: Kabul. [ADR-0004](0004-structured-streaming.md)'ün "oturum durumu `applyInPandasWithState` ile tutulur" maddesinin yerini alır. Hız katmanı Spark Structured Streaming olarak kalır.
- Tarih: 2026-11-23

## Bağlam

ADR-0004, oturum durumunun `applyInPandasWithState` ile tutulmasını öngörüyordu. Bu tasarım RT-02'de uygulanıp ölçüldü:

- Durum fonksiyonu oturum (grup) başına bir kez çağrılır. Her çağrı küçük bir PyTorch ileri geçişi yapar. Bir micro-batch'te on binlerce oturum olduğunda çağrı ve tensör kurma maliyeti baskın hale geliyor. Shuffle bölümü 4'e indirildikten sonra bile ölçülen hız yaklaşık **1.100 log/sn** oldu. Tüzükteki 10.000 log/sn alım hedefinin (resmi ölçüt, satır 2) çok altında.
- PySpark 4.2'de durum şemasında ARRAY tipi `KeyError: 'elementType'` hatası verdi; durumun metne serileştirilmesi gerekti.
- Durum şeması değiştiğinde checkpoint uyumsuzlaşıyor (`STATE_STORE_VALUE_SCHEMA_NOT_COMPATIBLE`), checkpoint silinmeden iş başlamıyor.

Risk R-03'ün B planı skorlamayı Spark dışına (düz Kafka consumer) taşımaktı. Bu, önerideki Spark Streaming taahhüdüyle çelişir.

## Karar

- Tespit job'u (`jobs/detect.py`) Kafka okumasını, offset ve checkpoint yönetimini, satırın içindeki her blok kimliğine ayrılmasını Spark Structured Streaming'de yapar.
- Her micro-batch `foreachBatch` ile sürücüye toplanır ve `ml.detector.Detector` tarafından **toplu** skorlanır: batch'teki tüm oturumların pencereleri tek ileri geçişte değerlendirilir (`ml.stream_score.advance_many`, `close_many`).
- Oturum durumu (son `h` olay, sayım vektörü, alarm bayrağı) sürücü belleğinde tutulur. Oturum zaman aşımı, veri gelmediğinde de çalışan ayrı bir iş parçacığıyla 5 sn'de bir denetlenir.
- Karar mantığı Spark'tan bağımsızdır ve birim testlidir. Skorlama offline değerlendirmeyle aynı fonksiyonları kullanır; parça bağımsızlığı ve offline eşitliği testlerle güvence altındadır (RT-05).
- Alarmlar Kafka'ya en az bir kez yazılır. `alert_id` (model, tür, oturum)'dan UUIDv5 ile türetilir. Böylece yeniden işlenen bir batch aynı kimlikleri üretir.

## Sonuçlar

- Artı: Aynı makinede ölçülen hız ~**9.500 log/sn**'ye çıktı (yaklaşık 8,6 kat). Spark Structured Streaming kullanımı sürüyor; öneriyle çelişki yok.
- Artı: Durum şemasına bağlı checkpoint uyumsuzluğu ortadan kalktı. Karar mantığı Spark olmadan test edilebiliyor.
- Eksi: Skorlama tek süreçte, sürücüde yapılır; yatay ölçekleme yoktur. Tek düğümlü geliştirme ortamında bunun pratik bir kaybı yoktur. Gerekirse oturum anahtarına göre bölünmüş birden çok tespit job'u çalıştırılabilir.
- Eksi: Durum bellekte olduğundan job yeniden başlarsa açık oturumlar sıfırdan başlar. Oturumun ilk olayları bağlamsız değerlendirildiği için yanlış alarm çıkabilir. Kalıcı duruma geçiş RT-08 olarak backlog'dadır (M4 öncesi).
- Takip: Sürücü belleği 1,5 GB'a çıkarıldı (Mimari §7). Gecikme ve canlı/offline uyumu EXP-03'te ölçülür.

## Değerlendirilen alternatifler

- **`applyInPandasWithState` ile devam ve ayar:** Shuffle bölümleri ve trigger süresi ayarlandı, hız 1.100 log/sn'de kaldı. Darboğaz oturum başına çağrı yapısının kendisi.
- **`transformWithStateInPandas` (Spark 4):** Aynı şekilde grup başına çağrılır; aynı darboğazı taşır.
- **Düz Python Kafka consumer (R-03 B planı):** Hızlıdır ama Spark Streaming taahhüdünden sapar. `foreachBatch` aynı hızı Spark içinde sağladığı için gerek kalmadı.
