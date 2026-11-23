# ADR-0004: Hız katmanında Spark Structured Streaming

- Durum: Kabul. Sürüm bilgisi [ADR-0007](0007-spark-4-ve-python-310.md) ile güncellendi (Spark 4.2.1). Oturum durumunun tutulma biçimi [ADR-0010](0010-tespit-foreachbatch.md) ile değişti (`applyInPandasWithState` yerine `foreachBatch`).
- Tarih: 2026-11-17

## Bağlam

Öneride "Spark Streaming" yazıyor, ancak bu adla anılan DStream API'si artık eski kabul ediliyor. Önceki prototipteki hız katmanı ise düz bir Python Kafka consumer'ıydı ve öneriyle çelişiyordu.

## Karar

- Hız katmanında Spark 3.5 ile Structured Streaming kullanılır.
- Oturum durumu `applyInPandasWithState` ile tutulur (PySpark 3.4 ve üzeri gerekir).
- Model, state fonksiyonunun içinde her worker'da yalnızca bir kez yüklenir.
- Trigger için `processingTime` başlangıçta 2 saniyedir; kesin değer EXP-03'te ayarlanır.
- Checkpoint'ler HDFS'te tutulur.
- Kafka hedefine yazım at-least-once garantilidir, yani aynı alarm birden fazla kez yazılabilir. Bu yüzden `alerts` topic'inin tüketicileri `alert_id` alanıyla tekrarları ayıklar (REM-06).

## Sonuçlar

- Artı: Öneriyle uyumludur, ölçeklenebilir ve dosya hedefine yazımda exactly-once garanti verir.
- Eksi: Python'daki state API, Scala sürümüne göre daha yavaştır. Bu durum gecikme riski R-03 olarak kayıtlıdır.

## Değerlendirilen alternatifler

- **Düz Python Kafka consumer:** Daha basittir ama öneriyle çelişir. R-03 için B planı olarak tutuluyor.
- **Apache Flink:** Öneri kapsamında değil ve ek öğrenme maliyeti getiriyor.
