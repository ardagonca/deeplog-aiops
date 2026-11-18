# ADR-0002: Kafka (KRaft) → Structured Streaming → HDFS Parquet; Flume ve ZooKeeper kullanılmaz

- Durum: Kabul
- Tarih: 2026-11-17
- Düzeltme (2026-11-18): İlk metinde "Flume'un aktif geliştirmesi durmuştur" yazıyordu. Bu iddia doğrulanamadı; Flume Apache Logging Services çatısı altında sürdürülüyor. Cümle çıkarıldı. Karar değişmedi.

## Bağlam

Öneri metninde veri alımı için Kafka, Flume ve ZooKeeper birlikte geçiyor. Depolama formatı ise "Parquet veya Avro" olarak açık bırakılmış. Projenin RAM bütçesi dar ([Mimari §7](../01-mimari.md#7-servisler-portlar-ve-kaynak-bütçesi)).

## Karar

- **Mesajlaşma:** Kafka, KRaft modunda ve tek broker ile çalışır. ZooKeeper kullanılmaz; Kafka 4.0 sürümünden itibaren ZooKeeper desteği zaten tamamen kaldırılmıştır.
- **Flume kullanılmaz.** Flume, Kafka'nın zaten yaptığı alım işini tekrarlar ve ek bakım ile RAM maliyeti getirir.
- **HDFS'e yazma:** Structured Streaming ingest job'u ile yapılır. Checkpoint'li dosya hedefi kullanıldığı için tekrar ve kayıp oluşmaz.
- **Depolama formatı:** Parquet. Analitik okumalar sütun bazlı olduğu için Parquet uygundur. Avro satır bazlıdır ve asıl avantajı akış serileştirmesindedir; bu projede bu ihtiyacı JSON karşılar.
- **Kafka mesajları:** JSON formatında ve `schema_version` alanıyla. Schema Registry kullanılmaz. `shortcut: üretici ve tüketici aynı depoda; şema sık evrilirse Avro + Schema Registry'ye geçilir.`

## Sonuçlar

- Öneri formundaki Flume ve ZooKeeper ifadeleri ile Şekil 1 güncellenmelidir (GOV-05).
- Bir konteyner eksildiği için RAM tasarrufu sağlanır.

## Değerlendirilen alternatifler

- **Flume + Kafka:** Gereksiz bir katman ekler; seçilmedi.
- **Kafka Connect HDFS Sink:** Ek bir bağımlılık ve lisans karmaşası getirir. Spark zaten kurulu olduğu için seçilmedi.
