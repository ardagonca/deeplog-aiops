# Mimari Karar Kayıtları (ADR)

Her ADR tek bir kararı ve o kararın gerekçesini kayda geçirir. Kabul edilmiş bir ADR değiştirilmez. Karar değişirse yeni bir ADR yazılır ve eski kaydın durumu "X tarafından geçersiz kılındı" olarak güncellenir.

| No | Karar | Durum |
|---|---|---|
| [0001](0001-lambda-mimarisi.md) | Lambda mimarisi | Kabul |
| [0002](0002-veri-alimi.md) | Kafka (KRaft) → Structured Streaming → HDFS Parquet; Flume ve ZooKeeper kullanılmaz | Kabul |
| [0003](0003-deeplog-ana-model.md) | Drain + DeepLog ana model, Isolation Forest baseline | Kabul |
| [0004](0004-structured-streaming.md) | Hız katmanında Spark Structured Streaming | Kabul (sürüm: 0007) |
| [0005](0005-remediator-ayri-servis.md) | Otonom aksiyonlar ayrı bir servis (Remediator) üzerinden | Kabul |
| [0006](0006-veri-ve-degerlendirme.md) | İki izli veri ve değerlendirme stratejisi | Kabul |
| [0007](0007-spark-4-ve-python-310.md) | Spark 4.2.1 ve Python 3.10 | Kabul |

## Şablon

```markdown
# ADR-NNNN: <Başlık>

- Durum: Önerildi | Kabul | X tarafından geçersiz kılındı
- Tarih: YYYY-MM-DD

## Bağlam
Hangi problem, hangi kısıtlar?

## Karar
Ne yapıyoruz?

## Sonuçlar
Kazanılanlar, kaybedilenler, yapılması gereken takip işleri.

## Değerlendirilen alternatifler
Seçenek: neden seçilmedi?
```
