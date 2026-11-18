# ADR-0008: Şablon kaynağı — HDFS_v1'de resmi Loghub şablonları, demo sistemde Drain

- Durum: Önerildi
- Tarih: 2026-11-18
- İlgili: [ADR-0003](0003-deeplog-ana-model.md) (Drain + DeepLog), [ADR-0006](0006-veri-ve-degerlendirme.md) (iki iz)

## Bağlam

ADR-0003 log şablonlamasını Drain ile yapmayı öngörüyordu. ML-01 ölçümleri ([parsing.md](../../experiments/ML-01/parsing.md)) şunu gösterdi:

- Loghub'ın elle etiketlenmiş 2.000 satırlık HDFS örneğinde Drain'in gruplama doğruluğu 1,0'dır (blok listesi maskesinden önce 0,9975).
- Tam HDFS_v1'de (11.175.629 satır) ise Drain **46** şablon bulur; Loghub'ın resmi listesinde **29** şablon var. Drain hem bölüyor hem birleştiriyor:
  - **Bölme:** Seyrek istisna mesajları istisna türüne göre ayrı şablonlara ayrılıyor.
  - **Birleştirme:** Daha zararlı olanı bu. `PacketResponder <NUM> for block <BLK> <*>` şablonunda son kelime jokere dönüşüyor; resmi listede ayrı olaylar olan E11 ("terminating", 1.706.679 satır) ile E8 ("Interrupted", 49 satır) aynı şablonda birleşiyor. Toplam 1.706.728, Drain kümesinin boyutuyla birebir aynı. Seyrek ve muhtemelen arızayla ilişkili 49 olay, 1,7 milyonluk normal olayın içinde kayboluyor; DeepLog bunları ayırt edemez.
- DeepLog makalesi (Du vd., 2017) ve HDFS_v1 üzerinde raporlanan literatür sonuçları, Loghub'ın resmi olay kimliklerini (blok dizileri `Event_traces.csv`) kullanıyor. K3 ve K4 hedeflerinin literatürle karşılaştırılabilir olması için aynı olay tanımları gerekiyor.

Resmi şablonlarla deterministik eşleştiricinin (`ml/official_templates.py`) tam verideki ölçümü ([official_templates.md](../../experiments/ML-01/official_templates.md)):

- Kapsama: 11.175.629 satırın **%100**'ü resmi şablonlardan birine uyuyor.
- Birden çok şablona uyan satır: **0**. Yine de çakışma ihtimaline karşı en özgül şablon seçilir.
- Loghub `Event_traces.csv` ile blok başına olay dizisi uyumu: rastgele seçilen 2.000 blokta (seed 42) **%100**. Eşleştiricimiz, literatürde kullanılan blok dizilerini birebir yeniden üretiyor.

## Önerilen karar

- **İz A, HDFS_v1:** Olay kimlikleri Loghub'ın resmi 29 şablonundan, `ml/official_templates.py` eşleştiricisiyle üretilir. Batch katmanı ve hız katmanı aynı eşleştiriciyi kullanır (ADR-0001: tek kod yolu). Hiçbir şablona uymayan satır `UNKNOWN` olayıdır ve DeepLog'da anormallik sinyali sayılır (ADR-0003).
- **İz B, demo sistem:** Resmi şablon listesi olmadığı için Drain (`ml/parsing.py`) kullanılır. Şablon sözlüğü normal yük loglarından çıkarılır ve modelle birlikte sürümlenir; hız katmanında Drain yalnızca eşleştirme modunda çalışır, eşleşmeyen satır `UNKNOWN` olur.

## Sonuçlar

- K3/K4 sonuçları DeepLog makalesi ve Loghub tabanlı çalışmalarla aynı olay tanımları üzerinden karşılaştırılabilir.
- Resmi eşleştirici öğrenme gerektirmez; hız katmanında durum tutmadan, düşük maliyetle çalışır.
- İki iz farklı şablonlayıcı kullanır; bu bir sınırlılık olarak raporlanır. Drain'in tam HDFS_v1'deki davranışı (46 şablon, birleştirme sorunu) tezde gerekçe olarak sunulur.
- ADR-0003'teki "Drain ile şablonlama" kararı İz A için bu ADR ile daraltılır.

## Değerlendirilen alternatifler

- **Her iki izde Drain:** Tek kod yolu sağlar, ama HDFS_v1'de olay birleştirme ve literatürle karşılaştırılamama sorunlarını getirir.
- **Drain parametrelerini tam veriye göre ayarlamak:** Test verisine göre ayar yapma riski taşır ve birleştirme sorununu çözeceği garanti değildir.
