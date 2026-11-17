# ADR-0001: Lambda mimarisi

- Durum: Kabul
- Tarih: 2026-11-17

## Bağlam

Sistemin iki farklı işi aynı anda yapması gerekiyor: geçmiş veriyle modeli eğitmek ve yeniden eğitmek (dakikalar ile saatler süren batch işler) ve canlı akışta saniyeler içinde anomali tespit etmek. TÜBİTAK önerisi de Lambda mimarisini taahhüt ediyor.

## Karar

- **Batch katmanı:** HDFS üzerinde çalışan Spark batch işleri. Şablonlama, oturum oluşturma ve günlük görünümleri üretir. Model eğitimi PyTorch ile yapılır ([ADR-0003](0003-deeplog-ana-model.md)).
- **Hız katmanı:** Spark Structured Streaming ([ADR-0004](0004-structured-streaming.md)).
- **Sunum katmanı:** Kafka'daki `alerts` ve `actions` topic'leri, panel API'si ve HDFS'teki `views/` bölgesi.

## Sonuçlar

- Artı: Yeniden eğitim gerçek zamanlı yolu durdurmaz.
- Artı: Mimari öneri metniyle birebir uyumludur.
- Eksi: İki ayrı kod yolu olduğu için eğitimdeki skorla canlı skor birbirinden sapabilir (train/serve skew). Önlem olarak şablon eşleştirme ve skor kodu tek bir modüldedir ve iki yol da bu modülü çağırır. Bu durum RT-05 testiyle doğrulanır.

## Değerlendirilen alternatifler

- **Kappa (yalnızca akış; geçmiş veri Kafka'dan yeniden oynatılarak işlenir):** Daha az kod gerektirir. Ancak Kafka'da uzun süreli saklama ister ve öneri metniyle çelişir. Bu yüzden seçilmedi.
