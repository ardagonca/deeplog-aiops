# ADR-0006: İki izli veri ve değerlendirme stratejisi

- Durum: Kabul
- Tarih: 2026-11-17

## Bağlam

Projede iki farklı doğrulama ihtiyacı var:

- **Bilimsel geçerlilik:** Gerçek etiketli bir benchmark gerektirir.
- **Otonom döngünün denenmesi:** Üzerinde aksiyon alınabilecek canlı bir sistem gerektirir.

HDFS_v1 tarihsel bir log kaydıdır; üzerinde aksiyon alınamaz. Önceki prototip ise modeli sentetik olarak enjekte edilmiş hatalarla ölçüyordu. Bu hatalar çoğunlukla modelin hiç görmediği yeni şablonlar olduğu için sonuçlar olduğundan yüksek çıkıyordu.

## Karar

- **İz A, HDFS_v1 (Loghub):** K3 ve K4 hedefleri burada ölçülür. Gerçek etiketler kullanılır; veri kronolojik olarak bölünür ve test seti yalnızca bir kez kullanılır. K5 de bu izdeki test oturumlarının yeniden oynatılmasıyla ölçülür. Model sürümleri `deeplog-hdfs-vN` olarak adlandırılır.
- **İz B, demo hedef sistem ve arıza enjektörü:** K6, K7, K8 ve K9 burada ölçülür. DeepLog, demo sistemin normal loglarıyla ayrıca eğitilir. Model sürümleri `deeplog-demo-vN` olarak adlandırılır.
- Ayrıntılar için bkz. [Test Stratejisi §3](../04-test-stratejisi.md#3-veri-setleri-ve-bölme-adr-0006).

## Sonuçlar

- İki ayrı model sürümü yönetilir.
- Raporlarda iki iz ayrı başlıklar altında sunulur.
- Demo sistemin gerçekçiliği bir sınırlılık olarak raporlanır (R-07).

## Değerlendirilen alternatifler

- **Yalnızca sentetik veri kullanmak:** Bilimsel olarak zayıf olduğu için seçilmedi.
- **Yalnızca HDFS_v1 kullanmak:** Otonom iyileştirme gösterilemeyeceği için seçilmedi.
