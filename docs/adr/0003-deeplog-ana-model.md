# ADR-0003: Drain + DeepLog ana model, Isolation Forest baseline

- Durum: Kabul. Şablon kaynağı HDFS_v1 izi için [ADR-0008](0008-sablon-kaynagi.md) ile daraltıldı (resmi Loghub şablonları); demo sistemde Drain geçerli.
- Tarih: 2026-11-17

## Bağlam

Projenin, etiket gerektirmeyen ve log dizisinin sırasını modelleyebilen bir yönteme ihtiyacı var. Yöntemin HDFS_v1 üzerinde yayınlanmış sonuçlarının olması da karşılaştırma için önemli.

Durum tespitleri:

- Önceki prototipte Isolation Forest devre dışıydı; model olarak Bi-LSTM kullanılıyordu.
- Spark MLlib'de Isolation Forest bulunmuyor.
- Öneri metnindeki sabit 0,85 eşiği herhangi bir veriye dayanmıyor.

## Karar

- **Şablonlama:** Drain3 kullanılır. Batch katmanında şablon sözlüğü üretilir; hız katmanında Drain3 yalnızca eşleştirme modunda çalışır.
- **Ana model:** DeepLog (Du vd., 2017). Tek yönlü LSTM, sıradaki olayı tahmin eder ve gerçekleşen olay ilk g aday arasında değilse olay anormal sayılır. Makaledeki varsayılan değerler h=10, g=9 başlangıç olarak alınır. Tek yönlü LSTM'in seçilme nedeni makaleye sadık kalmak ve sonuçları karşılaştırılabilir tutmaktır.
- **Risk skoru:** Bir oturumdaki son W tahmin içinde ilk g'ye girmeyenlerin oranıdır ve [0, 1] aralığında bir değer alır. Oturumda bilinmeyen bir şablon (`UNKNOWN`) görülürse skor doğrudan 1 olur.
- **Eşik:** Val setinde F1'i en yüksek yapan değer seçilir. Sabit 0,85 eşiği kaldırılır.
- **Baseline:** Oturum başına olay sayım vektörü üzerinde Isolation Forest (sklearn).
- **Eğitim ortamı:** Eğitim Spark dışında, tek düğümde PyTorch ile (CPU) yapılır; oturum dizileri küçük olduğu için GPU gerekmez. Spark, veri hazırlama ve dağıtık çıkarım için kullanılır.

## Sonuçlar

- Öneri formunda düzeltilmesi gerekenler (GOV-05): "Spark MLlib ile Isolation Forest" ifadesi, K-Means ve Şekil 2'deki 0,85 eşiği.
- K-Means kapsamdan çıkarıldı. Önceki prototipte eşik ve test aynı veri üzerinde yapıldığı için K-Means'in katkısı gösterilememişti.

## Değerlendirilen alternatifler

- **Isolation Forest'ı ana model yapmak:** Log sırası bilgisini kullanmaz; bu yüzden baseline olarak tutuldu.
- **LogAnomaly, LogBERT veya Autoencoder:** Daha ağır yöntemler. R-02 için B planı olarak bekletiliyor.
