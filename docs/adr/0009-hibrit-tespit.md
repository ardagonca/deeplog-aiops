# ADR-0009: Oturum kararı sayım yeniliğiyle, erken uyarı DeepLog ile (hibrit tespit)

- Durum: Önerildi
- Tarih: 2026-11-18
- İlgili: [ADR-0003](0003-deeplog-ana-model.md), [ADR-0006](0006-veri-ve-degerlendirme.md), [ADR-0008](0008-sablon-kaynagi.md), [EXP-02](../../experiments/EXP-02/)

## Bağlam

EXP-02, HDFS_v1 test setinde dört yöntemi aynı protokolle karşılaştırdı. Protokolde tüm seçimler yalnızca doğrulama setinde yapıldı ve test seti bir kez kullanıldı. İlk koşunun sonuçları (5 tohum, ortalama):

| Yöntem | Precision | Recall | F1 |
|---|---|---|---|
| Olay sayımı yeniliği | 0,980 | 1,000 | 0,990 |
| DeepLog + oturum sonu (EOS) | 0,726 | 0,696 | 0,708 |
| DeepLog (makaledeki hali) | 0,696 | 0,550 | 0,611 |
| Isolation Forest (ızgarada seçilen ayar) | 0,796 | 0,292 | 0,402 |

Bulgular:

- **Normal davranış çok düzenli.** 333.878 normal eğitim oturumunda yalnızca 207 farklı olay sayım vektörü var. Doğrulama ve test setindeki anormal oturumların **hiçbiri** bir normal oturumla aynı diziye sahip değil.
- **DeepLog'un kuralı yerel.** "Gerçek olay ilk g tahmin arasında mı" kuralı, her adımı tek başına makul görünen ama bir olayın fazla ya da eksik geçtiği oturumları kaçırıyor. Oturum sonu (EOS) olayı recall'u artırıyor ama yeterli değil.
- **Isolation Forest zayıf ve kararsız.** Varsayılan ayarla (max_samples=256) F1 0,20 çıkıyor. Ayarlı haliyle bile tohumlar arasında 0,27 ile 0,60 arasında oynuyor.
- **Kabul edilen önerideki ölçüt** (test verisinde Precision > %85) yalnızca sayım yeniliğiyle sağlanıyor.

Öte yandan sayım yeniliği oturum **tamamlanmadan** karar veremez. Projenin "hata öngörüsü" iddiası (K9, H2) ise oturum sürerken erken uyarı gerektiriyor. DeepLog her olayda tahmin yaptığı için bu işi görebilir.

## Karar

- **Oturum düzeyindeki anormallik kararı** olay sayımı yeniliğiyle verilir. Bir oturum, eğitimdeki normal oturumlarda hiç görülmemiş bir olay sayım vektörüne ulaşırsa anormaldir. Hız katmanında bu karar oturum kapandığında (ya da zaman aşımında) üretilir.
- **Erken uyarı (risk skoru)** DeepLog + EOS ile üretilir. Skor, oturum sürerken her olayda güncellenir; eşiği doğrulama setinde seçilir. Otonom aksiyon kararında erken uyarının ağırlığı ve yanlış alarm maliyeti M3–M4'te (K5, K8, K9) ölçülerek belirlenir.
- **Isolation Forest,** kabul edilen öneride taahhüt edilen yöntem olarak karşılaştırma için eğitilir ve raporlanır. Sistemde karar verici olarak kullanılmaz.
- Makaledeki DeepLog (g = 9) da literatürle karşılaştırma için raporlanır.

## Sonuçlar

- Formdaki ölçüt (Precision > %85) ve K3, sistemin oturum kararıyla sağlanır. K4 (DeepLog > Isolation Forest) ayrıca sağlanmıştır (McNemar p ≈ 10⁻²¹¹).
- **Sınırlılık 1:** Sayım yeniliği normal davranışın sayım desenlerini ezberler. Bu yüzden normal davranışı HDFS kadar düzenli olmayan sistemlerde yanlış alarm üretebilir. Bu durum İz B'de (demo sistem) ölçülecek ve raporda sınırlılık olarak yer alacak.
- **Sınırlılık 2:** DeepLog'un sonuçları makalenin raporladığından (F1 ≈ 0,96) düşüktür. Olası nedenler şunlardır:
  - kronolojik bölme (makale rastgele bir alt küme kullanmıştı);
  - doğrulama ve test setleri arasındaki anormal oranı farkı (%3,40 / %2,16);
  - eğitim kararlılığı.

  Tohumlar arası oynaklık için ağırlığa orantılı örneklemeli eğitim inceleniyor.
- **Sapma kaydı (GOV-05):** Kabul edilen önerideki "Isolation Forest ile anomali tespiti" yerine, karar verici olarak sayım yeniliği ve DeepLog kullanılıyor; Isolation Forest karşılaştırmada kalıyor.

## Değerlendirilen alternatifler

- **Yalnızca DeepLog:** Formdaki Precision ölçütünü sağlamıyor.
- **Yalnızca sayım yeniliği:** Erken uyarı veremiyor; öngörü (K9, H2) ölçülemez.
- **Isolation Forest'ı karar verici yapmak:** Zayıf ve kararsız.
