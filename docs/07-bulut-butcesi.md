# Bulut (AWS) Bütçe Planı

- Hazırlanma: 23.11.2026. Fiyatlar ve kur bu tarihteki kamuya açık kaynaklardan alınmıştır; harcama başlamadan önce yeniden doğrulanır.
- Bütçe üst sınırı: **9.000 TL** (2209-A proje bütçesi). Bulut harcaması bu tutarı hiçbir koşulda aşmaz.
- İlgili: [Proje Tüzüğü §9](00-proje-tuzugu.md#9-kısıtlar-ve-varsayımlar), [Risk Kaydı R-13](05-risk-kaydi.md), [Mimari §8](01-mimari.md#8-ortamlar).

## 1. Neden bulut

Geliştirme, birim testleri ve HDFS_v1 deneyleri yerel makinede yapılır ve ücretsizdir. Bulut, yerel makinenin sınırını aşan iki deney için kullanılır:

1. **Büyük ölçekli veri seti:** Thunderbird (Loghub; 211.212.192 satır, 29,6 GB, etiketli). Ham veri, Kafka ve HDFS kopyalarıyla birlikte ~70–80 GB disk ister; yerel diskte yer yetersizdir.
2. **Gerçek dağıtık küme:** Birden çok düğümde Kafka, HDFS ve Spark worker'ları. Worker sayısına göre alım ve tespit hızının ölçeklenmesi ölçülür.

Öneride bulut, donanım yetmezse başvurulacak bir seçenek olarak yer alıyor ("Amazon AWS veya Google Cloud ... kredileri kullanılarak proje bulut ortamına taşınacaktır"). Mimari değişmez: aynı imajlar ve aynı compose yapısı EC2 üzerinde çalışır. Yönetilen servisler (MSK, EMR) kullanılmaz; hem saatlik sabit ücretleri vardır hem de önerideki mimariden uzaklaştırırlar.

## 2. Varsayımlar

| Varsayım | Değer | Kaynak |
|---|---|---|
| Bölge | eu-central-1 (Frankfurt) | – |
| Kur | ~50 TL/$ (29.10.2026: 49,84) | [Bigpara](https://bigpara.hurriyet.com.tr/doviz/dolar/) |
| Bütçe (dolar) | 9.000 TL ≈ 180 $ | – |
| Ödeme modeli | Kullandıkça öde; peşin ödeme ve abonelik yok | AWS |

## 3. Birim fiyatlar

| Kalem | Fiyat | Kaynak |
|---|---|---|
| m6i.4xlarge (16 vCPU, 64 GB), isteğe bağlı | 0,92 $/sa | [DoiT](https://www.doit.com/compute/spot/eu-central-1/m6i.4xlarge), [Devzero](https://www.devzero.io/instances/aws/m6i.4xlarge) |
| m6i.4xlarge, spot (değişken) | ~0,43–0,50 $/sa | aynı |
| m6i.xlarge (4 vCPU, 16 GB), isteğe bağlı | 0,23 $/sa | [DoiT](https://www.doit.com/compute/spot/eu-central-1/m6i.xlarge) |
| m6i.xlarge, spot (değişken) | ~0,11 $/sa | aynı |
| EBS gp3 disk | ~0,095 $/GB-ay | AWS fiyat sayfası (doğrulanacak) |
| S3 Standard | ~0,025 $/GB-ay | AWS fiyat sayfası (doğrulanacak) |
| Açık IPv4 adresi | 0,005 $/sa | AWS fiyat sayfası (doğrulanacak) |
| İnternete veri çıkışı | Ayda ilk 100 GB ücretsiz | AWS fiyat sayfası (doğrulanacak) |

## 4. Deney bazında tahmin (isteğe bağlı fiyat)

Süreler tahmindir ve yeniden koşu payı (×1,5) içerir. Thunderbird süreleri, aynı kodla yerelde BGL üzerinde ölçülen hızla yeniden hesaplanacaktır (bkz. §8).

| İş | Makine | Süre | Maliyet |
|---|---|---|---|
| Thunderbird: indirme, Kafka → HDFS oynatma (~6 sa, 10.000 log/sn), Spark ile şablonlama, eğitim, değerlendirme, canlı tespit | 1 × m6i.4xlarge | ~30 sa | 28 $ |
| Ölçeklenme deneyi: 1, 2 ve 3 worker × 3 tekrar, kurulum dahil | 5 × m6i.xlarge (Kafka; master + NameNode; 3 worker + DataNode) | ~22 sa | 26 $ |
| M6 son koşu: bulut KPI'larının tek koşuda yeniden üretilmesi | Tek makine 10 sa + küme 5 sa | – | 15 $ |
| EBS diskler (yalnızca deney süresince; bitince silinir) | – | – | 25 $ |
| S3: veri setleri ve sonuçlar (~40 GB × 6 ay) | – | – | 6 $ |
| Açık IPv4 adresleri | – | – | 2 $ |
| **Ara toplam** | | | **102 $** |
| Beklenmeyen giderler (%20) | | | 20 $ |
| **Toplam** | | | **~122 $ ≈ 6.100 TL** |

## 5. Senaryolar

| Senaryo | Dolar | TL (≈50) | Bütçenin |
|---|---|---|---|
| Spot makinelerle | ~82 $ | ~4.100 TL | %46 |
| **İsteğe bağlı fiyat (§4)** | ~122 $ | ~6.100 TL | %68 |
| İsteğe bağlı fiyat + %20 KDV ihtimali | ~146 $ | ~7.300 TL | %81 |
| Yeni hesap ücretsiz kredisi varsa (doğrulanacak) | 0 – 22 $ | 0 – 1.100 TL | ~%0–12 |

Her senaryo bütçe sınırının altında kalır. Spot makine kesilebilir; deneyler checkpoint'ten devam edebildiği için bu kabul edilebilir bir risktir.

## 6. Zamanlama

| Dönem | İş | AWS harcaması |
|---|---|---|
| Aralık 2026 – Mart 2027 | M4 otonom iyileştirme (yerel); BGL ile pencereleme kodu (yerel) | 0 $ |
| Mart – Nisan 2027 | M5: Thunderbird ve ölçeklenme deneyleri | ~80–90 $ |
| Nisan – Mayıs 2027 | M6: son koşu | ~20–30 $ |

## 7. Harcama denetimi

AWS'de varsayılan bir harcama tavanı yoktur; açık unutulan kaynak ücret yazmaya devam eder. Örnek: m6i.4xlarge bir ay açık kalırsa ~670 $ tutar, yani bütçenin 3,7 katı. Bu yüzden aşağıdaki denetimler ilk makine açılmadan önce kurulur:

| Denetim | Ayar |
|---|---|
| Bütçe alarmları (AWS Budgets) | 30 $, 60 $ ve 100 $'da e-posta |
| Bütçe aksiyonu | 130 $'da tüm EC2 makinelerini durdurur |
| Otomatik kapanma | Her makine açılışta belirlenen süre sonunda kendini kapatacak şekilde başlar |
| Temizlik | Deney bitince makineler ve EBS diskleri silinir; kalıcı olan yalnızca S3 |
| Etiketleme | Tüm kaynaklar `project=deeplog` etiketi taşır; temizlik betiği bu etikete göre çalışır |
| Takip | Deney günlerinde Billing panelindeki günlük maliyet kontrol edilir |

"Durdurulmuş" makine işlem ücreti almaz ama diski ve açık IP adresi ücret yazmaya devam eder. Bu yüzden deney sonunda kaynaklar durdurulmaz, silinir.

Güvenlik: portlar internete açılmaz, erişim SSH tüneliyle yapılır. AWS erişim anahtarları depoya girmez.

## 8. Açık konular

| Konu | Sorumlu | Durum |
|---|---|---|
| Harcamanın 2209-A bütçesinden yapılabilirliği (yurt dışı, kullanım bazlı, kredi kartıyla ödenen hizmet) ve faturaların sunulma biçimi | Proje yürütücüsü; üniversitenin proje birimi | Sorulacak |
| Güncel kur, KDV ve bankanın yurt dışı işlem komisyonu | Proje yürütücüsü | Harcama öncesi |
| Yeni hesaba tanınan ücretsiz kredi | Proje yürütücüsü | Hesap açılırken |
| Thunderbird işlem süresinin BGL ölçümüyle yeniden tahmini | Geliştirme | BGL çalışmasında |

Fiyatlar veya süreler değişirse bu belge güncellenir; harcamalar sonuç raporunda fiili tutarlarıyla raporlanır.
