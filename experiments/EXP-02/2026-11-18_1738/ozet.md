# EXP-02 Özeti: HDFS_v1 üzerinde anomali tespiti

`ml.exp02` üretir; elle düzenlenmez. Tüm seçimler (IF ızgarası, eşikler, DeepLog g) yalnızca doğrulama setinde yapıldı; test seti bir kez değerlendirildi.

## Sonuç (test seti)

| Yöntem | Formdaki ölçüt: Precision > %85 (her tohumda) | K3: P ≥ 0,90 ve R ≥ 0,90 |
|---|---|---|
| DeepLog (makaledeki hali, g doğrulamada seçildi) | **SAĞLANMADI** | **SAĞLANMADI** |
| DeepLog + oturum sonu (EOS) | **SAĞLANMADI** | **SAĞLANMADI** |
| Isolation Forest (ızgarada seçilen ayar) | **SAĞLANMADI** | **SAĞLANMADI** |
| Olay sayımı yeniliği | **SAĞLANDI** | **SAĞLANDI** |

**K4** (DeepLog F1 > Isolation Forest F1, McNemar p < 0,05): **SAĞLANDI**

## Test seti (5 tohum, ortalama ± std; sayım yeniliği deterministik)

| Yöntem | Precision | Recall | F1 | Precision %95 GA (medyan tohum) | Seçilen eşik |
|---|---|---|---|---|---|
| DeepLog (makaledeki hali, g doğrulamada seçildi) | 0.6964 ± 0.0626 | 0.5501 ± 0.0970 | 0.6106 ± 0.0613 | [0.666, 0.700] | g ∈ [4, 5, 6, 8] |
| DeepLog + oturum sonu (EOS) | 0.7258 ± 0.0596 | 0.6958 ± 0.1145 | 0.7075 ± 0.0798 | [0.801, 0.828] | g ∈ [5, 6, 8] |
| Isolation Forest (ızgarada seçilen ayar) | 0.7955 ± 0.0159 | 0.2921 ± 0.1753 | 0.4024 ± 0.1775 | [0.778, 0.834] | max_samples=65536, n=300; skor eşiği |
| Olay sayımı yeniliği | 0.9795 ± 0.0000 | 1.0000 ± 0.0000 | 0.9896 ± 0.0000 | [0.974, 0.984] | – |
| DeepLog (makaledeki sabit g=9, karşılaştırma için) | 0.7946 ± 0.0522 | 0.4213 ± 0.0460 | 0.5490 ± 0.0396 | – | g = 9 |

## McNemar karşılaştırmaları (test, medyan-F1 tohumu)

b: ilk yöntem doğru, ikinci yanlış; c: tersi.

| Karşılaştırma | b | c | p |
|---|---|---|---|
| deeplog_eos vs iforest | 1,980 | 450 | 3.15e-211 |
| novelty vs deeplog_eos | 1,650 | 0 | 0 |
| novelty vs iforest | 3,180 | 0 | 0 |

## Isolation Forest ızgarası (doğrulama seti, tohum 0)

| max_samples | n_estimators | Precision | Recall | F1 |
|---|---|---|---|---|
| 256 | 100 | 0.1144 | 0.9377 | 0.2039 |
| 256 | 300 | 0.1144 | 0.9377 | 0.2039 |
| 1024 | 100 | 0.4048 | 0.4660 | 0.4333 |
| 1024 | 300 | 0.4048 | 0.4660 | 0.4333 |
| 4096 | 100 | 0.4938 | 0.6975 | 0.5783 |
| 4096 | 300 | 0.4938 | 0.6975 | 0.5783 |
| 16384 | 100 | 0.8527 | 0.4645 | 0.6014 |
| 16384 | 300 | 0.8527 | 0.4645 | 0.6014 |
| 65536 | 100 | 0.9555 | 0.4604 | 0.6214 |
| 65536 | 300 | 0.7742 | 0.6919 | 0.7307 |
| tümü | 100 | 0.8675 | 0.5253 | 0.6544 |
| tümü | 300 | 0.8667 | 0.5217 | 0.6514 |

## Ayarlar

```
commit: 6893a3c
tohumlar: [0, 1, 2, 3, 4]
DeepLog: h=10, LSTM 2x64, epoch=50, batch=256, lr=0.001
iş parçacığı: 3
süre: 1478 sn
```
