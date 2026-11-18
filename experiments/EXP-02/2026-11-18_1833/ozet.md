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
| DeepLog (makaledeki hali, g doğrulamada seçildi) | 0.6780 ± 0.0512 | 0.5961 ± 0.1235 | 0.6269 ± 0.0603 | [0.748, 0.782] | g ∈ [3, 4, 6] |
| DeepLog + oturum sonu (EOS) | 0.7625 ± 0.0489 | 0.6786 ± 0.0848 | 0.7168 ± 0.0637 | [0.787, 0.815] | g ∈ [5, 6] |
| Isolation Forest (ızgarada seçilen ayar) | 0.8240 ± 0.0141 | 0.3010 ± 0.1382 | 0.4272 ± 0.1247 | [0.807, 0.852] | max_samples=tümü, n=300; skor eşiği |
| Olay sayımı yeniliği | 0.9795 ± 0.0000 | 1.0000 ± 0.0000 | 0.9896 ± 0.0000 | [0.974, 0.984] | – |
| DeepLog (makaledeki sabit g=9, karşılaştırma için) | 0.8299 ± 0.0344 | 0.3808 ± 0.0144 | 0.5215 ± 0.0082 | – | g = 9 |

## McNemar karşılaştırmaları (test, medyan-F1 tohumu)

b: ilk yöntem doğru, ikinci yanlış; c: tersi.

| Karşılaştırma | b | c | p |
|---|---|---|---|
| deeplog_eos vs iforest | 1,708 | 489 | 7.21e-149 |
| novelty vs deeplog_eos | 1,714 | 0 | 0 |
| novelty vs iforest | 2,933 | 0 | 0 |

## Isolation Forest ızgarası (doğrulama seti; tüm tohumların ortalaması ± std)

| max_samples | n_estimators | Doğrulama F1 |
|---|---|---|
| 256 | 100 | 0.2051 ± 0.0028 |
| 256 | 300 | 0.2051 ± 0.0028 |
| 1024 | 100 | 0.4572 ± 0.0566 |
| 1024 | 300 | 0.4322 ± 0.0025 |
| 4096 | 100 | 0.5768 ± 0.0020 |
| 4096 | 300 | 0.5782 ± 0.0001 |
| 16384 | 100 | 0.6014 ± 0.0001 |
| 16384 | 300 | 0.6014 ± 0.0000 |
| 65536 | 100 | 0.6201 ± 0.0022 |
| 65536 | 300 | 0.6652 ± 0.0601 |
| tümü | 100 | 0.6703 ± 0.0496 |
| tümü | 300 | 0.6706 ± 0.0494 |

## Ayarlar

```
commit: b4091ab
tohumlar: [0, 1, 2, 3, 4]
DeepLog: h=10, LSTM 2x64, epoch=50, batch=256, lr=0.001
iş parçacığı: 3
süre: 2596 sn
```
