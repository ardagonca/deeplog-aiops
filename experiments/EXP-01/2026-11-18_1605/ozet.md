# EXP-01 Özeti: Alım hızı ve veri kaybı

Bu dosya `experiments.exp01_summary` tarafından üretilir; elle düzenlenmez.

## Sonuç

- **K1** (≥ 5,000 log/sn, 10 dk kesintisiz): **SAĞLANDI**
- **K2** (%0 veri kaybı, tekrar yok): **SAĞLANDI**

## Hız başına sonuçlar

| Hedef (log/sn) | Koşu | Gönderim hızı | Ingest hızı (ort. ± std) | p95 gecikme, en kötü (sn) | Gönderilen | Kayıp | Tekrar | Yetişti mi |
|---|---|---|---|---|---|---|---|---|
| 1,000 | 3 | 1,000 | 1,000 ± 0 | 28.5 | 1,980,000 | 0 | 0 | evet |
| 2,500 | 3 | 2,500 | 2,500 ± 0 | 28.5 | 4,950,000 | 0 | 0 | evet |
| 5,000 | 3 | 5,000 | 5,000 ± 0 | 28.5 | 9,900,000 | 0 | 0 | evet |
| 10,000 | 3 | 10,000 | 10,000 ± 0 | 29.1 | 19,800,000 | 0 | 0 | evet |

## Koşu ayarları

```
commit: e6672ae
hızlar (log/sn): 1000 2500 5000 10000
tekrar: 3
koşu süresi (sn): 660 (ilk 60 sn ısınma)
ingest tetikleyici: 30 sn, uygulama başına 1 çekirdek / 1 GB executor
başlangıç (UTC): 2026-11-18T13:05:14Z
--- devam: tekrar 3..3 ---
commit: f926ea7
hızlar (log/sn): 1000 2500 5000 10000
tekrar: 3..3
koşu süresi (sn): 660 (ilk 60 sn ısınma)
ingest tetikleyici: 30 sn, uygulama başına 1 çekirdek / 1 GB executor
başlangıç (UTC): 2026-11-18T15:33:19Z
bitiş (UTC): 2026-11-18T16:22:00Z
```
