# EXP-03 Özeti: Canlı tespit (spark-detect)

`ml.exp03` üretir; elle düzenlenmez.

- Test bölmesinden 20,000 oturum (432 anormal) eksiksiz olarak 5,000 log/sn ile oynatıldı; 902 alarm üretildi.
- **K5** (erken DeepLog alarmı gecikmesi p95 ≤ 10 sn): **SAĞLANDI** (p50 3.9 sn, p95 6.9 sn, max 8.9 sn, n = 372)

| Karar | Canlı P / R / F1 | Offline P / R / F1 | Canlı = offline (oturum oranı) |
|---|---|---|---|
| DeepLog + EOS | 0.7697 / 0.8125 / 0.7905 | 0.7697 / 0.8125 / 0.7905 | 100.0000% |
| Sayım yeniliği | 0.9686 / 1.0000 / 0.9841 | 0.9686 / 1.0000 / 0.9841 | 100.0000% |
