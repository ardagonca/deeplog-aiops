# EXP-03 Özeti: Canlı tespit (spark-detect)

`ml.exp03` üretir; elle düzenlenmez.

- Test bölmesinden 20,000 oturum (432 anormal) eksiksiz olarak 5,000 log/sn ile oynatıldı; 902 alarm üretildi (aynı alert_id ile tekrar: 0).
- Oynatma başladıktan 30 sn sonra spark-detect yeniden başlatıldı (RT-08); kesinti süresindeki alarmların gecikmesi bu yüzden yüksektir.
- Erken DeepLog alarmı gecikmesi (kesinti dahil; K5 ölçümü değildir): (p50 10.5 sn, p95 25.9 sn, max 30.4 sn, n = 372)

| Karar | Canlı P / R / F1 | Offline P / R / F1 | Canlı = offline (oturum oranı) |
|---|---|---|---|
| DeepLog + EOS | 0.7697 / 0.8125 / 0.7905 | 0.7697 / 0.8125 / 0.7905 | 100.0000% |
| Sayım yeniliği | 0.9686 / 1.0000 / 0.9841 | 0.9686 / 1.0000 / 0.9841 | 100.0000% |
