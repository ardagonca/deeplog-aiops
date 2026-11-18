# ML-02: HDFS_v1 oturumları ve kronolojik bölme

`ml.evaluate_sessions` üretir; elle düzenlenmez. Olay kimliği: Drain (sim_th=0,5). Commit: 4ff7823

## Loghub resmi Event_traces.csv ile karşılaştırma

| Ölçüt | Değer |
|---|---|
| Bizim blok sayımız | 575,061 |
| Resmi blok sayısı | 575,061 |
| Yalnızca bizde / yalnızca resmide | 0 / 0 |
| Dizi uzunluğu uyumu | 100.0000% (575,061 / 575,061) |
| Etiket uyuşmazlığı (anomaly_label.csv ↔ Event_traces) | 0 |

## Kronolojik bölme (60/10/30, eğitimde yalnızca normal)

| Bölme | Oturum | Anormal | Anormal oranı |
|---|---|---|---|
| train | 333,878 | 0 | 0.00% |
| val | 57,506 | 1,957 | 3.40% |
| test | 172,519 | 3,723 | 2.16% |
