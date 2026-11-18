# Genelleme denetimi: `hdfs-v1`

`ml.generalization` üretir; elle düzenlenmez. Yanlış alarm = normal oturumların anormal sayılma oranı. Eğitimde belirgin şekilde düşük olması ezberleme belirtisidir.

DeepLog + EOS (g = 5, paket commit'i f3e89a6); sayım yeniliği 207 bilinen desen.

| Bölme | Normal oturum | DeepLog yanlış alarm | DeepLog recall | Sayım yanlış alarm | Sayım recall |
|---|---|---|---|---|---|
| train | 333,878 | 0.5625% | – | 0.0000% | – |
| val | 55,549 | 0.8857% | 0.9177 | 0.0036% | 1.0000 |
| test | 168,796 | 0.5533% | 0.7956 | 0.0462% | 1.0000 |

## Paketin test sonucu (bir kez)

| Model | Precision | Recall | F1 |
|---|---|---|---|
| DeepLog + EOS | 0.7603 | 0.7956 | 0.7775 |
| Sayım yeniliği | 0.9795 | 1.0000 | 0.9896 |
