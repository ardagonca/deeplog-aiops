"""Olay sayımı yeniliği: eğitimdeki normal oturumlarda hiç görülmemiş bir olay sayım vektörüne
sahip oturum anormaldir. Etiket kullanmaz; yalnızca normal davranışın sayım desenlerini ezberler.

HDFS_v1'de normal davranış çok düzenlidir (333.878 eğitim oturumunda 207 farklı sayım vektörü);
EXP-02 bu basit referans yöntemi DeepLog ve Isolation Forest ile aynı protokolde karşılaştırır.
Oturum tamamlanmadan karar veremez; erken uyarı DeepLog'un işidir.
"""

import numpy as np


class CountNovelty:
    def fit(self, X: np.ndarray) -> "CountNovelty":
        self.known = {row.tobytes() for row in np.asarray(X, dtype=np.int32)}
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Görülmemiş sayım vektörü -> True (anormal)."""
        return np.fromiter(
            (row.tobytes() not in self.known for row in np.asarray(X, dtype=np.int32)),
            dtype=bool,
            count=len(X),
        )
