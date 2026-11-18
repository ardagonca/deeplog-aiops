"""INF-06 duman testi (Spark tarafı): infra/smoke_test.sh çağırır.

Kafka topic'ini batch olarak okur, executor'larda pandas/pyarrow/torch/drain3'ün çalıştığını
doğrular, sonucu HDFS'e Parquet olarak yazar ve geri okuyup kayıt sayısını karşılaştırır.
Kullanım: smoke_test.py <topic> <beklenen_kayıt_sayısı> <hdfs_yolu>
"""

import sys
from collections.abc import Iterator

import pandas as pd
from pyspark.sql import SparkSession


def _check_executor_libs(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    import drain3  # noqa: F401  (executor imajında kurulu olduğunu doğrular)
    import torch

    for batch in batches:
        values = torch.tensor(batch["value"].astype(int).to_numpy())
        yield pd.DataFrame({"value": (values * 2).numpy()})


def main(topic: str, expected: int, path: str) -> None:
    spark = SparkSession.builder.appName(f"smoke-{topic}").getOrCreate()
    source = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
    )
    source.mapInPandas(_check_executor_libs, "value long").write.mode("error").parquet(path)

    back = spark.read.parquet(path)
    total, unique = back.count(), back.distinct().count()
    want = {2 * i for i in range(1, expected + 1)}
    got = {row.value for row in back.collect()}
    assert total == unique == expected and got == want, (
        f"beklenen {expected} benzersiz kayıt, okunan {total} (benzersiz {unique})"
    )
    print(f"SMOKE OK: {total} kayıt Kafka -> Spark (pandas/torch/drain3) -> HDFS -> Spark")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]), sys.argv[3])
