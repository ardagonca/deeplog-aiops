"""EXP-01 (K1 alım hızı, K2 veri kaybı): tek bir replayer koşusunun HDFS'e yazılma başarımı.

Kullanım: exp01_analyze.py --source exp01-r5000-1-... --sent 3300000 --rate 5000
Koşunun kayıtları kaynak adıyla seçilir; ingest yetişene kadar (en fazla --timeout) beklenir, sonra
"EXP01_RESULT {...}" satırı basılır. Tanımlar: docs/04-test-stratejisi.md §2 ve §4.
- Isınma (--warmup, varsayılan 60 sn) sonrası "kararlı bölüm" ölçülür.
- ingest_rate = kararlı bölümdeki kayıt sayısı / bu kayıtların HDFS'e yazılma süresi.
- Gecikme (lag) = ingest_time - event_time. Ingest yetişemiyorsa gecikme dakikadan dakikaya büyür;
  keeps_up bunu, ilk ve son kararlı dakikanın p95 gecikmesini karşılaştırarak söyler.
"""

import argparse
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.ingest_raw import STREAMS

RAW_LOGS = next(s.raw_path for s in STREAMS if s.topic == "logs.raw")
LAG_GROWTH_LIMIT = 30.0  # sn; ilk ve son dakika p95 gecikme farkı bunu aşarsa ingest yetişemiyor


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--sent", type=int, required=True)
    parser.add_argument("--rate", type=int, required=True)
    parser.add_argument("--warmup", type=int, default=60)
    parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"exp01-{args.source}").getOrCreate()
    deadline = time.monotonic() + args.timeout
    while True:  # ingest 30 sn'lik micro-batch'lerle yazar; tüm kayıtlar gelene kadar bekle
        df = spark.read.parquet(RAW_LOGS).where(F.col("source") == args.source)
        if df.count() >= args.sent or time.monotonic() > deadline:
            break
        time.sleep(15)

    seconds = lambda c: F.col(c).cast("double")  # noqa: E731
    df = df.withColumn("lag", seconds("ingest_time") - seconds("event_time")).cache()
    total = df.agg(
        F.count("*").alias("written"),
        F.countDistinct("id").alias("unique"),
        F.min("event_time").alias("start"),
    ).first()
    steady = df.where(seconds("event_time") >= total.start.timestamp() + args.warmup)
    s = steady.agg(
        F.count("*").alias("n"),
        (F.max(seconds("event_time")) - F.min(seconds("event_time"))).alias("send_secs"),
        (F.max(seconds("ingest_time")) - F.min(seconds("ingest_time"))).alias("ingest_secs"),
        F.percentile_approx("lag", 0.5).alias("p50"),
        F.percentile_approx("lag", 0.95).alias("p95"),
        F.max("lag").alias("max"),
    ).first()
    minutes = (
        steady.groupBy(F.window("event_time", "1 minute").alias("w"))
        .agg(F.percentile_approx("lag", 0.95).alias("p95"))
        .orderBy("w")
        .collect()
    )
    first_p95, last_p95 = minutes[0].p95, minutes[-1].p95
    result = {
        "source": args.source,
        "rate_target": args.rate,
        "sent": args.sent,
        "written": total.written,
        "unique": total.unique,
        "duplicates": total.written - total.unique,
        "lost": args.sent - total.unique,
        "steady_records": s.n,
        "input_rate": round(s.n / s.send_secs, 1),
        "ingest_rate": round(s.n / s.ingest_secs, 1),
        "lag_p50": round(s.p50, 1),
        "lag_p95": round(s.p95, 1),
        "lag_max": round(s.max, 1),
        "lag_p95_first_min": round(first_p95, 1),
        "lag_p95_last_min": round(last_p95, 1),
        "keeps_up": last_p95 - first_p95 <= LAG_GROWTH_LIMIT,
    }
    print("EXP01_RESULT " + json.dumps(result), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
