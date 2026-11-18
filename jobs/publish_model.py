"""ML-07: Model paketini HDFS'e yükler: /opt/deeplog/data/models/<ad>/ -> /models/deeplog/<ad>/

Kullanım (infra/ dizininde):
  docker compose --profile jobs run --rm spark-job /opt/deeplog/jobs/publish_model.py hdfs-v1
Sürümler değiştirilmez: hedef dizin HDFS'te varsa iş hata verir.
"""

import sys

from pyspark.sql import SparkSession

LOCAL_ROOT = "/opt/deeplog/data/models"
HDFS_ROOT = "/models/deeplog"


def main(name: str) -> None:
    spark = SparkSession.builder.appName(f"publish-model-{name}").getOrCreate()
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    target = jvm.org.apache.hadoop.fs.Path(f"{HDFS_ROOT}/{name}")
    if fs.exists(target):
        raise SystemExit(f"{HDFS_ROOT}/{name} zaten var; sürümler değiştirilmez")
    fs.mkdirs(target.getParent())
    fs.copyFromLocalFile(
        False, False, jvm.org.apache.hadoop.fs.Path(f"file://{LOCAL_ROOT}/{name}"), target
    )
    for status in fs.listStatus(target):
        print(f"{status.getPath()}  {status.getLen():,} bayt")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1])
