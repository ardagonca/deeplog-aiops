FROM apache/spark:4.2.1-scala2.13-java17-python3-ubuntu

USER root

# Kafka kaynağı temel imajda yok. Sürümler spark-parent_2.13:4.2.1 POM'undaki
# kafka.version ve commons-pool2.version ile aynı olmalı; Spark sürümü değişirse birlikte güncellenir.
RUN set -eux; cd /opt/spark/jars; M=https://repo1.maven.org/maven2; \
    for p in \
      org/apache/spark/spark-sql-kafka-0-10_2.13/4.2.1/spark-sql-kafka-0-10_2.13-4.2.1.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.2.1/spark-token-provider-kafka-0-10_2.13-4.2.1.jar \
      org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar \
      org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar; do \
      f=$(basename "$p"); \
      curl -fsSLO "$M/$p"; \
      echo "$(curl -fsSL "$M/$p.sha1" | cut -c1-40)  $f" | sha1sum -c -; \
    done

# requirements.txt, requirements.in'den üretilir (bkz. docs/runbooks/altyapi.md).
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu \
      -r /tmp/requirements.txt && rm /tmp/requirements.txt

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
