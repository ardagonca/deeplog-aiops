# Python ajanları (metrics-agent; ileride remediator) için hafif imaj.
# Kod imaja kopyalanmaz; depo kökü /opt/deeplog'a salt okunur bağlanır (docker-compose.yml).
FROM python:3.10.22-slim-bookworm

# procps: ps/pkill (hata ayıklama ve infra/failure_test.sh'teki çökme senaryosu için).
RUN apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

# Sürüm uv.lock ile aynı tutulur.
RUN pip install --no-cache-dir confluent-kafka==2.16.2

USER nobody
WORKDIR /opt/deeplog
ENV PYTHONPATH=/opt/deeplog PYTHONUNBUFFERED=1
