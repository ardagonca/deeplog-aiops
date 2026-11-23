# Demo hedef sistem (REM-01, ADR-0011). Kod imaja kopyalanmaz; depo kökü /opt/deeplog'a salt okunur
# bağlanır (docker-compose.yml).
FROM python:3.10.22-slim-bookworm

# Sürümler uv.lock ile aynı tutulur (uvicorn yalnızca bu imajda).
RUN pip install --no-cache-dir fastapi==0.162.5 starlette==1.7.0 pydantic==2.14.4 \
    uvicorn==0.54.0 confluent-kafka==2.16.2

USER nobody
WORKDIR /opt/deeplog
ENV PYTHONPATH=/opt/deeplog PYTHONUNBUFFERED=1
