# Remediator (REM-04..06, ADR-0005): Ansible + Docker API bağlantısı. Kod imaja kopyalanmaz; depo kökü
# /opt/deeplog'a salt okunur bağlanır (docker-compose.yml).
# Python 3.12: güncel ansible-core denetleyici olarak 3.10'u desteklemiyor. Hedef (demo-app) 3.10'dur.
FROM python:3.12.12-slim-bookworm

RUN pip install --no-cache-dir ansible-core==2.21.6 requests==2.34.2 confluent-kafka==2.16.2 \
    && ansible-galaxy collection install --collections-path /usr/share/ansible/collections \
       community.docker:==5.4.1 \
    && ansible-galaxy collection list --collections-path /usr/share/ansible/collections

USER nobody
WORKDIR /opt/deeplog
ENV PYTHONPATH=/opt/deeplog PYTHONUNBUFFERED=1 HOME=/tmp \
    ANSIBLE_HOST_KEY_CHECKING=False ANSIBLE_STDOUT_CALLBACK=default ANSIBLE_NOCOLOR=1 \
    ANSIBLE_LOCAL_TEMP=/tmp/.ansible-local
