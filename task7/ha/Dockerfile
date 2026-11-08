FROM postgres:16-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv python3-pip curl && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/patroni && /opt/patroni/bin/pip install --no-cache-dir 'patroni[etcd3]>=4.0,<5' psycopg2-binary
COPY task7/ha/patroni.yml /etc/patroni.yml
USER postgres
ENTRYPOINT ["/opt/patroni/bin/patroni"]
CMD ["/etc/patroni.yml"]
