FROM python:3.11.10-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/market-pipeline/src

RUN pip install --no-cache-dir \
      cassandra-driver==3.29.2 \
      'confluent-kafka[avro,schemaregistry]==2.6.1' \
      fastavro==1.9.7

WORKDIR /opt/market-pipeline
COPY src/ src/
COPY schemas/ schemas/
COPY fixtures/ fixtures/
COPY grafana/ grafana/

USER 65534:65534
CMD ["python", "-m", "market_pipeline.ops.provision"]
