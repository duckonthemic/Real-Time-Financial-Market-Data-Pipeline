ARG SPARK_BASE_IMAGE=apache/spark:3.5.5
ARG PYTHON_BASE_IMAGE=python:3.11.10-slim-bullseye

FROM ${PYTHON_BASE_IMAGE} AS python-runtime

FROM maven:3.9.9-eclipse-temurin-17 AS resolver
WORKDIR /closure
COPY docker/spark/pom.xml .
RUN mvn -B -q dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=/closure/jars

FROM ${SPARK_BASE_IMAGE}

USER root
COPY --from=python-runtime /usr/local/ /usr/local/
RUN python3 -m pip install --no-cache-dir --upgrade pip==24.3.1 \
    && python3 -m pip install --no-cache-dir cassandra-driver==3.29.2 'confluent-kafka==2.6.1' fastavro==1.9.7
COPY --from=resolver /closure/jars/ /opt/spark/jars/
COPY docker/spark/jars.lock.json /opt/market-pipeline/docker/spark/jars.lock.json
COPY docker/spark/verify_jars.py /opt/market-pipeline/docker/spark/verify_jars.py
RUN python3 /opt/market-pipeline/docker/spark/verify_jars.py
COPY src/ /opt/market-pipeline/src/
COPY schemas/ /opt/market-pipeline/schemas/
COPY fixtures/ /opt/market-pipeline/fixtures/
ENV PYTHONPATH=/opt/market-pipeline/src \
    PYSPARK_PYTHON=/usr/local/bin/python3.11 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /opt/market-pipeline
RUN python3 --version | grep '^Python 3\.11\.' \
    && python3 -m compileall -q /opt/market-pipeline/src \
    && python3 -c 'import market_pipeline.streaming.main'
RUN mkdir -p /checkpoints && chown -R 185:185 /checkpoints
USER 185
