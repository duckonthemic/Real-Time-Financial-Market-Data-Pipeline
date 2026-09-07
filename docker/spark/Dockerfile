ARG SPARK_BASE_IMAGE=apache/spark:3.5.5
FROM maven:3.9.9-eclipse-temurin-17 AS resolver
WORKDIR /closure
COPY docker/spark/pom.xml .
RUN mvn -B -q dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=/closure/jars

FROM ${SPARK_BASE_IMAGE}

USER root
RUN python3 -m pip install --no-cache-dir --upgrade pip==24.3.1 \
    && python3 -m pip install --no-cache-dir cassandra-driver==3.29.2 'confluent-kafka==2.6.1' fastavro==1.9.7
COPY --from=resolver /closure/jars/ /opt/spark/jars/
COPY docker/spark/jars.lock.json /opt/market-pipeline/docker/spark/jars.lock.json
COPY src/ /opt/market-pipeline/src/
COPY schemas/ /opt/market-pipeline/schemas/
COPY fixtures/ /opt/market-pipeline/fixtures/
ENV PYTHONPATH=/opt/market-pipeline/src \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /opt/market-pipeline
RUN mkdir -p /checkpoints && chown -R 185:185 /checkpoints
USER 185
