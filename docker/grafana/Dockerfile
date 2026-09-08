ARG GRAFANA_IMAGE=grafana/grafana:11.4.0
FROM ${GRAFANA_IMAGE}
ARG GF_PLUGIN_VERSION=3.3.0
RUN grafana cli plugins install hadesarchitect-cassandra-datasource ${GF_PLUGIN_VERSION}
