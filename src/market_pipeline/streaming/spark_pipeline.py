"""Spark expressions and concrete Cassandra/Kafka writers for recovery_pipeline."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from market_pipeline.contracts.models import RunConfig
from market_pipeline.ops.runtime import artifact, atomic_json
from market_pipeline.streaming.batch import BatchFrames


def derive_batch_bounds(frame: Any) -> dict[int, tuple[int, int]]:
    from pyspark.sql import functions as functions

    rows = frame.groupBy("partition").agg(
        functions.min("offset").alias("start"),
        (functions.max("offset") + functions.lit(1)).alias("end"),
    ).collect()
    return {int(row["partition"]): (int(row["start"]), int(row["end"])) for row in rows}


def _header_text(name: str) -> Any:
    from pyspark.sql import functions as functions

    return functions.decode(functions.element_at(functions.col("header_map"), functions.lit(name)), "UTF-8")


def transform_batch(frame: Any, config: RunConfig, schema_json: str, min_event_time_ms: int, max_event_time_ms: int) -> BatchFrames:
    from pyspark.sql import functions as functions
    from pyspark.sql.avro.functions import from_avro

    with_headers = frame.withColumn("header_map", functions.map_from_entries(functions.col("headers")))
    enriched = (
        with_headers
        .withColumn("claimed_run_id", _header_text("run_id"))
        .withColumn("claimed_dataset_id", _header_text("dataset_id"))
        .withColumn("claimed_source_sequence", _header_text("source_sequence").cast("long"))
        .withColumn("claimed_payload_type", _header_text("payload_type"))
        .withColumn("claimed_produced_at_ms", _header_text("produced_at_ms").cast("long"))
        .withColumn("magic_hex", functions.hex(functions.substring("value", 1, 1)))
        .withColumn("wire_schema_id", functions.conv(functions.hex(functions.substring("value", 2, 4)), 16, 10).cast("int"))
        .withColumn(
            "wire_ok",
            (functions.length("value") > 5)
            & (functions.col("magic_hex") == functions.lit("00"))
            & (functions.col("wire_schema_id") == functions.lit(config.schema_id)),
        )
    )
    decoded = enriched.withColumn(
        "event",
        from_avro(
            functions.when(
                functions.col("wire_ok"),
                functions.substring("value", 6, 2_147_483_647),
            ),
            schema_json,
            {"mode": "PERMISSIVE"},
        ),
    )
    required_headers_ok = (
        functions.col("claimed_run_id").isNotNull()
        & (functions.length("claimed_run_id") > 0)
        & functions.col("claimed_dataset_id").isNotNull()
        & (functions.length("claimed_dataset_id") > 0)
        & functions.col("claimed_source_sequence").isNotNull()
        & (functions.col("claimed_source_sequence") >= 0)
        & functions.col("claimed_produced_at_ms").isNotNull()
        & (functions.col("claimed_produced_at_ms") > 0)
        & functions.col("claimed_payload_type").isin("avro-confluent-v1", "malformed-test-v1")
    )
    expected_event_id = functions.sha2(
        functions.concat(
            functions.col("claimed_dataset_id"),
            functions.lit("\x00"),
            functions.col("claimed_source_sequence").cast("string"),
        ),
        256,
    )
    attribution_ok = (
        (functions.col("claimed_run_id") == functions.lit(config.run_id))
        & (functions.col("claimed_dataset_id") == functions.lit(config.dataset_id))
        & (functions.col("event.run_id") == functions.col("claimed_run_id"))
        & (functions.col("event.dataset_id") == functions.col("claimed_dataset_id"))
        & (functions.col("event.source_sequence") == functions.col("claimed_source_sequence"))
        & (functions.col("event.event_id") == expected_event_id)
        & (functions.col("event.schema_version") == functions.lit(1))
    )
    rejection = (
        functions.when(~required_headers_ok, functions.lit("MISSING_OR_INVALID_HEADER"))
        .when(~functions.col("wire_ok") | functions.col("event").isNull(), functions.lit("BAD_WIRE"))
        .when(~attribution_ok, functions.lit("HEADER_PAYLOAD_MISMATCH"))
        .when(functions.length(functions.trim(functions.col("event.symbol"))) == 0, functions.lit("EMPTY_SYMBOL"))
        .when(
            functions.col("event.price").isNull()
            | functions.isnan(functions.col("event.price"))
            | (functions.abs(functions.col("event.price")) == functions.lit(float("inf")))
            | (functions.col("event.price") <= 0),
            functions.lit("NON_POSITIVE_PRICE"),
        )
        .when(functions.col("event.volume").isNull() | (functions.col("event.volume") <= 0), functions.lit("NON_POSITIVE_VOLUME"))
        .when(
            (functions.col("event.event_time_ms") < functions.lit(min_event_time_ms))
            | (functions.col("event.event_time_ms") > functions.lit(max_event_time_ms)),
            functions.lit("TIMESTAMP_OUT_OF_RANGE"),
        )
    )
    classified = decoded.withColumn("rejection_code", rejection).withColumn(
        "rejection_detail",
        functions.when(functions.col("rejection_code").isNotNull(), functions.concat(functions.lit("Rejected by deterministic rule: "), functions.col("rejection_code"))),
    )
    bronze = classified.select(
        functions.lit(config.run_id).alias("owner_run_id"),
        functions.col("topic").alias("source_topic"),
        functions.col("partition").alias("source_partition"),
        functions.col("offset").alias("source_offset"),
        functions.col("timestamp").alias("kafka_timestamp"),
        functions.col("key").alias("kafka_key"),
        functions.col("value").alias("raw_value"),
        functions.when(functions.col("magic_hex") == "00", functions.col("wire_schema_id")).alias("schema_id"),
        "claimed_run_id",
        "claimed_dataset_id",
        "claimed_source_sequence",
        "claimed_payload_type",
        "claimed_produced_at_ms",
    )
    accepted = classified.where(functions.col("rejection_code").isNull())
    event_timestamp = functions.timestamp_millis(functions.col("event.event_time_ms"))
    silver = accepted.select(
        functions.lit(config.run_id).alias("run_id"),
        functions.col("event.event_id").alias("event_id"),
        functions.upper(functions.trim(functions.col("event.symbol"))).alias("symbol"),
        event_timestamp.alias("event_time"),
        functions.to_date(event_timestamp).alias("event_date"),
        functions.col("event.price").alias("price"),
        functions.col("event.volume").alias("volume"),
        functions.col("event.conditions").alias("conditions"),
        functions.col("event.source_sequence").alias("source_sequence"),
    )
    dlq = classified.where(functions.col("rejection_code").isNotNull()).select(
        functions.lit(config.run_id).alias("owner_run_id"),
        functions.col("topic").alias("source_topic"),
        functions.col("partition").alias("source_partition"),
        functions.col("offset").alias("source_offset"),
        "claimed_run_id",
        "rejection_code",
        "rejection_detail",
        functions.col("value").alias("raw_value"),
        functions.col("timestamp").alias("source_timestamp"),
    )
    input_count = int(classified.count())
    valid_count = int(silver.count())
    invalid_count = int(dlq.count())
    return BatchFrames(
        bronze=bronze,
        silver=silver,
        dlq=dlq,
        counts={"input": input_count, "valid": valid_count, "invalid": invalid_count},
    )


class CassandraWriter:
    def __init__(self, keyspace: str, table: str):
        self.keyspace = keyspace
        self.table = table

    def write(self, frame: Any) -> None:
        frame.write.format("org.apache.spark.sql.cassandra").mode("append").options(
            keyspace=self.keyspace,
            table=self.table,
        ).save()


class SilverWriter(CassandraWriter):
    def __init__(self, keyspace: str):
        super().__init__(keyspace, "silver_events_by_run")

    def write(self, frame: Any) -> None:
        from pyspark.sql import functions as functions

        super().write(frame)
        projection = frame.select(
            "run_id", "symbol", "event_date", "event_time", "event_id", "price", "volume", "conditions"
        )
        projection.write.format("org.apache.spark.sql.cassandra").mode("append").options(
            keyspace=self.keyspace,
            table="silver_events_by_symbol_day",
        ).save()


class KafkaDlqPublisher:
    def __init__(self, bootstrap_servers: str, topic: str):
        from confluent_kafka import Producer

        self.topic = topic
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
            }
        )

    def write(self, frame: Any) -> None:
        errors: list[str] = []

        def callback(error: Any, message: Any) -> None:
            if error is not None:
                errors.append(str(error))

        for row in frame.collect():
            key = f"{row.source_topic}:{row.source_partition}:{row.source_offset}"
            value = json.dumps(
                {
                    "owner_run_id": row.owner_run_id,
                    "coordinate": key,
                    "rejection_code": row.rejection_code,
                },
                separators=(",", ":"),
            ).encode("utf-8")
            while True:
                try:
                    self.producer.produce(self.topic, key=key.encode("utf-8"), value=value, on_delivery=callback)
                    break
                except BufferError:
                    self.producer.poll(0.1)
            self.producer.poll(0)
        remaining = self.producer.flush(15)
        if errors or remaining:
            raise RuntimeError(f"Kafka DLQ acknowledgement failed: errors={len(errors)}, remaining={remaining}")


class CassandraLedger:
    def __init__(self, config: RunConfig):
        from cassandra.cluster import Cluster

        self.config = config
        self.cluster = Cluster([config.cassandra_host])
        self.session = self.cluster.connect(config.cassandra_keyspace)
        parsed = json.loads(config.start_offsets_json).get(config.input_topic, {})
        self.start_offsets = {int(partition): int(offset) for partition, offset in parsed.items()}
        self.cumulative = dict(self.start_offsets)
        self.progress_path = Path(config.artifact_path) / "streaming-progress.json"

    def status(self, run_id: str, query_name: str, batch_id: int) -> str | None:
        row = self.session.execute(
            "SELECT status, cumulative_next_offsets FROM stream_batches_by_query WHERE run_id=%s AND query_name=%s AND batch_id=%s",
            (run_id, query_name, batch_id),
        ).one()
        if row and row.cumulative_next_offsets:
            self.cumulative.update({int(key): int(value) for key, value in row.cumulative_next_offsets.items()})
        if row and row.status == "COMPLETED":
            processed = sum(
                self.cumulative.get(partition, start) - start
                for partition, start in self.start_offsets.items()
            )
            atomic_json(
                self.progress_path,
                artifact(
                    "streaming-progress",
                    run_id,
                    state="RUNNING",
                    records=processed,
                    latest_batch_id=batch_id,
                    cumulative_next_offsets=self.cumulative,
                    replay_skipped=True,
                ),
            )
        return row.status if row else None

    def _attempt_count(self, run_id: str, query_name: str, batch_id: int) -> int:
        row = self.session.execute(
            "SELECT attempt_count FROM stream_batches_by_query WHERE run_id=%s AND query_name=%s AND batch_id=%s",
            (run_id, query_name, batch_id),
        ).one()
        return int(row.attempt_count or 0) if row else 0

    def started(self, run_id: str, query_name: str, batch_id: int, bounds: Mapping[int, tuple[int, int]]) -> None:
        starts = {partition: pair[0] for partition, pair in bounds.items()}
        ends = {partition: pair[1] for partition, pair in bounds.items()}
        self.session.execute(
            "INSERT INTO stream_batches_by_query (run_id,query_name,batch_id,status,batch_start_offsets,batch_end_offsets_exclusive,attempt_count,started_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (run_id, query_name, batch_id, "STARTED", starts, ends, self._attempt_count(run_id, query_name, batch_id) + 1, datetime.now(timezone.utc)),
        )

    def completed(self, run_id: str, query_name: str, batch_id: int, counts: Mapping[str, int], bounds: Mapping[int, tuple[int, int]]) -> None:
        for partition, (_, end) in bounds.items():
            self.cumulative[partition] = max(self.cumulative.get(partition, end), end)
        now = datetime.now(timezone.utc)
        self.session.execute(
            "UPDATE stream_batches_by_query SET status=%s,input_count=%s,valid_count=%s,invalid_count=%s,cumulative_next_offsets=%s,completed_at=%s WHERE run_id=%s AND query_name=%s AND batch_id=%s",
            ("COMPLETED", counts["input"], counts["valid"], counts["invalid"], self.cumulative, now, run_id, query_name, batch_id),
        )
        processed = sum(self.cumulative.get(partition, start) - start for partition, start in self.start_offsets.items())
        atomic_json(
            self.progress_path,
            artifact(
                "streaming-progress",
                run_id,
                state="RUNNING",
                records=processed,
                latest_batch_id=batch_id,
                cumulative_next_offsets=self.cumulative,
            ),
        )

    def failed(self, run_id: str, query_name: str, batch_id: int, error: str) -> None:
        self.session.execute(
            "UPDATE stream_batches_by_query SET status=%s,last_error=%s WHERE run_id=%s AND query_name=%s AND batch_id=%s",
            ("FAILED", error[:1000], run_id, query_name, batch_id),
        )
        atomic_json(self.progress_path, artifact("streaming-progress", run_id, state="FAILED", records=0, error=error[:1000]))


def build_spark_session(config: RunConfig) -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("recovery_pipeline")
        .config("spark.cassandra.connection.host", config.cassandra_host)
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.streaming.stopGracefullyOnShutdown", "false")
        .getOrCreate()
    )
