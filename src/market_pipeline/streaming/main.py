"""Launch the single run-scoped Structured Streaming recovery query."""

from __future__ import annotations

from functools import partial
from pathlib import Path

from market_pipeline.ops.runtime import manifest_from_env, run_config_from_env
from market_pipeline.streaming.batch import RecoveryBatchDependencies, process_recovery_batch
from market_pipeline.streaming.spark_pipeline import (
    CassandraLedger,
    CassandraWriter,
    KafkaDlqPublisher,
    SilverWriter,
    build_spark_session,
    derive_batch_bounds,
    transform_batch,
)


def main() -> int:
    config = run_config_from_env()
    manifest = manifest_from_env()
    schema_json = Path(config.schema_path).read_text(encoding="utf-8")
    spark = build_spark_session(config)
    ledger = CassandraLedger(config)
    dependencies = RecoveryBatchDependencies(
        run_id=config.run_id,
        query_name="recovery_pipeline",
        ledger=ledger,
        bronze_writer=CassandraWriter(config.cassandra_keyspace, "bronze_records_by_run_partition"),
        silver_writer=SilverWriter(config.cassandra_keyspace),
        dlq_writer=CassandraWriter(config.cassandra_keyspace, "dlq_records_by_run"),
        dlq_publisher=KafkaDlqPublisher(config.kafka_bootstrap_servers, config.dlq_topic),
        derive_bounds=derive_batch_bounds,
        transform=partial(
            transform_batch,
            config=config,
            schema_json=schema_json,
            min_event_time_ms=int(manifest["event_time_min_ms"]),
            max_event_time_ms=int(manifest["event_time_max_ms"]),
        ),
    )
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.input_topic)
        .option("startingOffsets", config.start_offsets_json)
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        .option("includeHeaders", "true")
        .option("failOnDataLoss", "true")
        .load()
    )
    query = (
        source.writeStream.queryName("recovery_pipeline")
        .foreachBatch(
            lambda frame, batch_id: process_recovery_batch(frame, int(batch_id), dependencies)
        )
        .outputMode("append")
        .trigger(processingTime=f"{config.trigger_seconds} seconds")
        .option("checkpointLocation", config.checkpoint_path)
        .start()
    )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
