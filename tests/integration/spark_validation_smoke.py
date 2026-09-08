"""Run with spark-submit inside the built image; no Kafka/Cassandra required."""

from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from market_pipeline.contracts.models import RunConfig
from market_pipeline.producer.encoding import encode_event, load_avro_schema, malformed_value
from market_pipeline.producer.fixture import iter_fixture, load_manifest
from market_pipeline.streaming.spark_pipeline import transform_batch


def main() -> None:
    root = Path("/opt/market-pipeline")
    manifest = load_manifest(root / "fixtures/standard/manifest.json")
    schema_path = root / "schemas/trade_event_v1.avsc"
    schema = load_avro_schema(schema_path)
    run_id = "run-spark-validation"
    config = RunConfig(
        run_id=run_id,
        dataset_id=manifest["dataset_id"],
        schema_id=1,
        schema_path=str(schema_path),
        input_topic="test",
        dlq_topic="test-dlq",
        kafka_bootstrap_servers="unused",
        schema_registry_url="unused",
        cassandra_host="unused",
        cassandra_keyspace="unused",
        checkpoint_path="unused",
        artifact_path="unused",
    )
    rows = []
    for offset, record in enumerate(iter_fixture(manifest, run_id)):
        value = (
            malformed_value(record.malformed_kind, 1)
            if record.event is None
            else encode_event(record.event, schema, 1)
        )
        rows.append(
            ("test", 0, offset, datetime.now(UTC), record.key.encode(), value, list(record.headers))
        )
    spark = SparkSession.builder.master("local[1]").appName("validation-regression").getOrCreate()
    try:
        frame = spark.createDataFrame(
            rows,
            "topic string, partition int, offset long, timestamp timestamp, key binary, "
            "value binary, headers array<struct<key:string,value:binary>>",
        )
        result = transform_batch(
            frame,
            config,
            schema_path.read_text(),
            manifest["event_time_min_ms"],
            manifest["event_time_max_ms"],
        )
        actual = Counter(row.rejection_code for row in result.dlq.collect())
        assert dict(actual) == manifest["rejection_counts"], (actual, manifest)
        assert result.counts["invalid"] == manifest["invalid_records"]
        print(f"PASS: Spark Avro validation matches all rejection reasons: {dict(actual)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
