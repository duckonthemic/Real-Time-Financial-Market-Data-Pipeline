"""Create topics, register the fixed Avro schema, create Cassandra tables, and capture start offsets."""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from typing import Any

from market_pipeline.ops.runtime import artifact, atomic_json, required_env


def _request_json(url: str, *, method: str = "GET", payload: dict[str, Any] | None = None) -> Any:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Content-Type", "application/vnd.schemaregistry.v1+json")
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def create_topics() -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": required_env("KAFKA_BOOTSTRAP_SERVERS")})
    input_topic = required_env("INPUT_TOPIC")
    dlq_topic = required_env("DLQ_TOPIC")
    partitions = int(required_env("INPUT_PARTITIONS"))
    futures = admin.create_topics(
        [
            NewTopic(
                input_topic,
                num_partitions=partitions,
                replication_factor=1,
                config={"cleanup.policy": "delete", "retention.ms": "86400000"},
            ),
            NewTopic(
                dlq_topic,
                num_partitions=partitions,
                replication_factor=1,
                config={"cleanup.policy": "compact,delete", "retention.ms": "604800000"},
            ),
        ]
    )
    for topic, future in futures.items():
        try:
            future.result(30)
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise RuntimeError(f"cannot create topic {topic}: {exc}") from exc


def register_schema() -> int:
    registry = required_env("SCHEMA_REGISTRY_URL").rstrip("/")
    subject = "market.trades.v1-value"
    schema_text = Path("schemas/trade_event_v1.avsc").read_text(encoding="utf-8")
    _request_json(
        f"{registry}/config/{subject}",
        method="PUT",
        payload={"compatibility": "BACKWARD_TRANSITIVE"},
    )
    result = _request_json(
        f"{registry}/subjects/{subject}/versions", method="POST", payload={"schema": schema_text}
    )
    return int(result["id"])


def create_cassandra_schema() -> None:
    from cassandra.cluster import Cluster

    cluster = Cluster([required_env("CASSANDRA_HOST")])
    session = cluster.connect()
    try:
        for path in (
            Path("schemas/cassandra/001_keyspace.cql"),
            Path("schemas/cassandra/002_tables.cql"),
        ):
            script = path.read_text(encoding="utf-8")
            for statement in (part.strip() for part in script.split(";") if part.strip()):
                session.execute(statement)
    finally:
        cluster.shutdown()


def capture_end_offsets(topic: str, partitions: int) -> dict[int, int]:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": required_env("KAFKA_BOOTSTRAP_SERVERS"),
            "group.id": f"provision-{required_env('RUN_ID')}",
            "enable.auto.commit": False,
        }
    )
    try:
        result = {}
        for partition in range(partitions):
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10, cached=False
            )
            result[partition] = high
        return result
    finally:
        consumer.close()


def spark_starting_offsets(topic: str, offsets: dict[int, int]) -> str:
    return json.dumps(
        {topic: {str(partition): offset for partition, offset in offsets.items()}},
        separators=(",", ":"),
    )


def main() -> int:
    run_id = required_env("RUN_ID")
    output = Path(required_env("ARTIFACT_PATH")) / "provision.json"
    try:
        create_topics()
        schema_id = register_schema()
        create_cassandra_schema()
        partitions = int(required_env("INPUT_PARTITIONS"))
        starts = capture_end_offsets(required_env("INPUT_TOPIC"), partitions)
        atomic_json(
            output,
            artifact(
                "provision",
                run_id,
                state="COMPLETED",
                schema_id=schema_id,
                start_offsets_inclusive=starts,
                spark_starting_offsets=spark_starting_offsets(required_env("INPUT_TOPIC"), starts),
            ),
        )
        return 0
    except Exception as exc:
        atomic_json(output, artifact("provision", run_id, state="FAILED", error=str(exc)[:1000]))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
