"""Publish the deterministic fixture and count only acknowledged Kafka deliveries."""

from __future__ import annotations

import json
import time
from pathlib import Path
from threading import Lock
from typing import Any

from market_pipeline.ops.provision import capture_end_offsets
from market_pipeline.ops.runtime import artifact, atomic_json, manifest_from_env, required_env, run_config_from_env
from market_pipeline.producer.encoding import encode_event, load_avro_schema, malformed_value
from market_pipeline.producer.fixture import iter_fixture


def main() -> int:
    from confluent_kafka import Producer

    config = run_config_from_env()
    manifest = manifest_from_env()
    output = Path(config.artifact_path) / "producer-progress.json"
    schema = load_avro_schema(Path(config.schema_path))
    rate = float(required_env("PRODUCER_RATE"))
    starts = json.loads(config.start_offsets_json).get(config.input_topic, {})
    delivered = 0
    errors: list[str] = []
    coordinates: list[tuple[int, int]] = []
    lock = Lock()

    producer = Producer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "retry.backoff.ms": 100,
            "linger.ms": 5,
            "compression.type": "snappy",
            "max.in.flight.requests.per.connection": 5,
        }
    )

    def write_progress(state: str) -> None:
        atomic_json(
            output,
            artifact(
                "producer-progress",
                config.run_id,
                state=state,
                records=delivered,
                delivery_errors=list(errors),
                start_offsets_inclusive=starts,
                acknowledged_coordinates=coordinates[-10:],
            ),
        )

    def callback(error: Any, message: Any) -> None:
        nonlocal delivered
        with lock:
            if error is not None:
                errors.append(str(error)[:500])
            else:
                delivered += 1
                coordinates.append((int(message.partition()), int(message.offset())))

    write_progress("STARTING")
    interval = 1.0 / rate if rate > 0 else 0.0
    next_send = time.monotonic()
    for index, record in enumerate(iter_fixture(manifest, config.run_id), start=1):
        if record.event is None:
            value = malformed_value(record.malformed_kind or "corrupt-avro", config.schema_id)
        else:
            value = encode_event(record.event, schema, config.schema_id)
        while True:
            try:
                producer.produce(config.input_topic, key=record.key.encode("utf-8"), value=value, headers=list(record.headers), on_delivery=callback)
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
        if index == 1 or index % 50 == 0:
            write_progress("RUNNING")
        if interval:
            next_send += interval
            delay = next_send - time.monotonic()
            if delay > 0:
                time.sleep(delay)

    remaining = producer.flush(30)
    if remaining:
        errors.append(f"{remaining} records remained after bounded flush")
    end_offsets = capture_end_offsets(config.input_topic, int(required_env("INPUT_PARTITIONS")))
    state = "COMPLETED" if not errors and delivered == int(manifest["total_records"]) else "FAILED"
    atomic_json(
        output,
        artifact(
            "producer-progress",
            config.run_id,
            state=state,
            records=delivered,
            expected_records=int(manifest["total_records"]),
            delivery_errors=errors,
            start_offsets_inclusive=starts,
            end_offsets_exclusive=end_offsets,
            acknowledged_coordinates=coordinates[-10:],
        ),
    )
    if state != "COMPLETED":
        raise RuntimeError(f"producer delivery accounting failed: delivered={delivered}, errors={len(errors)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
