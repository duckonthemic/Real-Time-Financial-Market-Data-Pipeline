"""Query Kafka/Cassandra independently and emit machine-readable plus HTML evidence."""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from market_pipeline.analytics.ohlcv import aggregate_ohlcv
from market_pipeline.contracts.models import VerificationConfig
from market_pipeline.ops.runtime import artifact, atomic_json, manifest_from_env, required_env, run_config_from_env
from market_pipeline.producer.encoding import encode_event, load_avro_schema
from market_pipeline.producer.fixture import canonical_event
from market_pipeline.verification.invariants import ValueSample, VerificationSnapshot, verify
from market_pipeline.verification.report import write_report


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    output = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            output.append(json.loads(line))
    return output


def _row_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "_asdict"):
        return dict(row._asdict())
    return dict(row)


def _epoch_ms(value: Any) -> int:
    if isinstance(value, datetime):
        aware = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
        return int(aware.timestamp() * 1000)
    return int(value)


def _timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _gold_contract_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "symbol": str(row["symbol"]),
        "window_start_ms": _epoch_ms(row["window_start"]),
        "window_end_ms": _epoch_ms(row["window_end"]),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": int(row["volume"]),
        "trade_count": int(row["trade_count"]),
        "vwap": float(row["vwap"]),
    }


def publish_evidence(
    config: Any,
    report: Mapping[str, Any],
    timeline: list[dict[str, Any]],
    metrics: list[dict[str, Any]],
) -> None:
    """Publish run-scoped observer projections without participating in verification."""
    from cassandra.cluster import Cluster

    producer = _read_json(Path(config.artifact_path) / "producer-progress.json")
    starts = {int(key): int(value) for key, value in producer["start_offsets_inclusive"].items()}
    ends = {int(key): int(value) for key, value in producer["end_offsets_exclusive"].items()}
    states = {str(item.get("state")): item for item in timeline}
    latest = timeline[-1] if timeline else {}
    cluster = Cluster([config.cassandra_host])
    session = cluster.connect(config.cassandra_keyspace)
    try:
        session.execute(
            "INSERT INTO pipeline_runs (run_id,dataset_id,state,expected_counts,start_offsets_inclusive,end_offsets_exclusive,data_contract_status,portfolio_release_status,created_at,started_at,failure_at,recovered_at,completed_at,state_reason) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                config.run_id,
                config.dataset_id,
                "PASSED" if report.get("data_contract_status") == "PASSED" else "FAILED",
                {key: int(value) for key, value in report.get("expected_counts", {}).items()},
                starts,
                ends,
                report.get("data_contract_status"),
                report.get("portfolio_release_status"),
                _timestamp(states.get("CREATED", {}).get("occurred_at")),
                _timestamp(states.get("PRODUCING", {}).get("occurred_at")),
                _timestamp(states.get("FAILURE_INJECTED", {}).get("occurred_at")),
                _timestamp(states.get("RECOVERING", {}).get("occurred_at")),
                _timestamp(report.get("updated_at")),
                str(latest.get("reason", "independent verification complete"))[:500],
            ),
        )
        for item in timeline:
            session.execute(
                "INSERT INTO run_state_transitions_by_run (run_id,transition_seq,state,actor,occurred_at,reason) VALUES (%s,%s,%s,%s,%s,%s)",
                (
                    config.run_id,
                    int(item["sequence"]),
                    str(item["state"]),
                    str(item["actor"]),
                    _timestamp(item["occurred_at"]),
                    str(item.get("reason", ""))[:500],
                ),
            )
        for check in report.get("checks", []):
            session.execute(
                "INSERT INTO run_checks_by_run (run_id,check_name,expected_value,actual_value,status,detail,checked_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    config.run_id,
                    str(check["name"]),
                    json.dumps(check.get("expected"), sort_keys=True, separators=(",", ":")),
                    json.dumps(check.get("actual"), sort_keys=True, separators=(",", ":")),
                    "PASS" if check.get("passed") else "FAIL",
                    str(check.get("detail", ""))[:1000],
                    _timestamp(report.get("updated_at")),
                ),
            )
        for item in metrics:
            session.execute(
                "INSERT INTO run_metrics_by_run (run_id,sampled_at,query_name,total_lag,partition_lag,processed_count,processing_latency_p95_ms) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    config.run_id,
                    _timestamp(item["sampled_at"]),
                    "recovery_pipeline",
                    int(item.get("total_lag", 0)),
                    {int(key): int(value) for key, value in item.get("partition_lag", {}).items()},
                    int(item.get("processed_frontier", 0)),
                    item.get("processing_latency_p95_ms"),
                ),
            )
    finally:
        cluster.shutdown()


def read_kafka_dlq_coordinates(run_id: str, topic: str, bootstrap_servers: str, partitions: int) -> frozenset[tuple[str, int, int]]:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"verify-dlq-{run_id}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    bounds: dict[int, int] = {}
    assignments = []
    try:
        for partition in range(partitions):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10, cached=False)
            assignments.append(TopicPartition(topic, partition, low))
            bounds[partition] = high
        consumer.assign(assignments)
        coordinates: set[tuple[str, int, int]] = set()
        completed: set[int] = set()
        idle_polls = 0
        while len(completed) < partitions and idle_polls < 20:
            message = consumer.poll(0.5)
            if message is None:
                idle_polls += 1
                positions = consumer.position([TopicPartition(topic, partition) for partition in range(partitions)])
                completed.update(position.partition for position in positions if position.offset >= bounds[position.partition])
                continue
            idle_polls = 0
            if message.error():
                continue
            try:
                payload = json.loads(message.value().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if payload.get("owner_run_id") != run_id:
                continue
            coordinate = str(payload.get("coordinate", "")).split(":")
            if len(coordinate) == 3:
                coordinates.add((coordinate[0], int(coordinate[1]), int(coordinate[2])))
            if message.offset() + 1 >= bounds[message.partition()]:
                completed.add(message.partition())
        return frozenset(coordinates)
    finally:
        consumer.close()


def load_snapshot(config: Any, manifest: Mapping[str, Any]) -> tuple[VerificationSnapshot, list[dict[str, Any]], list[dict[str, Any]]]:
    from cassandra.cluster import Cluster

    artifact_root = Path(config.artifact_path)
    producer = _read_json(artifact_root / "producer-progress.json")
    starts = {int(key): int(value) for key, value in producer["start_offsets_inclusive"].items()}
    ends = {int(key): int(value) for key, value in producer["end_offsets_exclusive"].items()}
    cluster = Cluster([config.cassandra_host])
    session = cluster.connect(config.cassandra_keyspace)
    bronze_rows: list[dict[str, Any]] = []
    silver_rows: list[dict[str, Any]] = []
    dlq_rows: list[dict[str, Any]] = []
    batch_rows: list[dict[str, Any]] = []
    canonical_limit = int(manifest["canonical_records"])
    expected_events = [canonical_event(manifest, config.run_id, sequence) for sequence in range(canonical_limit)]
    expected_gold_source = aggregate_ohlcv(expected_events)
    actual_gold_rows: list[dict[str, Any]] = []
    try:
        for partition, start in starts.items():
            rows = session.execute(
                "SELECT * FROM bronze_records_by_run_partition WHERE owner_run_id=%s AND source_topic=%s AND source_partition=%s AND source_offset>=%s AND source_offset<%s",
                (config.run_id, config.input_topic, partition, start, ends[partition]),
            )
            bronze_rows.extend(_row_dict(row) for row in rows)
        silver_rows = [_row_dict(row) for row in session.execute("SELECT * FROM silver_events_by_run WHERE run_id=%s", (config.run_id,))]
        dlq_rows = [_row_dict(row) for row in session.execute("SELECT * FROM dlq_records_by_run WHERE owner_run_id=%s", (config.run_id,))]
        batch_rows = [_row_dict(row) for row in session.execute("SELECT * FROM stream_batches_by_query WHERE run_id=%s AND query_name=%s", (config.run_id, "recovery_pipeline"))]
        for symbol, window_date in sorted(
            {(str(row["symbol"]), row["window_date"]) for row in expected_gold_source},
            key=lambda item: (item[0], str(item[1])),
        ):
            rows = session.execute(
                "SELECT * FROM gold_ohlcv_5m_by_symbol_day WHERE run_id=%s AND symbol=%s AND window_date=%s",
                (config.run_id, symbol, window_date),
            )
            actual_gold_rows.extend(_row_dict(row) for row in rows)
    finally:
        cluster.shutdown()

    coordinates = frozenset((row["source_topic"], int(row["source_partition"]), int(row["source_offset"])) for row in bronze_rows)
    dlq_coordinates = frozenset((row["source_topic"], int(row["source_partition"]), int(row["source_offset"])) for row in dlq_rows)
    sequence_counts = Counter(
        int(row["claimed_source_sequence"])
        for row in bronze_rows
        if row.get("claimed_source_sequence") is not None and 0 <= int(row["claimed_source_sequence"]) < canonical_limit
    )
    observed_duplicates = sum(count - 1 for count in sequence_counts.values() if count > 1)
    latest = max(batch_rows, key=lambda row: int(row["batch_id"]), default={})
    cumulative = {int(key): int(value) for key, value in (latest.get("cumulative_next_offsets") or {}).items()}
    timeline = _read_jsonl(artifact_root / "state-transitions.jsonl")
    metrics = _read_jsonl(artifact_root / "metrics.jsonl")
    states = {str(item.get("state")) for item in timeline}
    lag_rise = max((int(item.get("total_lag", 0)) for item in metrics), default=0)

    selected = sorted(expected_events, key=lambda event: event["event_id"])[: int(manifest.get("sample_size", 256))]
    for boundary in (expected_events[0], expected_events[-1]):
        if boundary not in selected:
            selected.append(boundary)
    actual_by_id = {row["event_id"]: row for row in silver_rows}
    value_samples = []
    for expected in selected:
        actual_row = actual_by_id.get(expected["event_id"], {})
        actual = {
            "symbol": actual_row.get("symbol"),
            "price": actual_row.get("price"),
            "volume": actual_row.get("volume"),
            "event_time_ms": _epoch_ms(actual_row["event_time"]) if actual_row.get("event_time") else None,
            "conditions": list(actual_row.get("conditions") or []),
            "source_sequence": actual_row.get("source_sequence"),
        }
        value_samples.append(ValueSample(expected["event_id"], expected, actual))

    schema = load_avro_schema(Path(config.schema_path))
    expected_digest_by_sequence = {
        int(event["source_sequence"]): hashlib.sha256(encode_event(event, schema, config.schema_id)).hexdigest()
        for event in selected
    }
    actual_digests: dict[int, list[str]] = {}
    for row in bronze_rows:
        sequence = row.get("claimed_source_sequence")
        if sequence is not None and int(sequence) in expected_digest_by_sequence:
            actual_digests.setdefault(int(sequence), []).append(hashlib.sha256(bytes(row["raw_value"])).hexdigest())
    bronze_digest_samples = tuple(
        (
            str(sequence),
            "MATCH" if values and all(value == expected_digest for value in values) else "MISMATCH",
        )
        for sequence, expected_digest in expected_digest_by_sequence.items()
        for values in (actual_digests.get(sequence, []),)
    )
    kafka_dlq = read_kafka_dlq_coordinates(
        config.run_id,
        config.dlq_topic,
        config.kafka_bootstrap_servers,
        int(required_env("INPUT_PARTITIONS")),
    )
    snapshot = VerificationSnapshot(
        start_offsets_inclusive=starts,
        end_offsets_exclusive=ends,
        bronze_coordinates=coordinates,
        acknowledged_inputs=int(producer["records"]),
        silver_event_ids=frozenset(str(row["event_id"]) for row in silver_rows),
        observed_duplicate_inputs=observed_duplicates,
        dlq_coordinates=dlq_coordinates,
        kafka_dlq_coordinates=kafka_dlq,
        dlq_rejection_codes=tuple(str(row["rejection_code"]) for row in dlq_rows),
        cumulative_next_offsets=cumulative,
        terminal_lag=int(metrics[-1].get("total_lag", 0)) if metrics else sum(ends.values()) - sum(cumulative.values()),
        batch_statuses=tuple(str(row["status"]) for row in batch_rows),
        recovery_observed={"FAILURE_INJECTED", "RECOVERING"}.issubset(states),
        lag_rise=lag_rise,
        value_samples=tuple(value_samples),
        bronze_digest_samples=bronze_digest_samples,
        expected_gold_rows=tuple(_gold_contract_row(row) for row in expected_gold_source),
        actual_gold_rows=tuple(_gold_contract_row(row) for row in actual_gold_rows),
    )
    return snapshot, timeline, metrics


def main() -> int:
    config = run_config_from_env()
    manifest = manifest_from_env()
    verification_config = VerificationConfig(
        run_id=config.run_id,
        dataset_id=str(manifest["dataset_id"]),
        input_topic=config.input_topic,
        expected_canonical=int(manifest["canonical_records"]),
        expected_duplicates=int(manifest["duplicate_records"]),
        expected_invalid=int(manifest["invalid_records"]),
        expected_total=int(manifest["total_records"]),
        expected_rejections={str(key): int(value) for key, value in manifest["rejection_counts"].items()},
        expected_event_set_sha256=str(manifest["canonical_event_set_sha256"]),
        sample_size=int(os.environ.get("VERIFY_SAMPLE_SIZE", "256")),
    )
    snapshot, timeline, metrics = load_snapshot(config, manifest)
    checks = verify(verification_config, snapshot)
    passed = all(check.passed for check in checks)
    output = artifact(
        "run-report",
        config.run_id,
        scenario=required_env("SCENARIO_NAME"),
        data_contract_status="PASSED" if passed else "FAILED",
        portfolio_release_status="NOT_READY",
        dataset_id=verification_config.dataset_id,
        expected_counts={
            "canonical": verification_config.expected_canonical,
            "duplicates": verification_config.expected_duplicates,
            "invalid": verification_config.expected_invalid,
            "total": verification_config.expected_total,
        },
        checks=[check.to_dict() for check in checks],
    )
    artifact_root = Path(config.artifact_path)
    atomic_json(artifact_root / "run-report.json", output)
    write_report(artifact_root / "report.html", output, timeline=timeline, lag_samples=metrics)
    try:
        publish_evidence(config, output, timeline, metrics)
    except Exception as exc:
        output["release_checks"] = {
            "cassandra_evidence": "FAILED",
            "failures": [str(exc)[:1000]],
        }
        atomic_json(artifact_root / "run-report.json", output)
        write_report(artifact_root / "report.html", output, timeline=timeline, lag_samples=metrics)
    print("PASS" if passed else "FAIL")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
