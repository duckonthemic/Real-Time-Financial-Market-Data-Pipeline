"""Immutable application-boundary configuration models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RunConfig:
    run_id: str
    dataset_id: str
    schema_id: int
    schema_path: str
    input_topic: str
    dlq_topic: str
    kafka_bootstrap_servers: str
    schema_registry_url: str
    cassandra_host: str
    cassandra_keyspace: str
    checkpoint_path: str
    artifact_path: str
    start_offsets_json: str = "{}"
    max_offsets_per_trigger: int = 1000
    trigger_seconds: int = 5


@dataclass(frozen=True)
class VerificationConfig:
    run_id: str
    dataset_id: str
    input_topic: str
    expected_canonical: int
    expected_duplicates: int
    expected_invalid: int
    expected_total: int
    expected_rejections: dict[str, int]
    expected_event_set_sha256: str
    sample_size: int = 256
    price_absolute_tolerance: float = 1e-6
