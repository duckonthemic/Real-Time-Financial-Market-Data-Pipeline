"""Application-boundary environment parsing and atomic container artifacts."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from market_pipeline.contracts.models import RunConfig


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise ValueError(f"required environment variable is missing: {name}")
    return value


def run_config_from_env() -> RunConfig:
    return RunConfig(
        run_id=required_env("RUN_ID"),
        dataset_id=manifest_from_env()["dataset_id"],
        schema_id=int(required_env("SCHEMA_ID")),
        schema_path="schemas/trade_event_v1.avsc",
        input_topic=required_env("INPUT_TOPIC"),
        dlq_topic=required_env("DLQ_TOPIC"),
        kafka_bootstrap_servers=required_env("KAFKA_BOOTSTRAP_SERVERS"),
        schema_registry_url=required_env("SCHEMA_REGISTRY_URL"),
        cassandra_host=required_env("CASSANDRA_HOST"),
        cassandra_keyspace=required_env("CASSANDRA_KEYSPACE"),
        checkpoint_path=required_env("CHECKPOINT_PATH"),
        artifact_path=required_env("ARTIFACT_PATH"),
        start_offsets_json=required_env("START_OFFSETS_JSON"),
        max_offsets_per_trigger=int(required_env("SPARK_MAX_OFFSETS")),
        trigger_seconds=int(required_env("SPARK_TRIGGER_SECONDS")),
    )


def manifest_from_env() -> dict[str, Any]:
    return json.loads(Path(required_env("SCENARIO_MANIFEST")).read_text(encoding="utf-8"))


def atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def artifact(artifact_type: str, run_id: str, **values: Any) -> dict[str, Any]:
    return {"artifact_type": artifact_type, "schema_version": 1, "run_id": run_id, "updated_at": utc_now(), **values}
