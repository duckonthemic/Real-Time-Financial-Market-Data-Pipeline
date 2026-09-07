"""Read and validate the authoritative demo TOML, then render Compose inputs."""

from __future__ import annotations

import hashlib
import os
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .errors import ConfigFailure


RUN_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{7,63}$")


@dataclass(frozen=True)
class DemoConfig:
    path: Path
    root: Path
    values: Mapping[str, Any]

    @property
    def artifact_root(self) -> Path:
        return self.root / str(self.values["project"]["artifact_root"])

    @property
    def compose_file(self) -> Path:
        return self.root / str(self.values["project"]["compose_file"])

    def scenario(self, name: str) -> Mapping[str, Any]:
        try:
            return self.values["scenario"][name]
        except KeyError as exc:
            raise ConfigFailure(f"Unknown scenario: {name}") from exc


def _require(mapping: Mapping[str, Any], dotted: str, expected_type: type[Any]) -> Any:
    value: Any = mapping
    for part in dotted.split("."):
        if not isinstance(value, Mapping) or part not in value:
            raise ConfigFailure(f"Missing required setting: {dotted}")
        value = value[part]
    if not isinstance(value, expected_type) or isinstance(value, bool) and expected_type is int:
        raise ConfigFailure(f"Setting {dotted} must be {expected_type.__name__}")
    return value


def load_demo_config(path: Path) -> DemoConfig:
    resolved = path.resolve()
    if not resolved.is_file():
        raise ConfigFailure(f"Configuration file not found: {resolved}")
    try:
        values = tomllib.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigFailure(f"Cannot read configuration: {exc}") from exc

    required = {
        "project.name": str,
        "project.artifact_root": str,
        "project.checkpoint_root": str,
        "project.compose_file": str,
        "project.schema_subject": str,
        "project.schema_path": str,
        "topics.input.name": str,
        "topics.input.partitions": int,
        "topics.dlq.name": str,
        "spark.query_name": str,
        "spark.trigger_seconds": int,
        "spark.max_offsets_per_trigger": int,
        "scenario.standard.manifest": str,
        "scenario.recovery-showcase.manifest": str,
        "timeouts.infrastructure_seconds": int,
        "timeouts.scenario_seconds": int,
        "ports.grafana": int,
        "ports.spark_ui": int,
    }
    for dotted, expected_type in required.items():
        _require(values, dotted, expected_type)
    for scenario in ("standard", "recovery-showcase"):
        manifest = resolved.parent.parent / str(values["scenario"][scenario]["manifest"])
        if not manifest.is_file():
            raise ConfigFailure(f"Scenario manifest not found: {manifest}")
    cpu_total = sum(float(spec["cpus"]) for spec in values["resources"].values() if isinstance(spec, Mapping))
    if cpu_total > float(values["resources"]["required_cpus"]):
        raise ConfigFailure("Per-service CPU limits exceed resources.required_cpus")
    return DemoConfig(path=resolved, root=resolved.parent.parent, values=values)


def validate_run_id(run_id: str) -> str:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ConfigFailure("run_id must be 8-64 lowercase letters, digits, or hyphens")
    return run_id


def compose_environment(
    config: DemoConfig,
    run_id: str,
    schema_id: int,
    grafana_password: str,
    *,
    scenario_name: str = "standard",
    start_offsets_json: str = "{}",
) -> dict[str, str]:
    validate_run_id(run_id)
    values = config.values
    scenario = values["scenario"]
    selected = config.scenario(scenario_name)
    checkpoint = f"{values['project']['checkpoint_root']}/{run_id}/{values['spark']['query_name']}"
    resources = values["resources"]
    return {
        "COMPOSE_PROJECT_NAME": f"market-recovery-{run_id}",
        "RUN_ID": run_id,
        "SCHEMA_ID": str(schema_id),
        "INPUT_TOPIC": str(values["topics"]["input"]["name"]),
        "DLQ_TOPIC": str(values["topics"]["dlq"]["name"]),
        "INPUT_PARTITIONS": str(values["topics"]["input"]["partitions"]),
        "CHECKPOINT_PATH": checkpoint,
        "ARTIFACT_PATH": f"/artifacts/{run_id}",
        "GRAFANA_ADMIN_PASSWORD": grafana_password,
        "GRAFANA_HOST_PORT": str(values["ports"]["grafana"]),
        "SPARK_UI_HOST_PORT": str(values["ports"]["spark_ui"]),
        "KAFKA_IMAGE": str(values["images"]["kafka"]),
        "SCHEMA_REGISTRY_IMAGE": str(values["images"]["schema_registry"]),
        "SPARK_BASE_IMAGE": str(values["images"]["spark"]),
        "CASSANDRA_IMAGE": str(values["images"]["cassandra"]),
        "GRAFANA_IMAGE": str(values["images"]["grafana"]),
        "SPARK_TRIGGER_SECONDS": str(values["spark"]["trigger_seconds"]),
        "SPARK_MAX_OFFSETS": str(values["spark"]["max_offsets_per_trigger"]),
        "STANDARD_MANIFEST": str(scenario["standard"]["manifest"]),
        "SCENARIO_NAME": scenario_name,
        "SCENARIO_MANIFEST": str(selected["manifest"]),
        "PRODUCER_RATE": str(selected["producer_rate_per_second"]),
        "START_OFFSETS_JSON": start_offsets_json,
        "KAFKA_CPUS": str(resources["kafka"]["cpus"]),
        "KAFKA_MEMORY": str(resources["kafka"]["memory"]),
        "SCHEMA_REGISTRY_CPUS": str(resources["schema_registry"]["cpus"]),
        "SCHEMA_REGISTRY_MEMORY": str(resources["schema_registry"]["memory"]),
        "CASSANDRA_CPUS": str(resources["cassandra"]["cpus"]),
        "CASSANDRA_MEMORY": str(resources["cassandra"]["memory"]),
        "SPARK_MASTER_CPUS": str(resources["spark_master"]["cpus"]),
        "SPARK_MASTER_MEMORY": str(resources["spark_master"]["memory"]),
        "SPARK_WORKER_CPUS": str(resources["spark_worker"]["cpus"]),
        "SPARK_WORKER_MEMORY_LIMIT": str(resources["spark_worker"]["memory"]),
        "SPARK_RECOVERY_CPUS": str(resources["spark_recovery"]["cpus"]),
        "SPARK_RECOVERY_MEMORY": str(resources["spark_recovery"]["memory"]),
        "GRAFANA_CPUS": str(resources["grafana"]["cpus"]),
        "GRAFANA_MEMORY": str(resources["grafana"]["memory"]),
    }


def render_compose_env(path: Path, values: Mapping[str, str]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{key}={values[key]}" for key in sorted(values)]
    payload = "\n".join(lines) + "\n"
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(payload, encoding="utf-8", newline="\n")
    try:
        os.chmod(temporary, 0o600)
    except OSError:
        pass
    os.replace(temporary, path)
    redacted = "\n".join(
        f"{key}=<redacted>" if "PASSWORD" in key or "SECRET" in key else f"{key}={values[key]}"
        for key in sorted(values)
    ) + "\n"
    return hashlib.sha256(redacted.encode("utf-8")).hexdigest()
