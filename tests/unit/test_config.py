from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from demo_support.config import compose_environment, load_demo_config, render_compose_env
from demo_support.errors import ConfigFailure


ROOT = Path(__file__).parents[2]


def test_demo_toml_loads_and_resources_fit() -> None:
    config = load_demo_config(ROOT / "config" / "demo.toml")
    assert config.values["topics"]["input"]["partitions"] == 3
    assert config.values["scenario"]["standard"]["producer_rate_per_second"] == 50
    assert config.values["spark"]["trigger_seconds"] == 5


def test_compose_environment_is_run_scoped_and_complete() -> None:
    config = load_demo_config(ROOT / "config" / "demo.toml")
    values = compose_environment(config, "run-20260907-a1", 17, "secret-value")
    assert values["COMPOSE_PROJECT_NAME"] == "market-recovery-run-20260907-a1"
    assert values["CHECKPOINT_PATH"].endswith("run-20260907-a1/recovery_pipeline")
    assert values["SCHEMA_ID"] == "17"
    assert values["GRAFANA_ADMIN_PASSWORD"] == "secret-value"
    assert values["INPUT_TOPIC"] == "market.trades.v1"


def test_rendered_environment_hash_redacts_password(tmp_path: Path) -> None:
    target = tmp_path / "compose.env"
    digest = render_compose_env(target, {"RUN_ID": "run-20260907-a1", "GRAFANA_ADMIN_PASSWORD": "hunter2"})
    assert target.read_text(encoding="utf-8") == "GRAFANA_ADMIN_PASSWORD=hunter2\nRUN_ID=run-20260907-a1\n"
    expected = hashlib.sha256(b"GRAFANA_ADMIN_PASSWORD=<redacted>\nRUN_ID=run-20260907-a1\n").hexdigest()
    assert digest == expected


def test_unknown_scenario_fails_as_configuration_error() -> None:
    config = load_demo_config(ROOT / "config" / "demo.toml")
    with pytest.raises(ConfigFailure, match="Unknown scenario"):
        config.scenario("live")
