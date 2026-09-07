from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

from demo_support.artifacts import DemoLock, artifact, atomic_write_json, read_artifact, safe_run_directory, write_primary_failure
from demo_support.errors import ConfigFailure


ROOT = Path(__file__).parents[2]


def test_atomic_artifact_round_trip(tmp_path: Path) -> None:
    target = tmp_path / "run.json"
    value = artifact("run", "run-contract-01", state="CREATED", scenario="standard")
    atomic_write_json(target, value)
    assert read_artifact(target, run_id="run-contract-01", artifact_type="run") == value


def test_run_schema_accepts_representative_artifact() -> None:
    schema = json.loads((ROOT / "schemas" / "artifacts" / "run.schema.json").read_text(encoding="utf-8"))
    jsonschema.validate(artifact("run", "run-contract-01", state="CREATED", scenario="standard"), schema)


def test_lock_is_exclusive_and_reports_owner(tmp_path: Path) -> None:
    lock_path = tmp_path / "demo.lock"
    with DemoLock(lock_path, "run-contract-01"):
        with pytest.raises(ConfigFailure, match="run_id=run-contract-01"):
            with DemoLock(lock_path, "run-contract-02"):
                pass
    assert not lock_path.exists()


def test_safe_run_directory_rejects_traversal(tmp_path: Path) -> None:
    with pytest.raises(ConfigFailure):
        safe_run_directory(tmp_path, "../escape")


def test_first_failure_remains_primary(tmp_path: Path) -> None:
    target = tmp_path / "failure.json"
    first = artifact("failure", "run-contract-01", category="INVARIANT", message="first", failed_phase="verify", remediation="inspect", cleanup_errors=[])
    second = artifact("failure", "run-contract-01", category="UNEXPECTED", message="second", failed_phase="cleanup", remediation="inspect", cleanup_errors=[])
    write_primary_failure(target, first)
    write_primary_failure(target, second, cleanup_error="cleanup failed")
    stored = json.loads(target.read_text(encoding="utf-8"))
    assert stored["message"] == "first"
    assert stored["cleanup_errors"] == ["cleanup failed"]
