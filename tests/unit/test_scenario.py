from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Sequence

import pytest

from demo_support.artifacts import artifact, atomic_write_json
from demo_support.compose import CommandResult
from demo_support.config import DemoConfig, load_demo_config
from demo_support.errors import ConfigFailure, DeadlineFailure, InfrastructureFailure
from demo_support.scenario import RecoveryScenario, cleanup_run, purge_evidence


ROOT = Path(__file__).parents[2]
RUN_ID = "run-scenario-01"


class Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.on_sleep = None

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        if self.on_sleep:
            self.on_sleep()


def make_test_config(tmp_path: Path) -> DemoConfig:
    original = load_demo_config(ROOT / "config" / "demo.toml")
    values = copy.deepcopy(dict(original.values))
    values["project"]["artifact_root"] = "artifacts"
    values["project"]["checkpoint_root"] = "checkpoints"
    values["project"]["compose_file"] = "compose.yaml"
    values["scenario"]["recovery-showcase"].update(
        manifest="fixture.json",
        kill_after_processed=500,
        kill_publish_min=650,
        kill_publish_max=1000,
        failure_dwell_seconds=2,
        minimum_lag_rise=150,
    )
    (tmp_path / "fixture.json").write_text(
        json.dumps({"dataset_id": "fixture-v1", "expected": {"total_input": 1270}}),
        encoding="utf-8",
    )
    (tmp_path / "compose.yaml").write_text("services: {}\n", encoding="utf-8")
    return DemoConfig(tmp_path / "config.toml", tmp_path, values)


class ScenarioRunner:
    def __init__(self, root: Path, clock: Clock, *, fail_down: bool = False) -> None:
        self.root = root
        self.clock = clock
        self.fail_down = fail_down
        self.calls: list[tuple[str, ...]] = []
        self.run_dir = root / "artifacts" / RUN_ID
        self.clock.on_sleep = self._raise_lag

    def _write_progress(self, name: str, **values: object) -> None:
        atomic_write_json(self.run_dir / name, artifact(name.removesuffix(".json"), RUN_ID, **values))

    def _raise_lag(self) -> None:
        producer = self.run_dir / "producer-progress.json"
        if producer.exists():
            self._write_progress("producer-progress.json", state="RUNNING", records=900, expected_records=1270)

    def run(self, args: Sequence[str], *, cwd: Path, timeout: float | None = None) -> CommandResult:
        call = tuple(args)
        self.calls.append(call)
        if "ps" in call and "spark-recovery" in call:
            return CommandResult(0, "container-id\n", "")
        if "down" in call and self.fail_down:
            return CommandResult(1, "", "cannot stop runtime")
        if call[-3:] == ("run", "--rm", "provision"):
            atomic_write_json(
                self.run_dir / "provision.json",
                artifact(
                    "provision",
                    RUN_ID,
                    state="COMPLETED",
                    schema_id=7,
                    start_offsets_inclusive={"0": 0, "1": 0, "2": 0},
                    spark_starting_offsets='{"market.trades.v1":{"0":0,"1":0,"2":0}}',
                ),
            )
        elif call[-1] == "producer" and "up" in call:
            self._write_progress("producer-progress.json", state="RUNNING", records=700, expected_records=1270)
            self._write_progress("streaming-progress.json", state="RUNNING", records=500)
        elif call[-1] == "spark-recovery" and "--force-recreate" in call:
            self._write_progress("producer-progress.json", state="COMPLETED", records=1270, expected_records=1270)
            self._write_progress("streaming-progress.json", state="RUNNING", records=1270)
        elif call[-3:] == ("run", "--rm", "verifier"):
            atomic_write_json(
                self.run_dir / "run-report.json",
                artifact("run-report", RUN_ID, data_contract_status="PASSED", portfolio_release_status="READY"),
            )
            (self.run_dir / "report.html").write_text("<!doctype html><title>PASS</title>", encoding="utf-8")
        return CommandResult(0, "", "")


def test_recovery_scenario_reaches_pass_with_kill_and_recreate(tmp_path: Path) -> None:
    config = make_test_config(tmp_path)
    clock = Clock()
    runner = ScenarioRunner(tmp_path, clock)
    scenario = RecoveryScenario(
        config,
        run_id=RUN_ID,
        scenario_name="recovery-showcase",
        runner=runner,
        monotonic=clock.monotonic,
        sleeper=clock.sleep,
        dashboard=False,
    )
    scenario.preflight = lambda: None  # type: ignore[method-assign]
    result = scenario.run()
    states = [json.loads(line)["state"] for line in scenario.timeline_file.read_text(encoding="utf-8").splitlines()]
    assert result.state == "PASSED"
    assert states == ["CREATED", "INFRA_READY", "RUN_READY", "PRODUCING", "FAILURE_INJECTED", "RECOVERING", "VERIFYING", "PASSED"]
    assert any("kill" in call and "SIGKILL" in call for call in runner.calls)
    assert any("--force-recreate" in call for call in runner.calls)


def test_wait_raises_stable_timeout_category(tmp_path: Path) -> None:
    clock = Clock()
    scenario = RecoveryScenario(
        make_test_config(tmp_path),
        run_id=RUN_ID,
        scenario_name="recovery-showcase",
        monotonic=clock.monotonic,
        sleeper=clock.sleep,
        dashboard=False,
    )
    with pytest.raises(DeadlineFailure) as failure:
        scenario._wait("never", lambda: False, 3)
    assert failure.value.category == "TIMEOUT"


def test_existing_lock_does_not_consume_run_id(tmp_path: Path) -> None:
    config = make_test_config(tmp_path)
    lock = tmp_path / ".gstack" / "demo.lock"
    lock.parent.mkdir()
    lock.write_text("run_id=another-run\n", encoding="utf-8")
    scenario = RecoveryScenario(config, run_id=RUN_ID, scenario_name="recovery-showcase", dashboard=False)
    with pytest.raises(ConfigFailure, match="another-run"):
        scenario.run()
    assert not scenario.run_directory.exists()


def test_cleanup_refuses_wrong_compose_owner(tmp_path: Path) -> None:
    config = make_test_config(tmp_path)
    directory = tmp_path / "artifacts" / RUN_ID
    directory.mkdir(parents=True)
    atomic_write_json(directory / "run.json", artifact("run", RUN_ID, state="FAILED"))
    (directory / "compose.env").write_text("COMPOSE_PROJECT_NAME=someone-else\n", encoding="utf-8")
    with pytest.raises(ConfigFailure, match="ownership"):
        cleanup_run(config, RUN_ID)


def test_failed_compose_down_does_not_mark_runtime_cleaned(tmp_path: Path) -> None:
    config = make_test_config(tmp_path)
    directory = tmp_path / "artifacts" / RUN_ID
    directory.mkdir(parents=True)
    atomic_write_json(directory / "run.json", artifact("run", RUN_ID, state="FAILED"))
    (directory / "compose.env").write_text(
        f"COMPOSE_PROJECT_NAME=market-recovery-{RUN_ID}\n", encoding="utf-8"
    )
    with pytest.raises(InfrastructureFailure):
        cleanup_run(config, RUN_ID, ScenarioRunner(tmp_path, Clock(), fail_down=True))
    stored = json.loads((directory / "run.json").read_text(encoding="utf-8"))
    assert "runtime_cleaned" not in stored


def test_purge_requires_cleaned_runtime(tmp_path: Path) -> None:
    config = make_test_config(tmp_path)
    directory = tmp_path / "artifacts" / RUN_ID
    directory.mkdir(parents=True)
    atomic_write_json(directory / "run.json", artifact("run", RUN_ID, state="FAILED"))
    with pytest.raises(ConfigFailure, match="must be cleaned"):
        purge_evidence(config, RUN_ID)
    assert directory.exists()
