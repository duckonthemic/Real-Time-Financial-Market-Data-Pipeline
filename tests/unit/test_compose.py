from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from demo_support.compose import CommandResult, Compose
from demo_support.errors import DeadlineFailure, InfrastructureFailure


class RecordingRunner:
    def __init__(self, result: CommandResult | None = None) -> None:
        self.result = result or CommandResult(0, "", "")
        self.calls: list[tuple[tuple[str, ...], Path, float | None]] = []

    def run(
        self, args: tuple[str, ...], *, cwd: Path, timeout: float | None = None
    ) -> CommandResult:
        self.calls.append((tuple(args), cwd, timeout))
        return self.result


def compose(tmp_path: Path, runner: RecordingRunner) -> Compose:
    return Compose(tmp_path, tmp_path / "compose.yaml", tmp_path / "compose.env", runner)


def test_compose_uses_argument_arrays_and_explicit_env_file(tmp_path: Path) -> None:
    runner = RecordingRunner()
    client = compose(tmp_path, runner)
    client.kill("spark-recovery")
    args, cwd, timeout = runner.calls[0]
    assert args == (
        "docker",
        "compose",
        "--env-file",
        str(tmp_path / "compose.env"),
        "-f",
        str(tmp_path / "compose.yaml"),
        "--profile",
        "scenario",
        "kill",
        "-s",
        "SIGKILL",
        "spark-recovery",
    )
    assert cwd == tmp_path
    assert timeout is None


def test_compose_failure_has_bounded_diagnostics(tmp_path: Path) -> None:
    runner = RecordingRunner(CommandResult(1, "", "x" * 1500))
    with pytest.raises(InfrastructureFailure) as failure:
        compose(tmp_path, runner).up("kafka")
    assert failure.value.category == "INFRASTRUCTURE"
    assert len(failure.value.message) < 1200


def test_cleanup_command_failure_is_not_silenced(tmp_path: Path) -> None:
    runner = RecordingRunner(CommandResult(1, "", "daemon unavailable"))
    with pytest.raises(InfrastructureFailure, match="daemon unavailable"):
        compose(tmp_path, runner).down(volumes=True)


def test_cleanup_includes_scenario_and_dashboard_profiles(tmp_path: Path) -> None:
    runner = RecordingRunner()
    compose(tmp_path, runner).down(volumes=True)
    args = runner.calls[0][0]
    assert args[-7:] == (
        "--profile",
        "scenario",
        "--profile",
        "dashboard",
        "down",
        "--remove-orphans",
        "--volumes",
    )


def test_compose_uses_configured_default_timeout(tmp_path: Path) -> None:
    runner = RecordingRunner()
    client = Compose(
        tmp_path,
        tmp_path / "compose.yaml",
        tmp_path / "compose.env",
        runner,
        default_timeout=123,
    )
    client.validate()
    assert runner.calls[0][2] == 123


def test_compose_timeout_uses_stable_deadline_category(tmp_path: Path) -> None:
    class TimeoutRunner(RecordingRunner):
        def run(
            self, args: tuple[str, ...], *, cwd: Path, timeout: float | None = None
        ) -> CommandResult:
            raise subprocess.TimeoutExpired(args, timeout or 0)

    client = Compose(
        tmp_path,
        tmp_path / "compose.yaml",
        tmp_path / "compose.env",
        TimeoutRunner(),
        default_timeout=123,
    )
    with pytest.raises(DeadlineFailure, match="timed out after 123 seconds"):
        client.validate()


def test_service_running_requires_a_container_id(tmp_path: Path) -> None:
    assert not compose(tmp_path, RecordingRunner(CommandResult(0, "", ""))).is_running(
        "spark-recovery"
    )
    assert compose(tmp_path, RecordingRunner(CommandResult(0, "abc123\n", ""))).is_running(
        "spark-recovery"
    )


def test_host_visible_services_join_observer_network() -> None:
    compose_text = (Path(__file__).parents[2] / "compose.yaml").read_text(encoding="utf-8")
    spark = compose_text.split("  spark-master:", 1)[1].split("  spark-worker:", 1)[0]
    grafana = compose_text.split("  grafana:", 1)[1].split("\nnetworks:", 1)[0]
    assert "aliases: [spark-rpc]" in spark
    assert '"--host", "spark-rpc"' in spark
    assert "observer: {}" in spark
    assert "</dev/tcp/spark-rpc/7077" in spark
    assert "</dev/tcp/spark-master/7077" not in spark
    assert "networks: [pipeline, observer]" in grafana
