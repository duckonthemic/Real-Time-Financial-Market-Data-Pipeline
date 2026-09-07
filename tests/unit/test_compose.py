from __future__ import annotations

from pathlib import Path

import pytest

from demo_support.compose import CommandResult, Compose
from demo_support.errors import InfrastructureFailure


class RecordingRunner:
    def __init__(self, result: CommandResult | None = None) -> None:
        self.result = result or CommandResult(0, "", "")
        self.calls: list[tuple[tuple[str, ...], Path, float | None]] = []

    def run(self, args: tuple[str, ...], *, cwd: Path, timeout: float | None = None) -> CommandResult:
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


def test_service_running_requires_a_container_id(tmp_path: Path) -> None:
    assert not compose(tmp_path, RecordingRunner(CommandResult(0, "", ""))).is_running("spark-recovery")
    assert compose(tmp_path, RecordingRunner(CommandResult(0, "abc123\n", ""))).is_running("spark-recovery")
