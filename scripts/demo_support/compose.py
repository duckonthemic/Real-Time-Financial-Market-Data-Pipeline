"""Argument-array Docker Compose commands with captured diagnostics."""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .errors import DeadlineFailure, InfrastructureFailure


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


class CommandRunner(Protocol):
    def run(
        self, args: Sequence[str], *, cwd: Path, timeout: float | None = None
    ) -> CommandResult: ...


class SubprocessRunner:
    def run(self, args: Sequence[str], *, cwd: Path, timeout: float | None = None) -> CommandResult:
        completed = subprocess.run(
            list(args),
            cwd=cwd,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        return CommandResult(completed.returncode, completed.stdout, completed.stderr)


class Compose:
    def __init__(
        self,
        root: Path,
        compose_file: Path,
        env_file: Path,
        runner: CommandRunner | None = None,
        *,
        default_timeout: float | None = None,
    ):
        self.root = root
        self.compose_file = compose_file
        self.env_file = env_file
        self.runner = runner or SubprocessRunner()
        self.default_timeout = default_timeout

    def command(
        self, *parts: str, timeout: float | None = None, check: bool = True
    ) -> CommandResult:
        args = (
            "docker",
            "compose",
            "--env-file",
            str(self.env_file),
            "-f",
            str(self.compose_file),
            *parts,
        )
        command_timeout = self.default_timeout if timeout is None else timeout
        try:
            result = self.runner.run(args, cwd=self.root, timeout=command_timeout)
        except subprocess.TimeoutExpired as exc:
            raise DeadlineFailure(
                f"Docker Compose command timed out after {exc.timeout:g} seconds: {' '.join(parts)}",
                phase="compose",
            ) from exc
        if check and result.returncode != 0:
            detail = (result.stderr or result.stdout).strip()[-1000:]
            raise InfrastructureFailure(
                f"Docker Compose command failed: {' '.join(parts)}. {detail}",
                phase="compose",
                remediation="Open the captured Compose output, fix the reported service, and rerun.",
            )
        return result

    def validate(self) -> None:
        self.command("config", "--quiet")

    def build(self, *services: str) -> None:
        self.command("--profile", "scenario", "build", *services)

    def up(self, *services: str, build: bool = False) -> None:
        parts = ["--profile", "scenario", "up", "-d"]
        if build:
            parts.append("--build")
        parts.extend(services)
        self.command(*parts)

    def run(self, service: str, *, check: bool = True) -> CommandResult:
        return self.command("--profile", "scenario", "run", "--rm", service, check=check)

    def kill(self, service: str) -> None:
        self.command("--profile", "scenario", "kill", "-s", "SIGKILL", service)

    def recreate(self, service: str) -> None:
        self.command("--profile", "scenario", "up", "-d", "--force-recreate", service)

    def is_running(self, service: str) -> bool:
        result = self.command(
            "--profile", "scenario", "ps", "--status", "running", "-q", service, check=False
        )
        return result.returncode == 0 and bool(result.stdout.strip())

    def down(self, *, volumes: bool = False) -> None:
        parts = [
            "--profile",
            "scenario",
            "--profile",
            "dashboard",
            "down",
            "--remove-orphans",
        ]
        if volumes:
            parts.append("--volumes")
        self.command(*parts)
