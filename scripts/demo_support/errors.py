"""Stable user-facing exit categories for the host demo."""

from __future__ import annotations

from dataclasses import dataclass


PASSED = 0
CONFIGURATION = 2
INFRASTRUCTURE = 3
INVARIANT = 4
TIMEOUT = 5
UNEXPECTED = 6


@dataclass(frozen=True)
class DemoFailure(Exception):
    category: str
    message: str
    phase: str
    exit_code: int
    remediation: str

    def __str__(self) -> str:
        return self.message


class ConfigFailure(DemoFailure):
    def __init__(self, message: str, phase: str = "preflight", remediation: str = "Fix config/demo.toml and rerun."):
        super().__init__("CONFIGURATION", message, phase, CONFIGURATION, remediation)


class InfrastructureFailure(DemoFailure):
    def __init__(self, message: str, phase: str, remediation: str):
        super().__init__("INFRASTRUCTURE", message, phase, INFRASTRUCTURE, remediation)


class InvariantFailure(DemoFailure):
    def __init__(self, message: str, phase: str = "verifying", remediation: str = "Open run-report.json for the failed invariant."):
        super().__init__("INVARIANT", message, phase, INVARIANT, remediation)


class DeadlineFailure(DemoFailure):
    def __init__(self, message: str, phase: str, remediation: str = "Inspect captured service logs and retry with healthy resources."):
        super().__init__("TIMEOUT", message, phase, TIMEOUT, remediation)
