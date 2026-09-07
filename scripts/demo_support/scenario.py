"""Host-side recovery state machine with injectable time and Compose boundaries."""

from __future__ import annotations

import json
import secrets
import shutil
import socket
import sys
import time
import webbrowser
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from .artifacts import DemoLock, append_jsonl, artifact, atomic_write_json, read_artifact, safe_run_directory, utc_now, write_primary_failure
from .compose import CommandRunner, Compose, SubprocessRunner
from .config import DemoConfig, compose_environment, render_compose_env, validate_run_id
from .errors import ConfigFailure, DeadlineFailure, DemoFailure, InfrastructureFailure, InvariantFailure, UNEXPECTED


SOURCE_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


TERMINAL_STATES = {"PASSED", "FAILED", "TIMED_OUT"}
TRANSITIONS = {
    "CREATED": {"INFRA_READY", "FAILED", "TIMED_OUT"},
    "INFRA_READY": {"RUN_READY", "FAILED", "TIMED_OUT"},
    "RUN_READY": {"PRODUCING", "FAILED", "TIMED_OUT"},
    "PRODUCING": {"FAILURE_INJECTED", "FAILED", "TIMED_OUT"},
    "FAILURE_INJECTED": {"RECOVERING", "FAILED", "TIMED_OUT"},
    "RECOVERING": {"VERIFYING", "FAILED", "TIMED_OUT"},
    "VERIFYING": {"PASSED", "FAILED", "TIMED_OUT"},
}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def generated_run_id() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    return f"run-{timestamp}-{secrets.token_hex(3)}"


def _port_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
        connection.settimeout(0.2)
        return connection.connect_ex(("127.0.0.1", port)) != 0


@dataclass(frozen=True)
class ScenarioResult:
    run_id: str
    state: str
    report_path: Path | None
    dashboard_url: str


class RecoveryScenario:
    def __init__(
        self,
        config: DemoConfig,
        *,
        run_id: str,
        scenario_name: str,
        runner: CommandRunner | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        dashboard: bool = True,
        open_dashboard: bool = False,
    ) -> None:
        self.config = config
        self.run_id = validate_run_id(run_id)
        self.scenario_name = scenario_name
        self.settings = config.scenario(scenario_name)
        self.runner = runner or SubprocessRunner()
        self.monotonic = monotonic
        self.sleeper = sleeper
        self.dashboard = dashboard
        self.open_dashboard = open_dashboard
        self.run_directory = safe_run_directory(config.artifact_root, self.run_id)
        self.env_file = self.run_directory / "compose.env"
        self.compose = Compose(config.root, config.compose_file, self.env_file, self.runner)
        self.run_file = self.run_directory / "run.json"
        self.timeline_file = self.run_directory / "state-transitions.jsonl"
        self.metrics_file = self.run_directory / "metrics.jsonl"
        self._sequence = 0
        self._state: str | None = None
        self._run_values: dict[str, Any] = {}

    @property
    def dashboard_url(self) -> str:
        port = int(self.config.values["ports"]["grafana"])
        return f"http://127.0.0.1:{port}/d/market-data-reliability?var-run_id={self.run_id}"

    def _transition(self, state: str, actor: str, reason: str) -> None:
        if self._state is not None:
            if self._state in TERMINAL_STATES:
                raise RuntimeError(f"cannot leave terminal state {self._state}")
            if state not in TRANSITIONS[self._state]:
                raise RuntimeError(f"invalid run transition {self._state} -> {state}")
        self._sequence += 1
        self._state = state
        append_jsonl(
            self.timeline_file,
            artifact(
                "state-transition",
                self.run_id,
                sequence=self._sequence,
                state=state,
                actor=actor,
                occurred_at=utc_now(),
                reason=reason[:500],
            ),
        )
        self._run_values.update(state=state, state_reason=reason[:500])
        atomic_write_json(self.run_file, artifact("run", self.run_id, **self._run_values))

    def _progress(self, name: str) -> dict[str, Any] | None:
        path = self.run_directory / name
        if not path.is_file():
            return None
        return read_artifact(path, run_id=self.run_id)

    def _wait(self, description: str, predicate: Callable[[], Any], timeout_seconds: float) -> Any:
        deadline = self.monotonic() + timeout_seconds
        while self.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            self.sleeper(float(self.config.values["timeouts"]["poll_seconds"]))
        raise DeadlineFailure(f"Timed out waiting for {description}", phase=self._state or "unknown")

    def _base_environment(self, schema_id: int = 0, start_offsets_json: str = "{}") -> dict[str, str]:
        password = self._run_values.get("grafana_password")
        if not isinstance(password, str):
            password = secrets.token_urlsafe(24)
            self._run_values["grafana_password"] = password
        return compose_environment(
            self.config,
            self.run_id,
            schema_id,
            password,
            scenario_name=self.scenario_name,
            start_offsets_json=start_offsets_json,
        )

    def preflight(self) -> None:
        if sys.version_info < (3, 11):
            raise ConfigFailure("Python 3.11 or newer is required", remediation="Install Python 3.11+ and rerun the command.")
        for args, message in (
            (("docker", "--version"), "Docker CLI is not installed"),
            (("docker", "compose", "version"), "Docker Compose v2 is not installed"),
            (("docker", "info"), "Docker Desktop daemon is not available"),
        ):
            result = self.runner.run(args, cwd=self.config.root, timeout=20)
            if result.returncode != 0:
                raise ConfigFailure(message, remediation="Start Docker Desktop and rerun the command.")
        free_gib = shutil.disk_usage(self.config.root).free / 1024**3
        required_disk = float(self.config.values["resources"]["required_disk_gib"])
        if free_gib < required_disk:
            raise ConfigFailure(f"At least {required_disk:g} GiB free disk is required; found {free_gib:.1f} GiB")
        for name, port in self.config.values["ports"].items():
            if name == "grafana" and not self.dashboard:
                continue
            if not _port_available(int(port)):
                raise ConfigFailure(
                    f"Loopback port {port} ({name}) is already occupied",
                    remediation=f"Stop the process on 127.0.0.1:{port} or change config/demo.toml.",
                )
        self.compose.validate()

    def _sample_lag(self) -> int:
        producer = self._progress("producer-progress.json") or {}
        streaming = self._progress("streaming-progress.json") or {}
        produced = int(producer.get("records", 0))
        processed = int(streaming.get("records", 0))
        lag = max(0, produced - processed)
        append_jsonl(
            self.metrics_file,
            artifact(
                "lag-sample",
                self.run_id,
                sampled_at=utc_now(),
                state=self._state,
                produced_frontier=produced,
                processed_frontier=processed,
                total_lag=lag,
            ),
        )
        return lag

    def _wait_for_kill_gate(self) -> tuple[dict[str, Any], dict[str, Any]]:
        minimum_processed = int(self.settings["kill_after_processed"])
        minimum_published = int(self.settings["kill_publish_min"])
        maximum_published = int(self.settings["kill_publish_max"])

        def gate() -> tuple[dict[str, Any], dict[str, Any]] | None:
            if not self.compose.is_running("spark-recovery"):
                raise InfrastructureFailure(
                    "Spark recovery driver exited before the failure gate",
                    "producing",
                    "Inspect the spark-recovery container logs and retry after fixing the reported cause.",
                )
            producer = self._progress("producer-progress.json") or {}
            streaming = self._progress("streaming-progress.json") or {}
            delivered = int(producer.get("records", 0))
            processed = int(streaming.get("records", 0))
            if producer.get("state") == "FAILED":
                raise InfrastructureFailure("Producer failed before the recovery gate", "producing", "Inspect producer-progress.json.")
            if producer.get("state") == "COMPLETED" and processed < minimum_processed:
                raise InfrastructureFailure("Producer completed before the failure gate", "producing", "Use the configured rate and reduced fixture together.")
            if delivered > maximum_published:
                raise InfrastructureFailure("Producer passed the maximum safe kill gate", "producing", "Inspect Spark throughput before retrying.")
            self._sample_lag()
            if delivered >= minimum_published and processed >= minimum_processed:
                return producer, streaming
            return None

        return self._wait("the deterministic failure gate", gate, float(self.config.values["timeouts"]["scenario_seconds"]))

    def run(self) -> ScenarioResult:
        if self.run_directory.exists():
            raise ConfigFailure(f"run_id already exists and cannot be reused: {self.run_id}")
        with DemoLock(self.config.root / ".gstack" / "demo.lock", self.run_id):
            self.run_directory.mkdir(parents=True)
            self._run_values = {
                "scenario": self.scenario_name,
                "dataset_id": "pending",
                "created_at": utc_now(),
                "dashboard_url": self.dashboard_url,
            }
            environment = self._base_environment()
            self._run_values["compose_env_sha256"] = render_compose_env(self.env_file, environment)
            try:
                self._transition("CREATED", "harness", "run identity and immutable host configuration created")
                self.preflight()
                self.compose.build("provision", "spark-master")
                self.compose.up("kafka", "schema-registry", "cassandra", "spark-master", "spark-worker")
                self._transition("INFRA_READY", "harness", "required services passed health checks")
                self.compose.run("provision")
                provision = read_artifact(self.run_directory / "provision.json", run_id=self.run_id, artifact_type="provision")
                if provision.get("state") != "COMPLETED":
                    raise InfrastructureFailure("Provisioning did not complete", "provision", "Inspect provision.json.")
                manifest = json.loads((self.config.root / str(self.settings["manifest"])).read_text(encoding="utf-8"))
                self._run_values.update(
                    dataset_id=manifest["dataset_id"],
                    schema_id=int(provision["schema_id"]),
                    start_offsets_inclusive=provision["start_offsets_inclusive"],
                )
                environment = self._base_environment(int(provision["schema_id"]), str(provision["spark_starting_offsets"]))
                self._run_values["compose_env_sha256"] = render_compose_env(self.env_file, environment)
                self._transition("RUN_READY", "harness", "schema and run-scoped starting offsets provisioned")
                self.compose.up("spark-recovery")
                self.compose.up("producer")
                if self.dashboard:
                    self.compose.command("--profile", "dashboard", "up", "-d", "--build", "grafana")
                self._transition("PRODUCING", "producer", "first acknowledged deliveries and recovery query are active")
                self._wait_for_kill_gate()
                self.compose.kill("spark-recovery")
                self._transition("FAILURE_INJECTED", "harness", "Spark driver stopped with SIGKILL while producer remained active")
                baseline_lag = self._sample_lag()
                dwell_deadline = self.monotonic() + float(self.settings["failure_dwell_seconds"])
                peak_lag = baseline_lag
                while self.monotonic() < dwell_deadline:
                    self.sleeper(min(2.0, dwell_deadline - self.monotonic()))
                    peak_lag = max(peak_lag, self._sample_lag())
                if peak_lag - baseline_lag < int(self.settings["minimum_lag_rise"]):
                    raise InvariantFailure(
                        f"Run-scoped lag rose by {peak_lag - baseline_lag}, below required {self.settings['minimum_lag_rise']}",
                        phase="failure-injected",
                    )
                self.compose.recreate("spark-recovery")
                self._transition("RECOVERING", "harness", "Spark driver recreated with the same checkpoint volume")

                def producer_complete() -> dict[str, Any] | None:
                    progress = self._progress("producer-progress.json")
                    if progress and progress.get("state") == "FAILED":
                        raise InfrastructureFailure("Producer delivery accounting failed", "recovering", "Inspect producer-progress.json.")
                    self._sample_lag()
                    return progress if progress and progress.get("state") == "COMPLETED" else None

                produced = self._wait("producer delivery completion", producer_complete, float(self.config.values["timeouts"]["scenario_seconds"]))
                expected_total = int(produced["expected_records"])

                def query_caught_up() -> dict[str, Any] | None:
                    progress = self._progress("streaming-progress.json")
                    if progress and progress.get("state") == "FAILED":
                        raise InfrastructureFailure("Recovery query failed", "recovering", "Inspect streaming-progress.json and Spark logs.")
                    self._sample_lag()
                    return progress if progress and int(progress.get("records", 0)) >= expected_total else None

                self._wait("recovery query to reach the captured end frontier", query_caught_up, float(self.config.values["timeouts"]["scenario_seconds"]))
                self.compose.run("gold-finalizer")
                self._transition("VERIFYING", "harness", "producer flushed and the query caught up")
                verification = self.compose.run("verifier", check=False)
                report = read_artifact(self.run_directory / "run-report.json", run_id=self.run_id, artifact_type="run-report")
                if verification.returncode != 0 or report.get("data_contract_status") != "PASSED":
                    raise InvariantFailure("One or more independent invariants failed")
                self._run_values["completed_at"] = utc_now()
                self._run_values["data_contract_status"] = report["data_contract_status"]
                self._run_values["portfolio_release_status"] = report["portfolio_release_status"]
                self._transition("PASSED", "reconciler", "all named correctness and recovery invariants passed")
                if self.dashboard:
                    self.compose.command("--profile", "dashboard", "run", "--rm", "dashboard-smoke", check=False)
                    report = read_artifact(
                        self.run_directory / "run-report.json",
                        run_id=self.run_id,
                        artifact_type="run-report",
                    )
                    self._run_values["portfolio_release_status"] = report["portfolio_release_status"]
                    atomic_write_json(self.run_file, artifact("run", self.run_id, **self._run_values))
                from market_pipeline.verification.report import write_report

                write_report(
                    self.run_directory / "report.html",
                    report,
                    timeline=_read_jsonl(self.timeline_file),
                    lag_samples=_read_jsonl(self.metrics_file),
                )
                if self.open_dashboard and self.dashboard:
                    webbrowser.open(self.dashboard_url)
                return ScenarioResult(self.run_id, "PASSED", self.run_directory / "report.html", self.dashboard_url)
            except DemoFailure as failure:
                terminal = "TIMED_OUT" if failure.category == "TIMEOUT" else "FAILED"
                if self._state not in TERMINAL_STATES:
                    self._transition(terminal, "harness", failure.message)
                write_primary_failure(
                    self.run_directory / "failure.json",
                    artifact(
                        "failure",
                        self.run_id,
                        category=failure.category,
                        message=failure.message[:1000],
                        failed_phase=failure.phase,
                        remediation=failure.remediation,
                        cleanup_errors=[],
                    ),
                )
                raise
            except Exception as exc:
                if self._state not in TERMINAL_STATES:
                    self._transition("FAILED", "harness", str(exc)[:500])
                write_primary_failure(
                    self.run_directory / "failure.json",
                    artifact(
                        "failure",
                        self.run_id,
                        category="UNEXPECTED",
                        message=str(exc)[:1000],
                        failed_phase=self._state or "unknown",
                        remediation="Inspect captured artifacts and service logs.",
                        cleanup_errors=[],
                    ),
                )
                raise DemoFailure("UNEXPECTED", str(exc), self._state or "unknown", UNEXPECTED, "Inspect captured artifacts and service logs.") from exc


def cleanup_run(config: DemoConfig, run_id: str, runner: CommandRunner | None = None) -> Path:
    directory = safe_run_directory(config.artifact_root, validate_run_id(run_id), must_exist=True)
    run = read_artifact(directory / "run.json", run_id=run_id, artifact_type="run")
    if (config.root / ".gstack" / "demo.lock").exists():
        raise ConfigFailure("Cannot clean a run while the orchestrator lock is active", phase="cleanup")
    expected_project = f"market-recovery-{run_id}"
    env_file = directory / "compose.env"
    env = env_file.read_text(encoding="utf-8")
    if f"COMPOSE_PROJECT_NAME={expected_project}\n" not in env.replace("\r\n", "\n"):
        raise ConfigFailure("Run Compose ownership does not match the requested run", phase="cleanup")
    Compose(config.root, config.compose_file, env_file, runner).down(volumes=True)
    run["runtime_cleaned"] = True
    run["updated_at"] = utc_now()
    atomic_write_json(directory / "run.json", run)
    return directory


def purge_evidence(config: DemoConfig, run_id: str) -> None:
    directory = safe_run_directory(config.artifact_root, validate_run_id(run_id), must_exist=True)
    run = read_artifact(directory / "run.json", run_id=run_id, artifact_type="run")
    if (config.root / ".gstack" / "demo.lock").exists():
        raise ConfigFailure("Cannot purge evidence while the orchestrator lock is active", phase="cleanup")
    if not run.get("runtime_cleaned"):
        raise ConfigFailure("Run runtime must be cleaned before evidence can be purged", phase="cleanup")
    if directory.is_symlink() or directory.parent != config.artifact_root.resolve():
        raise ConfigFailure("Refusing to purge an unsafe evidence path", phase="cleanup")
    shutil.rmtree(directory)
