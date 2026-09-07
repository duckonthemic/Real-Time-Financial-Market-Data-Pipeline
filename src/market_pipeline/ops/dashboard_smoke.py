"""Validate the provisioned Grafana observer and update the portfolio verdict."""

from __future__ import annotations

import base64
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Mapping

from market_pipeline.ops.runtime import artifact, atomic_json, required_env
from market_pipeline.ops.runtime import run_config_from_env
from market_pipeline.verification.main import publish_evidence
from market_pipeline.verification.report import write_report


REQUIRED_TABLES = {
    "pipeline_runs",
    "run_checks_by_run",
    "run_state_transitions_by_run",
    "run_metrics_by_run",
}
REQUIRED_PANELS = {
    "Run verdict",
    "Invariant scorecard",
    "Recovery timeline",
    "Run-scoped lag",
}


def validate_dashboard_definition(dashboard: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if dashboard.get("uid") != "market-data-reliability":
        errors.append("dashboard UID must be market-data-reliability")
    panels = list(dashboard.get("panels") or [])
    titles = {str(panel.get("title")) for panel in panels}
    for title in sorted(REQUIRED_PANELS - titles):
        errors.append(f"missing panel: {title}")
    queries = "\n".join(
        str(target.get("target") or target.get("query") or "")
        for panel in panels
        for target in panel.get("targets") or []
    )
    for table in sorted(REQUIRED_TABLES):
        if table not in queries:
            errors.append(f"no panel queries {table}")
    if "${run_id}" not in queries:
        errors.append("dashboard queries are not scoped by ${run_id}")
    variables = list((dashboard.get("templating") or {}).get("list") or [])
    if not any(variable.get("name") == "run_id" for variable in variables):
        errors.append("missing run_id template variable")
    return errors


def _request_json(url: str, username: str, password: str) -> Any:
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    request = urllib.request.Request(url, headers={"Authorization": f"Basic {token}"})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _poll_endpoint(
    name: str,
    url: str,
    password: str,
    failures: list[str],
) -> Any:
    for attempt in range(12):
        try:
            return _request_json(url, "admin", password)
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            if attempt == 11:
                failures.append(f"Grafana {name} endpoint unavailable: {exc}")
            else:
                time.sleep(2)
    return {}


def main() -> int:
    run_id = required_env("RUN_ID")
    artifact_root = Path(required_env("ARTIFACT_PATH"))
    report_path = artifact_root / "run-report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    dashboard_path = Path(os.environ.get("DASHBOARD_JSON", "grafana/dashboards/market-data-main.json"))
    failures = validate_dashboard_definition(json.loads(dashboard_path.read_text(encoding="utf-8")))
    grafana_url = os.environ.get("GRAFANA_URL", "http://grafana:3000").rstrip("/")
    password = required_env("GRAFANA_ADMIN_PASSWORD")
    observations: dict[str, Any] = {}
    observations["health"] = _poll_endpoint(
        "health", f"{grafana_url}/api/health", password, failures
    )
    observations["dashboard"] = _poll_endpoint(
        "dashboard",
        f"{grafana_url}/api/dashboards/uid/market-data-reliability",
        password,
        failures,
    )
    observations["datasource"] = _poll_endpoint(
        "datasource",
        f"{grafana_url}/api/datasources/uid/cassandra/health",
        password,
        failures,
    )
    if observations.get("health", {}).get("database") != "ok":
        failures.append("Grafana database health is not ok")
    datasource_status = str(observations.get("datasource", {}).get("status", "")).lower()
    if datasource_status not in {"ok", "success"}:
        failures.append("Cassandra datasource health is not successful")
    if observations.get("dashboard", {}).get("dashboard", {}).get("uid") != "market-data-reliability":
        failures.append("provisioned dashboard UID was not returned by Grafana")

    release_status = "READY" if not failures and report.get("data_contract_status") == "PASSED" else "NOT_READY"
    report["portfolio_release_status"] = release_status
    report["release_checks"] = {
        "grafana_smoke": "PASSED" if not failures else "FAILED",
        "failures": failures,
    }
    atomic_json(report_path, report)
    write_report(
        artifact_root / "report.html",
        report,
        timeline=_read_jsonl(artifact_root / "state-transitions.jsonl"),
        lag_samples=_read_jsonl(artifact_root / "metrics.jsonl"),
    )
    atomic_json(
        artifact_root / "dashboard-smoke.json",
        artifact(
            "dashboard-smoke",
            run_id,
            state="PASSED" if not failures else "FAILED",
            failures=failures,
            observations=observations,
        ),
    )
    try:
        publish_evidence(
            run_config_from_env(),
            report,
            _read_jsonl(artifact_root / "state-transitions.jsonl"),
            _read_jsonl(artifact_root / "metrics.jsonl"),
        )
    except Exception as exc:
        failures.append(f"could not publish portfolio status: {exc}")
        return 1
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
