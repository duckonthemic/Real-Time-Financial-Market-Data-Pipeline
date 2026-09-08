from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_pipeline.ops import dashboard_smoke
from market_pipeline.ops.dashboard_smoke import validate_dashboard_definition

ROOT = Path(__file__).parents[2]


def test_provisioned_dashboard_contains_release_evidence_contract() -> None:
    dashboard = json.loads(
        (ROOT / "grafana" / "dashboards" / "market-data-main.json").read_text(encoding="utf-8")
    )
    assert validate_dashboard_definition(dashboard) == []


def test_dashboard_contract_reports_unscoped_widget_mosaic() -> None:
    dashboard = {
        "uid": "old",
        "panels": [{"title": "Trades", "targets": [{"target": "SELECT * FROM trades"}]}],
    }
    failures = validate_dashboard_definition(dashboard)
    assert "dashboard UID must be market-data-reliability" in failures
    assert "dashboard queries are not scoped by ${run_id}" in failures


@pytest.mark.parametrize(
    "response",
    [{}, {"results": {"A": {"error": "bad column"}}}, {"results": {"A": {"frames": []}}}],
)
def test_query_errors_and_empty_results_block_release(monkeypatch, response) -> None:
    monkeypatch.setattr(dashboard_smoke, "_request_json", lambda *args: response)
    dashboard = {
        "panels": [
            {
                "title": "Run verdict",
                "targets": [
                    {
                        "refId": "A",
                        "target": "SELECT state FROM pipeline_runs WHERE run_id = '${run_id}'",
                    }
                ],
            }
        ]
    }
    assert dashboard_smoke.check_panel_queries(dashboard, "run-test-01", "http://test", "test")


def test_publication_failure_is_persisted_as_not_ready(tmp_path, monkeypatch) -> None:
    definition = ROOT / "grafana" / "dashboards" / "market-data-main.json"
    dashboard = json.loads(definition.read_text())
    for key, value in {
        "RUN_ID": "run-test-01",
        "ARTIFACT_PATH": str(tmp_path),
        "GRAFANA_ADMIN_PASSWORD": "test",
        "DASHBOARD_JSON": str(definition),
    }.items():
        monkeypatch.setenv(key, value)
    (tmp_path / "run-report.json").write_text(json.dumps({"data_contract_status": "PASSED"}))

    def endpoint(name, *args):
        return {
            "health": {"database": "ok"},
            "datasource": {"status": "ok"},
            "dashboard": {"dashboard": dashboard},
        }[name]

    monkeypatch.setattr(dashboard_smoke, "_poll_endpoint", endpoint)
    monkeypatch.setattr(dashboard_smoke, "check_panel_queries", lambda *args: [])
    monkeypatch.setattr(dashboard_smoke, "run_config_from_env", lambda: None)

    def fail(*args):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(dashboard_smoke, "publish_evidence", fail)
    assert dashboard_smoke.main() == 1
    report = json.loads((tmp_path / "run-report.json").read_text())
    smoke = json.loads((tmp_path / "dashboard-smoke.json").read_text())
    assert report["portfolio_release_status"] == "NOT_READY"
    assert smoke["state"] == "FAILED"
    assert "database unavailable" in smoke["failures"][0]
