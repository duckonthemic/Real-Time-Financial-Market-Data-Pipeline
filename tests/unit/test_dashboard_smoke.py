from __future__ import annotations

import json
from pathlib import Path

from market_pipeline.ops.dashboard_smoke import validate_dashboard_definition


ROOT = Path(__file__).parents[2]


def test_provisioned_dashboard_contains_release_evidence_contract() -> None:
    dashboard = json.loads((ROOT / "grafana" / "dashboards" / "market-data-main.json").read_text(encoding="utf-8"))
    assert validate_dashboard_definition(dashboard) == []


def test_dashboard_contract_reports_unscoped_widget_mosaic() -> None:
    dashboard = {"uid": "old", "panels": [{"title": "Trades", "targets": [{"target": "SELECT * FROM trades"}]}]}
    failures = validate_dashboard_definition(dashboard)
    assert "dashboard UID must be market-data-reliability" in failures
    assert "dashboard queries are not scoped by ${run_id}" in failures
