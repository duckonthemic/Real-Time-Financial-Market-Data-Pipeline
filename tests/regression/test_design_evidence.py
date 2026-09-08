from __future__ import annotations

import copy
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from demo_support.config import DemoConfig, load_demo_config
from demo_support.scenario import RecoveryScenario
from market_pipeline.verification.report import render_report

ROOT = Path(__file__).parents[2]
RUN_ID = "run-design-regression-01"


def _config(tmp_path: Path) -> DemoConfig:
    original = load_demo_config(ROOT / "config" / "demo.toml")
    values = copy.deepcopy(dict(original.values))
    values["project"].update(
        artifact_root="artifacts",
        checkpoint_root="checkpoints",
        compose_file="compose.yaml",
    )
    return DemoConfig(tmp_path / "config.toml", tmp_path, values)


def test_dashboard_verdicts_use_numeric_observer_projections() -> None:
    schema = (ROOT / "schemas" / "cassandra" / "002_tables.cql").read_text(encoding="utf-8")
    dashboard = (ROOT / "grafana" / "dashboards" / "market-data-main.json").read_text(
        encoding="utf-8"
    )
    publisher = (ROOT / "src" / "market_pipeline" / "verification" / "main.py").read_text(
        encoding="utf-8"
    )

    for field in ("data_contract_code", "portfolio_release_code"):
        assert f"{field} int" in schema
        assert f"SELECT {field}" in dashboard
        assert field in publisher


def test_dashboard_url_bounds_the_run_metrics(tmp_path: Path) -> None:
    scenario = RecoveryScenario(
        _config(tmp_path),
        run_id=RUN_ID,
        scenario_name="recovery-showcase",
    )
    scenario.run_directory.mkdir(parents=True)
    scenario.metrics_file.write_text(
        "\n".join(
            [
                json.dumps({"sampled_at": "2026-09-07T09:02:27.500Z"}),
                json.dumps({"sampled_at": "2026-09-07T09:03:50.900Z"}),
            ]
        ),
        encoding="utf-8",
    )

    query = parse_qs(urlparse(scenario.dashboard_url).query)

    assert query["var-run_id"] == [RUN_ID]
    assert int(query["from"][0]) == 1788771732500
    assert int(query["to"][0]) == 1788771845900


def test_report_wraps_long_evidence_inside_the_viewport() -> None:
    rendered = render_report(
        {
            "run_id": RUN_ID,
            "updated_at": "2026-09-07T09:04:05Z",
            "scenario": "recovery-showcase",
            "data_contract_status": "PASSED",
            "portfolio_release_status": "READY",
            "checks": [
                {
                    "name": "sampled_value_integrity",
                    "passed": True,
                    "expected": {"sha256": "a" * 64},
                    "actual": {"sha256": "a" * 64},
                }
            ],
        }
    )

    assert 'class="checks-table"' in rendered
    assert "overflow-wrap:anywhere" in rendered
