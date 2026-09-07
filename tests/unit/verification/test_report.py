from __future__ import annotations

from market_pipeline.verification.report import render_report


def test_report_has_semantic_accessible_evidence() -> None:
    report = {
        "run_id": "run-report-01",
        "updated_at": "2026-09-07T06:30:00Z",
        "scenario": "standard",
        "data_contract_status": "PASSED",
        "portfolio_release_status": "READY",
        "checks": [{"name": "source_coverage", "passed": True, "expected": 10, "actual": 10}],
    }
    rendered = render_report(
        report,
        timeline=[{"occurred_at": "2026-09-07T06:29:00Z", "state": "RECOVERING", "reason": "checkpoint retained"}],
        lag_samples=[{"sampled_at": "2026-09-07T06:29:00Z", "state": "RECOVERING", "produced_frontier": 10, "processed_frontier": 5, "total_lag": 5}],
    )
    assert '<a class="skip" href="#main">' in rendered
    assert "<main id=\"main\">" in rendered
    assert "<caption>Expected and actual values remain adjacent.</caption>" in rendered
    assert "aria-live=\"polite\"" in rendered
    assert "prefers-reduced-motion" in rendered
    assert "at-least-once processing with replay-safe projections" in rendered


def test_split_verdict_keeps_data_pass_and_release_warning() -> None:
    rendered = render_report(
        {
            "run_id": "run-report-01",
            "updated_at": "2026-09-07T06:30:00Z",
            "scenario": "standard",
            "data_contract_status": "PASSED",
            "portfolio_release_status": "NOT_READY",
            "checks": [],
        }
    )
    assert "Data Contract Passed" in rendered
    assert "NOT READY FOR CV" in rendered


def test_report_escapes_untrusted_values() -> None:
    rendered = render_report(
        {
            "run_id": "<script>alert(1)</script>",
            "updated_at": "now",
            "scenario": "standard",
            "data_contract_status": "FAILED",
            "portfolio_release_status": "NOT_READY",
            "checks": [{"name": "<img src=x>", "passed": False, "expected": "<x>", "actual": "<y>"}],
        }
    )
    assert "<script>alert(1)</script>" not in rendered
    assert "&lt;script&gt;" in rendered
    assert "&lt;img src=x&gt;" in rendered
