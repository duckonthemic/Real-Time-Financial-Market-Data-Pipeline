#!/usr/bin/env python3
"""One-command host entry point for the deterministic recovery lab."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from demo_support.config import load_demo_config
from demo_support.errors import DemoFailure, UNEXPECTED
from demo_support.scenario import RecoveryScenario, cleanup_run, generated_run_id, purge_evidence


ROOT = Path(__file__).resolve().parent.parent


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description="Run or clean the Market Data Reliability Lab")
    actions = command.add_mutually_exclusive_group()
    actions.add_argument("--cleanup-run", metavar="RUN_ID", help="Remove only one run's Compose resources and checkpoint; keep evidence")
    actions.add_argument("--purge-evidence", metavar="RUN_ID", help="Delete one already-cleaned run's evidence directory")
    command.add_argument("--scenario", choices=("recovery", "recovery-showcase"), default="recovery")
    command.add_argument("--run-id", help="Optional non-reusable run ID")
    command.add_argument("--no-dashboard", action="store_true", help="Skip Grafana for reduced CI runs")
    command.add_argument("--open-dashboard", action="store_true", help="Open the loopback Grafana deep link after PASS")
    return command


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        config = load_demo_config(ROOT / "config" / "demo.toml")
        if args.cleanup_run:
            directory = cleanup_run(config, args.cleanup_run)
            print(f"Runtime cleaned; evidence retained at {directory}")
            return 0
        if args.purge_evidence:
            purge_evidence(config, args.purge_evidence)
            print(f"Evidence purged for {args.purge_evidence}")
            return 0
        scenario_name = "standard" if args.scenario == "recovery" else "recovery-showcase"
        scenario = RecoveryScenario(
            config,
            run_id=args.run_id or generated_run_id(),
            scenario_name=scenario_name,
            dashboard=not args.no_dashboard,
            open_dashboard=args.open_dashboard,
        )
        print(f"RUN_ID: {scenario.run_id}")
        print(f"DASHBOARD: {scenario.dashboard_url}")
        result = scenario.run()
        print(f"PASS: {result.run_id}")
        print(f"REPORT: {result.report_path}")
        print(f"Clean runtime later: python scripts/demo.py --cleanup-run {result.run_id}")
        return 0
    except DemoFailure as failure:
        print(f"{failure.category}: {failure.message}", file=sys.stderr)
        print(f"Remediation: {failure.remediation}", file=sys.stderr)
        return failure.exit_code
    except KeyboardInterrupt:
        print("Interrupted by user; run evidence and checkpoints were retained.", file=sys.stderr)
        return 6
    except Exception as exc:
        print(f"UNEXPECTED: {exc}", file=sys.stderr)
        return UNEXPECTED


if __name__ == "__main__":
    raise SystemExit(main())
