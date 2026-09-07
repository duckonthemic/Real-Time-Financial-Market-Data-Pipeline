from __future__ import annotations

from collections import Counter
from dataclasses import replace
from pathlib import Path

import pytest

from market_pipeline.contracts.models import VerificationConfig
from market_pipeline.producer.fixture import iter_fixture, load_manifest
from market_pipeline.verification.invariants import ValueSample, VerificationSnapshot, verify


ROOT = Path(__file__).parents[3]
TOPIC = "market.trades.v1"


@pytest.fixture(scope="module")
def good_case() -> tuple[VerificationConfig, VerificationSnapshot]:
    manifest = load_manifest(ROOT / "fixtures" / "recovery-showcase" / "manifest.json")
    generated = list(iter_fixture(manifest, "run-verify-01"))
    silver_ids = frozenset(
        record.event["event_id"]
        for record in generated
        if record.event is not None and 0 <= int(record.event["source_sequence"]) < manifest["canonical_records"]
    )
    starts = {0: 100, 1: 200, 2: 300}
    sizes = {0: 412, 1: 412, 2: 412}
    ends = {partition: starts[partition] + size for partition, size in sizes.items()}
    coordinates = frozenset((TOPIC, partition, offset) for partition in starts for offset in range(starts[partition], ends[partition]))
    rejection_codes = tuple(code for code, count in manifest["rejection_counts"].items() for _ in range(count))
    dlq_coordinates = frozenset(sorted(coordinates)[: manifest["invalid_records"]])
    sample_event = next(record.event for record in generated if record.event is not None and record.event["source_sequence"] == 0)
    sample = ValueSample(event_id=sample_event["event_id"], expected=sample_event, actual=dict(sample_event))
    gold_row = {
        "symbol": "AAPL",
        "window_start_ms": 1735828200000,
        "window_end_ms": 1735828500000,
        "open": 90.0,
        "high": 91.0,
        "low": 89.5,
        "close": 90.5,
        "volume": 100,
        "trade_count": 4,
        "vwap": 90.25,
    }
    config = VerificationConfig(
        run_id="run-verify-01",
        dataset_id=manifest["dataset_id"],
        input_topic=TOPIC,
        expected_canonical=manifest["canonical_records"],
        expected_duplicates=manifest["duplicate_records"],
        expected_invalid=manifest["invalid_records"],
        expected_total=manifest["total_records"],
        expected_rejections=manifest["rejection_counts"],
        expected_event_set_sha256=manifest["canonical_event_set_sha256"],
    )
    snapshot = VerificationSnapshot(
        start_offsets_inclusive=starts,
        end_offsets_exclusive=ends,
        bronze_coordinates=coordinates,
        acknowledged_inputs=manifest["total_records"],
        silver_event_ids=silver_ids,
        observed_duplicate_inputs=manifest["duplicate_records"],
        dlq_coordinates=dlq_coordinates,
        kafka_dlq_coordinates=dlq_coordinates,
        dlq_rejection_codes=rejection_codes,
        cumulative_next_offsets=ends,
        terminal_lag=0,
        batch_statuses=("COMPLETED", "COMPLETED"),
        recovery_observed=True,
        lag_rise=160,
        value_samples=(sample,),
        bronze_digest_samples=(("0:100", "MATCH"),),
        expected_gold_rows=(gold_row,),
        actual_gold_rows=(dict(gold_row),),
    )
    return config, snapshot


def failed_names(config: VerificationConfig, snapshot: VerificationSnapshot) -> set[str]:
    return {check.name for check in verify(config, snapshot) if not check.passed}


def test_known_good_snapshot_passes(good_case) -> None:
    config, snapshot = good_case
    assert failed_names(config, snapshot) == set()


def test_missing_bronze_coordinate_fails_source_coverage(good_case) -> None:
    config, snapshot = good_case
    mutated = replace(snapshot, bronze_coordinates=frozenset(list(snapshot.bronze_coordinates)[1:]))
    assert failed_names(config, mutated) == {"source_coverage"}


def test_extra_silver_identity_fails_identity_set(good_case) -> None:
    config, snapshot = good_case
    mutated = replace(snapshot, silver_event_ids=snapshot.silver_event_ids | {"f" * 64})
    assert failed_names(config, mutated) == {"silver_identity_set", "logical_reconciliation"}


def test_wrong_dlq_reason_fails_dlq_reconciliation(good_case) -> None:
    config, snapshot = good_case
    codes = list(snapshot.dlq_rejection_codes)
    codes[0] = "OTHER"
    mutated = replace(snapshot, dlq_rejection_codes=tuple(codes))
    assert failed_names(config, mutated) == {"dlq_reconciliation"}


def test_unfinished_frontier_fails_offset_check(good_case) -> None:
    config, snapshot = good_case
    frontier = dict(snapshot.cumulative_next_offsets)
    frontier[0] -= 1
    assert failed_names(config, replace(snapshot, cumulative_next_offsets=frontier)) == {"offset_frontier"}


def test_nonzero_terminal_lag_fails_lag_check(good_case) -> None:
    config, snapshot = good_case
    assert failed_names(config, replace(snapshot, terminal_lag=1)) == {"terminal_lag_zero"}


def test_incomplete_batch_fails_ledger_check(good_case) -> None:
    config, snapshot = good_case
    assert failed_names(config, replace(snapshot, batch_statuses=("COMPLETED", "STARTED"))) == {"batch_ledger_complete"}


def test_value_corruption_fails_sampled_integrity_only(good_case) -> None:
    config, snapshot = good_case
    sample = snapshot.value_samples[0]
    corrupted = dict(sample.actual)
    corrupted["price"] += 1.0
    mutated = replace(snapshot, value_samples=(replace(sample, actual=corrupted),))
    assert failed_names(config, mutated) == {"sampled_value_integrity"}


def test_missing_recovery_fails_checkpoint_check(good_case) -> None:
    config, snapshot = good_case
    assert failed_names(config, replace(snapshot, recovery_observed=False)) == {"checkpoint_recovery"}


def test_gold_value_corruption_fails_gold_only(good_case) -> None:
    config, snapshot = good_case
    corrupt = dict(snapshot.actual_gold_rows[0])
    corrupt["vwap"] += 1.0
    assert failed_names(config, replace(snapshot, actual_gold_rows=(corrupt,))) == {"gold_ohlcv_exact"}
