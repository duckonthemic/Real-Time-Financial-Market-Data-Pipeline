"""Independent, named correctness checks over run-scoped projections."""

from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from market_pipeline.contracts.models import VerificationConfig


Coordinate = tuple[str, int, int]


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    expected: Any
    actual: Any
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "expected": self.expected,
            "actual": self.actual,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class ValueSample:
    event_id: str
    expected: Mapping[str, Any]
    actual: Mapping[str, Any]


@dataclass(frozen=True)
class VerificationSnapshot:
    start_offsets_inclusive: Mapping[int, int]
    end_offsets_exclusive: Mapping[int, int]
    bronze_coordinates: frozenset[Coordinate]
    acknowledged_inputs: int
    silver_event_ids: frozenset[str]
    observed_duplicate_inputs: int
    dlq_coordinates: frozenset[Coordinate]
    kafka_dlq_coordinates: frozenset[Coordinate]
    dlq_rejection_codes: tuple[str, ...]
    cumulative_next_offsets: Mapping[int, int]
    terminal_lag: int
    batch_statuses: tuple[str, ...]
    recovery_observed: bool
    lag_rise: int
    value_samples: tuple[ValueSample, ...] = field(default_factory=tuple)
    bronze_digest_samples: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    expected_gold_rows: tuple[Mapping[str, Any], ...] = field(default_factory=tuple)
    actual_gold_rows: tuple[Mapping[str, Any], ...] = field(default_factory=tuple)


def expected_coordinates(config: VerificationConfig, snapshot: VerificationSnapshot) -> frozenset[Coordinate]:
    return frozenset(
        (config.input_topic, partition, offset)
        for partition, start in snapshot.start_offsets_inclusive.items()
        for offset in range(start, snapshot.end_offsets_exclusive[partition])
    )


def _event_set_digest(event_ids: Iterable[str]) -> str:
    return hashlib.sha256("\n".join(sorted(event_ids)).encode("ascii")).hexdigest()


def verify(config: VerificationConfig, snapshot: VerificationSnapshot) -> list[Check]:
    checks: list[Check] = []
    expected_source = expected_coordinates(config, snapshot)
    checks.append(
        Check(
            "producer_delivery",
            snapshot.acknowledged_inputs == config.expected_total,
            config.expected_total,
            snapshot.acknowledged_inputs,
            "Only successful Kafka delivery callbacks count toward the input total.",
        )
    )
    checks.append(
        Check(
            "source_coverage",
            snapshot.bronze_coordinates == expected_source,
            len(expected_source),
            len(snapshot.bronze_coordinates),
            "Bronze must contain exactly one row for every captured Kafka coordinate.",
        )
    )
    actual_event_digest = _event_set_digest(snapshot.silver_event_ids)
    checks.append(
        Check(
            "silver_identity_set",
            len(snapshot.silver_event_ids) == config.expected_canonical
            and actual_event_digest == config.expected_event_set_sha256,
            {"count": config.expected_canonical, "sha256": config.expected_event_set_sha256},
            {"count": len(snapshot.silver_event_ids), "sha256": actual_event_digest},
            "Unique Silver event identities must equal the manifest set.",
        )
    )
    rejection_counts = Counter(snapshot.dlq_rejection_codes)
    checks.append(
        Check(
            "dlq_reconciliation",
            len(snapshot.dlq_coordinates) == config.expected_invalid
            and dict(rejection_counts) == config.expected_rejections,
            {"count": config.expected_invalid, "reasons": config.expected_rejections},
            {"count": len(snapshot.dlq_coordinates), "reasons": dict(rejection_counts)},
            "DLQ coordinates and first-match rejection reasons must match the fixture.",
        )
    )
    checks.append(
        Check(
            "kafka_dlq_distinct_keys",
            snapshot.kafka_dlq_coordinates == snapshot.dlq_coordinates,
            len(snapshot.dlq_coordinates),
            len(snapshot.kafka_dlq_coordinates),
            "Distinct Kafka DLQ coordinate keys must equal the Cassandra quarantine projection.",
        )
    )
    checks.append(
        Check(
            "logical_reconciliation",
            len(snapshot.silver_event_ids) + snapshot.observed_duplicate_inputs + len(snapshot.dlq_coordinates)
            == config.expected_total,
            config.expected_total,
            len(snapshot.silver_event_ids) + snapshot.observed_duplicate_inputs + len(snapshot.dlq_coordinates),
            "Canonical, duplicate, and invalid outcomes reconcile to the physical input count.",
        )
    )
    checks.append(
        Check(
            "offset_frontier",
            dict(snapshot.cumulative_next_offsets) == dict(snapshot.end_offsets_exclusive),
            dict(snapshot.end_offsets_exclusive),
            dict(snapshot.cumulative_next_offsets),
            "The single recovery query must reach every captured end offset.",
        )
    )
    checks.append(
        Check(
            "terminal_lag_zero",
            snapshot.terminal_lag == 0,
            0,
            snapshot.terminal_lag,
            "Run-clamped lag must return to zero before verification.",
        )
    )
    incomplete = [status for status in snapshot.batch_statuses if status != "COMPLETED"]
    checks.append(
        Check(
            "batch_ledger_complete",
            not incomplete,
            "all COMPLETED",
            list(snapshot.batch_statuses),
            "No STARTED or FAILED micro-batch may remain at verification time.",
        )
    )
    checks.append(
        Check(
            "checkpoint_recovery",
            snapshot.recovery_observed and snapshot.lag_rise > 0,
            {"recovery_observed": True, "lag_rise_min": 1},
            {"recovery_observed": snapshot.recovery_observed, "lag_rise": snapshot.lag_rise},
            "A non-graceful driver failure and retained-checkpoint recovery must be observed.",
        )
    )
    mismatches = []
    for sample in snapshot.value_samples:
        expected = sample.expected
        actual = sample.actual
        try:
            price_mismatch = (
                abs(float(expected.get("price", 0)) - float(actual.get("price", 0)))
                > config.price_absolute_tolerance
            )
        except (TypeError, ValueError):
            price_mismatch = True
        if price_mismatch or (
            expected.get("symbol") != actual.get("symbol")
            or expected.get("volume") != actual.get("volume")
            or expected.get("event_time_ms") != actual.get("event_time_ms")
            or list(expected.get("conditions") or []) != list(actual.get("conditions") or [])
            or expected.get("source_sequence") != actual.get("source_sequence")
        ):
            mismatches.append(sample.event_id)
    digest_mismatches = [coordinate for coordinate, state in snapshot.bronze_digest_samples if state != "MATCH"]
    checks.append(
        Check(
            "sampled_value_integrity",
            not mismatches and not digest_mismatches,
            {"field_mismatches": 0, "bronze_digest_mismatches": 0},
            {"field_mismatches": len(mismatches), "bronze_digest_mismatches": len(digest_mismatches)},
            "Deterministic samples compare Silver values and Bronze source-record digests; this is not exhaustive equality.",
        )
    )
    expected_gold = {
        (str(row["symbol"]), int(row["window_start_ms"])): row
        for row in snapshot.expected_gold_rows
    }
    actual_gold = {
        (str(row["symbol"]), int(row["window_start_ms"])): row
        for row in snapshot.actual_gold_rows
    }
    gold_mismatches: list[str] = []
    for key, expected in expected_gold.items():
        actual = actual_gold.get(key)
        if actual is None:
            gold_mismatches.append(f"{key}:missing")
            continue
        numeric_fields = ("open", "high", "low", "close", "vwap")
        if any(
            abs(float(expected[field]) - float(actual[field]))
            > config.price_absolute_tolerance
            for field in numeric_fields
        ) or any(expected[field] != actual[field] for field in ("volume", "trade_count", "window_end_ms")):
            gold_mismatches.append(f"{key}:value")
    extra_gold = sorted(set(actual_gold) - set(expected_gold))
    gold_passed = bool(expected_gold) and not gold_mismatches and not extra_gold
    checks.append(
        Check(
            "gold_ohlcv_exact",
            gold_passed,
            {"rows": len(expected_gold), "mismatches": 0, "extra": 0},
            {
                "rows": len(actual_gold),
                "mismatches": len(gold_mismatches),
                "extra": len(extra_gold),
            },
            "Bounded Gold OHLCV/VWAP must exactly cover the completed Silver run within numeric tolerance.",
        )
    )
    return checks
