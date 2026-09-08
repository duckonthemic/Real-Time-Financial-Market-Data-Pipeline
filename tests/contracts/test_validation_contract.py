from __future__ import annotations

import pytest

from market_pipeline.contracts.validation import (
    BAD_WIRE,
    EMPTY_SYMBOL,
    HEADER_PAYLOAD_MISMATCH,
    MISSING_OR_INVALID_HEADER,
    NON_POSITIVE_PRICE,
    NON_POSITIVE_VOLUME,
    TIMESTAMP_OUT_OF_RANGE,
    validate_event,
)

RUN = "run-20260907-a1"
DATASET = "dataset-v1"


def headers(**overrides: str) -> dict[str, str]:
    values = {
        "run_id": RUN,
        "dataset_id": DATASET,
        "source_sequence": "4",
        "payload_type": "avro-confluent-v1",
        "produced_at_ms": "1735828201000",
    }
    values.update(overrides)
    return values


def event(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "run_id": RUN,
        "dataset_id": DATASET,
        "source_sequence": 4,
        "symbol": "AAPL",
        "price": 150.0,
        "volume": 100,
        "event_time_ms": 1500,
    }
    values.update(overrides)
    return values


def outcome(
    value: dict[str, object] | None, header_values: dict[str, str], *, wire_error: bool = False
):
    return validate_event(
        value,
        header_values,
        wire_error=wire_error,
        expected_run_id=RUN,
        expected_dataset_id=DATASET,
        min_event_time_ms=1000,
        max_event_time_ms=2000,
    )


@pytest.mark.parametrize(
    ("value", "header_values", "wire_error", "expected"),
    [
        (event(), headers(run_id=""), False, MISSING_OR_INVALID_HEADER),
        (None, headers(), True, BAD_WIRE),
        (event(dataset_id="other"), headers(), False, HEADER_PAYLOAD_MISMATCH),
        (event(symbol="  "), headers(), False, EMPTY_SYMBOL),
        (event(price=0.0), headers(), False, NON_POSITIVE_PRICE),
        (event(volume=0), headers(), False, NON_POSITIVE_VOLUME),
        (event(event_time_ms=999), headers(), False, TIMESTAMP_OUT_OF_RANGE),
    ],
)
def test_rejection_codes(value, header_values, wire_error: bool, expected: str) -> None:
    assert outcome(value, header_values, wire_error=wire_error).rejection_code == expected


def test_first_matching_business_rule_wins() -> None:
    result = outcome(event(price=0.0, volume=0, event_time_ms=999), headers())
    assert result.rejection_code == NON_POSITIVE_PRICE


def test_valid_event_is_accepted() -> None:
    assert outcome(event(), headers()).accepted
