"""Deterministic validation precedence for decoded trade events."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

MISSING_OR_INVALID_HEADER = "MISSING_OR_INVALID_HEADER"
BAD_WIRE = "BAD_WIRE"
HEADER_PAYLOAD_MISMATCH = "HEADER_PAYLOAD_MISMATCH"
EMPTY_SYMBOL = "EMPTY_SYMBOL"
NON_POSITIVE_PRICE = "NON_POSITIVE_PRICE"
NON_POSITIVE_VOLUME = "NON_POSITIVE_VOLUME"
TIMESTAMP_OUT_OF_RANGE = "TIMESTAMP_OUT_OF_RANGE"

REJECTION_PRECEDENCE = (
    MISSING_OR_INVALID_HEADER,
    BAD_WIRE,
    HEADER_PAYLOAD_MISMATCH,
    EMPTY_SYMBOL,
    NON_POSITIVE_PRICE,
    NON_POSITIVE_VOLUME,
    TIMESTAMP_OUT_OF_RANGE,
)

REQUIRED_HEADERS = ("run_id", "dataset_id", "source_sequence", "payload_type", "produced_at_ms")


@dataclass(frozen=True)
class ValidationOutcome:
    accepted: bool
    rejection_code: str | None = None
    detail: str | None = None


def _header_text(headers: Mapping[str, Any], key: str) -> str | None:
    value = headers.get(key)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    return value if isinstance(value, str) else None


def validate_event(
    event: Mapping[str, Any] | None,
    headers: Mapping[str, Any],
    *,
    wire_error: bool,
    expected_run_id: str,
    expected_dataset_id: str,
    min_event_time_ms: int,
    max_event_time_ms: int,
) -> ValidationOutcome:
    """Apply the contract's first-match rejection order."""
    decoded_headers = {key: _header_text(headers, key) for key in REQUIRED_HEADERS}
    try:
        source_sequence = int(decoded_headers["source_sequence"] or "")
        produced_at_ms = int(decoded_headers["produced_at_ms"] or "")
    except ValueError:
        source_sequence = -1
        produced_at_ms = -1
    if (
        any(decoded_headers[key] in (None, "") for key in REQUIRED_HEADERS)
        or source_sequence < 0
        or produced_at_ms <= 0
        or decoded_headers["payload_type"] not in {"avro-confluent-v1", "malformed-test-v1"}
    ):
        return ValidationOutcome(
            False, MISSING_OR_INVALID_HEADER, "required Kafka header missing or invalid"
        )
    if wire_error or event is None:
        return ValidationOutcome(
            False, BAD_WIRE, "value does not match the provisioned Confluent Avro envelope"
        )
    if (
        decoded_headers["run_id"] != expected_run_id
        or decoded_headers["dataset_id"] != expected_dataset_id
        or event.get("run_id") != decoded_headers["run_id"]
        or event.get("dataset_id") != decoded_headers["dataset_id"]
        or event.get("source_sequence") != source_sequence
    ):
        return ValidationOutcome(
            False, HEADER_PAYLOAD_MISMATCH, "header attribution differs from payload or run"
        )
    symbol = event.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        return ValidationOutcome(False, EMPTY_SYMBOL, "symbol is empty after trimming")
    price = event.get("price")
    if (
        not isinstance(price, int | float)
        or isinstance(price, bool)
        or not math.isfinite(price)
        or price <= 0
    ):
        return ValidationOutcome(
            False, NON_POSITIVE_PRICE, "price must be finite and greater than zero"
        )
    volume = event.get("volume")
    if not isinstance(volume, int) or isinstance(volume, bool) or volume <= 0:
        return ValidationOutcome(False, NON_POSITIVE_VOLUME, "volume must be a positive integer")
    event_time_ms = event.get("event_time_ms")
    if (
        not isinstance(event_time_ms, int)
        or not min_event_time_ms <= event_time_ms <= max_event_time_ms
    ):
        return ValidationOutcome(
            False, TIMESTAMP_OUT_OF_RANGE, "event_time_ms is outside the fixture range"
        )
    return ValidationOutcome(True)
