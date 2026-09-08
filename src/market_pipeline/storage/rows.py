"""Exact Cassandra row mappings shared by streaming integration tests."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any


def utc_datetime_from_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def header_text(headers: Mapping[str, Any], key: str) -> str | None:
    value = headers.get(key)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    return value if isinstance(value, str) else None


def header_int(headers: Mapping[str, Any], key: str) -> int | None:
    value = header_text(headers, key)
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def bronze_row(
    raw: Mapping[str, Any], *, owner_run_id: str, schema_id: int | None
) -> dict[str, Any]:
    headers = dict(raw.get("headers") or {})
    return {
        "owner_run_id": owner_run_id,
        "source_topic": str(raw["topic"]),
        "source_partition": int(raw["partition"]),
        "source_offset": int(raw["offset"]),
        "kafka_timestamp": raw["timestamp"],
        "kafka_key": raw.get("key"),
        "raw_value": bytes(raw.get("value") or b""),
        "schema_id": schema_id,
        "claimed_run_id": header_text(headers, "run_id"),
        "claimed_dataset_id": header_text(headers, "dataset_id"),
        "claimed_source_sequence": header_int(headers, "source_sequence"),
        "claimed_payload_type": header_text(headers, "payload_type"),
        "claimed_produced_at_ms": header_int(headers, "produced_at_ms"),
    }


def silver_row(event: Mapping[str, Any], *, run_id: str) -> dict[str, Any]:
    event_time = utc_datetime_from_ms(int(event["event_time_ms"]))
    return {
        "run_id": run_id,
        "event_id": str(event["event_id"]),
        "symbol": str(event["symbol"]).strip().upper(),
        "event_time": event_time,
        "event_date": date(event_time.year, event_time.month, event_time.day),
        "price": float(event["price"]),
        "volume": int(event["volume"]),
        "conditions": list(event.get("conditions") or []),
        "source_sequence": int(event["source_sequence"]),
    }


def dlq_row(
    raw: Mapping[str, Any],
    *,
    owner_run_id: str,
    rejection_code: str,
    rejection_detail: str,
) -> dict[str, Any]:
    headers = dict(raw.get("headers") or {})
    return {
        "owner_run_id": owner_run_id,
        "source_topic": str(raw["topic"]),
        "source_partition": int(raw["partition"]),
        "source_offset": int(raw["offset"]),
        "claimed_run_id": header_text(headers, "run_id"),
        "rejection_code": rejection_code,
        "rejection_detail": rejection_detail[:500],
        "raw_value": bytes(raw.get("value") or b""),
        "source_timestamp": raw["timestamp"],
    }


def raw_value_sha256(row: Mapping[str, Any]) -> str:
    return hashlib.sha256(bytes(row.get("raw_value") or b"")).hexdigest()
