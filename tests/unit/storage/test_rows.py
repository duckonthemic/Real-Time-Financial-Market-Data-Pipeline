from __future__ import annotations

from datetime import UTC, datetime

from market_pipeline.storage.rows import bronze_row, dlq_row, silver_row


def test_bronze_storage_uses_owner_run_not_claimed_header() -> None:
    raw = {
        "topic": "market.trades.v1",
        "partition": 2,
        "offset": 17,
        "timestamp": datetime(2025, 1, 2, tzinfo=UTC),
        "key": b"AAPL",
        "value": b"raw",
        "headers": {"run_id": b"spoofed", "dataset_id": b"dataset", "source_sequence": b"7"},
    }
    row = bronze_row(raw, owner_run_id="run-owner-01", schema_id=4)
    assert row["owner_run_id"] == "run-owner-01"
    assert row["claimed_run_id"] == "spoofed"
    assert row["source_partition"] == 2
    assert row["source_offset"] == 17


def test_silver_mapping_uses_deterministic_primary_key_fields() -> None:
    row = silver_row(
        {
            "event_id": "a" * 64,
            "symbol": " aapl ",
            "event_time_ms": 1735828200000,
            "price": 150.25,
            "volume": 10,
            "conditions": ["REGULAR"],
            "source_sequence": 4,
        },
        run_id="run-owner-01",
    )
    assert row["run_id"] == "run-owner-01"
    assert row["event_id"] == "a" * 64
    assert row["symbol"] == "AAPL"
    assert row["event_time"].tzinfo is UTC


def test_dlq_source_timestamp_is_stable_kafka_timestamp() -> None:
    timestamp = datetime(2025, 1, 2, tzinfo=UTC)
    raw = {"topic": "market.trades.v1", "partition": 0, "offset": 2, "timestamp": timestamp, "value": b"bad", "headers": {}}
    row = dlq_row(raw, owner_run_id="run-owner-01", rejection_code="BAD_WIRE", rejection_detail="bad")
    assert row["source_timestamp"] is timestamp
    assert row["rejection_code"] == "BAD_WIRE"
