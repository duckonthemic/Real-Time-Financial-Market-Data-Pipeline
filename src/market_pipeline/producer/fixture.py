"""Deterministically generate standard and short recovery fixtures."""

from __future__ import annotations

import hashlib
import json
import random
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from market_pipeline.contracts.identity import replay_event_id


@dataclass(frozen=True)
class FixtureRecord:
    key: str
    headers: tuple[tuple[str, bytes], ...]
    event: dict[str, Any] | None
    malformed_kind: str | None = None
    duplicate: bool = False


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError("manifest must contain a JSON object")
    required = {
        "manifest_version",
        "dataset_id",
        "seed",
        "symbols",
        "event_time_min_ms",
        "event_time_max_ms",
        "canonical_records",
        "duplicate_records",
        "invalid_records",
        "total_records",
        "rejection_counts",
    }
    missing = sorted(required - manifest.keys())
    if missing:
        raise ValueError(f"manifest missing fields: {', '.join(missing)}")
    if (
        manifest["canonical_records"] + manifest["duplicate_records"] + manifest["invalid_records"]
        != manifest["total_records"]
    ):
        raise ValueError("manifest counts do not reconcile")
    if sum(manifest["rejection_counts"].values()) != manifest["invalid_records"]:
        raise ValueError("manifest rejection counts do not reconcile")
    return cast(dict[str, Any], manifest)


def canonical_event(
    manifest: Mapping[str, Any], run_id: str, source_sequence: int
) -> dict[str, Any]:
    symbols = list(manifest["symbols"])
    symbol = symbols[source_sequence % len(symbols)]
    symbol_index = symbols.index(symbol)
    price = round(90.0 + symbol_index * 37.5 + (source_sequence % 211) * 0.013, 6)
    volume = 1 + ((source_sequence * 37) % 900)
    event_time_ms = int(manifest["event_time_min_ms"]) + source_sequence * 250
    return {
        "event_id": replay_event_id(str(manifest["dataset_id"]), source_sequence),
        "run_id": run_id,
        "source": "REPLAY",
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "event_time_ms": event_time_ms,
        "conditions": ["REGULAR"] if source_sequence % 5 else ["REGULAR", "ODD_LOT"],
        "dataset_id": str(manifest["dataset_id"]),
        "source_sequence": source_sequence,
        "schema_version": 1,
    }


def canonical_event_set_sha256(manifest: Mapping[str, Any]) -> str:
    event_ids = [
        replay_event_id(str(manifest["dataset_id"]), sequence)
        for sequence in range(int(manifest["canonical_records"]))
    ]
    return hashlib.sha256("\n".join(sorted(event_ids)).encode("ascii")).hexdigest()


def _headers(
    *,
    run_id: str,
    dataset_id: str,
    source_sequence: int,
    payload_type: str = "avro-confluent-v1",
    produced_at_ms: int,
) -> tuple[tuple[str, bytes], ...]:
    return tuple(
        (key, str(value).encode("utf-8"))
        for key, value in (
            ("run_id", run_id),
            ("dataset_id", dataset_id),
            ("source_sequence", source_sequence),
            ("payload_type", payload_type),
            ("produced_at_ms", produced_at_ms),
        )
    )


def _invalid_records(manifest: Mapping[str, Any], run_id: str) -> list[FixtureRecord]:
    sequence = int(manifest["canonical_records"])
    records: list[FixtureRecord] = []
    now_base = int(manifest["event_time_max_ms"]) + 60_000
    for code, count in manifest["rejection_counts"].items():
        for ordinal in range(int(count)):
            event_data = canonical_event(manifest, run_id, sequence)
            event: dict[str, Any] | None = event_data
            headers = _headers(
                run_id=run_id,
                dataset_id=str(manifest["dataset_id"]),
                source_sequence=sequence,
                produced_at_ms=now_base + sequence,
            )
            malformed_kind: str | None = None
            if code == "BAD_WIRE":
                event = None
                malformed_kind = (
                    "wrong-magic",
                    "truncated",
                    "unknown-schema",
                    "empty-payload",
                    "corrupt-avro",
                )[ordinal % 5]
                headers = _headers(
                    run_id=run_id,
                    dataset_id=str(manifest["dataset_id"]),
                    source_sequence=sequence,
                    payload_type="malformed-test-v1",
                    produced_at_ms=now_base + sequence,
                )
            elif code == "HEADER_PAYLOAD_MISMATCH":
                event_data["dataset_id"] = "mismatched-dataset"
            elif code == "EMPTY_SYMBOL":
                event_data["symbol"] = "   "
            elif code == "NON_POSITIVE_PRICE":
                event_data["price"] = 0.0 if ordinal % 2 == 0 else -1.0
                if ordinal == 0:
                    event_data["volume"] = 0
            elif code == "NON_POSITIVE_VOLUME":
                event_data["volume"] = 0 if ordinal % 2 == 0 else -1
            elif code == "TIMESTAMP_OUT_OF_RANGE":
                event_data["event_time_ms"] = int(manifest["event_time_max_ms"]) + 1
            else:
                raise ValueError(f"unsupported rejection code in manifest: {code}")
            records.append(
                FixtureRecord(
                    key=f"invalid-{code.lower()}-{ordinal}",
                    headers=headers,
                    event=event,
                    malformed_kind=malformed_kind,
                )
            )
            sequence += 1
    return records


def iter_fixture(manifest: Mapping[str, Any], run_id: str) -> Iterator[FixtureRecord]:
    """Yield the complete fixture in a deterministic, out-of-order publish order."""
    count = int(manifest["canonical_records"])
    produced_base = int(manifest["event_time_max_ms"]) + 30_000
    canonical = [canonical_event(manifest, run_id, sequence) for sequence in range(count)]
    rng = random.Random(int(manifest["seed"]))
    rng.shuffle(canonical)
    output = [
        FixtureRecord(
            key=event["symbol"],
            headers=_headers(
                run_id=run_id,
                dataset_id=str(manifest["dataset_id"]),
                source_sequence=int(event["source_sequence"]),
                produced_at_ms=produced_base + position,
            ),
            event=event,
        )
        for position, event in enumerate(canonical)
    ]
    duplicate_count = int(manifest["duplicate_records"])
    duplicate_indexes = [((index * 47) + 13) % count for index in range(duplicate_count)]
    for ordinal, canonical_index in enumerate(duplicate_indexes):
        original = canonical_event(manifest, run_id, canonical_index)
        output.append(
            FixtureRecord(
                key=original["symbol"],
                headers=_headers(
                    run_id=run_id,
                    dataset_id=str(manifest["dataset_id"]),
                    source_sequence=canonical_index,
                    produced_at_ms=produced_base + count + ordinal,
                ),
                event=original,
                duplicate=True,
            )
        )
    output.extend(_invalid_records(manifest, run_id))
    rng.shuffle(output)
    if len(output) != int(manifest["total_records"]):
        raise AssertionError("generated fixture count does not match manifest")
    yield from output
