"""Avro encoding plus the five deterministic malformed-wire variants."""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any, Mapping

from market_pipeline.contracts.wire import wrap_confluent_payload


def load_avro_schema(path: Path) -> dict[str, Any]:
    from fastavro import parse_schema

    return parse_schema(json.loads(path.read_text(encoding="utf-8")))


def encode_event(event: Mapping[str, Any], schema: Mapping[str, Any], schema_id: int) -> bytes:
    from fastavro import schemaless_writer

    buffer = io.BytesIO()
    schemaless_writer(buffer, schema, dict(event))
    return wrap_confluent_payload(schema_id, buffer.getvalue())


def malformed_value(kind: str, schema_id: int) -> bytes:
    if kind == "wrong-magic":
        return b"\x01" + schema_id.to_bytes(4, "big") + b"not-avro"
    if kind == "truncated":
        return b"\x00\x00\x00"
    if kind == "unknown-schema":
        return b"\x00" + (schema_id + 1).to_bytes(4, "big") + b"not-avro"
    if kind == "empty-payload":
        return b"\x00" + schema_id.to_bytes(4, "big")
    if kind == "corrupt-avro":
        return b"\x00" + schema_id.to_bytes(4, "big") + b"\xff\xff\xff"
    raise ValueError(f"unknown malformed fixture kind: {kind}")
