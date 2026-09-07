"""Confluent Avro wire-envelope inspection without Registry network calls."""

from __future__ import annotations

import struct
from dataclasses import dataclass


MAGIC_BYTE = 0
ENVELOPE_BYTES = 5


class WireContractError(ValueError):
    """Raised when a Kafka value violates the supported envelope contract."""


@dataclass(frozen=True)
class WirePayload:
    schema_id: int
    avro_payload: bytes


def wrap_confluent_payload(schema_id: int, avro_payload: bytes) -> bytes:
    if schema_id < 0 or schema_id > 0xFFFFFFFF:
        raise ValueError("schema_id must fit an unsigned 32-bit integer")
    if not avro_payload:
        raise ValueError("avro_payload must not be empty")
    return bytes((MAGIC_BYTE,)) + struct.pack(">I", schema_id) + avro_payload


def inspect_confluent_payload(value: bytes | bytearray | memoryview, expected_schema_id: int) -> WirePayload:
    raw = bytes(value)
    if len(raw) < ENVELOPE_BYTES:
        raise WireContractError("BAD_WIRE: truncated Confluent envelope")
    if raw[0] != MAGIC_BYTE:
        raise WireContractError("BAD_WIRE: unsupported magic byte")
    schema_id = struct.unpack(">I", raw[1:5])[0]
    if schema_id != expected_schema_id:
        raise WireContractError("BAD_WIRE: schema ID does not match the provisioned run schema")
    payload = raw[ENVELOPE_BYTES:]
    if not payload:
        raise WireContractError("BAD_WIRE: empty Avro payload")
    return WirePayload(schema_id=schema_id, avro_payload=payload)
