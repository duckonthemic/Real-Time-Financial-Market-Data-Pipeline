from __future__ import annotations

import pytest

from market_pipeline.contracts.wire import WireContractError, inspect_confluent_payload, wrap_confluent_payload


def test_round_trip_supported_envelope() -> None:
    wrapped = wrap_confluent_payload(37, b"avro-record")
    inspected = inspect_confluent_payload(wrapped, 37)
    assert inspected.schema_id == 37
    assert inspected.avro_payload == b"avro-record"


@pytest.mark.parametrize(
    "value",
    [
        b"",
        b"\x00\x00\x00\x00",
        b"\x01\x00\x00\x00\x25payload",
        b"\x00\x00\x00\x00\x26payload",
        b"\x00\x00\x00\x00\x25",
    ],
)
def test_invalid_envelopes_are_bad_wire(value: bytes) -> None:
    with pytest.raises(WireContractError, match="BAD_WIRE"):
        inspect_confluent_payload(value, 37)
