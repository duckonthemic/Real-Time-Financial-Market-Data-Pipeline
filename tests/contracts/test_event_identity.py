from __future__ import annotations

import hashlib

import pytest

from market_pipeline.contracts.identity import replay_event_id


@pytest.mark.parametrize(
    ("dataset_id", "sequence"),
    [
        ("market-replay-standard-v1", 0),
        ("market-replay-standard-v1", 12479),
        ("dữ-liệu-v1", 42),
    ],
)
def test_replay_identity_matches_byte_contract(dataset_id: str, sequence: int) -> None:
    expected = hashlib.sha256(dataset_id.encode("utf-8") + b"\x00" + str(sequence).encode("ascii")).hexdigest()
    assert replay_event_id(dataset_id, sequence) == expected


def test_replay_identity_rejects_ambiguous_material() -> None:
    with pytest.raises(ValueError):
        replay_event_id("bad\x00dataset", 1)
    with pytest.raises(ValueError):
        replay_event_id("dataset", -1)
