from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest

from market_pipeline.producer.fixture import canonical_event_set_sha256, iter_fixture, load_manifest


ROOT = Path(__file__).parents[2]


@pytest.mark.parametrize("fixture", ["standard", "recovery-showcase"])
def test_fixture_matches_manifest(fixture: str) -> None:
    manifest = load_manifest(ROOT / "fixtures" / fixture / "manifest.json")
    records = list(iter_fixture(manifest, "run-contract-01"))
    assert len(records) == manifest["total_records"]
    assert sum(record.duplicate for record in records) == manifest["duplicate_records"]
    assert sum(record.event is None for record in records) == manifest["rejection_counts"]["BAD_WIRE"]
    assert canonical_event_set_sha256(manifest) == manifest["canonical_event_set_sha256"]
    event_ids = Counter(record.event["event_id"] for record in records if record.event and not record.event["event_id"].startswith("invalid"))
    assert len(event_ids) >= manifest["canonical_records"]


def test_standard_counts_are_portfolio_contract() -> None:
    manifest = load_manifest(ROOT / "fixtures" / "standard" / "manifest.json")
    assert (manifest["canonical_records"], manifest["duplicate_records"], manifest["invalid_records"], manifest["total_records"]) == (12480, 246, 37, 12763)
