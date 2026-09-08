"""Deterministic semantic identities for replay events."""

from __future__ import annotations

import hashlib


def replay_event_id(dataset_id: str, source_sequence: int) -> str:
    """Return the lowercase SHA-256 identity defined by the event contract."""
    if not dataset_id or "\x00" in dataset_id:
        raise ValueError("dataset_id must be non-empty and cannot contain NUL")
    if source_sequence < 0:
        raise ValueError("source_sequence must be non-negative")
    material = dataset_id.encode("utf-8") + b"\x00" + str(source_sequence).encode("ascii")
    return hashlib.sha256(material).hexdigest()
