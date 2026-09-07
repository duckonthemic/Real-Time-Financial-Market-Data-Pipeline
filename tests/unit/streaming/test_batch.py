from __future__ import annotations

from dataclasses import dataclass

import pytest

from market_pipeline.streaming.batch import BatchFrames, RecoveryBatchDependencies, process_recovery_batch


class FakeFrame:
    def __init__(self) -> None:
        self.events: list[str] = []

    def persist(self):
        self.events.append("persist")
        return self

    def unpersist(self) -> None:
        self.events.append("unpersist")


class FakeLedger:
    def __init__(self, events: list[str], status: str | None = None) -> None:
        self.events = events
        self.current_status = status

    def status(self, run_id, query_name, batch_id):
        self.events.append("ledger.status")
        return self.current_status

    def started(self, run_id, query_name, batch_id, bounds):
        self.events.append("ledger.started")

    def completed(self, run_id, query_name, batch_id, counts, bounds):
        self.events.append("ledger.completed")

    def failed(self, run_id, query_name, batch_id, error):
        self.events.append("ledger.failed")


@dataclass
class FakeWriter:
    name: str
    events: list[str]
    fail: bool = False

    def write(self, frame) -> None:
        self.events.append(self.name)
        if self.fail:
            raise RuntimeError(f"{self.name} failed")


def dependencies(events: list[str], *, status: str | None = None, failing: str | None = None):
    return RecoveryBatchDependencies(
        run_id="run-batch-01",
        query_name="recovery_pipeline",
        ledger=FakeLedger(events, status),
        bronze_writer=FakeWriter("bronze", events, failing == "bronze"),
        silver_writer=FakeWriter("silver", events, failing == "silver"),
        dlq_writer=FakeWriter("dlq", events, failing == "dlq"),
        derive_bounds=lambda frame: events.append("bounds") or {0: (10, 20)},
        transform=lambda frame: events.append("transform") or BatchFrames("b", "s", "d", {"input": 10}),
    )


def test_projection_order_and_complete_last() -> None:
    events: list[str] = []
    frame = FakeFrame()
    process_recovery_batch(frame, 7, dependencies(events))
    assert events == ["ledger.status", "bounds", "ledger.started", "transform", "bronze", "silver", "dlq", "ledger.completed"]
    assert frame.events == ["persist", "unpersist"]


def test_completed_batch_skips_projection_writes() -> None:
    events: list[str] = []
    frame = FakeFrame()
    process_recovery_batch(frame, 7, dependencies(events, status="COMPLETED"))
    assert events == ["ledger.status"]
    assert frame.events[-1] == "unpersist"


@pytest.mark.parametrize("failing", ["bronze", "silver", "dlq"])
def test_failure_is_recorded_and_frame_is_unpersisted(failing: str) -> None:
    events: list[str] = []
    frame = FakeFrame()
    with pytest.raises(RuntimeError, match=failing):
        process_recovery_batch(frame, 7, dependencies(events, failing=failing))
    assert events[-1] == "ledger.failed"
    assert "ledger.completed" not in events
    assert frame.events[-1] == "unpersist"
