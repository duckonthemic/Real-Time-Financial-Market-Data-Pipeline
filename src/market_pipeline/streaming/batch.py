"""Replay-safe micro-batch ordering and ledger state transitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol


class BatchFrame(Protocol):
    def persist(self) -> "BatchFrame": ...
    def unpersist(self) -> None: ...


class LedgerWriter(Protocol):
    def status(self, run_id: str, query_name: str, batch_id: int) -> str | None: ...
    def started(self, run_id: str, query_name: str, batch_id: int, bounds: Mapping[int, tuple[int, int]]) -> None: ...
    def completed(self, run_id: str, query_name: str, batch_id: int, counts: Mapping[str, int], bounds: Mapping[int, tuple[int, int]]) -> None: ...
    def failed(self, run_id: str, query_name: str, batch_id: int, error: str) -> None: ...


class ProjectionWriter(Protocol):
    def write(self, frame: Any) -> None: ...


@dataclass(frozen=True)
class BatchFrames:
    bronze: Any
    silver: Any
    dlq: Any
    counts: Mapping[str, int]


@dataclass(frozen=True)
class RecoveryBatchDependencies:
    run_id: str
    query_name: str
    ledger: LedgerWriter
    bronze_writer: ProjectionWriter
    silver_writer: ProjectionWriter
    dlq_writer: ProjectionWriter
    derive_bounds: Callable[[BatchFrame], Mapping[int, tuple[int, int]]]
    transform: Callable[[BatchFrame], BatchFrames]
    dlq_publisher: ProjectionWriter | None = None


def process_recovery_batch(raw_batch: BatchFrame, batch_id: int, dependencies: RecoveryBatchDependencies) -> None:
    """Write deterministic projections, completing the ledger last."""
    persisted = raw_batch.persist()
    try:
        if dependencies.ledger.status(dependencies.run_id, dependencies.query_name, batch_id) == "COMPLETED":
            return
        bounds = dependencies.derive_bounds(persisted)
        dependencies.ledger.started(dependencies.run_id, dependencies.query_name, batch_id, bounds)
        frames = dependencies.transform(persisted)
        dependencies.bronze_writer.write(frames.bronze)
        dependencies.silver_writer.write(frames.silver)
        dependencies.dlq_writer.write(frames.dlq)
        if dependencies.dlq_publisher is not None:
            dependencies.dlq_publisher.write(frames.dlq)
        dependencies.ledger.completed(
            dependencies.run_id,
            dependencies.query_name,
            batch_id,
            frames.counts,
            bounds,
        )
    except Exception as exc:
        try:
            dependencies.ledger.failed(
                dependencies.run_id,
                dependencies.query_name,
                batch_id,
                str(exc)[:1000],
            )
        except Exception:
            pass
        raise
    finally:
        persisted.unpersist()
