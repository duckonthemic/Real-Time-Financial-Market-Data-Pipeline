# CV and interview notes

This project is strongest when presented as a reliability lab with measured evidence, not as a production trading system. Run it yourself, retain the generated report, and use only numbers you can explain.

Latest local evidence: `run-final-20260908-d` passed all 12 invariants over
12,763 input records, recovered from an actual Spark driver `SIGKILL`, returned
lag from a peak of 2,275 to zero, matched 66 Gold rows, and passed the Grafana
publication/query smoke. See `docs/RELEASE_STATUS.md` for release boundaries.

## Recommended CV bullet

> Built a deterministic Kafka–Spark Structured Streaming–Cassandra reliability lab that injects a real `SIGKILL`, resumes from the same checkpoint, reconciles 12 correctness invariants across 1,236-event and 12,763-event fixtures, and publishes run-scoped Grafana plus immutable HTML evidence.

Shorter version:

> Implemented and tested replay-safe market-data processing with Kafka, Spark checkpoints, Cassandra query-first tables, Avro contracts, DLQ reconciliation, exact Gold OHLCV verification, and Docker Compose.

Vietnamese version:

> Xây dựng lab kiểm chứng độ tin cậy cho pipeline Kafka–Spark Structured Streaming–Cassandra; chủ động `SIGKILL` Spark driver, khôi phục từ checkpoint, đối soát 12 invariant và xuất bằng chứng qua Grafana cùng báo cáo HTML.

Do not write “exactly-once end to end,” “highly available,” or “production-ready.” The implementation proves at-least-once processing with replay-safe projections on a single-node local stack.

## What to demonstrate in an interview

1. Start with `report.html`. Show expected and actual values for all 12 checks.
2. Use the recovery timeline to point out `FAILURE_INJECTED` and `RECOVERING`.
3. Show lag rising while Spark is dead and returning to zero after restart.
4. Explain why Bronze uses Kafka coordinates while Silver uses deterministic event identity.
5. Show the Kafka/Cassandra DLQ coordinate comparison.
6. Show one 5-minute Gold row and explain OHLCV plus VWAP calculation.
7. Open `jars.lock.json` and explain why connector versions alone do not lock a transitive Maven closure.

## Questions you should be ready to answer

### Why deterministic fixtures instead of a live market API?

A live feed changes with time and market hours, so a failure run cannot know the exact expected result. The fixture keeps the demo repeatable and allows exhaustive coordinate/identity counts plus exact Gold comparison.

### What happens when Spark dies between writes?

The same micro-batch may run again. Bronze and DLQ use source-coordinate primary keys; Silver uses deterministic event IDs; the batch ledger records attempts and completion. Repeated projections converge instead of creating logical duplicates.

### Why is this not exactly-once?

Kafka checkpoint progress and Cassandra writes do not share one distributed transaction. A crash can replay work. The system handles replay safely, but that is different from proving one atomic delivery across both systems.

### Why calculate Gold after stream completion?

The lab needs a closed expected set for exact comparison. Bounded Gold isolates recovery correctness from watermark timing and late-data policy. A real continuous pipeline would choose and document its watermark, correction, and serving strategy.

### Why two release statuses?

`data_contract_status` answers whether the data is correct. `portfolio_release_status` answers whether the human-facing Grafana evidence also works. A presentation failure should not rewrite a passed data proof, and a passed data proof should not hide a broken demo.

### How is cleanup kept safe?

Cleanup accepts one validated run ID, reads its ownership artifact and environment, checks the exact Compose project name, refuses to run while the global lock exists, and deletes only that run's Compose volumes. Evidence deletion is a separate action that requires runtime cleanup first.

## Portfolio checklist

- Run `python -m ruff check .`, `python -m ruff format --check .`, `python -m mypy`, and `python -m pytest`.
- Execute a new dashboard-enabled `recovery-showcase` run.
- Confirm both statuses are `PASSED` / `READY`.
- Keep the report and dashboard screenshots for your portfolio, but do not commit generated credentials.
- Link the GitHub repository from the CV only after the tested commits are pushed.
- Be able to explain one trade-off from [Architecture](ARCHITECTURE.md) in your own words.

## Suggested project description

**Market Data Reliability Lab** is a local, deterministic data-engineering project that demonstrates failure injection and independent reconciliation across Kafka, Spark Structured Streaming, Cassandra, Avro Schema Registry, and Grafana. Its main artifact is evidence: exact source coverage, replay-safe identity, DLQ agreement, checkpoint recovery, zero terminal lag, and exact bounded OHLCV/VWAP.

## Related

- [Project README](../README.md)
- [Architecture](ARCHITECTURE.md)
- [Testing and verification](TESTING.md)
