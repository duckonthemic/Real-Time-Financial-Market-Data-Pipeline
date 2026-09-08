# Changelog

All notable changes to this project are documented in this file.

## [1.0.0.0] - 2026-09-08

### Added

- Added a deterministic Kafka, Spark Structured Streaming, Cassandra, and
  Grafana recovery lab with run-scoped evidence.
- Added standard and short fixtures, Avro contracts, replay-safe Bronze/Silver
  projections, DLQ reconciliation, and exact bounded Gold OHLCV/VWAP checks.
- Added CI checks, a manual recovery workflow, architecture/testing guides,
  truthful CV wording, and machine-readable artifact schemas.

### Changed

- Replaced the unverified live-market prototype with a reproducible fixture
  pipeline that can prove expected results without external credentials.
- Made the dashboard and HTML evidence responsive and scoped every panel query
  to the active run.
- Locked the Spark connector closure by filename, size, and SHA-256 and aligned
  the Spark image with the project's Python 3.11 runtime.

### Fixed

- Bound Spark RPC to a pipeline-only network alias so multi-network Docker DNS
  cannot advertise the observer address to workers or drivers.
- Classified corrupt permissive-mode Avro records as `BAD_WIRE`, including
  decoded structs whose required fields are all null.
- Kept Grafana credentials out of published JSON evidence and made cleanup,
  deadlines, dashboard queries, and Linux artifact permissions verifiable.

### Removed

- Removed obsolete prototype services, Finnhub code, stale environment files,
  duplicate Compose configuration, generated test output, and unrelated agent
  boilerplate from the repository.
