# Architecture

The Market Data Reliability Lab answers one question: after a Spark driver dies without a graceful shutdown, can the pipeline resume from its existing checkpoint and prove that the bounded Kafka input became the intended Bronze, Silver, DLQ, and Gold outputs?

For commands and configuration, see [Testing and verification](TESTING.md). For a first run, start at the [README](../README.md).

## System boundary

The lab owns a single local Docker Compose project per `run_id`. It starts Kafka in KRaft mode, Confluent Schema Registry, Cassandra, one Spark master, one Spark worker, the recovery driver, and optional Grafana. The host orchestrator is the control plane; containers implement the data plane.

```text
Host: scripts/demo.py
  │
  ├─ provisions topics, schema, tables, and starting offsets
  ├─ starts producer + streaming query
  ├─ waits for deterministic kill gate
  ├─ SIGKILLs and recreates the Spark driver
  ├─ waits for captured end offsets
  └─ runs Gold, verifier, and dashboard smoke checks

Internal Docker network
  fixture producer -> Kafka -> Spark -> Cassandra
                        └──── invalid -> Kafka DLQ

Loopback observer network
  host -> Spark UI :8080
  host -> Grafana  :3000 -> Cassandra
```

The `pipeline` network is internal. Only Spark UI and Grafana join the non-internal `observer` network and bind to `127.0.0.1`.

## Run lifecycle

The orchestrator accepts only these transitions:

```text
CREATED -> INFRA_READY -> RUN_READY -> PRODUCING
                                           │
                                           ▼
FAILURE_INJECTED -> RECOVERING -> VERIFYING -> PASSED

Any non-terminal state may instead become FAILED or TIMED_OUT.
```

Every transition is appended to `state-transitions.jsonl` and reflected atomically in `run.json`. A process-wide exclusive lock prevents two host orchestrators from sharing ports or confusing evidence ownership. A `run_id` cannot be reused.

## Data path

### Input contract

Each fixture manifest fixes the seed, symbols, event-time range, partition count, canonical record count, duplicate count, invalid count, rejection distribution, and SHA-256 of the canonical event-ID set.

The producer derives records instead of storing a large fixture file. Valid records use Confluent's Avro framing and carry run, dataset, sequence, payload-type, and production-time headers. Kafka delivery callbacks define the acknowledged input frontier.

### Bronze

`bronze_records_by_run_partition` uses `(owner_run_id, source_topic, source_partition)` as its partition key and `source_offset` as its clustering key. That makes each captured Kafka coordinate idempotent and queryable without `ALLOW FILTERING`.

Bronze stores the original key/value bytes, Kafka timestamp, schema ID, and claimed headers. Raw bytes allow digest-based checks without trusting the decoded projection.

### Silver and DLQ

Valid records become Silver rows keyed by deterministic `event_id`. A duplicate logical event therefore overwrites the same projection instead of inflating the business count.

Invalid records use first-match rejection codes:

- `BAD_WIRE`
- `HEADER_PAYLOAD_MISMATCH`
- `EMPTY_SYMBOL`
- `NON_POSITIVE_PRICE`
- `NON_POSITIVE_VOLUME`
- `TIMESTAMP_OUT_OF_RANGE`

The pipeline writes invalid coordinates to Cassandra and publishes them to the Kafka DLQ. The verifier compares the two sets.

### Batch ledger

`stream_batches_by_query` records each Spark `foreachBatch` attempt as `STARTED`, `COMPLETED`, or `FAILED`, including source bounds and the cumulative next-offset frontier. On replay, a `COMPLETED` batch is skipped; unfinished attempts can be repeated through idempotent coordinate/event keys.

### Gold

After the recovery query reaches the captured input frontier, a bounded finalizer reads Silver and writes 5-minute OHLCV, total volume, trade count, and VWAP. The verifier applies the same aggregation routine to fixture events and compares those expected rows with persisted Gold within a `1e-6` price tolerance. This detects storage and input differences; separate hand-calculated unit cases test the aggregation formula itself.

## Failure and recovery semantics

The kill gate requires both an acknowledged producer frontier and a processed Spark frontier. The host then sends `SIGKILL` to the driver while the producer remains active. During a configured dwell period, lag must rise by at least the scenario threshold. The host recreates the same service with the same named checkpoint volume.

Recovery passes only when:

1. the producer acknowledges the complete fixture;
2. Spark reaches every captured end offset;
3. run-scoped lag returns to zero;
4. the batch ledger contains no unfinished batch;
5. the independent data checks all pass.

This is at-least-once processing. Cassandra primary keys, deterministic identities, and batch-ledger replay rules make the projections safe to repeat. The design does not use a distributed transaction across Kafka and Cassandra, so it does not claim exactly-once delivery.

## Independent verification

The verifier reconstructs the expected fixture from the manifest and reads actual Kafka/Cassandra state after the finite input has been processed. The streaming query remains alive until cleanup. Verification checks source coordinates, event identities, rejection counts and coordinates, offsets, lag, batch status, failure/recovery evidence, sampled values and Bronze digests, and Gold rows.

`data_contract_status` describes correctness. `portfolio_release_status` is separate and becomes `READY` only after the Grafana API, Cassandra datasource, dashboard definition, and required evidence queries pass smoke checks.

## Reproducibility controls

- Container image tags are centralized in `config/demo.toml`.
- Python runtime and development dependencies use exact versions in `pyproject.toml`.
- Maven resolves the Spark connector closure from `docker/spark/pom.xml`.
- `docker/spark/jars.lock.json` records all 30 resolved connector JARs by filename, byte-size, and SHA-256.
- The Spark image build runs `verify_jars.py` and fails on a missing or changed locked JAR.
- Versioned Avro schemas and JSON Schemas define wire and evidence boundaries.

## Storage model

| Table | Partition purpose |
|---|---|
| `pipeline_runs` | One summary row per run |
| `bronze_records_by_run_partition` | Exact source-coordinate evidence |
| `silver_events_by_run` | Run-wide identity reconciliation |
| `silver_events_by_symbol_day` | Time-ordered analytics input |
| `dlq_records_by_run` | Run-scoped quarantine reconciliation |
| `stream_batches_by_query` | Recovery ledger and offset frontier |
| `run_state_transitions_by_run` | Dashboard recovery timeline |
| `run_checks_by_run` | Expected/actual invariant scorecard |
| `run_metrics_by_run` | Run-scoped lag samples |
| `gold_ohlcv_5m_by_symbol_day` | 5-minute market aggregates |

The complete CQL definitions are in [`schemas/cassandra/002_tables.cql`](../schemas/cassandra/002_tables.cql).

## Trade-offs

- A single-node stack keeps the demo runnable on a laptop, but does not test broker, worker, or database-node failover.
- Deterministic fixtures sacrifice live-market novelty in exchange for repeatable faults and exact expected results.
- The host state machine is Python standard library code, which reduces bootstrap dependencies but leaves Docker as the main integration boundary.
- Cassandra models are query-first and deliberately duplicate Silver data for run-wide reconciliation and symbol/day access.
- Grafana reads Cassandra directly for a small bounded portfolio demo; a larger system would usually add a serving layer or metrics store.

## Related

- [Testing and verification](TESTING.md)
- [CV and interview notes](CV_NOTES.md)
- [Detailed implementation design](designs/market-data-reliability-lab.md)
