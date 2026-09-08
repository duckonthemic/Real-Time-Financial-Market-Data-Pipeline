# Market Data Reliability Lab

[![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Kafka-KRaft-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3.5.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Cassandra](https://img.shields.io/badge/Cassandra-4.1.7-1287B1?logo=apachecassandra&logoColor=white)](https://cassandra.apache.org/)

A deterministic Kafka → Spark Structured Streaming → Cassandra recovery lab. One command publishes a fixed market-data fixture, kills the Spark driver with `SIGKILL`, restarts it from the same checkpoint, reconciles 12 correctness invariants, builds 5-minute OHLCV/VWAP rows, and produces a Grafana dashboard plus an immutable HTML evidence report.

This is a reliability demonstration, not a claim of a production trading platform. It proves a bounded, repeatable recovery path using local Docker infrastructure.

## What you can verify

- Kafka input coverage by exact `(topic, partition, offset)` coordinates.
- Avro wire compatibility through Confluent Schema Registry.
- Replay-safe Silver projections keyed by deterministic event identity.
- Invalid-record quarantine in both Kafka DLQ and Cassandra.
- A real non-graceful Spark driver failure, visible lag growth, checkpoint restart, and catch-up to zero lag.
- Exact bounded Gold OHLCV/VWAP reconciliation.
- A separate portfolio-release gate that smoke-tests Grafana, its Cassandra datasource, and the dashboard definition.

The guarantee is **at-least-once processing with replay-safe projections**. The project does not claim end-to-end exactly-once delivery.

## Architecture

```text
deterministic fixture
        │ Avro + headers
        ▼
 Kafka KRaft ───────────────► Kafka DLQ
        │ captured offsets          ▲
        ▼                           │ invalid
 Spark Structured Streaming ───────┘
        │ same checkpoint after SIGKILL
        ├──► Bronze: source-coordinate truth
        ├──► Silver: validated, replay-safe events
        └──► batch ledger
                  │
                  ▼
        bounded Gold finalizer ───► 5-minute OHLCV/VWAP
                  │
          independent verifier
             ┌────┴────┐
             ▼         ▼
       HTML report   Grafana
```

See [Architecture](docs/ARCHITECTURE.md) for the data model, state machine, failure semantics, and trade-offs.

## Quick start

You need Python 3.11+, Docker Desktop with Compose v2, at least 4 CPUs, 8 GiB RAM, 10 GiB free disk, and free loopback ports `3000` and `8080`.

1. Clone the repository and enter it.

   ```bash
   git clone https://github.com/duckonthemic/Real-Time-Financial-Market-Data-Pipeline.git
   cd Real-Time-Financial-Market-Data-Pipeline
   ```

2. Run the shorter recovery scenario. No market-data API key is required.

   ```bash
   python scripts/demo.py --scenario recovery-showcase --run-id run-local-showcase
   ```

The first run builds the local images. A successful run ends with output like:

```text
PASS: run-local-showcase
REPORT: .../artifacts/run-local-showcase/report.html
DASHBOARD: http://127.0.0.1:3000/d/market-data-reliability?...
```

The showcase fixture contains 1,200 canonical records, 24 deliberate duplicates, and 12 invalid records. Use the larger 12,763-record scenario when you want a longer recovery window:

```bash
python scripts/demo.py --scenario recovery --run-id run-local-standard
```

Open the printed report and dashboard URLs. Grafana uses username `admin`; its generated per-run password is stored locally in `artifacts/<run-id>/compose.env`. Generated evidence and credentials are ignored by Git.

Clean only that run's containers, networks, volumes, and checkpoint while retaining its evidence:

```bash
python scripts/demo.py --cleanup-run run-local-showcase
```

Evidence deletion is a separate, guarded action and is allowed only after runtime cleanup:

```bash
python scripts/demo.py --purge-evidence run-local-showcase
```

## Evidence contract

The verifier reports these 12 named checks:

| Area | Checks |
|---|---|
| Delivery and source | `producer_delivery`, `source_coverage`, `offset_frontier`, `terminal_lag_zero` |
| Identity and validity | `silver_identity_set`, `logical_reconciliation`, `sampled_value_integrity` |
| Quarantine | `dlq_reconciliation`, `kafka_dlq_distinct_keys` |
| Recovery | `batch_ledger_complete`, `checkpoint_recovery` |
| Analytics | `gold_ohlcv_exact` |

Each run writes versioned JSON/JSONL evidence and `report.html` under `artifacts/<run-id>/`. The report keeps expected and actual values adjacent so a green headline cannot hide a failed invariant.

## Development

```bash
python -m pip install -e ".[dev]"
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m pytest
```

Spark connector dependencies are resolved from `docker/spark/pom.xml`, locked by filename, byte-size, and SHA-256 in `docker/spark/jars.lock.json`, then verified during the Spark image build.

Useful make targets mirror the same commands:

```bash
make install-dev
make check
make demo-showcase RUN_ID=run-local-showcase
make cleanup RUN_ID=run-local-showcase
```

## Documentation

- [Measured release status](docs/RELEASE_STATUS.md): verified results and remaining release work.
- [Architecture](docs/ARCHITECTURE.md): component boundaries, storage model, state machine, and guarantees.
- [Testing and verification](docs/TESTING.md): fast checks, Docker recovery tests, artifacts, and troubleshooting.
- [CV and interview notes](docs/CV_NOTES.md): honest résumé bullets and technical talking points.
- [Implementation design](docs/designs/market-data-reliability-lab.md): the detailed reviewed build plan.
- [Visual system](DESIGN.md): dashboard and evidence-report design rules.

## Project layout

```text
src/market_pipeline/       application code and independent verifier
scripts/demo.py            one-command host orchestrator
fixtures/                  deterministic scenario manifests
schemas/                   Avro, artifact JSON Schema, and Cassandra CQL
docker/                    pinned runtime images and Spark JAR lock
grafana/                   provisioned datasource and run-scoped dashboard
tests/                     unit, contract, and visual-regression checks
docs/                      architecture, testing, and portfolio notes
compose.yaml               isolated per-run infrastructure
```

## CV-ready summary

> Built a deterministic Kafka–Spark Structured Streaming–Cassandra reliability lab that injects a real `SIGKILL`, resumes from the same checkpoint, reconciles 12 correctness invariants across 1,236-event and 12,763-event fixtures, and publishes run-scoped Grafana plus immutable HTML evidence.

Use the measured run size you personally execute and can explain. More examples and interview prompts are in [CV and interview notes](docs/CV_NOTES.md).

## Boundaries

- The lab runs one Kafka broker, one Spark worker, and one Cassandra node; it demonstrates application recovery, not infrastructure high availability.
- Gold aggregation is intentionally bounded after stream catch-up. This makes the expected result deterministic and independently comparable.
- Sampled field equality complements exhaustive identity/coordinate checks; it is not exhaustive value equality for every Silver row.
- The repository does not currently declare an open-source license.
