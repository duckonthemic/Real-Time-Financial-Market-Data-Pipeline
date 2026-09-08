# How to test and verify the recovery lab

This guide shows how to run fast code checks, execute the real Docker recovery path, inspect its evidence, and clean up one run safely.

## Prerequisites

- Python 3.11 or newer
- Docker Desktop with Docker Compose v2
- At least 4 CPUs, 8 GiB RAM, and 10 GiB free disk
- Free loopback ports `3000` and `8080`

## Run fast checks

Install the project and pinned development tools:

```bash
python -m pip install -e ".[dev]"
```

Run the same gates used by CI:

```bash
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m pytest
```

The fast suite does not require running containers. It covers deterministic fixture generation, wire framing, validation order, storage-row mapping, batch replay behavior, invariant logic, report output, orchestration state transitions, Compose contracts, dashboard structure, and Spark dependency-lock tamper detection.

## Run the real recovery scenario

Use a new lowercase `run_id` between 8 and 64 characters. IDs may contain digits and hyphens and cannot be reused.

```bash
python scripts/demo.py --scenario recovery-showcase --run-id run-local-showcase
```

This command:

1. validates Docker, ports, disk, configuration, and Compose;
2. builds the application, Spark, and Grafana images;
3. provisions topics, Avro schema, Cassandra tables, and captured start offsets;
4. starts the producer and Spark query;
5. kills the Spark driver at the deterministic gate;
6. observes lag growth and restarts from the same checkpoint;
7. waits for producer completion and zero terminal lag;
8. finalizes Gold, verifies 12 invariants, and smoke-tests Grafana.

Expected successful output:

```text
PASS: run-local-showcase
REPORT: .../artifacts/run-local-showcase/report.html
DASHBOARD: http://127.0.0.1:3000/d/market-data-reliability?...
```

For a resource-reduced CI run, omit Grafana:

```bash
python scripts/demo.py --scenario recovery-showcase --run-id run-ci-no-dashboard --no-dashboard
```

The data contract can pass in this mode, but `portfolio_release_status` remains `NOT_READY` because dashboard evidence was intentionally skipped.

Run the larger fixture with:

```bash
python scripts/demo.py --scenario recovery --run-id run-local-standard
```

## Inspect evidence

### Isolated Spark validation regression

After the scenario has built `market-spark-recovery:local`, run this from the
repository root (Bash or PowerShell):

```bash
docker run --rm --network none --hostname localhost -e SPARK_LOCAL_IP=127.0.0.1 --mount "type=bind,source=${PWD}/tests/integration,target=/tests,readonly" market-spark-recovery:local /opt/spark/bin/spark-submit --master "local[1]" /tests/spark_validation_smoke.py
```

This uses actual Spark Avro decoding over the standard fixture and checks all
37 rejection reasons, including the corrupt payload that PERMISSIVE mode
represents as a struct of null fields. It needs no Kafka or Cassandra and is
separate from the fast pytest suite. The manual recovery workflow runs it too.

### Run artifacts

Open `artifacts/<run-id>/report.html` first. It contains the correctness verdict, every expected/actual invariant pair, recovery transitions, and lag samples.

| Artifact | Purpose |
|---|---|
| `run.json` | Current/terminal state and run ownership |
| `provision.json` | Schema ID and captured start offsets |
| `producer-progress.json` | Acknowledged delivery frontier and end offsets |
| `streaming-progress.json` | Spark processed frontier |
| `state-transitions.jsonl` | Append-only recovery timeline |
| `metrics.jsonl` | Produced/processed frontiers and lag |
| `gold-progress.json` | Bounded Gold row count |
| `run-report.json` | Machine-readable invariant results |
| `dashboard-smoke.json` | Grafana/datasource/query smoke evidence |
| `report.html` | Human-readable immutable report |
| `failure.json` | Primary failure and remediation, when a run fails |

To query the terminal summary in PowerShell:

```powershell
Get-Content artifacts/run-local-showcase/run-report.json -Raw | ConvertFrom-Json |
  Select-Object data_contract_status, portfolio_release_status
```

To open Grafana, use the exact `DASHBOARD` URL printed after the run. It includes the `run_id` and the measured time window. Sign in as `admin` with the generated value from `GRAFANA_ADMIN_PASSWORD` in the run's local `compose.env`.

## Verify the Spark connector lock

The Spark image performs this check during every build. You can also run it inside an image:

```bash
docker run --rm --entrypoint python market-spark-recovery:local \
  /opt/market-pipeline/docker/spark/verify_jars.py
```

The verifier checks the expected Maven closure only. Spark's base-image JARs may coexist in `/opt/spark/jars`.

## Clean up

Remove only one run's Compose project and volumes while keeping its evidence:

```bash
python scripts/demo.py --cleanup-run run-local-showcase
```

The cleanup function verifies the run artifact, run ID, Compose project owner, environment file, and global lock before calling `docker compose down --volumes`.

Delete retained evidence only after cleanup:

```bash
python scripts/demo.py --purge-evidence run-local-showcase
```

## Troubleshooting

### Docker Desktop is unavailable

Start Docker Desktop and wait until `docker info` succeeds, then rerun the same command. A failed run ID is retained for diagnosis and cannot be reused; choose a new ID after fixing the cause.

### Port 3000 or 8080 is occupied

Stop the conflicting process or change `ports.grafana` / `ports.spark_ui` in `config/demo.toml`. Ports bind only to loopback.

### The run times out or a container exits

Read `artifacts/<run-id>/failure.json`, then inspect the run-owned services:

```bash
docker compose --env-file artifacts/<run-id>/compose.env -f compose.yaml --profile scenario ps
docker compose --env-file artifacts/<run-id>/compose.env -f compose.yaml --profile scenario logs spark-recovery
```

The artifact keeps the first failure as primary; cleanup errors are appended without replacing it.

### Grafana is healthy but release status is not ready

Inspect `dashboard-smoke.json`. `data_contract_status=PASSED` and `portfolio_release_status=NOT_READY` means the data proof passed but a dashboard, datasource, or evidence query failed.

## CI workflows

`.github/workflows/ci.yml` runs lint, formatting, type checks, and fast tests on pushes and pull requests. `.github/workflows/recovery.yml` is a manual integration job because it builds and runs the full Docker stack; it uploads run evidence even when the scenario fails.

## Related

- [Architecture](ARCHITECTURE.md)
- [CV and interview notes](CV_NOTES.md)
- [Project README](../README.md)
