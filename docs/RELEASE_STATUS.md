# Local portfolio verification

This file records measured implementation status. The reviewed design in
`designs/market-data-reliability-lab.md` remains planning history, not a claim
that every proposed test, benchmark, or presentation artifact is complete.

## Verified on 2026-09-08

- Ruff lint and formatting passed; strict mypy passed for 33 source files.
- 93 tests passed locally, including a clean export of the Git index.
- Spark uses Python 3.11.10 and verifies 30 connector JARs by size and SHA-256.
- A separate, network-isolated Spark regression processed the standard fixture
  and matched all 37 invalid-record rejection reasons, including corrupt Avro.
- Linux shared-group artifact creation and atomic replacement were exercised
  with separate host and container UIDs inside the application image.
- The full standard run `run-final-20260908-d` passed 12/12 data invariants
  over 12,763 records: 12,480 canonical events, 246 duplicates, and 37 invalid
  records. The real `SIGKILL` recovery produced a peak lag of 2,275, returned
  to zero, and exactly reconciled 66 Gold OHLCV/VWAP rows.
- The same run finished with `portfolio_release_status=READY`; its Grafana
  smoke verified health, datasource connectivity, the provisioned dashboard,
  and nonempty panel query results.

## Boundaries and remaining release work

- GitHub Actions workflows are implemented but have not run remotely yet.
- No public deployment or demo video has been published.
- The full infrastructure-failure matrix and a reproducible p95 performance
  benchmark from the original plan are not implemented. Do not claim their results.
- The JAR closure is hash-locked. Container tags, GitHub Actions references,
  and Python transitive dependencies are not all locked by immutable hashes.
- Grafana currently permits the configured Cassandra plugin without requiring
  signature enforcement. Use this stack with fixture data on local infrastructure.
- Gold expectations and production Gold share the aggregation routine;
  hand-calculated unit tests provide separate formula checks.
- Some TOML entries describe fixed design defaults rather than supported
  runtime overrides. Use the supplied fixtures; changing schema, query names,
  keyspace, or plugin version requires corresponding source/Compose changes.

The host command exits according to the data verdict. When Grafana is enabled,
the manual CI workflow also requires `portfolio_release_status=READY`.
Generated credentials belong only in the run's local `compose.env`; CI uploads
JSON/JSONL evidence and the HTML report, excluding that environment file.

For reproduction commands, see [Testing](TESTING.md). For truthful résumé
wording, see [CV notes](CV_NOTES.md).

The query smoke uses Grafana's documented
[data source query API](https://grafana.com/docs/grafana/latest/developer-resources/api-reference/http-api/api-legacy/data_source/#query-a-data-source).
