# TODOS

## Data Pipeline

### Add a tested live Finnhub source adapter

**What:** Add an optional live Finnhub ingestion adapter that maps provider messages into the canonical trade contract.

**Why:** A live source would demonstrate how the deterministic recovery pipeline accepts external market data without weakening the accepted replay proof.

**Pros:** Adds a realistic source and demonstrates reconnect, backoff, rate-limit, secret, and market-hours handling.

**Cons:** Introduces provider availability, credentials, weaker source identity, and integration-test complexity.

**Context:** The Recovery MVP deliberately removes the unverified prototype Finnhub client. Restart from the tested source-adapter boundary in `market_pipeline.producer`; do not restore the old implementation or advertise live ingestion until contract and integration tests pass.

**Effort:** L
**Priority:** P3
**Depends on:** Recovery MVP passes and the canonical event/header contract is stable.

### Add continuous event-time Gold with late-data correction

**What:** Replace completed-run Gold finalization with continuous event-time windows, bounded state, and idempotent late-event correction.

**Why:** This unlocks correct long-running aggregation when valid events arrive after their original window.

**Pros:** Demonstrates advanced Structured Streaming semantics and prepares Gold for a live source.

**Cons:** Requires a new contract for watermark advance, window closure, state eviction, correction, and restart behavior; it is substantially larger than the bounded portfolio scenario.

**Context:** Milestone 2 intentionally computes deterministic Gold only after Silver for a finite replay is complete. Start by defining the late-data and correction contract, then add state-size and restart tests before changing any “bounded batch” claim in README or CV material.

**Effort:** XL
**Priority:** P3
**Depends on:** Recovery MVP and bounded Gold Milestone 2 pass.

## Infrastructure

### Publish pinned runtime images to GHCR

**What:** Build, scan, attest, and publish the custom application, Spark, and Grafana images to GitHub Container Registry with immutable digests.

**Why:** Reviewers can start the portfolio demo without rebuilding a large Spark image or resolving its build-time dependency closure.

**Pros:** Faster onboarding, reproducible release artifacts, explicit provenance, and an auditable version-to-image mapping.

**Cons:** Adds registry lifecycle, architecture support, security scanning, cache, and tag/digest maintenance.

**Context:** Local Dockerfiles remain authoritative and must continue to build successfully. Add published images only after a real local release smoke passes; the demo may prefer an immutable digest but must retain a documented local-build fallback.

**Effort:** M
**Priority:** P2
**Depends on:** Milestone 1, stable JAR lock, and local image/recovery smoke tests pass.
