# Sprint 2: Ingestion & Buffering Layer Implementation Plan

## 🎯 Goal

Implement a complete data ingestion pipeline from Finnhub WebSocket to Kafka, with data validation, testing infrastructure, and orchestration scripts.

---

## ✅ Current State Analysis

### Already Implemented (Sprint 1)
| Component | File | Status |
|-----------|------|--------|
| FinnhubClient | `src/producer/finnhub_client.py` | ✅ WebSocket + reconnection |
| MarketDataProducer | `src/producer/kafka_producer.py` | ✅ Idempotent producer |
| TimeUtils | `src/utils/time_utils.py` | ✅ Market hours utility |
| Config | `src/producer/config.py` | ✅ Centralized config |
| Avro Schemas | `schemas/avro/*.avsc` | ✅ Trade & Quote schemas |
| Docker | `docker-compose.yml` | ✅ All services |

### Gap Analysis - Work Remaining
| Component | File | Priority |
|-----------|------|----------|
| Data Validator | `src/producer/validator.py` | 🔴 Critical |
| Pipeline Orchestrator | `src/producer/pipeline.py` | 🔴 Critical |
| Main Entry Point | `src/producer/main.py` | 🔴 Critical |
| Schema Registration | `scripts/register_schemas.py` | 🟡 High |
| Topic Creation Script | `scripts/create_topics.sh` | 🟡 High |
| Unit Tests | `tests/unit/test_*.py` | 🟡 High |
| Integration Tests | `tests/integration/test_*.py` | 🟡 High |
| conftest.py | `tests/conftest.py` | 🟡 High |
| pytest.ini | `pytest.ini` | 🟢 Medium |

---

## 📁 Proposed Changes

### Component 1: Data Validation

#### [NEW] `src/producer/validator.py`
Data quality enforcement module:
- `TradeValidator` class to validate incoming trade messages
- Filters: volume=0, negative prices, future timestamps
- Dead-letter queue support for invalid records
- Validation metrics counter

---

### Component 2: Pipeline Orchestration

#### [NEW] `src/producer/pipeline.py`
Main orchestrator connecting Finnhub → Validator → Kafka:
- `MarketDataPipeline` class
- Manages async coroutines and graceful shutdown
- Metrics collection (messages/sec, errors)
- Market hours awareness integration

#### [NEW] `src/producer/main.py`
Application entry point:
- CLI argument parsing
- Environment variable loading
- Signal handlers (SIGTERM, SIGINT)
- Logging configuration

---

### Component 3: Infrastructure Scripts

#### [NEW] `scripts/register_schemas.py`
Registers Avro schemas with Confluent Schema Registry:
- Reads `schemas/avro/*.avsc`
- Posts to Schema Registry REST API
- Supports compatibility mode configuration

#### [NEW] `scripts/create_topics.sh`
Bash script to create Kafka topics via `kafka-topics.sh`:
- `trades_raw` (10 partitions, 7-day retention)
- `quotes_raw` (5 partitions, 1-day retention)
- `crypto_raw` (5 partitions, 7-day retention)

---

### Component 4: Testing Infrastructure

#### [NEW] `tests/conftest.py`
Pytest fixtures shared across all tests:
- Mock Finnhub WebSocket server
- Mock Kafka producer/consumer
- Sample trade data fixtures

#### [NEW] `tests/unit/test_finnhub_client.py`
Unit tests for `FinnhubClient`:
- Test reconnection backoff calculation
- Test message parsing
- Test subscription logic

#### [NEW] `tests/unit/test_validator.py`
Unit tests for `TradeValidator`:
- Test volume=0 filtering
- Test negative price rejection
- Test timestamp validation

#### [NEW] `tests/unit/test_kafka_producer.py`
Unit tests for `MarketDataProducer`:
- Test message serialization
- Test delivery callback handling
- Test buffer management

#### [NEW] `tests/integration/test_pipeline.py`
Integration test for end-to-end flow:
- Simulated WebSocket → Kafka
- Verify messages reach Kafka topic

#### [NEW] `pytest.ini`
Pytest configuration:
- Test paths
- Coverage settings
- Markers for slow/integration tests

---

## 🧪 Verification Plan

### Automated Tests

| Test Type | Command | Description |
|-----------|---------|-------------|
| Unit Tests | `pytest tests/unit -v --cov=src` | Runs all unit tests with coverage |
| Integration | `pytest tests/integration -v -m integration` | Runs integration tests (requires Docker) |
| Full Suite | `make test` | Runs all tests via Makefile |

### Pre-requisites for Integration Tests
```bash
# Start Docker infrastructure first
docker-compose up -d kafka zookeeper schema-registry
# Wait for services to be healthy (~30 seconds)
docker-compose ps
```

### Manual Verification Steps

1. **Test Docker Services Start**
   ```bash
   cd c:\Users\hoang\Downloads\depj2
   docker-compose up -d
   docker-compose ps
   # Verify all services show "Up" status
   ```

2. **Test Kafka Topic Creation**
   ```bash
   docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
   # Should list: trades_raw, quotes_raw
   ```

3. **Test Producer Sends Messages** (requires valid API key)
   ```bash
   # Set environment variable with your Finnhub key
   set FINNHUB_API_KEY=your_key_here
   python -m src.producer.main --symbols AAPL,MSFT --dry-run
   # Should print received messages to console without Kafka
   ```

4. **Test End-to-End with Kafka UI**
   - Open http://localhost:8090 in browser
   - Navigate to Topics → trades_raw
   - Verify messages appear after running producer

---

## ⚠️ User Review Required

> [!IMPORTANT]
> **Finnhub API Key Required**: The integration tests and manual verification require a valid Finnhub API key set in the `FINNHUB_API_KEY` environment variable.

> [!NOTE]
> The existing implementation in Sprint 1 covers ~70% of Sprint 2's core functionality. This plan focuses on the remaining 30%: validation, orchestration, testing, and scripts.

---

## 📊 Task Breakdown

| Task | Agent | Priority | Est. Time |
|------|-------|----------|-----------|
| Create `validator.py` | backend-specialist | 🔴 Critical | 1h |
| Create `pipeline.py` | backend-specialist | 🔴 Critical | 1.5h |
| Create `main.py` | backend-specialist | 🔴 Critical | 1h |
| Create `register_schemas.py` | backend-specialist | 🟡 High | 0.5h |
| Create `create_topics.sh` | devops-specialist | 🟡 High | 0.5h |
| Create `pytest.ini` + `conftest.py` | testing-specialist | 🟡 High | 0.5h |
| Create unit tests (3 files) | testing-specialist | 🟡 High | 2h |
| Create integration test | testing-specialist | 🟡 High | 1h |
| Run tests & verify | all | 🔴 Critical | 1h |

**Total Estimated Time**: ~9 hours

---

## ✅ Definition of Done

- [ ] `validator.py` filters invalid trades (volume=0, bad timestamps)
- [ ] `pipeline.py` orchestrates Finnhub → Validator → Kafka
- [ ] `main.py` provides CLI entry point with graceful shutdown
- [ ] All unit tests pass with >80% coverage
- [ ] Integration test verifies end-to-end flow
- [ ] Documentation updated

---

*Plan created following `/plan` workflow*
