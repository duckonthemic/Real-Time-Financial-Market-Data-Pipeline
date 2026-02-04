# Real-Time Financial Market Data Pipeline

A scalable, real-time data pipeline for ingesting, processing, and visualizing stock market data from Finnhub.

## 🏗️ Architecture

```
Finnhub WebSocket → Python Producer → Kafka → Spark Streaming → Cassandra → Grafana
```

| Layer | Technology | Purpose |
|-------|------------|---------|
| Source | Finnhub WebSocket | Live market data streaming |
| Ingestion | Python + websockets | Data collection |
| Buffering | Apache Kafka + Avro | Message queue with schema |
| Processing | Spark Structured Streaming | Real-time transformations |
| Storage | Apache Cassandra | Time-series persistence |
| Visualization | Grafana | Real-time dashboards |

## 📁 Project Structure

```
├── src/
│   ├── producer/         # Finnhub → Kafka
│   ├── consumer/         # Spark processing jobs
│   ├── storage/          # Cassandra utilities
│   └── utils/            # Shared utilities
├── schemas/
│   ├── avro/             # Kafka message schemas
│   └── cassandra/        # CQL table definitions
├── grafana/              # Dashboard configurations
├── docker/               # Service Dockerfiles
├── tests/                # Unit & integration tests
├── docs/                 # Documentation
├── config/               # Environment configs
├── docker-compose.yml    # Service orchestration
├── Makefile              # Common commands
└── requirements.txt      # Python dependencies
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Finnhub API key (free tier: https://finnhub.io/)

### 1. Clone and Configure

```bash
# Clone the repository
git clone <repository-url>
cd finnhub-realtime-pipeline

# Copy environment template
cp config/dev.env .env

# Add your Finnhub API key
echo "FINNHUB_API_KEY=your_key_here" >> .env
```

### 2. Start Services

```bash
# Start all Docker services
make docker-up

# Or using docker-compose directly
docker-compose up -d
```

### 3. Initialize Infrastructure

```bash
# Create Kafka topics
make topics

# Initialize Cassandra schema
make cassandra-init
```

### 4. Run the Pipeline

```bash
# Install Python dependencies
make install

# Start the producer (ingests from Finnhub)
make producer

# In another terminal, start Spark job
make spark-job
```

### 5. Access Dashboards

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| Spark UI | http://localhost:8080 | - |
| Kafka UI | http://localhost:8090 | - |

## 🛠️ Development

### Setup Development Environment

```bash
make dev
```

### Run Tests

```bash
# All tests
make test

# Unit tests only
make test-unit

# Integration tests
make test-integration
```

### Code Quality

```bash
# Format code
make format

# Run linters
make lint

# Run all checks
make check
```

## 📊 Data Layers

| Layer | Table | TTL | Purpose |
|-------|-------|-----|---------|
| Bronze | trades_bronze | 7 days | Raw audit trail |
| Silver | trades_silver | 30 days | Cleaned data |
| Gold | trades_gold_5m | 90 days | 5-min OHLCV |
| Gold | trades_gold_1h | 365 days | 1-hour OHLCV |

## ⚠️ Finnhub Free Tier Limits

- 1 WebSocket connection per API key
- Max 50 symbols per connection
- 30 API calls/second
- Some symbols may return volume=0

## 🐛 Troubleshooting

### Services not starting
```bash
docker-compose logs <service-name>
```

### Kafka consumer lag
```bash
docker-compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group <group-id>
```

### Cassandra connection issues
```bash
docker-compose exec cassandra nodetool status
```

## 📚 Documentation

- [System Analysis & Architecture](PLAN/SYSTEM_ANALYSIS.md)
- [Deployment Guide](docs/DEPLOYMENT.md)
- [Operational Runbook](docs/RUNBOOK.md)
- [Research Findings](docs/research/)

## 🧪 Verify Installation

```bash
# Run smoke test
python scripts/smoke_test.py -v
```

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.
