# 📈 Real-Time Financial Market Data Pipeline

A production-ready streaming data pipeline for processing real-time stock market data using **Apache Kafka**, **Apache Spark**, **Apache Cassandra**, and **Grafana**.

![Architecture](https://img.shields.io/badge/Architecture-Lambda-blue)
![Kafka](https://img.shields.io/badge/Kafka-3.5-orange)
![Spark](https://img.shields.io/badge/Spark-3.4.1-yellow)
![Cassandra](https://img.shields.io/badge/Cassandra-4.1-green)
![Grafana](https://img.shields.io/badge/Grafana-10.2-purple)

## 🏗️ Architecture

```
┌─────────────┐    ┌─────────┐    ┌─────────────┐    ┌───────────┐    ┌─────────┐
│  Finnhub    │───▶│  Kafka  │───▶│    Spark    │───▶│ Cassandra │───▶│ Grafana │
│  WebSocket  │    │         │    │  Streaming  │    │           │    │         │
└─────────────┘    └─────────┘    └─────────────┘    └───────────┘    └─────────┘
     API            Message        Bronze/Silver       Time-Series      Real-Time
    Source          Queue          /Gold Layers         Storage        Dashboard
```

### Data Layers (Medallion Architecture)

| Layer | Table | Description |
|-------|-------|-------------|
| **Bronze** | `trades_bronze` | Raw trade data with Kafka metadata |
| **Silver** | `trades_silver` | Cleaned & validated trades |
| **Gold** | `trades_gold_5m` | 5-minute OHLCV aggregations |

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Finnhub API Key ([Get free key](https://finnhub.io/))

### 1. Clone & Configure

```bash
git clone <repository-url>
cd depj2

# Create .env file
cp .env.example .env
# Edit .env and add your FINNHUB_API_KEY
```

### 2. Start Infrastructure

```bash
docker-compose up -d
```

### 3. Initialize Schema

```bash
# Create Cassandra keyspace and tables
docker-compose exec cassandra cqlsh -f /docker-entrypoint-initdb.d/init.cql
```

### 4. Run Producer (Terminal 1)

```bash
pip install -r requirements.txt
python -m src.producer.main --ignore-market-hours --kafka-servers localhost:29092
```

### 5. Run Spark Consumer (Terminal 2)

```bash
docker-compose exec -e PYTHONPATH=/opt/spark/work-dir spark-master \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
  /opt/spark/work-dir/src/consumer/main.py \
  --kafka-servers kafka:9092 \
  --cassandra-host cassandra \
  --starting-offsets earliest \
  --enable-console
```

## 📊 Access Dashboards

| Service | URL | Credentials |
|---------|-----|-------------|
| **Grafana** | http://localhost:3000 | admin / admin |
| **Spark UI** | http://localhost:8080 | - |
| **Kafka UI** | http://localhost:8090 | - |

## 📁 Project Structure

```
depj2/
├── src/
│   ├── producer/          # Finnhub → Kafka producer
│   │   ├── main.py
│   │   ├── pipeline.py
│   │   ├── finnhub_client.py
│   │   └── config.py
│   ├── consumer/          # Spark streaming processor
│   │   ├── main.py
│   │   ├── spark_processor.py
│   │   ├── transformations.py
│   │   └── sinks.py
│   └── storage/           # Cassandra utilities
├── grafana/
│   ├── dashboards/        # Pre-configured dashboards
│   └── provisioning/      # Auto-provisioned datasources
├── schemas/               # Cassandra CQL schemas
├── docker-compose.yml
└── .env
```

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FINNHUB_API_KEY` | Finnhub API key | Required |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | localhost:9092 |
| `CASSANDRA_HOSTS` | Cassandra contact point | localhost |
| `SPARK_MASTER` | Spark master URL | spark://spark-master:7077 |

### Tracked Symbols

Default: `AAPL, GOOGL, MSFT, AMZN, META, TSLA, NVDA, JPM, V, WMT`

Customize via `--symbols` flag:
```bash
python -m src.producer.main --symbols AAPL,TSLA,NVDA
```

## 🔧 Troubleshooting

### No data in Grafana?

1. Check if Kafka is running: `docker-compose ps kafka`
2. Verify producer is connected to Finnhub WebSocket
3. US market hours: Mon-Fri 9:30 PM - 4:00 AM (GMT+7)

### Spark job not appearing?

Ensure source code is mounted:
```bash
docker-compose exec spark-master ls /opt/spark/work-dir/src/
```

### Cassandra connection issues?

Test connectivity:
```bash
docker-compose exec cassandra cqlsh -e "DESCRIBE KEYSPACES;"
```

## 📈 Sample Queries

```sql
-- Recent trades
SELECT * FROM market_data.trades_silver LIMIT 10;

-- OHLCV aggregations
SELECT symbol, window_start, open, high, low, close, volume 
FROM market_data.trades_gold_5m 
WHERE symbol = 'AAPL' LIMIT 5;
```

## 📄 License

MIT License - See [LICENSE](LICENSE) for details.

---

Built with ❤️ for real-time financial data processing
