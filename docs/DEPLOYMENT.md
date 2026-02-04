# Deployment Guide

> Complete guide for deploying the Real-Time Market Data Pipeline.

---

## 📋 Prerequisites

### System Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 4 cores | 8 cores |
| RAM | 8 GB | 16 GB |
| Disk | 20 GB | 50 GB SSD |
| Docker | v20.10+ | Latest |
| Docker Compose | v2.0+ | Latest |

### API Keys

1. **Finnhub API Key**
   - Sign up at [finnhub.io](https://finnhub.io)
   - Free tier: 60 API calls/minute
   - Get your key from Dashboard → API Keys

2. **(Optional) Slack Webhook**
   - Create an Incoming Webhook at [Slack API](https://api.slack.com/messaging/webhooks)
   - Used for alerting notifications

---

## 🚀 Quick Start

### 1. Clone Repository

```bash
git clone https://github.com/your-repo/finnhub-pipeline.git
cd finnhub-pipeline
```

### 2. Configure Environment

```bash
# Copy template
cp .env.example .env

# Edit with your API key
nano .env
```

Required variables:
```ini
FINNHUB_API_KEY=your_api_key_here
```

### 3. Start Infrastructure

```bash
# Start all services
docker-compose up -d

# Verify services are running
docker-compose ps
```

### 4. Initialize Cassandra Schema

```bash
# Wait for Cassandra to be ready (~60 seconds)
sleep 60

# Run schema initialization
python scripts/init_cassandra.py
```

### 5. Start Producer Pipeline

```bash
# Subscribe to symbols
python -m src.producer.main --symbols AAPL,MSFT,GOOGL
```

### 6. Start Spark Consumer

```bash
# Submit Spark job
docker-compose exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
  /opt/spark/work-dir/src/consumer/main.py
```

### 7. Access Dashboards

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| Kafka UI | http://localhost:8090 | - |
| Spark UI | http://localhost:8080 | - |

---

## 🔍 Verify Deployment

Run the smoke test:

```bash
python scripts/smoke_test.py -v
```

Expected output:
```
  ✅ PASS  Finnhub API
  ✅ PASS  Kafka Broker
  ✅ PASS  Schema Registry
  ✅ PASS  Cassandra
  ✅ PASS  Grafana
  ✅ PASS  Data Flow
```

---

## 🛑 Stopping the Pipeline

```bash
# Stop producer (Ctrl+C in terminal)

# Stop Spark job
docker-compose exec spark-master pkill -f spark-submit

# Stop all containers
docker-compose down

# Stop and remove volumes (CAUTION: deletes data)
docker-compose down -v
```

---

## 📊 Service Ports

| Service | Port | Description |
|---------|------|-------------|
| Zookeeper | 2181 | Kafka coordination |
| Kafka | 9092 | Internal broker |
| Kafka | 29092 | External access |
| Schema Registry | 8081 | Avro schemas |
| Cassandra | 9042 | CQL connections |
| Spark Master | 7077 | Cluster manager |
| Spark UI | 8080 | Web interface |
| Grafana | 3000 | Dashboards |
| Kafka UI | 8090 | Kafka monitoring |

---

## 🔧 Configuration Reference

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FINNHUB_API_KEY` | - | Required. API key for Finnhub |
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka broker address |
| `CASSANDRA_HOST` | localhost | Cassandra host |
| `LOG_LEVEL` | INFO | Application log level |

---

## 🆘 Troubleshooting

### Cassandra Not Ready

```bash
# Check Cassandra logs
docker-compose logs cassandra

# Wait and retry
sleep 60 && python scripts/init_cassandra.py
```

### Kafka Connection Refused

```bash
# Restart Kafka
docker-compose restart kafka

# Check broker logs
docker-compose logs kafka
```

### Grafana Dashboard Empty

1. Verify data exists in Cassandra:
   ```bash
   docker-compose exec cassandra cqlsh -e "SELECT * FROM market_data.trades_silver LIMIT 5;"
   ```

2. Check datasource configuration in Grafana:
   - Go to Settings → Data Sources → Cassandra
   - Verify connection settings

---

*Last updated: 2026-01-31*
