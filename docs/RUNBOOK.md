# Operational Runbook

> Troubleshooting and operational procedures for the Real-Time Market Data Pipeline.

---

## 📞 Escalation Matrix

| Severity | Response Time | Contact |
|----------|---------------|---------|
| Critical (P1) | 15 min | On-call engineer |
| High (P2) | 1 hour | Team lead |
| Medium (P3) | 4 hours | Sprint owner |
| Low (P4) | Next sprint | Backlog |

---

## 🚨 Common Alerts & Responses

### 1. Pipeline Down - Zero Trades

**Alert**: No trades received in the last 10 minutes.

**Symptoms**:
- Grafana dashboard shows 0 trades
- Kafka topic has no new messages

**Investigation**:

```bash
# 1. Check producer process
ps aux | grep "python -m src.producer"

# 2. Check Finnhub connection
curl "https://finnhub.io/api/v1/quote?symbol=AAPL&token=$FINNHUB_API_KEY"

# 3. Check Kafka topic
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic trades_raw \
  --from-beginning \
  --max-messages 5
```

**Resolution**:

```bash
# Restart producer
python -m src.producer.main --symbols AAPL,MSFT,GOOGL
```

---

### 2. High Latency (>10 seconds)

**Alert**: End-to-end latency exceeds threshold.

**Investigation**:

```bash
# 1. Check Spark job status
docker-compose exec spark-master spark-submit --status

# 2. Check Cassandra write latency
docker-compose exec cassandra nodetool tpstats

# 3. Check Kafka consumer lag
docker-compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group spark-consumer
```

**Resolution**:

```bash
# Restart Spark job with more resources
docker-compose exec spark-master spark-submit \
  --executor-memory 2G \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/work-dir/src/consumer/main.py
```

---

### 3. Kafka Broker Down

**Alert**: Kafka health check failing.

**Investigation**:

```bash
# 1. Check Kafka logs
docker-compose logs --tail=100 kafka

# 2. Check Zookeeper
docker-compose exec zookeeper zkCli.sh -server localhost:2181 <<< "ls /brokers/ids"
```

**Resolution**:

```bash
# Restart Kafka and dependent services
docker-compose restart kafka schema-registry
```

---

### 4. Cassandra Out of Disk

**Alert**: Disk usage > 80%

**Investigation**:

```bash
# 1. Check disk usage
docker-compose exec cassandra nodetool status
docker-compose exec cassandra nodetool info

# 2. Check table sizes
docker-compose exec cassandra nodetool cfstats market_data
```

**Resolution**:

```bash
# 1. Trigger compaction
docker-compose exec cassandra nodetool compact market_data

# 2. If still full, cleanup tombstones
docker-compose exec cassandra nodetool cleanup
```

---

## 🔄 Routine Maintenance

### Daily Checks

- [ ] Verify Grafana dashboard shows recent data
- [ ] Check Slack for any overnight alerts
- [ ] Review Kafka consumer lag

### Weekly Checks

- [ ] Run smoke test: `python scripts/smoke_test.py -v`
- [ ] Check Cassandra disk usage
- [ ] Review Spark job metrics

### Monthly Checks

- [ ] Update Docker images
- [ ] Rotate API keys if needed
- [ ] Review and tune alert thresholds

---

## 🛠️ Useful Commands

### Service Management

```bash
# View all service logs
docker-compose logs -f

# Restart specific service
docker-compose restart <service-name>

# Scale Spark workers
docker-compose up -d --scale spark-worker=3
```

### Kafka Operations

```bash
# List topics
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Create topic
docker-compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic new_topic \
  --partitions 6 \
  --replication-factor 1

# Delete topic
docker-compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic old_topic

# View consumer groups
docker-compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

### Cassandra Operations

```bash
# Interactive CQL shell
docker-compose exec cassandra cqlsh

# Common queries
docker-compose exec cassandra cqlsh -e "SELECT * FROM market_data.trades_silver LIMIT 10;"
docker-compose exec cassandra cqlsh -e "SELECT * FROM market_data.latest_prices;"

# Node status
docker-compose exec cassandra nodetool status
```

---

## 📈 Performance Tuning

### Kafka

| Parameter | Default | Tuned | Notes |
|-----------|---------|-------|-------|
| `num.partitions` | 6 | 12 | More parallelism |
| `log.retention.hours` | 168 | 72 | Reduce storage |

### Spark

| Parameter | Default | Tuned | Notes |
|-----------|---------|-------|-------|
| `spark.executor.memory` | 1G | 2G | More memory |
| `spark.sql.shuffle.partitions` | 200 | 12 | Match Kafka partitions |

### Cassandra

| Parameter | Default | Tuned | Notes |
|-----------|---------|-------|-------|
| `MAX_HEAP_SIZE` | 1G | 2G | Production sizing |
| `gc_grace_seconds` | 864000 | 86400 | Faster tombstone cleanup |

---

*Last updated: 2026-01-31*
