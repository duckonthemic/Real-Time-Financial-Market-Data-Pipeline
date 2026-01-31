# Apache Kafka Architecture Research

## Overview

Kafka serves as the buffering layer between the Finnhub producer and Spark consumers, ensuring reliable message delivery during traffic spikes.

## Architecture for Financial Data

```
┌─────────────┐     ┌─────────────────────────────────────┐     ┌─────────────┐
│  Producer   │────▶│            Kafka Cluster            │────▶│  Consumer   │
│  (Finnhub)  │     │  ┌─────────────────────────────┐    │     │  (Spark)    │
└─────────────┘     │  │     trades_raw topic        │    │     └─────────────┘
                    │  │  P0 │ P1 │ P2 │ ... │ P9    │    │
                    │  └─────────────────────────────┘    │
                    │        Schema Registry              │
                    └─────────────────────────────────────┘
```

## Topic Configuration

### trades_raw

| Setting | Value | Rationale |
|---------|-------|-----------|
| Partitions | 10 | Scale with symbol count |
| Replication | 2 | Fault tolerance |
| Retention | 7 days | Reprocessing capability |
| Cleanup | delete | Remove after TTL |

### Partitioning Strategy

**Partition by Symbol** ensures:
- All trades for a symbol go to same partition
- Maintains chronological order per symbol
- Enables parallel processing by symbol

```python
# Partition key = symbol
def get_partition(symbol: str, num_partitions: int) -> int:
    return hash(symbol) % num_partitions
```

## Producer Configuration

```python
producer_config = {
    "bootstrap.servers": "kafka:9092",
    "enable.idempotence": True,    # Exactly-once semantics
    "acks": "all",                 # Wait for all replicas
    "retries": 10,                 # Retry on failure
    "retry.backoff.ms": 100,
    "batch.size": 16384,           # 16KB batches
    "linger.ms": 5,                # Wait 5ms to batch
    "compression.type": "snappy"   # Fast compression
}
```

## Schema Registry

### Benefits
- Schema versioning and evolution
- Compatibility checking (BACKWARD, FORWARD, FULL)
- Prevents breaking changes

### Avro vs JSON

| Aspect | Avro | JSON |
|--------|------|------|
| Size | Compact | Verbose |
| Parsing | Fast | Slower |
| Schema | Required | Optional |
| Evolution | Built-in | Manual |

**Decision**: Use Avro with full compatibility mode.

## Consumer Group Strategy

```
┌─────────────────────────────────────────────┐
│            Consumer Group: spark-streaming   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │Consumer 1│  │Consumer 2│  │Consumer 3│   │
│  │  P0,P1   │  │  P2,P3   │  │ P4..P9   │   │
│  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────┘
```

## Monitoring Metrics

| Metric | Alert Threshold |
|--------|-----------------|
| Consumer lag | > 1000 messages |
| Under-replicated partitions | > 0 |
| Request latency p99 | > 100ms |
| Broker disk usage | > 80% |

## Recommendations

1. **Start with 10 partitions** - can increase later, not decrease
2. **Use idempotent producer** - prevents duplicates on retry
3. **Monitor consumer lag** - critical for real-time systems
4. **Enable compression** - Snappy for speed, LZ4 for ratio
