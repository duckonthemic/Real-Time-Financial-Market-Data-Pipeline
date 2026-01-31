# Cassandra Time-Series Modeling Research

## Overview

Cassandra is optimized for high write throughput and time-series data, making it ideal for storing streaming market data.

## Data Modeling Principles

### 1. Query-Driven Design

Design tables based on query patterns, not normalized structure.

**Common Queries**:
- "Get all trades for AAPL today"
- "Get 5-minute OHLCV for AAPL last hour"
- "Get latest price for all symbols"

### 2. Partition Key Strategy

```
PRIMARY KEY ((symbol, trade_date), trade_timestamp)
            └────────┬────────┘  └───────┬────────┘
              Partition Key         Clustering Column
```

**Benefits**:
- All daily trades for a symbol on one node
- Efficient range scans by time within partition
- Partition size ~10-100MB (optimal)

### 3. Clustering Column

`CLUSTERING ORDER BY (trade_timestamp DESC)`

- Stores newest data first on disk
- Optimizes "latest N trades" queries
- Natural ordering for time-series

## Schema Design

### Bronze Table (Raw)

```sql
CREATE TABLE trades_bronze (
    symbol text,
    trade_date date,
    trade_timestamp timestamp,
    price double,
    volume bigint,
    PRIMARY KEY ((symbol, trade_date), trade_timestamp)
) WITH CLUSTERING ORDER BY (trade_timestamp DESC);
```

### Gold Table (Aggregated)

```sql
CREATE TABLE trades_gold_5m (
    symbol text,
    window_date date,
    window_start timestamp,
    open double, high double, low double, close double,
    volume bigint,
    PRIMARY KEY ((symbol, window_date), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
```

## Write Optimization

### Idempotent Writes

```sql
-- Same primary key = upsert (no duplicates)
INSERT INTO trades_bronze (symbol, trade_date, trade_timestamp, ...)
VALUES ('AAPL', '2024-01-31', '2024-01-31 10:30:00', ...);
```

### TTL for Data Retention

| Layer | TTL | Reason |
|-------|-----|--------|
| Bronze | 7 days | Audit only |
| Silver | 30 days | Analysis |
| Gold 5m | 90 days | Short-term trends |
| Gold 1h | 365 days | Long-term analysis |

## Compaction Strategies

| Strategy | Use Case |
|----------|----------|
| TimeWindowCompaction | Time-series (our choice) |
| LeveledCompaction | Read-heavy workloads |
| SizeTieredCompaction | Write-heavy workloads |

```sql
WITH compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': 1,
    'compaction_window_unit': 'DAYS'
}
```

## Performance Considerations

### Anti-Patterns to Avoid

| Anti-Pattern | Problem | Solution |
|--------------|---------|----------|
| Large partitions (>100MB) | Memory issues | Date-based partitioning |
| Secondary indexes on high-cardinality | Slow queries | Materialized views |
| ALLOW FILTERING | Full table scan | Query-driven tables |

### Tuning Parameters

| Config | Value | Purpose |
|--------|-------|---------|
| gc_grace_seconds | 86400 | Tombstone cleanup |
| memtable_flush_writers | 2 | Write performance |
| concurrent_reads | 32 | Read parallelism |

## Recommendations

1. **Partition by (symbol, date)** - Keeps partitions manageable
2. **Use TimeWindowCompaction** - Optimized for time-series
3. **Set appropriate TTL** - Automatic data expiration
4. **Avoid secondary indexes** - Create separate query tables
5. **Monitor partition sizes** - Alert if >100MB
