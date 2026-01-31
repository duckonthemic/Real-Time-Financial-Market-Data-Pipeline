# Spark Structured Streaming Research

## Overview

Spark Structured Streaming provides the processing engine for transforming raw trades into aggregated insights in real-time.

## Processing Model

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│    Kafka     │───▶│    Spark     │───▶│  Cassandra   │
│   Source     │    │  Processing  │    │    Sink      │
└──────────────┘    └──────────────┘    └──────────────┘
        │                  │
        │           ┌──────┴──────┐
        │           │  Watermark  │
        │           │  Windowing  │
        │           └─────────────┘
        │
   Event Time
```

## Windowed Aggregations

### OHLCV Calculation

```python
ohlcv_df = trades_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        col("symbol"),
        window(col("timestamp"), "5 minutes", "1 minute")
    ) \
    .agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
        sum("volume").alias("volume")
    )
```

### Window Types

| Type | Use Case | Config |
|------|----------|--------|
| Tumbling | Non-overlapping aggregations | `window("ts", "5 minutes")` |
| Sliding | Rolling averages | `window("ts", "5 min", "1 min")` |
| Session | Activity-based grouping | Not used for market data |

## Watermarking for Late Data

```
                    Current Time
                          │
    ───────────────────────┼──────────────────────▶
                          │
    ◄─────────────────────│
       10 min watermark   │
                          │
    [Data arriving here   │  [Data arriving here
     is processed]        │   is dropped]
```

**Configuration**: 10-minute watermark allows for network delays while maintaining reasonable latency.

## Processing Modes

| Mode | Trigger | Use Case |
|------|---------|----------|
| Continuous | processingTime(0) | Lowest latency |
| Micro-batch | processingTime("5 seconds") | Balance latency/throughput |
| Once | once=True | Batch reprocessing |

**Decision**: 
- Bronze layer: Continuous processing
- Gold layer: 5-second micro-batch

## Checkpointing

```python
query = df.writeStream \
    .format("cassandra") \
    .option("checkpointLocation", "/tmp/checkpoints/gold_5m") \
    .start()
```

**Checkpoint includes**:
- Kafka offsets
- State data for aggregations
- Progress metadata

## Memory Management

| Config | Value | Purpose |
|--------|-------|---------|
| spark.sql.shuffle.partitions | 10 | Match Kafka partitions |
| spark.executor.memory | 2g | Per executor |
| spark.driver.memory | 1g | Driver process |

## Best Practices

1. **Match partitions** - Spark partitions = Kafka partitions
2. **Use event time** - Not processing time for accuracy
3. **Set appropriate watermark** - Balance latency vs completeness
4. **Monitor state size** - Aggregations accumulate state
5. **Test failure recovery** - Checkpoints must work correctly
