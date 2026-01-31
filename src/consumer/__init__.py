# src/consumer module
"""Spark Structured Streaming consumers for data processing."""

from .spark_processor import MarketDataProcessor, SparkProcessorConfig
from .transformations import (
    apply_watermark,
    calculate_ohlcv,
    calculate_ohlcv_5m,
    calculate_ohlcv_1h,
    parse_kafka_value,
    parse_with_event_time,
    to_bronze,
    to_silver,
)
from .sinks import (
    CassandraSink,
    CassandraSinkConfig,
    ConsoleSink,
    KafkaSink,
    SinkManager,
)

__all__ = [
    "MarketDataProcessor",
    "SparkProcessorConfig",
    "apply_watermark",
    "calculate_ohlcv",
    "calculate_ohlcv_5m",
    "calculate_ohlcv_1h",
    "parse_kafka_value",
    "parse_with_event_time",
    "to_bronze",
    "to_silver",
    "CassandraSink",
    "CassandraSinkConfig",
    "ConsoleSink",
    "KafkaSink",
    "SinkManager",
]
