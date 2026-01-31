"""
Spark Streaming Data Transformations
Provides transformation functions for the Bronze, Silver, and Gold data layers.
"""
import json
from datetime import datetime
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# =============================================================================
# Schema Definitions
# =============================================================================

TRADE_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("volume", LongType(), False),
    StructField("timestamp", LongType(), False),
    StructField("conditions", StringType(), True),
    StructField("ingestion_time", LongType(), True),
])

OHLCV_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("open", DoubleType(), False),
    StructField("high", DoubleType(), False),
    StructField("low", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", LongType(), False),
    StructField("trade_count", LongType(), False),
])


# =============================================================================
# Parsing Functions
# =============================================================================

def parse_kafka_value(df: DataFrame) -> DataFrame:
    """
    Parse JSON value from Kafka message into structured columns.
    
    Args:
        df: DataFrame with 'value' column containing JSON bytes
        
    Returns:
        DataFrame with parsed trade columns
    """
    return df.select(
        F.from_json(
            F.col("value").cast("string"),
            TRADE_SCHEMA
        ).alias("trade")
    ).select("trade.*")


def parse_with_event_time(df: DataFrame) -> DataFrame:
    """
    Parse Kafka message and add event_time column from timestamp.
    
    Args:
        df: Raw Kafka DataFrame
        
    Returns:
        DataFrame with event_time column for windowing
    """
    parsed = parse_kafka_value(df)
    return parsed.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp") / 1000)
    )


# =============================================================================
# Bronze Layer Transformations
# =============================================================================

def to_bronze(df: DataFrame) -> DataFrame:
    """
    Transform raw Kafka data to Bronze layer format.
    Bronze layer stores raw data as-is with metadata.
    
    Args:
        df: Parsed trade DataFrame
        
    Returns:
        DataFrame ready for Bronze layer storage
    """
    return df.withColumn(
        "trade_date", F.to_date(F.col("event_time"))
    ).withColumn(
        "processed_at", F.current_timestamp()
    ).withColumn(
        "source", F.lit("finnhub")
    )


# =============================================================================
# Silver Layer Transformations
# =============================================================================

def to_silver(df: DataFrame) -> DataFrame:
    """
    Transform Bronze data to Silver layer format.
    Silver layer contains cleaned and validated data.
    
    Filters applied:
    - Remove zero volume trades
    - Remove negative prices
    - Remove future timestamps (> 5 min ahead)
    - Deduplicate by symbol + timestamp
    
    Args:
        df: Bronze layer DataFrame
        
    Returns:
        DataFrame ready for Silver layer storage
    """
    current_ts = F.current_timestamp()
    
    return df.filter(
        (F.col("volume") > 0) &
        (F.col("price") > 0) &
        (F.col("event_time") <= current_ts + F.expr("INTERVAL 5 MINUTES"))
    ).dropDuplicates(["symbol", "timestamp"])


def clean_trades(df: DataFrame) -> DataFrame:
    """
    Apply data quality transformations.
    
    Args:
        df: Input trade DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    return df.withColumn(
        "price", F.round(F.col("price"), 4)
    ).withColumn(
        "volume", F.abs(F.col("volume"))
    )


# =============================================================================
# Gold Layer Transformations (OHLCV Aggregations)
# =============================================================================

def calculate_ohlcv(df: DataFrame, window_duration: str = "5 minutes") -> DataFrame:
    """
    Calculate OHLCV (Open, High, Low, Close, Volume) aggregations.
    
    Uses tumbling windows based on event time.
    
    Args:
        df: Silver layer DataFrame with event_time column
        window_duration: Window size (e.g., "5 minutes", "1 hour")
        
    Returns:
        DataFrame with OHLCV aggregations per symbol per window
    """
    return df.groupBy(
        F.col("symbol"),
        F.window(F.col("event_time"), window_duration)
    ).agg(
        F.first("price").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.last("price").alias("close"),
        F.sum("volume").alias("volume"),
        F.count("*").alias("trade_count")
    ).select(
        F.col("symbol"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("open"),
        F.col("high"),
        F.col("low"),
        F.col("close"),
        F.col("volume"),
        F.col("trade_count")
    )


def calculate_ohlcv_1h(df: DataFrame) -> DataFrame:
    """Calculate 1-hour OHLCV aggregations."""
    return calculate_ohlcv(df, "1 hour")


def calculate_ohlcv_5m(df: DataFrame) -> DataFrame:
    """Calculate 5-minute OHLCV aggregations."""
    return calculate_ohlcv(df, "5 minutes")


# =============================================================================
# Watermark Configuration
# =============================================================================

def apply_watermark(df: DataFrame, delay: str = "10 minutes") -> DataFrame:
    """
    Apply watermark for handling late data.
    
    Events arriving later than the watermark threshold are dropped.
    
    Args:
        df: DataFrame with event_time column
        delay: Maximum allowed lateness (e.g., "10 minutes")
        
    Returns:
        DataFrame with watermark applied
    """
    return df.withWatermark("event_time", delay)


# =============================================================================
# Utility Functions
# =============================================================================

def add_processing_metadata(df: DataFrame) -> DataFrame:
    """Add processing metadata columns."""
    return df.withColumn(
        "processed_at", F.current_timestamp()
    ).withColumn(
        "processing_date", F.current_date()
    )
