"""
Spark Structured Streaming Processor
Main processor for the market data pipeline.
"""
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from .sinks import CassandraSink, CassandraSinkConfig, ConsoleSink, SinkManager
from .transformations import (
    apply_watermark,
    calculate_ohlcv_5m,
    parse_with_event_time,
    to_bronze,
    to_silver,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class SparkProcessorConfig:
    """Configuration for the Spark processor."""
    # Kafka settings
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "trades_raw"
    kafka_starting_offsets: str = "latest"
    
    # Cassandra settings
    cassandra_host: str = "localhost"
    cassandra_port: int = 9042
    cassandra_keyspace: str = "market_data"
    
    # Processing settings
    checkpoint_location: str = "/tmp/spark-checkpoints"
    trigger_interval: str = "10 seconds"
    watermark_delay: str = "10 minutes"
    
    # Feature flags
    enable_bronze: bool = True
    enable_silver: bool = True
    enable_gold: bool = True
    enable_console: bool = False
    
    @classmethod
    def from_env(cls) -> "SparkProcessorConfig":
        """Load configuration from environment variables."""
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "trades_raw"),
            cassandra_host=os.getenv("CASSANDRA_HOST", "localhost"),
            cassandra_keyspace=os.getenv("CASSANDRA_KEYSPACE", "market_data"),
            checkpoint_location=os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints"),
            watermark_delay=os.getenv("WATERMARK_DELAY", "10 minutes"),
        )


# =============================================================================
# Spark Session Builder
# =============================================================================

def create_spark_session(
    app_name: str = "MarketDataProcessor",
    cassandra_host: str = "localhost"
) -> SparkSession:
    """
    Create and configure SparkSession with required dependencies.
    
    Args:
        app_name: Application name for Spark UI
        cassandra_host: Cassandra connection host
        
    Returns:
        Configured SparkSession
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", cassandra_host) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()


# =============================================================================
# Market Data Processor
# =============================================================================

class MarketDataProcessor:
    """
    Main processor for market data streaming pipeline.
    
    Reads from Kafka, applies transformations, and writes to Cassandra
    using the Medallion architecture (Bronze → Silver → Gold).
    """
    
    def __init__(self, config: SparkProcessorConfig, spark: Optional[SparkSession] = None):
        self.config = config
        self.spark = spark or create_spark_session(
            cassandra_host=config.cassandra_host
        )
        self.sink_manager = SinkManager()
        self._source_df: Optional[DataFrame] = None
        
    def read_from_kafka(self) -> DataFrame:
        """
        Read streaming data from Kafka topic.
        
        Returns:
            Streaming DataFrame with raw Kafka messages
        """
        logger.info(f"Reading from Kafka topic: {self.config.kafka_topic}")
        
        self._source_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers) \
            .option("subscribe", self.config.kafka_topic) \
            .option("startingOffsets", self.config.kafka_starting_offsets) \
            .option("failOnDataLoss", "false") \
            .load()
        
        return self._source_df
    
    def process(self) -> None:
        """
        Start the streaming pipeline with all configured layers.
        """
        if self._source_df is None:
            self.read_from_kafka()
            
        # Parse and add event time
        parsed_df = parse_with_event_time(self._source_df)
        
        # Apply watermark for late data handling
        watermarked_df = apply_watermark(parsed_df, self.config.watermark_delay)
        
        # Create Cassandra sink
        cassandra_config = CassandraSinkConfig(
            keyspace=self.config.cassandra_keyspace,
            checkpoint_location=self.config.checkpoint_location,
            trigger_interval=self.config.trigger_interval,
            host=self.config.cassandra_host,
        )
        cassandra_sink = CassandraSink(cassandra_config)
        
        # Bronze Layer: Raw data
        if self.config.enable_bronze:
            bronze_df = to_bronze(watermarked_df)
            query = cassandra_sink.write_to_bronze(bronze_df)
            self.sink_manager.add_query("bronze", query)
            logger.info("Started Bronze layer stream")
        
        # Silver Layer: Cleaned data
        if self.config.enable_silver:
            silver_df = to_silver(watermarked_df)
            query = cassandra_sink.write_to_silver(silver_df)
            self.sink_manager.add_query("silver", query)
            logger.info("Started Silver layer stream")
        
        # Gold Layer: OHLCV aggregations
        if self.config.enable_gold:
            gold_df = calculate_ohlcv_5m(watermarked_df)
            query = cassandra_sink.write_to_gold(gold_df)
            self.sink_manager.add_query("gold_5m", query)
            logger.info("Started Gold layer stream")
        
        # Console output for debugging
        if self.config.enable_console:
            console_sink = ConsoleSink()
            query = console_sink.write(watermarked_df, "debug_console")
            self.sink_manager.add_query("console", query)
            logger.info("Started Console debug stream")
    
    def await_termination(self) -> None:
        """Wait for all streaming queries to terminate."""
        logger.info("Awaiting termination of all streaming queries...")
        self.sink_manager.await_all()
    
    def stop(self) -> None:
        """Stop all streaming queries gracefully."""
        logger.info("Stopping all streaming queries...")
        self.sink_manager.stop_all()
        self.spark.stop()
        
    def get_status(self) -> Dict:
        """Get status of all streaming queries."""
        return self.sink_manager.get_status()
