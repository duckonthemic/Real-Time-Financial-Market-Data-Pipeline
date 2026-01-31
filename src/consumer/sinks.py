"""
Spark Streaming Output Sinks
Configures output destinations for streaming data.
"""
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)


# =============================================================================
# Sink Configuration
# =============================================================================

@dataclass
class CassandraSinkConfig:
    """Configuration for Cassandra sink."""
    keyspace: str = "market_data"
    checkpoint_location: str = "/tmp/checkpoints"
    trigger_interval: str = "10 seconds"
    output_mode: str = "append"
    # Connection settings
    host: str = "localhost"
    port: int = 9042


@dataclass 
class ConsoleSinkConfig:
    """Configuration for console sink (debugging)."""
    num_rows: int = 20
    truncate: bool = False
    trigger_interval: str = "10 seconds"


# =============================================================================
# Cassandra Sink
# =============================================================================

class CassandraSink:
    """
    Writes streaming DataFrames to Cassandra tables.
    
    Uses foreachBatch for micro-batch processing.
    """
    
    def __init__(self, config: CassandraSinkConfig):
        self.config = config
        self._batch_count = 0
        
    def write_to_bronze(self, df: DataFrame, table: str = "trades_bronze") -> StreamingQuery:
        """
        Write raw trades to Bronze layer table.
        
        Args:
            df: Streaming DataFrame with trade data
            table: Target Cassandra table name
            
        Returns:
            StreamingQuery handle
        """
        return self._start_stream(
            df, 
            table, 
            f"{self.config.checkpoint_location}/bronze"
        )
    
    def write_to_silver(self, df: DataFrame, table: str = "trades_silver") -> StreamingQuery:
        """Write cleaned trades to Silver layer table."""
        return self._start_stream(
            df,
            table,
            f"{self.config.checkpoint_location}/silver"
        )
    
    def write_to_gold(self, df: DataFrame, table: str = "trades_gold_5m") -> StreamingQuery:
        """Write OHLCV aggregations to Gold layer table."""
        return self._start_stream(
            df,
            table,
            f"{self.config.checkpoint_location}/gold",
            output_mode="update"  # Use update for aggregations
        )
    
    def _start_stream(
        self, 
        df: DataFrame, 
        table: str, 
        checkpoint: str,
        output_mode: Optional[str] = None
    ) -> StreamingQuery:
        """
        Start a streaming query to Cassandra.
        
        Args:
            df: Source streaming DataFrame
            table: Target table name
            checkpoint: Checkpoint location for fault tolerance
            output_mode: Output mode (append, update, complete)
            
        Returns:
            StreamingQuery handle
        """
        mode = output_mode or self.config.output_mode
        
        def write_batch(batch_df: DataFrame, batch_id: int) -> None:
            """Write a micro-batch to Cassandra."""
            if batch_df.isEmpty():
                return
                
            count = batch_df.count()
            logger.info(f"Writing batch {batch_id} to {table}: {count} rows")
            
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(
                    table=table,
                    keyspace=self.config.keyspace
                ) \
                .mode("append") \
                .save()
            
            self._batch_count += 1
        
        return df.writeStream \
            .foreachBatch(write_batch) \
            .option("checkpointLocation", checkpoint) \
            .trigger(processingTime=self.config.trigger_interval) \
            .start()


# =============================================================================
# Console Sink (for debugging)
# =============================================================================

class ConsoleSink:
    """
    Writes streaming DataFrames to console for debugging.
    """
    
    def __init__(self, config: Optional[ConsoleSinkConfig] = None):
        self.config = config or ConsoleSinkConfig()
    
    def write(self, df: DataFrame, query_name: str = "console") -> StreamingQuery:
        """
        Write streaming data to console.
        
        Args:
            df: Streaming DataFrame
            query_name: Name for the streaming query
            
        Returns:
            StreamingQuery handle
        """
        return df.writeStream \
            .queryName(query_name) \
            .format("console") \
            .option("numRows", self.config.num_rows) \
            .option("truncate", str(self.config.truncate).lower()) \
            .trigger(processingTime=self.config.trigger_interval) \
            .start()


# =============================================================================
# Kafka Sink (for publishing processed data)
# =============================================================================

class KafkaSink:
    """
    Writes streaming DataFrames back to Kafka topics.
    Useful for downstream consumers.
    """
    
    def __init__(self, bootstrap_servers: str, checkpoint_location: str):
        self.bootstrap_servers = bootstrap_servers
        self.checkpoint_location = checkpoint_location
    
    def write(self, df: DataFrame, topic: str) -> StreamingQuery:
        """
        Write streaming data to Kafka topic.
        
        The DataFrame must have 'key' and 'value' columns.
        
        Args:
            df: Streaming DataFrame with key/value columns
            topic: Target Kafka topic
            
        Returns:
            StreamingQuery handle
        """
        return df.selectExpr(
            "CAST(symbol AS STRING) as key",
            "to_json(struct(*)) AS value"
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("topic", topic) \
            .option("checkpointLocation", f"{self.checkpoint_location}/kafka_{topic}") \
            .start()


# =============================================================================
# Multi-Sink Manager
# =============================================================================

class SinkManager:
    """
    Manages multiple streaming sinks for a single source.
    """
    
    def __init__(self):
        self.queries: Dict[str, StreamingQuery] = {}
        
    def add_query(self, name: str, query: StreamingQuery) -> None:
        """Register a streaming query."""
        self.queries[name] = query
        logger.info(f"Registered query: {name}")
        
    def await_all(self) -> None:
        """Wait for all queries to terminate."""
        for name, query in self.queries.items():
            logger.info(f"Waiting for query: {name}")
            query.awaitTermination()
            
    def stop_all(self) -> None:
        """Stop all active queries."""
        for name, query in self.queries.items():
            logger.info(f"Stopping query: {name}")
            try:
                query.stop()
            except Exception as e:
                logger.error(f"Error stopping {name}: {e}")
                
    def get_status(self) -> Dict[str, Any]:
        """Get status of all queries."""
        return {
            name: {
                "is_active": query.isActive,
                "status": query.status
            }
            for name, query in self.queries.items()
        }
