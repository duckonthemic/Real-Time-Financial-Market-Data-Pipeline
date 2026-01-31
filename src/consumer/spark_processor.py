"""
Spark Structured Streaming Processor
Handles Bronze, Silver, and Gold data layer transformations.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, window, first, last, min as spark_min,
    max as spark_max, sum as spark_sum, avg, count,
    current_timestamp, lit, when, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, ArrayType
)
import logging

logger = logging.getLogger(__name__)


class SparkProcessor:
    """
    Spark Structured Streaming processor for market data.
    
    Implements Bronze → Silver → Gold data layer architecture.
    """
    
    # Schema for incoming trade data
    TRADE_SCHEMA = StructType([
        StructField("s", StringType(), True),      # symbol
        StructField("p", DoubleType(), True),      # price
        StructField("v", LongType(), True),        # volume
        StructField("t", LongType(), True),        # timestamp (ms)
        StructField("c", ArrayType(StringType()), True),  # conditions
    ])
    
    def __init__(
        self,
        spark: SparkSession,
        kafka_servers: str,
        cassandra_host: str,
        checkpoint_base: str = "/tmp/checkpoints"
    ):
        """
        Initialize Spark processor.
        
        Args:
            spark: SparkSession instance
            kafka_servers: Kafka bootstrap servers
            cassandra_host: Cassandra host address
            checkpoint_base: Base path for checkpoints
        """
        self.spark = spark
        self.kafka_servers = kafka_servers
        self.cassandra_host = cassandra_host
        self.checkpoint_base = checkpoint_base
        
    def read_kafka_stream(self, topic: str) -> DataFrame:
        """
        Create streaming DataFrame from Kafka topic.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            Streaming DataFrame
        """
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    
    def parse_trades(self, kafka_df: DataFrame) -> DataFrame:
        """
        Parse raw Kafka messages into structured trades.
        
        Args:
            kafka_df: Raw Kafka streaming DataFrame
            
        Returns:
            Parsed trades DataFrame
        """
        return kafka_df \
            .select(
                from_json(col("value").cast("string"), self.TRADE_SCHEMA).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ) \
            .select(
                col("data.s").alias("symbol"),
                col("data.p").alias("price"),
                col("data.v").alias("volume"),
                (col("data.t") / 1000).cast("timestamp").alias("trade_timestamp"),
                col("data.c").alias("conditions"),
                col("kafka_timestamp").alias("ingestion_time")
            )
    
    def bronze_pipeline(self, trades_df: DataFrame) -> None:
        """
        Bronze layer: Persist raw data without transformation.
        
        Args:
            trades_df: Parsed trades DataFrame
        """
        query = trades_df \
            .withColumn("trade_date", to_date("trade_timestamp")) \
            .writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "market_data") \
            .option("table", "trades_bronze") \
            .option("checkpointLocation", f"{self.checkpoint_base}/bronze") \
            .outputMode("append") \
            .start()
        
        logger.info("Bronze pipeline started")
        return query
    
    def silver_transformations(self, bronze_df: DataFrame) -> DataFrame:
        """
        Silver layer: Clean and validate data.
        
        Args:
            bronze_df: Bronze layer DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        return bronze_df \
            .filter(col("volume") > 0) \
            .filter(col("price") > 0) \
            .withColumn(
                "is_valid",
                when((col("volume") > 0) & (col("price") > 0), lit(True))
                .otherwise(lit(False))
            ) \
            .withColumn("processed_at", current_timestamp()) \
            .dropDuplicates(["symbol", "trade_timestamp"])
    
    def gold_aggregations(self, silver_df: DataFrame, window_duration: str = "5 minutes") -> DataFrame:
        """
        Gold layer: Windowed OHLCV aggregations.
        
        Args:
            silver_df: Silver layer DataFrame
            window_duration: Window size for aggregation
            
        Returns:
            Aggregated DataFrame
        """
        return silver_df \
            .withWatermark("trade_timestamp", "10 minutes") \
            .groupBy(
                col("symbol"),
                window(col("trade_timestamp"), window_duration, "1 minute")
            ) \
            .agg(
                first("price").alias("open"),
                spark_max("price").alias("high"),
                spark_min("price").alias("low"),
                last("price").alias("close"),
                spark_sum("volume").alias("volume"),
                count("*").alias("trade_count"),
                avg("price").alias("vwap")
            ) \
            .select(
                col("symbol"),
                to_date("window.start").alias("window_date"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("open"), col("high"), col("low"), col("close"),
                col("volume"), col("trade_count"), col("vwap")
            )


def create_spark_session(app_name: str = "MarketDataPipeline") -> SparkSession:
    """Create configured Spark session."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
        .getOrCreate()


# Example usage
if __name__ == "__main__":
    spark = create_spark_session()
    
    processor = SparkProcessor(
        spark=spark,
        kafka_servers="kafka:9092",
        cassandra_host="cassandra"
    )
    
    # Read from Kafka
    kafka_df = processor.read_kafka_stream("trades_raw")
    trades_df = processor.parse_trades(kafka_df)
    
    # Start bronze pipeline
    bronze_query = processor.bronze_pipeline(trades_df)
    bronze_query.awaitTermination()
