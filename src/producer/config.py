"""
Configuration Management
Centralized configuration for the market data pipeline.
"""
import os
from dataclasses import dataclass, field
from typing import List, Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class FinnhubConfig:
    """Finnhub API configuration."""
    api_key: str = field(default_factory=lambda: os.getenv("FINNHUB_API_KEY", ""))
    symbols: List[str] = field(default_factory=lambda: [
        "AAPL", "GOOGL", "MSFT", "AMZN", "META",
        "TSLA", "NVDA", "JPM", "V", "WMT"
    ])
    max_retries: int = 10
    base_backoff: float = 2.0
    ignore_market_hours: bool = False


@dataclass
class KafkaConfig:
    """Kafka configuration."""
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    schema_registry_url: str = field(
        default_factory=lambda: os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    )
    trades_topic: str = "trades_raw"
    quotes_topic: str = "quotes_raw"
    crypto_topic: str = "crypto_raw"
    
    # Producer settings
    acks: str = "all"
    retries: int = 10
    batch_size: int = 16384
    linger_ms: int = 5


@dataclass
class SparkConfig:
    """Spark configuration."""
    master: str = field(
        default_factory=lambda: os.getenv("SPARK_MASTER", "spark://spark-master:7077")
    )
    app_name: str = "MarketDataPipeline"
    checkpoint_location: str = "/tmp/checkpoints"
    
    # Processing settings
    trigger_interval: str = "5 seconds"
    watermark_threshold: str = "10 minutes"
    window_duration: str = "5 minutes"
    slide_duration: str = "1 minute"


@dataclass
class CassandraConfig:
    """Cassandra configuration."""
    hosts: List[str] = field(
        default_factory=lambda: os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
    )
    port: int = 9042
    keyspace: str = "market_data"
    
    # Table names
    bronze_table: str = "trades_bronze"
    silver_table: str = "trades_silver"
    gold_5m_table: str = "trades_gold_5m"
    gold_1h_table: str = "trades_gold_1h"
    
    # Write settings
    consistency_level: str = "LOCAL_QUORUM"
    ttl_bronze: int = 604800      # 7 days
    ttl_silver: int = 2592000     # 30 days
    ttl_gold: int = 7776000       # 90 days


@dataclass
class GrafanaConfig:
    """Grafana configuration."""
    host: str = field(
        default_factory=lambda: os.getenv("GRAFANA_HOST", "localhost")
    )
    port: int = 3000
    admin_user: str = field(
        default_factory=lambda: os.getenv("GRAFANA_ADMIN_USER", "admin")
    )
    admin_password: str = field(
        default_factory=lambda: os.getenv("GRAFANA_ADMIN_PASSWORD", "admin")
    )


@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""
    finnhub: FinnhubConfig = field(default_factory=FinnhubConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    cassandra: CassandraConfig = field(default_factory=CassandraConfig)
    grafana: GrafanaConfig = field(default_factory=GrafanaConfig)
    
    # General settings
    log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO")
    )
    environment: str = field(
        default_factory=lambda: os.getenv("ENVIRONMENT", "development")
    )
    
    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Load configuration from environment variables."""
        return cls()


def get_config() -> PipelineConfig:
    """Get pipeline configuration singleton."""
    return PipelineConfig()


# Export configuration
config = get_config()
