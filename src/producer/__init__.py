# src/producer module
"""Producer module for market data ingestion."""

from .config import PipelineConfig, get_config
from .finnhub_client import FinnhubClient
from .kafka_producer import MarketDataProducer
from .validator import TradeValidator
from .pipeline import MarketDataPipeline

__all__ = [
    "PipelineConfig",
    "get_config",
    "FinnhubClient",
    "MarketDataProducer",
    "TradeValidator",
    "MarketDataPipeline",
]
