"""
Pytest Configuration and Shared Fixtures
"""
import asyncio
import json
from datetime import datetime
from typing import Any, Dict, Generator, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# =============================================================================
# Async Support
# =============================================================================

@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# =============================================================================
# Sample Data Fixtures
# =============================================================================

@pytest.fixture
def sample_trade() -> Dict[str, Any]:
    """Sample valid trade from Finnhub."""
    return {
        "s": "AAPL",
        "p": 150.25,
        "v": 100,
        "t": int(datetime.utcnow().timestamp() * 1000),
        "c": ["1", "12"],
        "received_at": datetime.utcnow().isoformat()
    }


@pytest.fixture
def sample_trades() -> List[Dict[str, Any]]:
    """List of sample trades."""
    base_time = int(datetime.utcnow().timestamp() * 1000)
    return [
        {"s": "AAPL", "p": 150.25, "v": 100, "t": base_time},
        {"s": "GOOGL", "p": 140.50, "v": 50, "t": base_time + 1000},
        {"s": "MSFT", "p": 380.00, "v": 200, "t": base_time + 2000},
    ]


@pytest.fixture
def invalid_trade_zero_volume() -> Dict[str, Any]:
    """Trade with zero volume (should be filtered)."""
    return {
        "s": "AAPL",
        "p": 150.25,
        "v": 0,
        "t": int(datetime.utcnow().timestamp() * 1000),
    }


@pytest.fixture
def invalid_trade_negative_price() -> Dict[str, Any]:
    """Trade with negative price (invalid)."""
    return {
        "s": "AAPL",
        "p": -10.00,
        "v": 100,
        "t": int(datetime.utcnow().timestamp() * 1000),
    }


@pytest.fixture
def invalid_trade_missing_fields() -> Dict[str, Any]:
    """Trade missing required fields."""
    return {
        "s": "AAPL",
        # Missing: p, v, t
    }


@pytest.fixture
def invalid_trade_future_timestamp() -> Dict[str, Any]:
    """Trade with future timestamp (clock skew)."""
    future_time = int((datetime.utcnow().timestamp() + 21600) * 1000)  # 6 hours ahead
    return {
        "s": "AAPL",
        "p": 150.25,
        "v": 100,
        "t": future_time,
    }


# =============================================================================
# Mock Fixtures
# =============================================================================

@pytest.fixture
def mock_websocket():
    """Mock WebSocket connection."""
    ws = AsyncMock()
    ws.send = AsyncMock()
    ws.recv = AsyncMock()
    ws.close = AsyncMock()
    return ws


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka Producer."""
    with patch("src.producer.kafka_producer.Producer") as mock:
        producer = MagicMock()
        producer.produce = MagicMock()
        producer.poll = MagicMock(return_value=0)
        producer.flush = MagicMock(return_value=0)
        mock.return_value = producer
        yield producer


@pytest.fixture
def mock_finnhub_messages() -> List[str]:
    """Mock Finnhub WebSocket messages."""
    base_time = int(datetime.utcnow().timestamp() * 1000)
    return [
        json.dumps({
            "type": "trade",
            "data": [
                {"s": "AAPL", "p": 150.25, "v": 100, "t": base_time, "c": ["1"]},
                {"s": "AAPL", "p": 150.30, "v": 50, "t": base_time + 100, "c": ["1"]},
            ]
        }),
        json.dumps({"type": "ping"}),
        json.dumps({
            "type": "trade",
            "data": [
                {"s": "GOOGL", "p": 140.50, "v": 200, "t": base_time + 200, "c": ["1"]},
            ]
        }),
    ]


# =============================================================================
# Configuration Fixtures
# =============================================================================

@pytest.fixture
def test_config():
    """Test configuration with mocked values."""
    from src.producer.config import PipelineConfig, FinnhubConfig, KafkaConfig
    
    return PipelineConfig(
        finnhub=FinnhubConfig(
            api_key="test_api_key",
            symbols=["AAPL", "GOOGL"],
            max_retries=3,
            base_backoff=1.0,
            ignore_market_hours=True
        ),
        kafka=KafkaConfig(
            bootstrap_servers="localhost:9092",
            trades_topic="test_trades"
        )
    )


# =============================================================================
# Environment Fixtures
# =============================================================================

@pytest.fixture
def env_vars(monkeypatch):
    """Set test environment variables."""
    monkeypatch.setenv("FINNHUB_API_KEY", "test_key")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("ENVIRONMENT", "test")
