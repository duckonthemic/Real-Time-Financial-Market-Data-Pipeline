"""
Integration Tests for Market Data Pipeline
Requires Docker services to be running.
"""
import asyncio
import json
import os
import time
from datetime import datetime
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


pytestmark = pytest.mark.integration


class TestPipelineIntegration:
    """Integration tests for the full pipeline."""
    
    @pytest.fixture
    def pipeline(self, test_config):
        """Create pipeline instance."""
        from src.producer.pipeline import MarketDataPipeline
        
        return MarketDataPipeline(
            config=test_config,
            symbols=["AAPL"],
            dry_run=True  # Don't actually connect to Kafka
        )
    
    # =========================================================================
    # Component Initialization Tests
    # =========================================================================
    
    def test_pipeline_initialization(self, pipeline):
        """Test pipeline initializes correctly."""
        assert pipeline.symbols == ["AAPL"]
        assert pipeline.dry_run is True
        assert pipeline._running is False
    
    def test_pipeline_components_created(self, pipeline):
        """Test components are created on initialize."""
        pipeline._initialize_components()
        
        assert pipeline.finnhub_client is not None
        assert pipeline.validator is not None
        # Kafka producer should be None in dry-run mode
        assert pipeline.kafka_producer is None
    
    # =========================================================================
    # Message Processing Tests
    # =========================================================================
    
    def test_on_trade_received_valid(self, pipeline):
        """Test processing valid trade message."""
        pipeline._initialize_components()
        
        trade = {
            "s": "AAPL",
            "p": 150.25,
            "v": 100,
            "t": int(datetime.utcnow().timestamp() * 1000)
        }
        
        initial_count = pipeline._message_count
        pipeline._on_trade_received(trade)
        
        assert pipeline._message_count == initial_count + 1
    
    def test_on_trade_received_invalid(self, pipeline):
        """Test processing invalid trade message."""
        pipeline._initialize_components()
        
        trade = {
            "s": "AAPL",
            "p": 150.25,
            "v": 0,  # Zero volume - should be filtered
            "t": int(datetime.utcnow().timestamp() * 1000)
        }
        
        initial_count = pipeline._message_count
        pipeline._on_trade_received(trade)
        
        # Should not increment for invalid trade
        assert pipeline._message_count == initial_count
    
    # =========================================================================
    # Dead Letter Queue Tests
    # =========================================================================
    
    def test_dead_letter_queue_populated(self, pipeline):
        """Test invalid messages go to dead letter queue."""
        pipeline._initialize_components()
        
        invalid_trade = {
            "s": "AAPL",
            "p": -10.0,  # Negative price
            "v": 100,
            "t": int(datetime.utcnow().timestamp() * 1000)
        }
        
        initial_dead = len(pipeline._dead_letters)
        pipeline._on_trade_received(invalid_trade)
        
        assert len(pipeline._dead_letters) == initial_dead + 1
    
    # =========================================================================
    # Metrics Tests
    # =========================================================================
    
    def test_get_metrics(self, pipeline):
        """Test metrics collection."""
        pipeline._initialize_components()
        pipeline._start_time = datetime.utcnow()
        pipeline._message_count = 5
        
        metrics = pipeline.get_metrics()
        
        assert metrics["messages_processed"] == 5
        assert "validation" in metrics
        assert "uptime_seconds" in metrics


@pytest.mark.integration
class TestKafkaIntegration:
    """Integration tests requiring Kafka (skipped if not available)."""
    
    @pytest.fixture
    def kafka_available(self):
        """Check if Kafka is available."""
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', 9092))
            sock.close()
            return result == 0
        except Exception:
            return False
    
    def test_producer_connection(self, kafka_available):
        """Test producer can connect to Kafka."""
        if not kafka_available:
            pytest.skip("Kafka not available")
        
        from src.producer.kafka_producer import MarketDataProducer
        
        producer = MarketDataProducer(
            bootstrap_servers="localhost:9092",
            topic="test_trades"
        )
        
        # Send a test message
        producer.send("TEST", {"test": True, "timestamp": time.time()})
        remaining = producer.flush(timeout=5.0)
        
        assert remaining == 0
        producer.close()


@pytest.mark.integration
class TestSchemaRegistryIntegration:
    """Integration tests requiring Schema Registry."""
    
    @pytest.fixture
    def registry_available(self):
        """Check if Schema Registry is available."""
        try:
            import requests
            response = requests.get("http://localhost:8081/subjects", timeout=2)
            return response.status_code == 200
        except Exception:
            return False
    
    def test_schema_registration(self, registry_available):
        """Test schema can be registered."""
        if not registry_available:
            pytest.skip("Schema Registry not available")
        
        import requests
        
        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [{"name": "test", "type": "string"}]
        }
        
        response = requests.post(
            "http://localhost:8081/subjects/test-value/versions",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            json={"schema": json.dumps(schema)}
        )
        
        assert response.status_code in (200, 201, 409)  # 409 = already exists
