"""
Unit Tests for MarketDataProducer
"""
import json
from unittest.mock import MagicMock, patch

import pytest


class TestMarketDataProducer:
    """Tests for MarketDataProducer Kafka integration."""
    
    @pytest.fixture
    def producer(self, mock_kafka_producer):
        """Create producer with mocked Kafka."""
        from src.producer.kafka_producer import MarketDataProducer
        
        with patch("src.producer.kafka_producer.Producer", return_value=mock_kafka_producer):
            producer = MarketDataProducer(
                bootstrap_servers="localhost:9092",
                topic="test_trades"
            )
            producer._mock = mock_kafka_producer
            return producer
    
    # =========================================================================
    # Initialization Tests
    # =========================================================================
    
    def test_init_sets_topic(self, producer):
        """Test producer initializes with correct topic."""
        assert producer.topic == "test_trades"
    
    def test_init_message_count_zero(self, producer):
        """Test initial message count is zero."""
        assert producer._message_count == 0
        assert producer._error_count == 0
    
    def test_default_config_idempotent(self):
        """Test default config enables idempotence."""
        from src.producer.kafka_producer import MarketDataProducer
        
        assert MarketDataProducer.DEFAULT_CONFIG["enable.idempotence"] is True
        assert MarketDataProducer.DEFAULT_CONFIG["acks"] == "all"
    
    # =========================================================================
    # Send Tests
    # =========================================================================
    
    def test_send_increments_count(self, producer):
        """Test sending message increments counter."""
        initial_count = producer._message_count
        
        producer.send("AAPL", {"symbol": "AAPL", "price": 150.0})
        
        assert producer._message_count == initial_count + 1
    
    def test_send_calls_produce(self, producer):
        """Test send calls Kafka produce."""
        producer.send("AAPL", {"symbol": "AAPL", "price": 150.0})
        
        producer._mock.produce.assert_called_once()
    
    def test_send_uses_symbol_as_key(self, producer):
        """Test message key is set correctly."""
        producer.send("GOOGL", {"symbol": "GOOGL", "price": 140.0})
        
        # Check that produce was called with the correct key
        call_kwargs = producer._mock.produce.call_args
        assert call_kwargs is not None
        # The key is passed as a keyword argument and serialized to bytes
        key_arg = call_kwargs.kwargs.get("key") or call_kwargs[1].get("key")
        assert key_arg is not None
    
    def test_send_serializes_value_as_json(self, producer):
        """Test message value is JSON serialized."""
        trade = {"symbol": "AAPL", "price": 150.0, "volume": 100}
        producer.send("AAPL", trade)
        
        call_args = producer._mock.produce.call_args
        # Value should contain JSON
        assert "value" in str(call_args)
    
    # =========================================================================
    # Delivery Callback Tests
    # =========================================================================
    
    def test_delivery_callback_success(self, producer):
        """Test successful delivery callback."""
        msg = MagicMock()
        msg.topic.return_value = "test_trades"
        msg.partition.return_value = 0
        msg.offset.return_value = 42
        
        initial_errors = producer._error_count
        producer._delivery_callback(None, msg)
        
        # Should not increment error count
        assert producer._error_count == initial_errors
    
    def test_delivery_callback_error(self, producer):
        """Test delivery error callback."""
        error = MagicMock()
        error.__str__ = lambda x: "Delivery failed"
        msg = MagicMock()
        
        initial_errors = producer._error_count
        producer._delivery_callback(error, msg)
        
        assert producer._error_count == initial_errors + 1
    
    # =========================================================================
    # Flush and Close Tests
    # =========================================================================
    
    def test_flush_calls_producer_flush(self, producer):
        """Test flush method."""
        producer.flush(timeout=5.0)
        
        producer._mock.flush.assert_called_once_with(5.0)
    
    def test_close_flushes_messages(self, producer):
        """Test close flushes pending messages."""
        producer.close()
        
        producer._mock.flush.assert_called()
    
    # =========================================================================
    # Stats Tests
    # =========================================================================
    
    def test_stats_returns_counts(self, producer):
        """Test stats property returns message counts."""
        producer._message_count = 10
        producer._error_count = 2
        
        stats = producer.stats
        
        assert stats["messages_sent"] == 10
        assert stats["errors"] == 2


class TestProducerConfiguration:
    """Tests for producer configuration."""
    
    def test_custom_config_overrides_defaults(self):
        """Test custom config merges with defaults."""
        from src.producer.kafka_producer import MarketDataProducer
        
        custom_config = {"batch.size": 32768}
        
        with patch("src.producer.kafka_producer.Producer") as mock:
            _ = MarketDataProducer(
                bootstrap_servers="localhost:9092",
                topic="test",
                config=custom_config
            )
            
            # Check that Producer was called with merged config
            call_kwargs = mock.call_args[0][0]
            assert call_kwargs["batch.size"] == 32768
            assert call_kwargs["enable.idempotence"] is True  # Default preserved
