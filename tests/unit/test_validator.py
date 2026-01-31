"""
Unit Tests for TradeValidator
"""
import pytest
from datetime import datetime, timedelta

from src.producer.validator import TradeValidator, ValidationResult, ValidationMetrics


class TestTradeValidator:
    """Tests for TradeValidator class."""
    
    @pytest.fixture
    def validator(self):
        """Create a fresh validator instance."""
        return TradeValidator()
    
    # =========================================================================
    # Valid Trade Tests
    # =========================================================================
    
    def test_validate_valid_trade(self, validator, sample_trade):
        """Test validation of a valid trade."""
        result = validator.validate(sample_trade)
        
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_process_valid_trade(self, validator, sample_trade):
        """Test processing returns normalized trade."""
        is_valid, normalized = validator.process(sample_trade)
        
        assert is_valid is True
        assert normalized is not None
        assert normalized["symbol"] == "AAPL"
        assert normalized["price"] == 150.25
        assert normalized["volume"] == 100
        assert "ingestion_time" in normalized
    
    # =========================================================================
    # Invalid Trade Tests - Zero Volume
    # =========================================================================
    
    def test_validate_zero_volume(self, validator, invalid_trade_zero_volume):
        """Test that zero volume trades are rejected."""
        result = validator.validate(invalid_trade_zero_volume)
        
        assert result.is_valid is False
        assert any("volume" in err.lower() for err in result.errors)
    
    def test_process_zero_volume(self, validator, invalid_trade_zero_volume):
        """Test processing rejects zero volume trades."""
        is_valid, normalized = validator.process(invalid_trade_zero_volume)
        
        assert is_valid is False
        assert normalized is None
    
    def test_zero_volume_filter_disabled(self, invalid_trade_zero_volume):
        """Test zero volume filter can be disabled."""
        validator = TradeValidator(filter_zero_volume=False)
        result = validator.validate(invalid_trade_zero_volume)
        
        # Should now be valid (volume=0 is allowed)
        assert result.is_valid is True
    
    # =========================================================================
    # Invalid Trade Tests - Negative Price
    # =========================================================================
    
    def test_validate_negative_price(self, validator, invalid_trade_negative_price):
        """Test that negative prices are rejected."""
        result = validator.validate(invalid_trade_negative_price)
        
        assert result.is_valid is False
        assert any("price" in err.lower() for err in result.errors)
    
    # =========================================================================
    # Invalid Trade Tests - Missing Fields
    # =========================================================================
    
    def test_validate_missing_fields(self, validator, invalid_trade_missing_fields):
        """Test that missing required fields are caught."""
        result = validator.validate(invalid_trade_missing_fields)
        
        assert result.is_valid is False
        assert any("missing" in err.lower() for err in result.errors)
    
    # =========================================================================
    # Invalid Trade Tests - Future Timestamp
    # =========================================================================
    
    def test_validate_future_timestamp(self, validator):
        """Test that future timestamps are rejected."""
        import time
        # Create a timestamp 1 hour in the future - well beyond 5-minute tolerance
        future_ms = int((time.time() + 3600) * 1000)
        trade = {
            "s": "AAPL",
            "p": 150.25,
            "v": 100,
            "t": future_ms,
        }
        
        result = validator.validate(trade)
        
        assert result.is_valid is False
        assert any("future" in err.lower() for err in result.errors)
    
    # =========================================================================
    # Metrics Tests
    # =========================================================================
    
    def test_metrics_tracking(self, validator, sample_trade, invalid_trade_zero_volume):
        """Test that metrics are tracked correctly."""
        # Process one valid, one invalid
        validator.process(sample_trade)
        validator.process(invalid_trade_zero_volume)
        
        metrics = validator.get_metrics()
        
        assert metrics["total_received"] == 2
        assert metrics["total_valid"] == 1
        assert metrics["total_invalid"] == 1
        assert metrics["validation_rate"] == "50.00%"
    
    def test_metrics_reset(self, validator, sample_trade):
        """Test metrics can be reset."""
        validator.process(sample_trade)
        validator.reset_metrics()
        
        metrics = validator.get_metrics()
        assert metrics["total_received"] == 0
    
    # =========================================================================
    # Dead Letter Queue Tests
    # =========================================================================
    
    def test_dead_letter_callback(self, invalid_trade_zero_volume):
        """Test dead letter callback is called for invalid trades."""
        dead_letters = []
        
        def callback(msg):
            dead_letters.append(msg)
        
        validator = TradeValidator(dead_letter_callback=callback)
        validator.process(invalid_trade_zero_volume)
        
        assert len(dead_letters) == 1
        assert "errors" in dead_letters[0]
        assert "original" in dead_letters[0]
    
    # =========================================================================
    # Normalization Tests
    # =========================================================================
    
    def test_normalize_timestamp_milliseconds(self, validator):
        """Test timestamp normalization to milliseconds."""
        # Trade with seconds timestamp
        trade = {
            "s": "AAPL",
            "p": 150.0,
            "v": 100,
            "t": int(datetime.utcnow().timestamp())  # seconds
        }
        
        is_valid, normalized = validator.process(trade)
        
        assert is_valid
        # Should be converted to milliseconds
        assert normalized["timestamp"] > 1e12
    
    def test_normalize_adds_ingestion_time(self, validator, sample_trade):
        """Test that ingestion time is added."""
        _, normalized = validator.process(sample_trade)
        
        assert "ingestion_time" in normalized
        assert isinstance(normalized["ingestion_time"], int)


class TestValidationMetrics:
    """Tests for ValidationMetrics dataclass."""
    
    def test_validation_rate_empty(self):
        """Test validation rate when no messages received."""
        metrics = ValidationMetrics()
        assert metrics.validation_rate == 100.0
    
    def test_validation_rate_calculation(self):
        """Test validation rate calculation."""
        metrics = ValidationMetrics(total_received=10, total_valid=8, total_invalid=2)
        assert metrics.validation_rate == 80.0
    
    def test_to_dict(self):
        """Test metrics serialization."""
        metrics = ValidationMetrics(total_received=5, total_valid=4, total_invalid=1)
        result = metrics.to_dict()
        
        assert result["total_received"] == 5
        assert result["total_valid"] == 4
        assert "breakdown" in result
