"""
Trade Data Validator
Validates and filters incoming market data before Kafka ingestion.
"""
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a validation check."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class ValidationMetrics:
    """Tracks validation statistics."""
    total_received: int = 0
    total_valid: int = 0
    total_invalid: int = 0
    volume_zero_count: int = 0
    negative_price_count: int = 0
    future_timestamp_count: int = 0
    missing_field_count: int = 0
    
    def record_valid(self) -> None:
        self.total_received += 1
        self.total_valid += 1
        
    def record_invalid(self, reason: str) -> None:
        self.total_received += 1
        self.total_invalid += 1
        
        if "volume" in reason.lower():
            self.volume_zero_count += 1
        elif "price" in reason.lower():
            self.negative_price_count += 1
        elif "timestamp" in reason.lower():
            self.future_timestamp_count += 1
        elif "missing" in reason.lower():
            self.missing_field_count += 1
            
    @property
    def validation_rate(self) -> float:
        """Percentage of valid messages."""
        if self.total_received == 0:
            return 100.0
        return (self.total_valid / self.total_received) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_received": self.total_received,
            "total_valid": self.total_valid,
            "total_invalid": self.total_invalid,
            "validation_rate": f"{self.validation_rate:.2f}%",
            "breakdown": {
                "volume_zero": self.volume_zero_count,
                "negative_price": self.negative_price_count,
                "future_timestamp": self.future_timestamp_count,
                "missing_field": self.missing_field_count,
            }
        }


class TradeValidator:
    """
    Validates trade messages from Finnhub.
    
    Filters out:
    - Trades with volume=0 (known Finnhub issue)
    - Negative prices
    - Future timestamps (clock skew > 5 minutes)
    - Missing required fields
    """
    
    REQUIRED_FIELDS = {"s", "p", "v", "t"}  # symbol, price, volume, timestamp
    MAX_FUTURE_SECONDS = 300  # 5 minutes tolerance for clock skew
    
    def __init__(
        self,
        filter_zero_volume: bool = True,
        max_price: float = 1_000_000.0,
        min_price: float = 0.0001,
        dead_letter_callback: Optional[callable] = None
    ):
        """
        Initialize validator.
        
        Args:
            filter_zero_volume: Whether to filter volume=0 trades
            max_price: Maximum acceptable price
            min_price: Minimum acceptable price
            dead_letter_callback: Function to call with invalid messages
        """
        self.filter_zero_volume = filter_zero_volume
        self.max_price = max_price
        self.min_price = min_price
        self.dead_letter_callback = dead_letter_callback
        self.metrics = ValidationMetrics()
        
    def validate(self, trade: Dict[str, Any]) -> ValidationResult:
        """
        Validate a single trade message.
        
        Args:
            trade: Raw trade data from Finnhub
            
        Returns:
            ValidationResult with is_valid flag and any errors/warnings
        """
        errors = []
        warnings = []
        
        # Check required fields
        missing = self.REQUIRED_FIELDS - set(trade.keys())
        if missing:
            errors.append(f"Missing required fields: {missing}")
            
        # Validate symbol
        symbol = trade.get("s")
        if symbol and not isinstance(symbol, str):
            errors.append(f"Invalid symbol type: {type(symbol)}")
        elif symbol and len(symbol) > 10:
            warnings.append(f"Unusually long symbol: {symbol}")
            
        # Validate price
        price = trade.get("p")
        if price is not None:
            if not isinstance(price, (int, float)):
                errors.append(f"Invalid price type: {type(price)}")
            elif price < 0:
                errors.append(f"Negative price: {price}")
            elif price < self.min_price:
                warnings.append(f"Price below minimum: {price}")
            elif price > self.max_price:
                errors.append(f"Price exceeds maximum: {price}")
                
        # Validate volume
        volume = trade.get("v")
        if volume is not None:
            if not isinstance(volume, (int, float)):
                errors.append(f"Invalid volume type: {type(volume)}")
            elif volume < 0:
                errors.append(f"Negative volume: {volume}")
            elif volume == 0 and self.filter_zero_volume:
                errors.append("Zero volume trade filtered")
                
        # Validate timestamp
        timestamp = trade.get("t")
        if timestamp is not None:
            if not isinstance(timestamp, (int, float)):
                errors.append(f"Invalid timestamp type: {type(timestamp)}")
            else:
                # Finnhub sends milliseconds, normalize to seconds
                ts_seconds = timestamp / 1000 if timestamp > 1e12 else timestamp
                now_seconds = time.time()
                
                # Check if timestamp is too far in the future (> 5 minutes)
                if ts_seconds > now_seconds + self.MAX_FUTURE_SECONDS:
                    errors.append(f"Future timestamp: {ts_seconds} > {now_seconds + self.MAX_FUTURE_SECONDS}")
                # Check if timestamp is stale (> 1 day old)
                elif ts_seconds < now_seconds - 86400:
                    warnings.append(f"Stale timestamp (>1 day old): {ts_seconds}")
                    
        is_valid = len(errors) == 0
        return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)
    
    def process(self, trade: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Validate and process a trade message.
        
        Args:
            trade: Raw trade data
            
        Returns:
            Tuple of (is_valid, normalized_trade or None)
        """
        result = self.validate(trade)
        
        if result.is_valid:
            self.metrics.record_valid()
            normalized = self._normalize(trade)
            
            if result.warnings:
                logger.warning(f"Trade {trade.get('s')}: {result.warnings}")
                
            return True, normalized
        else:
            self.metrics.record_invalid(result.errors[0] if result.errors else "unknown")
            logger.debug(f"Invalid trade rejected: {result.errors}")
            
            if self.dead_letter_callback:
                self.dead_letter_callback({
                    "original": trade,
                    "errors": result.errors,
                    "timestamp": datetime.utcnow().isoformat()
                })
                
            return False, None
    
    def _normalize(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize trade data to consistent format.
        
        Args:
            trade: Validated trade data
            
        Returns:
            Normalized trade dictionary
        """
        timestamp = trade.get("t", 0)
        # Ensure milliseconds
        if timestamp < 1e12:
            timestamp = int(timestamp * 1000)
            
        return {
            "symbol": trade.get("s"),
            "price": float(trade.get("p", 0)),
            "volume": int(trade.get("v", 0)),
            "timestamp": timestamp,
            "conditions": trade.get("c", []),
            "ingestion_time": int(datetime.utcnow().timestamp() * 1000),
            "received_at": trade.get("received_at", datetime.utcnow().isoformat())
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current validation metrics."""
        return self.metrics.to_dict()
    
    def reset_metrics(self) -> None:
        """Reset validation metrics."""
        self.metrics = ValidationMetrics()
