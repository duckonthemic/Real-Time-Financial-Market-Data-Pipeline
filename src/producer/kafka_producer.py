"""
Kafka Producer for Market Data
Publishes validated trade data to Kafka with Avro serialization.
"""
import json
import logging
from typing import Any, Dict, Optional

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringSerializer
)

logger = logging.getLogger(__name__)


class MarketDataProducer:
    """
    Kafka producer for market data with exactly-once semantics.
    
    Features:
    - Idempotent producer configuration
    - Batch sending for efficiency
    - Partition key selection by symbol
    - Delivery callback for monitoring
    """
    
    DEFAULT_CONFIG = {
        "enable.idempotence": True,
        "acks": "all",
        "retries": 10,
        "retry.backoff.ms": 100,
        "batch.size": 16384,
        "linger.ms": 5,
        "compression.type": "snappy",
        "max.in.flight.requests.per.connection": 5,
    }
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Target topic for messages
            config: Additional producer configuration
        """
        self.topic = topic
        self.key_serializer = StringSerializer("utf_8")
        
        # Merge default config with provided config
        producer_config = {
            "bootstrap.servers": bootstrap_servers,
            **self.DEFAULT_CONFIG,
            **(config or {})
        }
        
        self.producer = Producer(producer_config)
        self._message_count = 0
        self._error_count = 0
        
        logger.info(f"Kafka producer initialized for topic: {topic}")
        
    def send(self, key: str, value: Dict[str, Any]) -> None:
        """
        Send a message to Kafka.
        
        Args:
            key: Message key (typically symbol for partitioning)
            value: Message payload
        """
        try:
            self.producer.produce(
                topic=self.topic,
                key=self.key_serializer(key),
                value=json.dumps(value).encode("utf-8"),
                on_delivery=self._delivery_callback
            )
            self.producer.poll(0)  # Trigger callbacks
            self._message_count += 1
            
        except BufferError:
            logger.warning("Producer buffer full, waiting...")
            self.producer.poll(1.0)
            self.send(key, value)  # Retry
            
        except Exception as e:
            logger.error(f"Failed to produce message: {e}")
            self._error_count += 1
            
    def _delivery_callback(self, err, msg) -> None:
        """Callback for message delivery confirmation."""
        if err:
            logger.error(f"Delivery failed: {err}")
            self._error_count += 1
        else:
            logger.debug(
                f"Delivered to {msg.topic()}[{msg.partition()}] @ {msg.offset()}"
            )
            
    def flush(self, timeout: float = 10.0) -> int:
        """
        Flush pending messages.
        
        Args:
            timeout: Maximum wait time in seconds
            
        Returns:
            Number of messages still in queue
        """
        return self.producer.flush(timeout)
    
    def close(self) -> None:
        """Close producer and flush remaining messages."""
        remaining = self.flush()
        if remaining > 0:
            logger.warning(f"{remaining} messages not delivered")
        logger.info(
            f"Producer closed. Sent: {self._message_count}, Errors: {self._error_count}"
        )
        
    @property
    def stats(self) -> Dict[str, int]:
        """Get producer statistics."""
        return {
            "messages_sent": self._message_count,
            "errors": self._error_count
        }


# Example usage
if __name__ == "__main__":
    producer = MarketDataProducer(
        bootstrap_servers="localhost:9092",
        topic="trades_raw"
    )
    
    # Send sample trade
    producer.send(
        key="AAPL",
        value={
            "symbol": "AAPL",
            "price": 150.25,
            "volume": 100,
            "timestamp": 1706684400000
        }
    )
    
    producer.close()
