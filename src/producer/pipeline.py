"""
Market Data Pipeline
Orchestrates data flow from Finnhub WebSocket to Kafka.
"""
import asyncio
import logging
import signal
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

from .config import PipelineConfig
from .finnhub_client import FinnhubClient
from .kafka_producer import MarketDataProducer
from .validator import TradeValidator
from ..utils.time_utils import MarketHours

logger = logging.getLogger(__name__)


class MarketDataPipeline:
    """
    Orchestrates the market data ingestion pipeline.
    
    Flow: Finnhub WebSocket -> Validator -> Kafka Producer
    
    Features:
    - Async message processing
    - Market hours awareness
    - Graceful shutdown
    - Metrics collection
    """
    
    def __init__(
        self,
        config: PipelineConfig,
        symbols: Optional[List[str]] = None,
        dry_run: bool = False
    ):
        """
        Initialize pipeline.
        
        Args:
            config: Pipeline configuration
            symbols: List of symbols to subscribe (overrides config)
            dry_run: If True, print messages instead of sending to Kafka
        """
        self.config = config
        self.symbols = symbols or config.finnhub.symbols
        self.dry_run = dry_run
        
        # Components
        self.finnhub_client: Optional[FinnhubClient] = None
        self.kafka_producer: Optional[MarketDataProducer] = None
        self.validator = TradeValidator(
            filter_zero_volume=True,
            dead_letter_callback=self._handle_dead_letter
        )
        
        # State
        self._running = False
        self._message_count = 0
        self._start_time: Optional[datetime] = None
        self._dead_letters: List[Dict[str, Any]] = []
        
    def _initialize_components(self) -> None:
        """Initialize pipeline components."""
        logger.info(f"Initializing pipeline with {len(self.symbols)} symbols")
        
        # Finnhub client
        self.finnhub_client = FinnhubClient(
            api_key=self.config.finnhub.api_key,
            symbols=self.symbols,
            on_message=self._on_trade_received,
            max_retries=self.config.finnhub.max_retries,
            base_backoff=self.config.finnhub.base_backoff
        )
        
        # Kafka producer (skip in dry-run mode)
        if not self.dry_run:
            self.kafka_producer = MarketDataProducer(
                bootstrap_servers=self.config.kafka.bootstrap_servers,
                topic=self.config.kafka.trades_topic
            )
            
        logger.info("Pipeline components initialized")
        
    def _on_trade_received(self, trade: Dict[str, Any]) -> None:
        """
        Callback for received trade messages.
        
        Args:
            trade: Raw trade data from Finnhub
        """
        is_valid, normalized = self.validator.process(trade)
        
        if is_valid and normalized:
            self._message_count += 1
            
            if self.dry_run:
                self._print_trade(normalized)
            else:
                self._send_to_kafka(normalized)
                
    def _print_trade(self, trade: Dict[str, Any]) -> None:
        """Print trade to console (dry-run mode)."""
        symbol = trade.get("symbol", "???")
        price = trade.get("price", 0)
        volume = trade.get("volume", 0)
        print(f"[DRY-RUN] {symbol}: ${price:.2f} x {volume}")
        
    def _send_to_kafka(self, trade: Dict[str, Any]) -> None:
        """Send validated trade to Kafka."""
        if self.kafka_producer:
            self.kafka_producer.send(
                key=trade["symbol"],
                value=trade
            )
            
    def _handle_dead_letter(self, invalid_message: Dict[str, Any]) -> None:
        """Handle invalid messages (dead-letter queue)."""
        self._dead_letters.append(invalid_message)
        
        # Log every 100th dead letter to avoid spam
        if len(self._dead_letters) % 100 == 0:
            logger.warning(f"Dead letter queue size: {len(self._dead_letters)}")
            
    async def run(self) -> None:
        """Run the pipeline."""
        self._initialize_components()
        self._running = True
        self._start_time = datetime.utcnow()
        
        # Setup signal handlers
        self._setup_signal_handlers()
        
        logger.info("Starting market data pipeline...")
        
        try:
            # Check market hours
            if not self.config.finnhub.ignore_market_hours:
                await self._wait_for_market_open()
            
            # Connect to Finnhub
            await self.finnhub_client.connect()
            
        except asyncio.CancelledError:
            logger.info("Pipeline cancelled")
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            raise
        finally:
            await self._cleanup()
            
    async def _wait_for_market_open(self) -> None:
        """Wait for market to open if currently closed."""
        while self._running and not MarketHours.is_market_open():
            next_open = MarketHours.next_market_open()
            wait_seconds = (next_open - MarketHours.now_et()).total_seconds()
            
            if wait_seconds > 0:
                logger.info(
                    f"Market closed. Waiting until {next_open.strftime('%Y-%m-%d %H:%M ET')} "
                    f"({wait_seconds / 3600:.1f} hours)"
                )
                # Wait in 5-minute chunks to allow shutdown
                await asyncio.sleep(min(wait_seconds, 300))
            else:
                break
                
    def _setup_signal_handlers(self) -> None:
        """Setup graceful shutdown on SIGTERM/SIGINT."""
        def handle_signal(sig, frame):
            logger.info(f"Received signal {sig}, shutting down...")
            self._running = False
            if self.finnhub_client:
                asyncio.create_task(self.finnhub_client.disconnect())
                
        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)
        
    async def _cleanup(self) -> None:
        """Cleanup resources on shutdown."""
        logger.info("Cleaning up pipeline...")
        
        # Disconnect Finnhub
        if self.finnhub_client:
            await self.finnhub_client.disconnect()
            
        # Flush and close Kafka producer
        if self.kafka_producer:
            self.kafka_producer.close()
            
        # Print final stats
        self._print_stats()
        
    def _print_stats(self) -> None:
        """Print pipeline statistics."""
        duration = datetime.utcnow() - self._start_time if self._start_time else None
        duration_str = str(duration).split(".")[0] if duration else "N/A"
        
        logger.info("=" * 50)
        logger.info("Pipeline Statistics")
        logger.info("=" * 50)
        logger.info(f"Duration: {duration_str}")
        logger.info(f"Messages processed: {self._message_count}")
        logger.info(f"Dead letters: {len(self._dead_letters)}")
        
        if self.kafka_producer:
            stats = self.kafka_producer.stats
            logger.info(f"Kafka - Sent: {stats['messages_sent']}, Errors: {stats['errors']}")
            
        validation_metrics = self.validator.get_metrics()
        logger.info(f"Validation rate: {validation_metrics['validation_rate']}")
        logger.info("=" * 50)
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get current pipeline metrics."""
        return {
            "running": self._running,
            "messages_processed": self._message_count,
            "dead_letters": len(self._dead_letters),
            "validation": self.validator.get_metrics(),
            "kafka": self.kafka_producer.stats if self.kafka_producer else None,
            "uptime_seconds": (
                (datetime.utcnow() - self._start_time).total_seconds()
                if self._start_time else 0
            )
        }
