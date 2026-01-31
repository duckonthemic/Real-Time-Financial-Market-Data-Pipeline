"""
Market Data Pipeline - Main Entry Point
CLI interface for running the Finnhub to Kafka pipeline.
"""
import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.producer.config import PipelineConfig
from src.producer.pipeline import MarketDataPipeline


def setup_logging(level: str = "INFO") -> None:
    """Configure logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )
    
    # Reduce noise from libraries
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Finnhub Market Data Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated list of symbols (e.g., AAPL,MSFT,GOOGL)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print messages to console instead of Kafka"
    )
    
    parser.add_argument(
        "--ignore-market-hours",
        action="store_true",
        help="Run even when market is closed"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    
    parser.add_argument(
        "--kafka-servers",
        type=str,
        default=None,
        help="Kafka bootstrap servers (overrides env/config)"
    )
    
    parser.add_argument(
        "--topic",
        type=str,
        default="trades_raw",
        help="Kafka topic for trades"
    )
    
    return parser.parse_args()


def validate_environment() -> bool:
    """Validate required environment variables."""
    api_key = os.getenv("FINNHUB_API_KEY")
    
    if not api_key:
        print("ERROR: FINNHUB_API_KEY environment variable is required")
        print("Set it with: set FINNHUB_API_KEY=your_key_here (Windows)")
        print("         or: export FINNHUB_API_KEY=your_key_here (Linux/Mac)")
        return False
        
    if api_key == "demo" or len(api_key) < 10:
        print("WARNING: Using demo/invalid API key. Limited functionality.")
        
    return True


def main() -> int:
    """Main entry point."""
    args = parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    # Validate environment
    if not args.dry_run and not validate_environment():
        return 1
    
    # Parse symbols
    symbols = None
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]
        
    # Load configuration
    try:
        config = PipelineConfig.from_env()
        
        # Override with CLI args
        if args.kafka_servers:
            config.kafka.bootstrap_servers = args.kafka_servers
        if args.topic:
            config.kafka.trades_topic = args.topic
        if args.ignore_market_hours:
            config.finnhub.ignore_market_hours = True
            
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return 1
    
    # Create and run pipeline
    pipeline = MarketDataPipeline(
        config=config,
        symbols=symbols,
        dry_run=args.dry_run
    )
    
    logger.info("=" * 50)
    logger.info("Finnhub Market Data Pipeline")
    logger.info("=" * 50)
    logger.info(f"Mode: {'DRY-RUN' if args.dry_run else 'PRODUCTION'}")
    logger.info(f"Symbols: {symbols or 'default from config'}")
    logger.info(f"Kafka: {config.kafka.bootstrap_servers}")
    logger.info(f"Topic: {config.kafka.trades_topic}")
    logger.info("=" * 50)
    
    try:
        asyncio.run(pipeline.run())
        return 0
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
