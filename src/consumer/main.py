"""
Spark Consumer Entry Point
CLI for running the market data streaming processor.
"""
import argparse
import logging
import os
import signal
import sys
from typing import Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Market Data Spark Streaming Processor",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Kafka settings
    parser.add_argument(
        "--kafka-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--kafka-topic",
        default=os.getenv("KAFKA_TOPIC", "trades_raw"),
        help="Kafka topic to consume"
    )
    parser.add_argument(
        "--starting-offsets",
        choices=["earliest", "latest"],
        default="latest",
        help="Kafka starting offsets"
    )
    
    # Cassandra settings
    parser.add_argument(
        "--cassandra-host",
        default=os.getenv("CASSANDRA_HOST", "localhost"),
        help="Cassandra host"
    )
    parser.add_argument(
        "--cassandra-keyspace",
        default=os.getenv("CASSANDRA_KEYSPACE", "market_data"),
        help="Cassandra keyspace"
    )
    
    # Processing settings
    parser.add_argument(
        "--checkpoint-dir",
        default=os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints"),
        help="Directory for streaming checkpoints"
    )
    parser.add_argument(
        "--trigger-interval",
        default="10 seconds",
        help="Trigger interval for micro-batches"
    )
    parser.add_argument(
        "--watermark-delay",
        default="10 minutes",
        help="Watermark delay for late data"
    )
    
    # Layer toggles
    parser.add_argument(
        "--disable-bronze",
        action="store_true",
        help="Disable Bronze layer output"
    )
    parser.add_argument(
        "--disable-silver",
        action="store_true",
        help="Disable Silver layer output"
    )
    parser.add_argument(
        "--disable-gold",
        action="store_true",
        help="Disable Gold layer output"
    )
    parser.add_argument(
        "--enable-console",
        action="store_true",
        help="Enable console output for debugging"
    )
    
    # General
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level"
    )
    
    return parser.parse_args()


def main() -> int:
    """Main entry point for the Spark consumer."""
    args = parse_args()
    
    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    logger.info("=" * 60)
    logger.info("Market Data Spark Streaming Processor")
    logger.info("=" * 60)
    logger.info(f"Kafka servers: {args.kafka_servers}")
    logger.info(f"Kafka topic: {args.kafka_topic}")
    logger.info(f"Cassandra host: {args.cassandra_host}")
    logger.info(f"Checkpoint dir: {args.checkpoint_dir}")
    logger.info("=" * 60)
    
    # Import here to avoid issues if pyspark is not installed
    try:
        from .spark_processor import MarketDataProcessor, SparkProcessorConfig
    except ImportError as e:
        logger.error(f"Failed to import Spark modules: {e}")
        logger.error("Make sure PySpark is installed: pip install pyspark")
        return 1
    
    # Create configuration
    config = SparkProcessorConfig(
        kafka_bootstrap_servers=args.kafka_servers,
        kafka_topic=args.kafka_topic,
        kafka_starting_offsets=args.starting_offsets,
        cassandra_host=args.cassandra_host,
        cassandra_keyspace=args.cassandra_keyspace,
        checkpoint_location=args.checkpoint_dir,
        trigger_interval=args.trigger_interval,
        watermark_delay=args.watermark_delay,
        enable_bronze=not args.disable_bronze,
        enable_silver=not args.disable_silver,
        enable_gold=not args.disable_gold,
        enable_console=args.enable_console,
    )
    
    # Create processor
    processor: Optional[MarketDataProcessor] = None
    
    def signal_handler(signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down...")
        if processor:
            processor.stop()
        sys.exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Initialize and start processor
        processor = MarketDataProcessor(config)
        processor.read_from_kafka()
        processor.process()
        
        logger.info("Streaming pipeline started successfully")
        logger.info("Press Ctrl+C to stop...")
        
        # Wait for termination
        processor.await_termination()
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.exception(f"Pipeline error: {e}")
        return 1
    finally:
        if processor:
            processor.stop()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
