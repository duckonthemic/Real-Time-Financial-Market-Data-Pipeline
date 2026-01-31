#!/usr/bin/env python3
"""
Cassandra Schema Initialization Script
Creates keyspace and tables for the market data pipeline.
"""
import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_cassandra_session(
    hosts: List[str],
    port: int = 9042,
    max_retries: int = 5,
    retry_delay: int = 5
):
    """
    Create a Cassandra session with retry logic.
    
    Args:
        hosts: List of Cassandra host addresses
        port: Cassandra native port
        max_retries: Maximum connection attempts
        retry_delay: Delay between retries in seconds
        
    Returns:
        Cassandra session object
    """
    try:
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
    except ImportError:
        logger.error("cassandra-driver not installed. Run: pip install cassandra-driver")
        sys.exit(1)
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connecting to Cassandra (attempt {attempt}/{max_retries})...")
            cluster = Cluster(hosts, port=port)
            session = cluster.connect()
            logger.info("Connected to Cassandra successfully!")
            return session, cluster
        except Exception as e:
            logger.warning(f"Connection failed: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached. Exiting.")
                raise


def load_schema_file(schema_path: Path) -> str:
    """Load CQL schema from file."""
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    
    return schema_path.read_text(encoding="utf-8")


def execute_schema(session, schema: str, ignore_errors: bool = True) -> None:
    """
    Execute CQL statements from schema string.
    
    Args:
        session: Cassandra session
        schema: CQL schema string with multiple statements
        ignore_errors: Continue on errors (e.g., IF NOT EXISTS)
    """
    # Split by semicolon and filter empty statements
    statements = [
        stmt.strip() 
        for stmt in schema.split(";") 
        if stmt.strip() and not stmt.strip().startswith("--")
    ]
    
    for i, stmt in enumerate(statements, 1):
        # Skip comments
        if stmt.strip().startswith("--") or stmt.strip().startswith("/*"):
            continue
            
        # Clean up statement
        clean_stmt = "\n".join(
            line for line in stmt.split("\n")
            if not line.strip().startswith("--")
        ).strip()
        
        if not clean_stmt:
            continue
            
        try:
            logger.info(f"Executing statement {i}/{len(statements)}...")
            logger.debug(f"Statement: {clean_stmt[:100]}...")
            session.execute(clean_stmt)
            logger.info(f"Statement {i} executed successfully")
        except Exception as e:
            if ignore_errors:
                logger.warning(f"Statement {i} warning: {e}")
            else:
                raise


def init_cassandra(
    hosts: Optional[List[str]] = None,
    schema_path: Optional[Path] = None,
    drop_existing: bool = False
) -> None:
    """
    Initialize Cassandra schema for the market data pipeline.
    
    Args:
        hosts: Cassandra hosts (default from env)
        schema_path: Path to CQL schema file
        drop_existing: Drop existing keyspace before creating
    """
    # Default values
    hosts = hosts or [os.getenv("CASSANDRA_HOST", "localhost")]
    
    if schema_path is None:
        # Find schema file relative to this script
        project_root = Path(__file__).parent.parent
        schema_path = project_root / "schemas" / "cassandra" / "keyspace.cql"
    
    logger.info(f"Cassandra hosts: {hosts}")
    logger.info(f"Schema file: {schema_path}")
    
    # Connect to Cassandra
    session, cluster = get_cassandra_session(hosts)
    
    try:
        # Optionally drop existing keyspace
        if drop_existing:
            logger.warning("Dropping existing keyspace 'market_data'...")
            session.execute("DROP KEYSPACE IF EXISTS market_data")
            logger.info("Keyspace dropped")
        
        # Load and execute schema
        logger.info("Loading schema file...")
        schema = load_schema_file(schema_path)
        
        logger.info("Executing schema statements...")
        execute_schema(session, schema)
        
        logger.info("=" * 50)
        logger.info("Cassandra initialization complete!")
        logger.info("=" * 50)
        
        # Verify tables
        session.execute("USE market_data")
        result = session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'market_data'"
        )
        tables = [row.table_name for row in result]
        logger.info(f"Created tables: {', '.join(tables)}")
        
    finally:
        cluster.shutdown()


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Initialize Cassandra schema for market data pipeline"
    )
    parser.add_argument(
        "--host",
        default=os.getenv("CASSANDRA_HOST", "localhost"),
        help="Cassandra host address"
    )
    parser.add_argument(
        "--schema",
        type=Path,
        help="Path to CQL schema file (default: schemas/cassandra/keyspace.cql)"
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop existing keyspace before creating (DANGER!)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging"
    )
    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        init_cassandra(
            hosts=[args.host],
            schema_path=args.schema,
            drop_existing=args.drop
        )
        return 0
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
