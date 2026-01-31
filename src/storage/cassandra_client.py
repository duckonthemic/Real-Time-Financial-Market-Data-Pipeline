"""
Cassandra Client
High-level client for interacting with Cassandra.
"""
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class CassandraConfig:
    """Cassandra connection configuration."""
    hosts: List[str] = field(default_factory=lambda: ["localhost"])
    port: int = 9042
    keyspace: str = "market_data"
    username: Optional[str] = None
    password: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> "CassandraConfig":
        """Load configuration from environment variables."""
        hosts = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
        return cls(
            hosts=hosts,
            port=int(os.getenv("CASSANDRA_PORT", "9042")),
            keyspace=os.getenv("CASSANDRA_KEYSPACE", "market_data"),
            username=os.getenv("CASSANDRA_USERNAME"),
            password=os.getenv("CASSANDRA_PASSWORD"),
        )


class CassandraClient:
    """
    High-level Cassandra client for market data operations.
    
    Provides prepared statements for efficient batch operations.
    """
    
    def __init__(self, config: Optional[CassandraConfig] = None):
        self.config = config or CassandraConfig.from_env()
        self._cluster = None
        self._session = None
        self._prepared_stmts: Dict[str, Any] = {}
        
    def connect(self) -> None:
        """Establish connection to Cassandra cluster."""
        try:
            from cassandra.cluster import Cluster
            from cassandra.auth import PlainTextAuthProvider
        except ImportError:
            raise ImportError("cassandra-driver not installed. Run: pip install cassandra-driver")
        
        logger.info(f"Connecting to Cassandra: {self.config.hosts}")
        
        auth_provider = None
        if self.config.username and self.config.password:
            auth_provider = PlainTextAuthProvider(
                username=self.config.username,
                password=self.config.password
            )
        
        self._cluster = Cluster(
            self.config.hosts,
            port=self.config.port,
            auth_provider=auth_provider
        )
        self._session = self._cluster.connect(self.config.keyspace)
        
        # Prepare common statements
        self._prepare_statements()
        
        logger.info("Connected to Cassandra successfully")
        
    def _prepare_statements(self) -> None:
        """Prepare commonly used CQL statements."""
        # Bronze layer insert
        self._prepared_stmts["insert_bronze"] = self._session.prepare("""
            INSERT INTO trades_bronze (
                symbol, trade_date, trade_timestamp, price, volume,
                conditions, ingestion_time, kafka_partition, kafka_offset
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        # Silver layer insert
        self._prepared_stmts["insert_silver"] = self._session.prepare("""
            INSERT INTO trades_silver (
                symbol, trade_date, trade_timestamp, price, volume,
                is_valid, quality_score, processed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        # Gold layer (5m) insert
        self._prepared_stmts["insert_gold_5m"] = self._session.prepare("""
            INSERT INTO trades_gold_5m (
                symbol, window_date, window_start, window_end,
                open, high, low, close, volume, trade_count, vwap
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        # Latest price update
        self._prepared_stmts["update_latest"] = self._session.prepare("""
            UPDATE latest_prices SET
                last_price = ?,
                last_volume = ?,
                last_trade_time = ?,
                price_change = ?,
                price_change_pct = ?,
                updated_at = ?
            WHERE symbol = ?
        """)
        
    @property
    def session(self):
        """Get the Cassandra session, connecting if needed."""
        if self._session is None:
            self.connect()
        return self._session
    
    def close(self) -> None:
        """Close the Cassandra connection."""
        if self._cluster:
            self._cluster.shutdown()
            logger.info("Cassandra connection closed")
            
    # =========================================================================
    # Bronze Layer Operations
    # =========================================================================
    
    def insert_trade_bronze(
        self,
        symbol: str,
        trade_date: date,
        trade_timestamp: datetime,
        price: float,
        volume: int,
        conditions: Optional[List[str]] = None,
        ingestion_time: Optional[datetime] = None,
        kafka_partition: int = 0,
        kafka_offset: int = 0
    ) -> None:
        """Insert a single trade into the Bronze layer."""
        self.session.execute(
            self._prepared_stmts["insert_bronze"],
            (
                symbol, trade_date, trade_timestamp, price, volume,
                conditions or [], ingestion_time or datetime.utcnow(),
                kafka_partition, kafka_offset
            )
        )
        
    # =========================================================================
    # Query Operations
    # =========================================================================
    
    def get_latest_trades(
        self,
        symbol: str,
        trade_date: Optional[date] = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        Get latest trades for a symbol.
        
        Args:
            symbol: Stock symbol
            trade_date: Date to query (default: today)
            limit: Maximum number of trades to return
            
        Returns:
            List of trade dictionaries
        """
        trade_date = trade_date or date.today()
        
        result = self.session.execute(
            """
            SELECT * FROM trades_silver
            WHERE symbol = %s AND trade_date = %s
            LIMIT %s
            """,
            (symbol, trade_date, limit)
        )
        
        return [dict(row._asdict()) for row in result]
    
    def get_ohlcv(
        self,
        symbol: str,
        window_date: Optional[date] = None,
        table: str = "trades_gold_5m"
    ) -> List[Dict]:
        """
        Get OHLCV data for a symbol.
        
        Args:
            symbol: Stock symbol
            window_date: Date to query (default: today)
            table: Target table (trades_gold_5m or trades_gold_1h)
            
        Returns:
            List of OHLCV dictionaries
        """
        window_date = window_date or date.today()
        
        result = self.session.execute(
            f"""
            SELECT * FROM {table}
            WHERE symbol = %s AND window_date = %s
            """,
            (symbol, window_date)
        )
        
        return [dict(row._asdict()) for row in result]
    
    def get_latest_price(self, symbol: str) -> Optional[Dict]:
        """Get the latest price for a symbol."""
        result = self.session.execute(
            "SELECT * FROM latest_prices WHERE symbol = %s",
            (symbol,)
        )
        row = result.one()
        return dict(row._asdict()) if row else None
    
    # =========================================================================
    # Health Check
    # =========================================================================
    
    def health_check(self) -> Tuple[bool, str]:
        """
        Check Cassandra connection health.
        
        Returns:
            Tuple of (is_healthy, message)
        """
        try:
            result = self.session.execute("SELECT release_version FROM system.local")
            version = result.one().release_version
            return True, f"Cassandra {version} is healthy"
        except Exception as e:
            return False, f"Cassandra health check failed: {e}"
