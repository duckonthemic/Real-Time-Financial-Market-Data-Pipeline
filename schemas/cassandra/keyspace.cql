-- =============================================================================
-- Market Data Pipeline - Cassandra Schema
-- =============================================================================
-- Optimized for time-series financial data with high write throughput
-- Partition key: (symbol, date) for efficient daily queries
-- Clustering: timestamp DESC for latest-first access pattern
-- =============================================================================

-- Create Keyspace
CREATE KEYSPACE IF NOT EXISTS market_data
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 2
}
AND durable_writes = true;

USE market_data;

-- =============================================================================
-- BRONZE LAYER: Raw trades (exactly as received)
-- =============================================================================
-- TTL: 7 days (604800 seconds)
-- Purpose: Audit trail, reprocessing capability

CREATE TABLE IF NOT EXISTS trades_bronze (
    symbol text,
    trade_date date,
    trade_timestamp timestamp,
    price double,
    volume bigint,
    conditions list<text>,
    ingestion_time timestamp,
    kafka_partition int,
    kafka_offset bigint,
    PRIMARY KEY ((symbol, trade_date), trade_timestamp)
) WITH CLUSTERING ORDER BY (trade_timestamp DESC)
AND default_time_to_live = 604800
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': 1,
    'compaction_window_unit': 'DAYS'
}
AND gc_grace_seconds = 86400
AND comment = 'Raw trade data - Bronze layer';

-- =============================================================================
-- SILVER LAYER: Cleaned and validated trades
-- =============================================================================
-- TTL: 30 days (2592000 seconds)
-- Purpose: Analysis-ready data with quality flags

CREATE TABLE IF NOT EXISTS trades_silver (
    symbol text,
    trade_date date,
    trade_timestamp timestamp,
    price double,
    volume bigint,
    is_valid boolean,
    quality_score float,
    processed_at timestamp,
    PRIMARY KEY ((symbol, trade_date), trade_timestamp)
) WITH CLUSTERING ORDER BY (trade_timestamp DESC)
AND default_time_to_live = 2592000
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': 1,
    'compaction_window_unit': 'DAYS'
}
AND comment = 'Cleaned trade data - Silver layer';

-- =============================================================================
-- GOLD LAYER: 5-Minute OHLCV Aggregations
-- =============================================================================
-- TTL: 90 days (7776000 seconds)
-- Purpose: Real-time dashboards, technical analysis

CREATE TABLE IF NOT EXISTS trades_gold_5m (
    symbol text,
    window_date date,
    window_start timestamp,
    window_end timestamp,
    open double,
    high double,
    low double,
    close double,
    volume bigint,
    trade_count int,
    vwap double,
    PRIMARY KEY ((symbol, window_date), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
AND default_time_to_live = 7776000
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': 7,
    'compaction_window_unit': 'DAYS'
}
AND comment = '5-minute OHLCV aggregations - Gold layer';

-- =============================================================================
-- GOLD LAYER: 1-Hour OHLCV Aggregations
-- =============================================================================
-- TTL: 365 days (31536000 seconds)
-- Purpose: Historical analysis, longer-term trends

CREATE TABLE IF NOT EXISTS trades_gold_1h (
    symbol text,
    window_date date,
    window_start timestamp,
    window_end timestamp,
    open double,
    high double,
    low double,
    close double,
    volume bigint,
    trade_count int,
    vwap double,
    PRIMARY KEY ((symbol, window_date), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
AND default_time_to_live = 31536000
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': 30,
    'compaction_window_unit': 'DAYS'
}
AND comment = '1-hour OHLCV aggregations - Gold layer';

-- =============================================================================
-- MATERIALIZED VIEW: Latest quotes per symbol
-- =============================================================================
-- For real-time price display

CREATE TABLE IF NOT EXISTS latest_prices (
    symbol text PRIMARY KEY,
    last_price double,
    last_volume bigint,
    last_trade_time timestamp,
    price_change double,
    price_change_pct float,
    updated_at timestamp
) WITH default_time_to_live = 0
AND comment = 'Latest price per symbol for real-time display';

-- =============================================================================
-- INDEXES for common query patterns
-- =============================================================================

-- Secondary index for querying by price range
CREATE INDEX IF NOT EXISTS idx_trades_silver_price 
ON trades_silver (price);

-- Secondary index for querying high-volume trades
CREATE INDEX IF NOT EXISTS idx_trades_silver_volume 
ON trades_silver (volume);

-- =============================================================================
-- Example Queries
-- =============================================================================

-- Get latest trades for AAPL today
-- SELECT * FROM trades_silver 
-- WHERE symbol = 'AAPL' AND trade_date = toDate(now()) 
-- LIMIT 100;

-- Get 5-minute OHLCV for AAPL today
-- SELECT * FROM trades_gold_5m 
-- WHERE symbol = 'AAPL' AND window_date = toDate(now());

-- Get hourly OHLCV for AAPL last 7 days
-- SELECT * FROM trades_gold_1h 
-- WHERE symbol = 'AAPL' 
-- AND window_date >= toDate(now()) - 7
-- AND window_date <= toDate(now());
