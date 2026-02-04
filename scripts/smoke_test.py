#!/usr/bin/env python3
"""
End-to-End Smoke Test for Market Data Pipeline
Validates the complete data flow from Finnhub to Grafana.
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================

class Config:
    """Test configuration."""
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
    GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3000")
    GRAFANA_USER = os.getenv("GRAFANA_ADMIN_USER", "admin")
    GRAFANA_PASSWORD = os.getenv("GRAFANA_ADMIN_PASSWORD", "admin")
    FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")


# =============================================================================
# Health Check Functions
# =============================================================================

def check_kafka() -> Tuple[bool, str]:
    """Check Kafka broker connectivity."""
    try:
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=5000
        )
        topics = admin.list_topics()
        admin.close()
        return True, f"Kafka OK - {len(topics)} topics"
    except ImportError:
        return False, "kafka-python not installed"
    except Exception as e:
        return False, f"Kafka error: {e}"


def check_schema_registry() -> Tuple[bool, str]:
    """Check Schema Registry connectivity."""
    try:
        response = requests.get(f"{Config.SCHEMA_REGISTRY_URL}/subjects", timeout=5)
        if response.status_code == 200:
            subjects = response.json()
            return True, f"Schema Registry OK - {len(subjects)} subjects"
        return False, f"Schema Registry returned {response.status_code}"
    except Exception as e:
        return False, f"Schema Registry error: {e}"


def check_cassandra() -> Tuple[bool, str]:
    """Check Cassandra connectivity and keyspace."""
    try:
        from cassandra.cluster import Cluster
        cluster = Cluster([Config.CASSANDRA_HOST])
        session = cluster.connect()
        
        # Check keyspace exists
        result = session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'market_data'"
        )
        if result.one():
            # Check tables
            tables = session.execute(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'market_data'"
            )
            table_count = len(list(tables))
            cluster.shutdown()
            return True, f"Cassandra OK - {table_count} tables in market_data"
        
        cluster.shutdown()
        return False, "Keyspace 'market_data' not found"
    except ImportError:
        return False, "cassandra-driver not installed"
    except Exception as e:
        return False, f"Cassandra error: {e}"


def check_grafana() -> Tuple[bool, str]:
    """Check Grafana connectivity."""
    try:
        response = requests.get(
            f"{Config.GRAFANA_URL}/api/health",
            timeout=5
        )
        if response.status_code == 200:
            data = response.json()
            return True, f"Grafana OK - {data.get('database', 'unknown')}"
        return False, f"Grafana returned {response.status_code}"
    except Exception as e:
        return False, f"Grafana error: {e}"


def check_finnhub() -> Tuple[bool, str]:
    """Check Finnhub API connectivity."""
    if not Config.FINNHUB_API_KEY:
        return False, "FINNHUB_API_KEY not configured"
    
    try:
        response = requests.get(
            f"https://finnhub.io/api/v1/quote?symbol=AAPL&token={Config.FINNHUB_API_KEY}",
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            price = data.get("c", 0)
            return True, f"Finnhub OK - AAPL: ${price}"
        return False, f"Finnhub returned {response.status_code}"
    except Exception as e:
        return False, f"Finnhub error: {e}"


# =============================================================================
# Data Validation
# =============================================================================

def validate_data_flow() -> Tuple[bool, str]:
    """Validate data exists in Cassandra tables."""
    try:
        from cassandra.cluster import Cluster
        cluster = Cluster([Config.CASSANDRA_HOST])
        session = cluster.connect("market_data")
        
        # Check for recent data
        result = session.execute(
            "SELECT COUNT(*) as cnt FROM trades_silver WHERE trade_date = toDate(now()) ALLOW FILTERING"
        )
        row = result.one()
        count = row.cnt if row else 0
        
        cluster.shutdown()
        
        if count > 0:
            return True, f"Data flow OK - {count} trades today"
        return False, "No trades found for today (pipeline may not be running)"
        
    except Exception as e:
        return False, f"Data validation error: {e}"


# =============================================================================
# Main Smoke Test
# =============================================================================

def run_smoke_test(verbose: bool = False) -> int:
    """
    Run complete smoke test suite.
    
    Returns:
        Exit code (0 = success, 1 = failure)
    """
    print("=" * 60)
    print("  MARKET DATA PIPELINE - SMOKE TEST")
    print("=" * 60)
    print()
    
    checks = [
        ("Finnhub API", check_finnhub),
        ("Kafka Broker", check_kafka),
        ("Schema Registry", check_schema_registry),
        ("Cassandra", check_cassandra),
        ("Grafana", check_grafana),
        ("Data Flow", validate_data_flow),
    ]
    
    results = []
    passed = 0
    failed = 0
    
    for name, check_fn in checks:
        try:
            success, message = check_fn()
            results.append((name, success, message))
            
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"  {status}  {name}")
            if verbose or not success:
                print(f"         {message}")
            
            if success:
                passed += 1
            else:
                failed += 1
                
        except Exception as e:
            results.append((name, False, str(e)))
            print(f"  ❌ FAIL  {name}")
            print(f"         Exception: {e}")
            failed += 1
    
    print()
    print("=" * 60)
    print(f"  RESULTS: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return 0 if failed == 0 else 1


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Pipeline smoke test")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    return run_smoke_test(verbose=args.verbose)


if __name__ == "__main__":
    sys.exit(main())
