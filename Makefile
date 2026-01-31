# =============================================================================
# Makefile for Real-Time Financial Market Data Pipeline
# =============================================================================

.PHONY: help install dev test lint format clean docker-up docker-down

# Default target
help:
	@echo "=========================================="
	@echo "Market Data Pipeline - Available Commands"
	@echo "=========================================="
	@echo "Setup:"
	@echo "  make install      - Install Python dependencies"
	@echo "  make dev          - Set up development environment"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-up    - Start all services"
	@echo "  make docker-down  - Stop all services"
	@echo "  make docker-logs  - View service logs"
	@echo "  make docker-ps    - Show running services"
	@echo ""
	@echo "Development:"
	@echo "  make test         - Run all tests"
	@echo "  make lint         - Run linters"
	@echo "  make format       - Format code"
	@echo "  make clean        - Clean temporary files"
	@echo ""
	@echo "Pipeline:"
	@echo "  make producer     - Start Finnhub producer"
	@echo "  make spark-job    - Submit Spark job"
	@echo "  make topics       - Create Kafka topics"

# =============================================================================
# Setup
# =============================================================================

install:
	pip install -r requirements.txt

dev: install
	pip install -e .
	pre-commit install

# =============================================================================
# Docker
# =============================================================================

docker-up:
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@docker-compose ps

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-ps:
	docker-compose ps

docker-clean:
	docker-compose down -v --remove-orphans
	docker system prune -f

# =============================================================================
# Kafka
# =============================================================================

topics:
	docker-compose exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic trades_raw \
		--partitions 10 \
		--replication-factor 1 \
		--if-not-exists
	docker-compose exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic quotes_raw \
		--partitions 5 \
		--replication-factor 1 \
		--if-not-exists
	docker-compose exec kafka kafka-topics --list \
		--bootstrap-server localhost:9092

# =============================================================================
# Cassandra
# =============================================================================

cassandra-init:
	docker-compose exec cassandra cqlsh -f /docker-entrypoint-initdb.d/keyspace.cql

cassandra-shell:
	docker-compose exec cassandra cqlsh

# =============================================================================
# Testing
# =============================================================================

test:
	pytest tests/ -v --cov=src --cov-report=term-missing

test-unit:
	pytest tests/unit -v

test-integration:
	pytest tests/integration -v

# =============================================================================
# Code Quality
# =============================================================================

lint:
	flake8 src/ tests/
	mypy src/

format:
	black src/ tests/
	isort src/ tests/

check: lint test
	@echo "All checks passed!"

# =============================================================================
# Pipeline
# =============================================================================

producer:
	python -m src.producer.finnhub_client

spark-job:
	docker-compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
		/opt/spark/work-dir/src/consumer/spark_processor.py

# =============================================================================
# Cleanup
# =============================================================================

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/ .coverage htmlcov/
