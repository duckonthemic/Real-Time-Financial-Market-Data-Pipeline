#!/bin/bash
# =============================================================================
# Kafka Topic Creation Script
# Creates required topics for the market data pipeline
# =============================================================================

set -e

KAFKA_CONTAINER=${KAFKA_CONTAINER:-kafka}
BOOTSTRAP_SERVER=${BOOTSTRAP_SERVER:-localhost:9092}

echo "=============================================="
echo "Creating Kafka Topics"
echo "=============================================="
echo "Bootstrap Server: $BOOTSTRAP_SERVER"
echo ""

# Function to create a topic
create_topic() {
    local topic=$1
    local partitions=$2
    local retention_ms=$3
    
    echo "Creating topic: $topic (partitions=$partitions, retention=${retention_ms}ms)"
    
    docker-compose exec -T $KAFKA_CONTAINER kafka-topics \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --create \
        --topic $topic \
        --partitions $partitions \
        --replication-factor 1 \
        --if-not-exists \
        --config retention.ms=$retention_ms \
        --config cleanup.policy=delete
        
    if [ $? -eq 0 ]; then
        echo "✅ Created $topic"
    else
        echo "❌ Failed to create $topic"
    fi
}

# Create topics
# trades_raw: 10 partitions, 7 days retention
create_topic "trades_raw" 10 604800000

# quotes_raw: 5 partitions, 1 day retention
create_topic "quotes_raw" 5 86400000

# crypto_raw: 5 partitions, 7 days retention
create_topic "crypto_raw" 5 604800000

# dead_letters: 3 partitions, 30 days retention (for invalid messages)
create_topic "dead_letters" 3 2592000000

echo ""
echo "=============================================="
echo "Listing all topics"
echo "=============================================="

docker-compose exec -T $KAFKA_CONTAINER kafka-topics \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --list

echo ""
echo "Done!"
