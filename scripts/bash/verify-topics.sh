#!/bin/bash
# Verify Kafka topics are created with correct configuration

set -euo pipefail

KAFKA_BROKER="${KAFKA_BROKER:-localhost:29092}"
EXPECTED_PARTITIONS="${EXPECTED_PARTITIONS:-3}"

echo "Verifying Kafka topics for CDC pipeline..."
echo "Kafka broker: $KAFKA_BROKER"
echo "Expected partitions: $EXPECTED_PARTITIONS"
echo ""

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
for i in {1..30}; do
    if docker exec cdc-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        echo "Kafka is ready!"
        break
    fi
    echo "Waiting for Kafka... (attempt $i/30)"
    sleep 2
done

# List all topics
echo ""
echo "=== All Topics ==="
docker exec cdc-kafka kafka-topics --list --bootstrap-server localhost:9092

# Check CDC topics
echo ""
echo "=== CDC Topic Details ==="

CDC_TOPICS=(
    "sqlserver.warehouse_source.dbo.customers"
    "sqlserver.warehouse_source.dbo.orders"
    "sqlserver.warehouse_source.dbo.line_items"
)

for topic in "${CDC_TOPICS[@]}"; do
    echo ""
    echo "Topic: $topic"

    if docker exec cdc-kafka kafka-topics --describe \
        --bootstrap-server localhost:9092 \
        --topic "$topic" 2>/dev/null; then

        # Check partition count
        PARTITION_COUNT=$(docker exec cdc-kafka kafka-topics --describe \
            --bootstrap-server localhost:9092 \
            --topic "$topic" | grep -c "Partition:")

        if [ "$PARTITION_COUNT" -eq "$EXPECTED_PARTITIONS" ]; then
            echo "✓ Partition count correct: $PARTITION_COUNT"
        else
            echo "✗ Partition count incorrect: expected $EXPECTED_PARTITIONS, got $PARTITION_COUNT"
        fi
    else
        echo "✗ Topic does not exist (will be auto-created by connector)"
    fi
done

# Check system topics
echo ""
echo "=== System Topics ==="

SYSTEM_TOPICS=(
    "connect-configs"
    "connect-offsets"
    "connect-status"
    "schema-changes.warehouse_source"
    "dlq-postgresql-sink"
)

for topic in "${SYSTEM_TOPICS[@]}"; do
    if docker exec cdc-kafka kafka-topics --list --bootstrap-server localhost:9092 | grep -q "^${topic}$"; then
        echo "✓ $topic exists"
    else
        echo "✗ $topic does not exist"
    fi
done

echo ""
echo "Topic verification complete!"
