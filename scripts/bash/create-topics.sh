#!/bin/bash
# Create Kafka topics for CDC pipeline
# Based on contracts/kafka-topics.yaml specification

set -euo pipefail

KAFKA_BROKER="${KAFKA_BROKER:-localhost:29092}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"

echo "Creating Kafka topics for CDC pipeline..."
echo "Kafka broker: $KAFKA_BROKER"
echo "Replication factor: $REPLICATION_FACTOR"
echo "Partitions: $PARTITIONS"
echo ""

# CDC Topics (auto-created by Debezium, but can be pre-created for custom config)
echo "Pre-creating CDC topics with custom configuration..."

docker exec cdc-kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic sqlserver.warehouse_source.dbo.customers \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR" \
  --config retention.ms=604800000 \
  --config compression.type=lz4 \
  --config min.insync.replicas=1 \
  --config segment.ms=3600000 \
  --if-not-exists

docker exec cdc-kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic sqlserver.warehouse_source.dbo.orders \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR" \
  --config retention.ms=604800000 \
  --config compression.type=lz4 \
  --config min.insync.replicas=1 \
  --config segment.ms=3600000 \
  --if-not-exists

docker exec cdc-kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic sqlserver.warehouse_source.dbo.line_items \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR" \
  --config retention.ms=604800000 \
  --config compression.type=lz4 \
  --config min.insync.replicas=1 \
  --config segment.ms=3600000 \
  --if-not-exists

# Dead Letter Queue Topic
echo "Creating Dead Letter Queue topic..."

docker exec cdc-kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic dlq-postgresql-sink \
  --partitions 1 \
  --replication-factor "$REPLICATION_FACTOR" \
  --config retention.ms=2592000000 \
  --config retention.bytes=10737418240 \
  --config compression.type=lz4 \
  --config min.insync.replicas=1 \
  --config segment.ms=86400000 \
  --if-not-exists

# Schema changes topic for Debezium
echo "Creating schema changes topic..."

docker exec cdc-kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic schema-changes.warehouse_source \
  --partitions 1 \
  --replication-factor "$REPLICATION_FACTOR" \
  --config cleanup.policy=compact \
  --config compression.type=gzip \
  --config min.insync.replicas=1 \
  --if-not-exists

echo ""
echo "Listing all topics:"
docker exec cdc-kafka kafka-topics --list --bootstrap-server kafka:9092

echo ""
echo "Topic creation complete!"
