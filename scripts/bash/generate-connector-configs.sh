#!/bin/bash
# Generate Kafka Connect connector configurations with Vault integration
# This script creates connector configs that fetch credentials from Vault

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="$SCRIPT_DIR/../../docker/configs"

echo "Generating Kafka Connect connector configurations..."
echo ""

# Create Debezium SQL Server source connector config with Vault
echo "Creating Debezium SQL Server source connector config..."
cat > "$CONFIG_DIR/debezium/sqlserver-source-vault.json" << 'EOF'
{
  "name": "sqlserver-cdc-source",
  "config": {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "tasks.max": "1",

    "database.hostname": "${vault:secret/database:sqlserver_host}",
    "database.port": "1433",
    "database.user": "${vault:secret/database:sqlserver_user}",
    "database.password": "${vault:secret/database:sqlserver_password}",
    "database.names": "warehouse_source",
    "database.encrypt": "false",
    "database.trustServerCertificate": "true",

    "table.include.list": "dbo.customers,dbo.orders,dbo.line_items",

    "topic.prefix": "sqlserver",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.warehouse_source",

    "snapshot.mode": "schema_only",
    "snapshot.locking.mode": "none",

    "decimal.handling.mode": "precise",
    "time.precision.mode": "adaptive",
    "tombstones.on.delete": "true",

    "max.batch.size": "2048",
    "max.queue.size": "8192",
    "poll.interval.ms": "500",

    "include.schema.changes": "true",
    "provide.transaction.metadata": "true",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "errors.tolerance": "none",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
  }
}
EOF

# Create PostgreSQL JDBC sink connector config with Vault
echo "Creating PostgreSQL JDBC sink connector config..."
cat > "$CONFIG_DIR/kafka-connect/postgresql-sink-vault.json" << 'EOF'
{
  "name": "postgresql-jdbc-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "3",

    "connection.url": "jdbc:postgresql://${vault:secret/database:postgres_host}:5432/warehouse_target",
    "connection.user": "${vault:secret/database:postgres_user}",
    "connection.password": "${vault:secret/database:postgres_password}",

    "topics": "sqlserver.warehouse_source.dbo.customers,sqlserver.warehouse_source.dbo.orders,sqlserver.warehouse_source.dbo.line_items",

    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "id",

    "table.name.format": "${topic}",
    "auto.create": "false",
    "auto.evolve": "false",

    "batch.size": "3000",
    "connection.pool.size": "10",
    "connection.attempts": "10",
    "connection.backoff.ms": "5000",

    "errors.retry.timeout": "300000",
    "errors.retry.delay.max.ms": "60000",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq-postgresql-sink",
    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",

    "transforms": "unwrap,route",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "sqlserver\\.warehouse_source\\.dbo\\.(.*)",
    "transforms.route.replacement": "$1"
  }
}
EOF

echo ""
echo "✓ Connector configurations with Vault integration created:"
echo "  - $CONFIG_DIR/debezium/sqlserver-source-vault.json"
echo "  - $CONFIG_DIR/kafka-connect/postgresql-sink-vault.json"
echo ""
echo "Note: These configs use Vault placeholders like:"
echo "  \${vault:secret/database:sqlserver_password}"
echo ""
echo "To use these configs, ensure Kafka Connect has the Vault config provider enabled."
