#!/bin/bash
# Simple connector config generator
# Substitutes env vars, preserves ${vault:...} and Kafka Connect ${...} placeholders

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load .env
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Create output dirs
mkdir -p "$PROJECT_ROOT/docker/configs/runtime/debezium"
mkdir -p "$PROJECT_ROOT/docker/configs/runtime/kafka-connect"

# Generate Debezium source config
# envsubst automatically preserves ${vault:...} because "vault" is not an env var
envsubst < "$PROJECT_ROOT/docker/configs/templates/debezium/sqlserver-source.json.template" \
    > "$PROJECT_ROOT/docker/configs/runtime/debezium/sqlserver-source.json"

# Generate PostgreSQL sink config
# Need to protect Kafka Connect placeholders like ${topic} from envsubst
# Replace ${topic} with __TOPIC__ temporarily, run envsubst, then restore
sed 's/\${topic}/__TOPIC__/g' "$PROJECT_ROOT/docker/configs/templates/kafka-connect/postgresql-sink.json.template" | \
    envsubst | \
    sed 's/__TOPIC__/${topic}/g' \
    > "$PROJECT_ROOT/docker/configs/runtime/kafka-connect/postgresql-sink.json"

echo "✓ Generated connector configs"
