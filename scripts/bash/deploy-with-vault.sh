#!/bin/bash
# Deploy connectors using credentials from Vault
# This script fetches secrets from Vault and substitutes them into connector configs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="$SCRIPT_DIR/../../docker/configs"

# Source vault helpers
source "$SCRIPT_DIR/vault-helpers.sh"

# Check Vault accessibility
if ! vault_is_ready; then
    echo "ERROR: Vault is not accessible. Please ensure Vault is running." >&2
    echo "  docker ps | grep cdc-vault" >&2
    exit 1
fi

# Load secrets from Vault
echo "Loading credentials from Vault..."
export_database_secrets

# Validate all required secrets are loaded
if ! validate_secrets; then
    echo "ERROR: Required secrets are missing" >&2
    exit 1
fi

echo "✓ All required secrets loaded from Vault"
echo ""

# Generate Debezium config with substituted values
echo "Generating Debezium connector config from Vault..."
DEBEZIUM_TEMPLATE="$CONFIG_DIR/debezium/sqlserver-source.json"
DEBEZIUM_CONFIG="/tmp/sqlserver-source-runtime.json"

# Read template and substitute Vault placeholders with actual values
# Escape special characters in passwords for sed
SQLSERVER_PASSWORD_ESCAPED=$(printf '%s\n' "$SQLSERVER_PASSWORD" | sed 's/[&/\]/\\&/g')
POSTGRES_PASSWORD_ESCAPED=$(printf '%s\n' "$POSTGRES_PASSWORD" | sed 's/[&/\]/\\&/g')

cat "$DEBEZIUM_TEMPLATE" | \
    sed "s|\\\${vault:secret/database:sqlserver_host}|$SQLSERVER_HOST|g" | \
    sed "s|\\\${vault:secret/database:sqlserver_user}|$SQLSERVER_USER|g" | \
    sed "s|\\\${vault:secret/database:sqlserver_password}|$SQLSERVER_PASSWORD_ESCAPED|g" \
    > "$DEBEZIUM_CONFIG"

echo "✓ Debezium config generated with Vault credentials"

# Generate PostgreSQL sink config with substituted values
echo "Generating PostgreSQL sink connector config from Vault..."
POSTGRES_TEMPLATE="$CONFIG_DIR/kafka-connect/postgresql-sink.json"
POSTGRES_CONFIG="/tmp/postgresql-sink-runtime.json"

cat "$POSTGRES_TEMPLATE" | \
    sed "s|\\\${vault:secret/database:postgres_host}|$POSTGRES_HOST|g" | \
    sed "s|\\\${vault:secret/database:postgres_user}|$POSTGRES_USER|g" | \
    sed "s|\\\${vault:secret/database:postgres_password}|$POSTGRES_PASSWORD_ESCAPED|g" \
    > "$POSTGRES_CONFIG"

echo "✓ PostgreSQL sink config generated with Vault credentials"
echo ""

# Deploy connectors
echo "Deploying connectors..."
echo ""

# Deploy Debezium connector
echo "Deploying Debezium SQL Server source connector..."
"$SCRIPT_DIR/deploy-connector.sh" "$DEBEZIUM_CONFIG"

echo ""

# Deploy PostgreSQL sink connector
echo "Deploying PostgreSQL JDBC sink connector..."
"$SCRIPT_DIR/deploy-connector.sh" "$POSTGRES_CONFIG"

echo ""
echo "✓ All connectors deployed successfully with Vault credentials!"
echo ""
echo "Verify connector status:"
echo "  curl -s http://localhost:8083/connectors | jq"
echo "  curl -s http://localhost:8083/connectors/sqlserver-cdc-source/status | jq"
echo "  curl -s http://localhost:8083/connectors/postgresql-jdbc-sink/status | jq"
echo ""
echo "Temporary config files:"
echo "  $DEBEZIUM_CONFIG"
echo "  $POSTGRES_CONFIG"
echo ""
echo "To clean up temporary files:"
echo "  rm $DEBEZIUM_CONFIG $POSTGRES_CONFIG"
