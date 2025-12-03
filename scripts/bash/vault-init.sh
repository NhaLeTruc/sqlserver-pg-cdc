#!/bin/bash
# Initialize HashiCorp Vault with database credentials for CDC pipeline
# This script sets up the KV v2 secrets engine and stores database credentials

set -euo pipefail

VAULT_ADDR="${VAULT_ADDR:-http://localhost:8200}"
VAULT_TOKEN="${VAULT_DEV_ROOT_TOKEN_ID:-dev-root-token}"

# Source database credentials
SQLSERVER_HOST="${SQLSERVER_HOST:-sqlserver}"
SQLSERVER_USER="${SQLSERVER_USER:-sa}"
SQLSERVER_PASSWORD="${SQLSERVER_PASSWORD:-YourStrong!Passw0rd}"

# Target database credentials
POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres_secure_password}"

echo "Initializing Vault at $VAULT_ADDR..."
echo ""

# Wait for Vault to be ready
echo "Waiting for Vault to be ready..."
for i in {1..30}; do
  if docker exec cdc-vault vault status > /dev/null 2>&1; then
    echo "Vault is ready!"
    break
  fi
  echo "Waiting for Vault... (attempt $i/30)"
  sleep 2
done

# Enable KV v2 secrets engine (if not already enabled)
echo ""
echo "Enabling KV v2 secrets engine..."
docker exec -e VAULT_TOKEN="$VAULT_TOKEN" cdc-vault \
  vault secrets enable -version=2 -path=secret kv || echo "KV engine already enabled"

# Store SQL Server credentials
echo ""
echo "Storing SQL Server credentials..."
docker exec -e VAULT_TOKEN="$VAULT_TOKEN" cdc-vault \
  vault kv put secret/database \
    sqlserver_host="$SQLSERVER_HOST" \
    sqlserver_user="$SQLSERVER_USER" \
    sqlserver_password="$SQLSERVER_PASSWORD" \
    postgres_host="$POSTGRES_HOST" \
    postgres_user="$POSTGRES_USER" \
    postgres_password="$POSTGRES_PASSWORD"

# Verify stored secrets
echo ""
echo "Verifying stored secrets..."
docker exec -e VAULT_TOKEN="$VAULT_TOKEN" cdc-vault \
  vault kv get secret/database

# Create Kafka Connect policy
echo ""
echo "Creating Kafka Connect policy..."
docker exec -e VAULT_TOKEN="$VAULT_TOKEN" cdc-vault sh -c 'cat <<EOF | vault policy write kafka-connect -
# Vault Policy for Kafka Connect
# Grants read-only access to database credentials

# Allow reading database credentials from KV v2 secrets engine
path "secret/data/database/*" {
  capabilities = ["read"]
}

# Allow listing secrets (for discovery)
path "secret/metadata/database/*" {
  capabilities = ["list"]
}

# Deny all other operations
path "secret/*" {
  capabilities = ["deny"]
}

# Allow token self-renewal (for long-running connectors)
path "auth/token/renew-self" {
  capabilities = ["update"]
}

# Allow token lookup (for validation)
path "auth/token/lookup-self" {
  capabilities = ["read"]
}
EOF
'

# Create a token for Kafka Connect (optional, dev-root-token is used in dev mode)
echo ""
echo "Creating token for Kafka Connect..."
CONNECT_TOKEN=$(docker exec -e VAULT_TOKEN="$VAULT_TOKEN" cdc-vault \
  vault token create -policy=kafka-connect -ttl=720h -format=json | jq -r '.auth.client_token')

echo ""
echo "Kafka Connect token: $CONNECT_TOKEN"
echo ""
echo "Export this token for production use:"
echo "export VAULT_KAFKA_CONNECT_TOKEN=$CONNECT_TOKEN"
echo ""

# Enable audit logging (optional)
echo "Enabling audit logging..."
docker exec -e VAULT_TOKEN="$VAULT_TOKEN" cdc-vault \
  vault audit enable file file_path=/vault/logs/audit.log || echo "Audit already enabled"

echo ""
echo "Vault initialization complete!"
echo ""
echo "Test credential retrieval:"
echo "  vault kv get secret/database"
echo ""
echo "Vault UI: http://localhost:8200/ui"
echo "Token: $VAULT_TOKEN"
