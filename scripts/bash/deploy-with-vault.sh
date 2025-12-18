#!/bin/bash
# Deploy connectors using credentials from Vault
# This script:
# 1. Generates configs from templates with env vars
# 2. Fetches secrets from Vault
# 3. Substitutes Vault placeholders in generated configs
# 4. Deploys to Kafka Connect

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNTIME_DIR="$PROJECT_ROOT/docker/configs/runtime"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Source vault helpers
source "$SCRIPT_DIR/vault-helpers.sh"

echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}Deploying Connectors with Vault Integration${NC}"
echo -e "${BLUE}================================================================${NC}"
echo ""

# Step 1: Generate configs from templates (if not already generated)
if [ ! -f "$RUNTIME_DIR/debezium/sqlserver-source.json" ] || \
   [ ! -f "$RUNTIME_DIR/kafka-connect/postgresql-sink.json" ]; then
    echo -e "${BLUE}Step 1: Generating connector configs from templates...${NC}"
    if ! "$SCRIPT_DIR/generate-connector-configs.sh"; then
        echo -e "${RED}ERROR: Config generation failed${NC}" >&2
        echo -e "${YELLOW}Falling back to legacy configs...${NC}"
        RUNTIME_DIR="$PROJECT_ROOT/docker/configs"
        echo -e "${YELLOW}Using legacy config directory: $RUNTIME_DIR${NC}"
    fi
    echo ""
else
    echo -e "${BLUE}Step 1: Using existing runtime configs${NC}"
    echo -e "${GREEN}✓ Runtime configs found${NC}"
    echo ""
fi

# Step 2: Check Vault accessibility
echo -e "${BLUE}Step 2: Checking Vault accessibility...${NC}"
if ! vault_is_ready; then
    echo -e "${RED}ERROR: Vault is not accessible. Please ensure Vault is running.${NC}" >&2
    echo -e "${YELLOW}Check Vault status:${NC}" >&2
    echo -e "${YELLOW}  docker ps | grep cdc-vault${NC}" >&2
    echo -e "${YELLOW}  docker logs cdc-vault${NC}" >&2
    exit 1
fi
echo -e "${GREEN}✓ Vault is accessible${NC}"
echo ""

# Step 3: Load secrets from Vault
echo -e "${BLUE}Step 3: Loading credentials from Vault...${NC}"
export_database_secrets

if ! validate_secrets; then
    echo -e "${RED}ERROR: Required secrets are missing${NC}" >&2
    echo -e "${YELLOW}Initialize Vault with:${NC}" >&2
    echo -e "${YELLOW}  make vault-init${NC}" >&2
    exit 1
fi

echo -e "${GREEN}✓ All required secrets loaded from Vault${NC}"
echo ""

# Step 4: Substitute Vault placeholders in runtime configs
echo -e "${BLUE}Step 4: Substituting Vault secrets into runtime configs...${NC}"

# Escape special characters in passwords for sed
SQLSERVER_PASSWORD_ESCAPED=$(printf '%s\n' "$SQLSERVER_PASSWORD" | sed 's/[&/\]/\\&/g')
POSTGRES_PASSWORD_ESCAPED=$(printf '%s\n' "$POSTGRES_PASSWORD" | sed 's/[&/\]/\\&/g')

# Create temporary deployment configs with Vault secrets substituted
DEBEZIUM_DEPLOY_CONFIG="/tmp/sqlserver-source-deploy-$(date +%s).json"
POSTGRES_DEPLOY_CONFIG="/tmp/postgresql-sink-deploy-$(date +%s).json"

cat "$RUNTIME_DIR/debezium/sqlserver-source.json" | \
    sed "s|\\\${vault:secret/database:sqlserver_host}|$SQLSERVER_HOST|g" | \
    sed "s|\\\${vault:secret/database:sqlserver_user}|$SQLSERVER_USER|g" | \
    sed "s|\\\${vault:secret/database:sqlserver_password}|$SQLSERVER_PASSWORD_ESCAPED|g" \
    > "$DEBEZIUM_DEPLOY_CONFIG"

cat "$RUNTIME_DIR/kafka-connect/postgresql-sink.json" | \
    sed "s|\\\${vault:secret/database:postgres_host}|$POSTGRES_HOST|g" | \
    sed "s|\\\${vault:secret/database:postgres_user}|$POSTGRES_USER|g" | \
    sed "s|\\\${vault:secret/database:postgres_password}|$POSTGRES_PASSWORD_ESCAPED|g" \
    > "$POSTGRES_DEPLOY_CONFIG"

echo -e "${GREEN}✓ Vault secrets substituted into deployment configs${NC}"
echo ""

# Step 5: Deploy connectors
echo -e "${BLUE}Step 5: Deploying connectors to Kafka Connect...${NC}"
echo ""

echo -e "${BLUE}Deploying Debezium SQL Server source connector...${NC}"
if "$SCRIPT_DIR/deploy-connector.sh" "$DEBEZIUM_DEPLOY_CONFIG"; then
    echo -e "${GREEN}✓ Debezium source connector deployed${NC}"
else
    echo -e "${RED}ERROR: Debezium source connector deployment failed${NC}" >&2
fi

echo ""

echo -e "${BLUE}Deploying PostgreSQL JDBC sink connector...${NC}"
if "$SCRIPT_DIR/deploy-connector.sh" "$POSTGRES_DEPLOY_CONFIG"; then
    echo -e "${GREEN}✓ PostgreSQL sink connector deployed${NC}"
else
    echo -e "${RED}ERROR: PostgreSQL sink connector deployment failed${NC}" >&2
fi

echo ""
echo -e "${GREEN}================================================================${NC}"
echo -e "${GREEN}✓ Connector Deployment Complete${NC}"
echo -e "${GREEN}================================================================${NC}"
echo ""
echo -e "${BLUE}Verify connector status:${NC}"
echo -e "  curl -s http://localhost:8083/connectors | jq"
echo -e "  curl -s http://localhost:8083/connectors/sqlserver-cdc-source/status | jq"
echo -e "  curl -s http://localhost:8083/connectors/postgresql-jdbc-sink/status | jq"
echo ""
echo -e "${BLUE}Or use make targets:${NC}"
echo -e "  make connector-status"
echo -e "  make connector-list"
echo ""
echo -e "${BLUE}Temporary deployment configs (will be cleaned up):${NC}"
echo -e "  $DEBEZIUM_DEPLOY_CONFIG"
echo -e "  $POSTGRES_DEPLOY_CONFIG"
echo ""

# Auto-cleanup temporary files after 60 seconds
(sleep 60 && rm -f "$DEBEZIUM_DEPLOY_CONFIG" "$POSTGRES_DEPLOY_CONFIG" 2>/dev/null) &
echo -e "${YELLOW}Temporary files will be auto-cleaned in 60 seconds${NC}"
echo ""
