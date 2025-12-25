#!/bin/bash
# Reset test environment to clean state
# - Truncates all tables in SQL Server and PostgreSQL
# - Deletes and recreates Kafka topics
# - Resets connector offsets by redeploying connectors
#
# Usage:
#   ./reset-test-environment.sh
#   ./reset-test-environment.sh --quick  # Skip connector restart

set -euo pipefail

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Parse arguments
QUICK_MODE=false
if [[ "${1:-}" == "--quick" ]]; then
    QUICK_MODE=true
fi

# Source vault helpers to load secrets
if [ -f "$SCRIPT_DIR/vault-helpers.sh" ]; then
    source "$SCRIPT_DIR/vault-helpers.sh"

    # Try to load secrets from Vault, fall back to environment variables
    if vault_is_ready; then
        echo -e "${BLUE}Loading credentials from Vault...${NC}"
        export_database_secrets || {
            echo -e "${YELLOW}WARNING: Failed to load from Vault, using environment variables${NC}"
        }
    else
        echo -e "${YELLOW}WARNING: Vault not available, using environment variables${NC}"
    fi
fi

# Set defaults if not loaded from Vault
SQLSERVER_HOST="${SQLSERVER_HOST:-localhost}"
SQLSERVER_USER="${SQLSERVER_USER:-sa}"
SQLSERVER_PASSWORD="${SQLSERVER_PASSWORD:-YourStrong!Passw0rd}"
SQLSERVER_DATABASE="${SQLSERVER_DATABASE:-warehouse_source}"

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres_secure_password}"
POSTGRES_DB="${POSTGRES_DB:-warehouse_target}"

KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Test Environment Reset${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to check if service is available
check_service() {
    local service_name=$1
    local check_command=$2

    if eval "$check_command" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ $service_name is available${NC}"
        return 0
    else
        echo -e "${RED}✗ $service_name is not available${NC}"
        return 1
    fi
}

# Check services
echo -e "${BLUE}Checking services...${NC}"
check_service "SQL Server" "docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U $SQLSERVER_USER -P '$SQLSERVER_PASSWORD' -C -Q 'SELECT 1'" || exit 1
check_service "PostgreSQL" "docker exec cdc-postgres pg_isready -U $POSTGRES_USER -d $POSTGRES_DB" || exit 1
check_service "Kafka" "docker exec cdc-kafka kafka-broker-api-versions --bootstrap-server localhost:9092" || exit 1
check_service "Kafka Connect" "curl -sf $KAFKA_CONNECT_URL/connectors" || exit 1
echo ""

# =============================================================================
# 1. Pause connectors (to prevent new data from being captured)
# =============================================================================
if [ "$QUICK_MODE" = false ]; then
    echo -e "${BLUE}Step 1: Pausing CDC connectors...${NC}"

    CONNECTORS=$(curl -s $KAFKA_CONNECT_URL/connectors 2>/dev/null || echo "[]")

    if [ "$CONNECTORS" != "[]" ] && [ -n "$CONNECTORS" ]; then
        echo "$CONNECTORS" | jq -r '.[]' | while read -r connector; do
            echo "  Pausing $connector..."
            curl -s -X PUT "$KAFKA_CONNECT_URL/connectors/$connector/pause" > /dev/null 2>&1 || true
        done
        echo -e "${GREEN}✓ Connectors paused${NC}"
        sleep 3
    else
        echo -e "${YELLOW}⚠ No connectors found${NC}"
    fi
    echo ""
fi

# =============================================================================
# 2. Truncate SQL Server tables
# =============================================================================
echo -e "${BLUE}Step 2: Truncating SQL Server tables...${NC}"

# Get list of tables (excluding CDC system tables)
SQLSERVER_TABLES=$(docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
    -d "$SQLSERVER_DATABASE" \
    -h -1 -W \
    -Q "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'" \
    | grep -v '^$' | grep -v 'TABLE_NAME' | grep -v '\-\-' | tr -d ' \r' || echo "")

if [ -n "$SQLSERVER_TABLES" ]; then
    # Disable foreign key constraints
    echo "  Disabling foreign key constraints..."
    docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
        -d "$SQLSERVER_DATABASE" \
        -Q "EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT all'" > /dev/null 2>&1

    # Truncate each table
    while IFS= read -r table; do
        if [ -n "$table" ]; then
            echo "  Truncating dbo.$table..."
            docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
                -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
                -d "$SQLSERVER_DATABASE" \
                -Q "TRUNCATE TABLE dbo.$table" > /dev/null 2>&1 || {
                    echo -e "${YELLOW}    ⚠ Could not truncate $table (may have FK constraints), using DELETE${NC}"
                    docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
                        -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
                        -d "$SQLSERVER_DATABASE" \
                        -Q "DELETE FROM dbo.$table" > /dev/null 2>&1
                }
        fi
    done <<< "$SQLSERVER_TABLES"

    # Re-enable foreign key constraints
    echo "  Re-enabling foreign key constraints..."
    docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
        -d "$SQLSERVER_DATABASE" \
        -Q "EXEC sp_MSforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all'" > /dev/null 2>&1

    echo -e "${GREEN}✓ SQL Server tables truncated${NC}"
else
    echo -e "${YELLOW}⚠ No tables found in SQL Server${NC}"
fi
echo ""

# =============================================================================
# 3. Truncate PostgreSQL tables
# =============================================================================
echo -e "${BLUE}Step 3: Truncating PostgreSQL tables...${NC}"

# Get list of tables
POSTGRES_TABLES=$(docker exec cdc-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "SELECT tablename FROM pg_tables WHERE schemaname = 'public'" || echo "")

if [ -n "$POSTGRES_TABLES" ]; then
    # Truncate each table with CASCADE
    while IFS= read -r table; do
        if [ -n "$table" ]; then
            echo "  Truncating $table..."
            docker exec cdc-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
                "TRUNCATE TABLE $table CASCADE" > /dev/null 2>&1 || {
                    echo -e "${YELLOW}    ⚠ Could not truncate $table${NC}"
                }
        fi
    done <<< "$POSTGRES_TABLES"

    echo -e "${GREEN}✓ PostgreSQL tables truncated${NC}"
else
    echo -e "${YELLOW}⚠ No tables found in PostgreSQL${NC}"
fi
echo ""

# =============================================================================
# 4. Delete and recreate Kafka topics
# =============================================================================
echo -e "${BLUE}Step 4: Resetting Kafka topics...${NC}"

# Get list of CDC topics (those created by Debezium)
CDC_TOPICS=$(docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null \
    | grep -E '^sqlserver\.|^warehouse_source\.' || echo "")

if [ -n "$CDC_TOPICS" ]; then
    while IFS= read -r topic; do
        if [ -n "$topic" ]; then
            echo "  Deleting topic: $topic..."
            docker exec cdc-kafka kafka-topics \
                --bootstrap-server localhost:9092 \
                --delete \
                --topic "$topic" > /dev/null 2>&1 || {
                    echo -e "${YELLOW}    ⚠ Could not delete topic $topic${NC}"
                }
        fi
    done <<< "$CDC_TOPICS"

    echo -e "${GREEN}✓ Kafka CDC topics deleted${NC}"

    # Wait for topics to be fully deleted
    sleep 2
else
    echo -e "${YELLOW}⚠ No CDC topics found${NC}"
fi
echo ""

# =============================================================================
# 5. Reset connector offsets by deleting and redeploying
# =============================================================================
if [ "$QUICK_MODE" = false ]; then
    echo -e "${BLUE}Step 5: Resetting connector offsets...${NC}"

    CONNECTORS=$(curl -s $KAFKA_CONNECT_URL/connectors 2>/dev/null || echo "[]")

    if [ "$CONNECTORS" != "[]" ] && [ -n "$CONNECTORS" ]; then
        # Delete all connectors
        echo "$CONNECTORS" | jq -r '.[]' | while read -r connector; do
            echo "  Deleting $connector..."
            curl -s -X DELETE "$KAFKA_CONNECT_URL/connectors/$connector" > /dev/null 2>&1 || true
        done

        echo -e "${GREEN}✓ Connectors deleted${NC}"
        echo "  Waiting for connector cleanup (5 seconds)..."
        sleep 5

        # Redeploy connectors
        echo "  Redeploying connectors..."
        if [ -f "$SCRIPT_DIR/deploy-with-vault.sh" ]; then
            "$SCRIPT_DIR/deploy-with-vault.sh" > /dev/null 2>&1 || {
                echo -e "${YELLOW}    ⚠ Could not redeploy connectors automatically${NC}"
                echo -e "${YELLOW}    Run 'make deploy' to redeploy connectors${NC}"
            }
            echo -e "${GREEN}✓ Connectors redeployed${NC}"
        else
            echo -e "${YELLOW}    ⚠ deploy-with-vault.sh not found${NC}"
            echo -e "${YELLOW}    Run 'make deploy' to redeploy connectors${NC}"
        fi
    else
        echo -e "${YELLOW}⚠ No connectors found${NC}"
    fi
    echo ""
fi

# =============================================================================
# Summary
# =============================================================================
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Test Environment Reset Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Completed actions:"
if [ "$QUICK_MODE" = false ]; then
    echo "  ✓ Paused CDC connectors"
fi
echo "  ✓ Truncated SQL Server tables"
echo "  ✓ Truncated PostgreSQL tables"
echo "  ✓ Deleted Kafka CDC topics"
if [ "$QUICK_MODE" = false ]; then
    echo "  ✓ Reset connector offsets"
    echo "  ✓ Redeployed connectors"
fi
echo ""

if [ "$QUICK_MODE" = true ]; then
    echo -e "${YELLOW}Note: Quick mode was used. Connectors were not reset.${NC}"
    echo -e "${YELLOW}Run without --quick flag for full reset.${NC}"
    echo ""
fi

echo "The test environment is now in a clean state."
echo ""