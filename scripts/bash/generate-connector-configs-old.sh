#!/bin/bash
# Generate Kafka Connect connector configurations from templates
# Supports BOTH environment variable substitution AND Vault placeholders
#
# This script:
# 1. Sources environment variables from .env file (if exists)
# 2. Validates required environment variables are set
# 3. Substitutes ${ENV_VAR} placeholders in templates
# 4. Preserves ${vault:secret/path:key} placeholders for later substitution
# 5. Generates runtime configs in docker/configs/runtime/
# 6. Validates JSON syntax of generated configs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEMPLATE_DIR="$PROJECT_ROOT/docker/configs/templates"
RUNTIME_DIR="$PROJECT_ROOT/docker/configs/runtime"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Source environment variables from .env if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo -e "${BLUE}Loading environment variables from $PROJECT_ROOT/.env${NC}"
    set -a  # automatically export all variables
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.env"
    set +a
else
    echo -e "${YELLOW}WARNING: No .env file found at $PROJECT_ROOT/.env${NC}"
    echo -e "${YELLOW}Using default values or system environment variables${NC}"
    echo -e "${YELLOW}To customize: cp .env.example .env && vi .env${NC}"
    echo ""
fi

# Validate required environment variables
validate_env_vars() {
    local required_vars=(
        # Debezium Source Parameters
        "DEBEZIUM_TASKS_MAX"
        "DEBEZIUM_MAX_BATCH_SIZE"
        "DEBEZIUM_MAX_QUEUE_SIZE"
        "DEBEZIUM_POLL_INTERVAL_MS"
        "DEBEZIUM_DATABASE_NAMES"
        "DEBEZIUM_TABLE_INCLUDE_LIST"
        "DEBEZIUM_SNAPSHOT_MODE"
        "DEBEZIUM_SNAPSHOT_LOCKING_MODE"
        "DEBEZIUM_TOPIC_PREFIX"
        "DEBEZIUM_SCHEMA_HISTORY_TOPIC"

        # PostgreSQL Sink Parameters
        "SINK_TASKS_MAX"
        "SINK_BATCH_SIZE"
        "SINK_CONNECTION_POOL_SIZE"
        "SINK_CONNECTION_ATTEMPTS"
        "SINK_CONNECTION_BACKOFF_MS"
        "SINK_ERRORS_RETRY_TIMEOUT"
        "SINK_ERRORS_RETRY_DELAY_MAX_MS"
        "SINK_TOPICS"
        "SINK_DLQ_TOPIC_NAME"
        "SINK_DLQ_REPLICATION_FACTOR"

        # Infrastructure Parameters
        "SQLSERVER_PORT"
        "POSTGRES_PORT"
        "POSTGRES_DB"
        "KAFKA_BROKER"
        "SCHEMA_REGISTRY_HOST"
        "SCHEMA_REGISTRY_PORT"
    )

    local missing=0
    for var in "${required_vars[@]}"; do
        if [ -z "${!var:-}" ]; then
            echo -e "${RED}ERROR: Required variable $var is not set${NC}" >&2
            missing=1
        fi
    done

    if [ $missing -eq 1 ]; then
        echo -e "${RED}Please set all required variables in .env file${NC}" >&2
        echo -e "${YELLOW}Copy .env.example to .env and customize:${NC}" >&2
        echo -e "${YELLOW}  cp .env.example .env${NC}" >&2
        echo -e "${YELLOW}  vi .env${NC}" >&2
        return 1
    fi

    return 0
}

# Function to substitute environment variables in template
# KEEPS Vault placeholders intact: ${vault:secret/path:key}
# SUBSTITUTES env vars: ${ENV_VAR}
substitute_template() {
    local template_file="$1"
    local output_file="$2"

    echo -e "${BLUE}Generating: $(basename "$output_file")${NC}"

    # Strategy: Replace ${vault:...} with a unique placeholder,
    # run envsubst, then restore vault placeholders

    # Read template
    local content
    content=$(cat "$template_file")

    # Step 1: Temporarily replace ${vault:...} with __VAULT_PLACEHOLDER_N__
    # Use a counter to make each placeholder unique
    local counter=0
    local vault_placeholders=()

    # Extract and store vault placeholders (one per line)
    # Extract vault placeholders and store in array
    local vault_matches
    vault_matches=$(echo "$content" | grep -o '\${vault:[^}]*}' | sort -u 2>/dev/null || echo "")

    if [ -n "$vault_matches" ]; then
        while IFS= read -r placeholder; do
            vault_placeholders+=("$placeholder")
        done <<< "$vault_matches"
    fi

    counter=${#vault_placeholders[@]}

    # Replace vault placeholders with numbered markers
    counter=0
    if [ ${#vault_placeholders[@]} -gt 0 ]; then
        for placeholder in "${vault_placeholders[@]}"; do
            if [ -n "$placeholder" ]; then
                content="${content//"$placeholder"/__VAULT_PLACEHOLDER_${counter}__}"
                ((counter++))
            fi
        done
    fi

    # Step 2: Substitute environment variables
    content=$(echo "$content" | envsubst)

    # Step 3: Restore vault placeholders
    counter=0
    if [ ${#vault_placeholders[@]} -gt 0 ]; then
        for placeholder in "${vault_placeholders[@]}"; do
            if [ -n "$placeholder" ]; then
                content="${content//__VAULT_PLACEHOLDER_${counter}__/$placeholder}"
                ((counter++))
            fi
        done
    fi

    # Write output
    echo "$content" > "$output_file"

    echo -e "${GREEN}✓ Generated: $(basename "$output_file")${NC}"
}

# Validate generated JSON syntax
validate_json() {
    local json_file="$1"
    local filename
    filename=$(basename "$json_file")

    if command -v jq > /dev/null 2>&1; then
        if jq empty "$json_file" 2>/dev/null; then
            echo -e "${GREEN}✓ Valid JSON: $filename${NC}"
            return 0
        else
            echo -e "${RED}ERROR: Invalid JSON syntax in $filename${NC}" >&2
            return 1
        fi
    else
        echo -e "${YELLOW}WARNING: jq not installed, skipping JSON validation for $filename${NC}"
        return 0
    fi
}

# Check for unsubstituted environment variables (except vault)
check_unsubstituted_vars() {
    local config_file="$1"
    local filename
    filename=$(basename "$config_file")

    # Look for ${...} patterns that are NOT ${vault:...}
    local unsubstituted
    unsubstituted=$(grep -o '\${[^v][^a][^u][^l][^t][^}]*}' "$config_file" 2>/dev/null || true)

    if [ -n "$unsubstituted" ]; then
        echo -e "${YELLOW}WARNING: Found unsubstituted environment variables in $filename:${NC}" >&2
        echo "$unsubstituted" | while read -r var; do
            echo -e "${YELLOW}  $var${NC}" >&2
        done
        return 1
    fi

    return 0
}

# Check that Vault placeholders still exist
check_vault_placeholders() {
    local config_file="$1"
    local filename
    filename=$(basename "$config_file")

    if grep -q '\${vault:secret/database:' "$config_file" 2>/dev/null; then
        echo -e "${GREEN}✓ Vault placeholders preserved in $filename${NC}"
        return 0
    else
        echo -e "${YELLOW}WARNING: No Vault placeholders found in $filename${NC}"
        echo -e "${YELLOW}  This might be intentional if you're not using Vault${NC}"
        return 0
    fi
}

# Main execution
main() {
    echo -e "${BLUE}================================================================${NC}"
    echo -e "${BLUE}Generating Connector Configurations from Templates${NC}"
    echo -e "${BLUE}================================================================${NC}"
    echo ""

    # Validate environment
    echo -e "${BLUE}Validating environment variables...${NC}"
    if ! validate_env_vars; then
        exit 1
    fi
    echo -e "${GREEN}✓ All required environment variables are set${NC}"
    echo ""

    # Create runtime directories
    echo -e "${BLUE}Creating runtime directories...${NC}"
    mkdir -p "$RUNTIME_DIR/debezium"
    mkdir -p "$RUNTIME_DIR/kafka-connect"
    echo -e "${GREEN}✓ Runtime directories created${NC}"
    echo ""

    # Generate Debezium source config
    echo -e "${BLUE}Generating Debezium Source Connector Config...${NC}"
    substitute_template \
        "$TEMPLATE_DIR/debezium/sqlserver-source.json.template" \
        "$RUNTIME_DIR/debezium/sqlserver-source.json"
    echo ""

    # Generate PostgreSQL sink config
    echo -e "${BLUE}Generating PostgreSQL Sink Connector Config...${NC}"
    substitute_template \
        "$TEMPLATE_DIR/kafka-connect/postgresql-sink.json.template" \
        "$RUNTIME_DIR/kafka-connect/postgresql-sink.json"
    echo ""

    # Validate generated configs
    echo -e "${BLUE}Validating generated configurations...${NC}"
    local validation_failed=0

    # Validate JSON syntax
    if ! validate_json "$RUNTIME_DIR/debezium/sqlserver-source.json"; then
        validation_failed=1
    fi
    if ! validate_json "$RUNTIME_DIR/kafka-connect/postgresql-sink.json"; then
        validation_failed=1
    fi

    # Check for unsubstituted vars
    if ! check_unsubstituted_vars "$RUNTIME_DIR/debezium/sqlserver-source.json"; then
        validation_failed=1
    fi
    if ! check_unsubstituted_vars "$RUNTIME_DIR/kafka-connect/postgresql-sink.json"; then
        validation_failed=1
    fi

    # Check Vault placeholders preserved
    check_vault_placeholders "$RUNTIME_DIR/debezium/sqlserver-source.json"
    check_vault_placeholders "$RUNTIME_DIR/kafka-connect/postgresql-sink.json"

    echo ""

    if [ $validation_failed -eq 1 ]; then
        echo -e "${YELLOW}================================================================${NC}"
        echo -e "${YELLOW}⚠  Config generation completed with warnings${NC}"
        echo -e "${YELLOW}================================================================${NC}"
        echo -e "${YELLOW}Please review the warnings above and fix any issues${NC}"
        echo ""
        return 1
    fi

    echo -e "${GREEN}================================================================${NC}"
    echo -e "${GREEN}✓ Connector configurations generated successfully!${NC}"
    echo -e "${GREEN}================================================================${NC}"
    echo ""
    echo -e "${BLUE}Generated files:${NC}"
    echo -e "  - $RUNTIME_DIR/debezium/sqlserver-source.json"
    echo -e "  - $RUNTIME_DIR/kafka-connect/postgresql-sink.json"
    echo ""
    echo -e "${BLUE}Configuration Summary:${NC}"
    echo -e "  Debezium Source:"
    echo -e "    - Tasks: ${DEBEZIUM_TASKS_MAX}"
    echo -e "    - Batch Size: ${DEBEZIUM_MAX_BATCH_SIZE}"
    echo -e "    - Queue Size: ${DEBEZIUM_MAX_QUEUE_SIZE}"
    echo -e "    - Poll Interval: ${DEBEZIUM_POLL_INTERVAL_MS}ms"
    echo -e "  PostgreSQL Sink:"
    echo -e "    - Tasks: ${SINK_TASKS_MAX}"
    echo -e "    - Batch Size: ${SINK_BATCH_SIZE}"
    echo -e "    - Connection Pool: ${SINK_CONNECTION_POOL_SIZE}"
    echo ""
    echo -e "${BLUE}Next steps:${NC}"
    echo -e "  1. Review generated configs: make show-config"
    echo -e "  2. Deploy connectors: make deploy"
    echo -e "     OR: ./scripts/bash/deploy-with-vault.sh"
    echo ""
}

main "$@"
