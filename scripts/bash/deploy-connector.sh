#!/bin/bash
# Deploy Kafka Connect connectors via REST API
# Supports Debezium source and JDBC sink connectors

set -euo pipefail

KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    cat <<EOF
Usage: $0 [OPTIONS] <connector-config-file>

Deploy a Kafka Connect connector via REST API.

OPTIONS:
    -u, --url URL          Kafka Connect REST API URL (default: $KAFKA_CONNECT_URL)
    -d, --delete           Delete the connector instead of creating/updating
    -s, --status           Check connector status only
    -h, --help             Show this help message

ARGUMENTS:
    connector-config-file  Path to connector JSON configuration file

EXAMPLES:
    # Deploy Debezium source connector
    $0 docker/configs/debezium/sqlserver-source.json

    # Deploy JDBC sink connector
    $0 docker/configs/kafka-connect/postgresql-sink.json

    # Delete a connector
    $0 --delete docker/configs/debezium/sqlserver-source.json

    # Check connector status
    $0 --status docker/configs/debezium/sqlserver-source.json

EOF
    exit 1
}

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

wait_for_kafka_connect() {
    local max_attempts=30
    local attempt=1

    log_info "Waiting for Kafka Connect to be ready at $KAFKA_CONNECT_URL..."

    while [ $attempt -le $max_attempts ]; do
        if curl -sf "$KAFKA_CONNECT_URL" > /dev/null 2>&1; then
            log_info "Kafka Connect is ready!"
            return 0
        fi
        log_warn "Attempt $attempt/$max_attempts: Kafka Connect not ready yet..."
        sleep 2
        attempt=$((attempt + 1))
    done

    log_error "Kafka Connect did not become ready within timeout"
    return 1
}

get_connector_name() {
    local config_file="$1"
    jq -r '.name' "$config_file"
}

check_connector_status() {
    local connector_name="$1"

    log_info "Checking status for connector: $connector_name"

    if ! curl -sf "$KAFKA_CONNECT_URL/connectors/$connector_name/status" | jq .; then
        log_error "Connector $connector_name not found"
        return 1
    fi

    return 0
}

delete_connector() {
    local connector_name="$1"

    log_info "Deleting connector: $connector_name"

    if curl -sf "$KAFKA_CONNECT_URL/connectors/$connector_name" > /dev/null; then
        if curl -sf -X DELETE "$KAFKA_CONNECT_URL/connectors/$connector_name"; then
            log_info "Connector $connector_name deleted successfully"
            return 0
        else
            log_error "Failed to delete connector $connector_name"
            return 1
        fi
    else
        log_warn "Connector $connector_name does not exist"
        return 0
    fi
}

deploy_connector() {
    local config_file="$1"
    local connector_name
    connector_name=$(get_connector_name "$config_file")

    log_info "Deploying connector: $connector_name"
    log_info "Config file: $config_file"

    # Check if connector already exists
    if curl -sf "$KAFKA_CONNECT_URL/connectors/$connector_name" > /dev/null 2>&1; then
        log_warn "Connector $connector_name already exists. Updating configuration..."

        # Update existing connector
        # Extract just the config object for PUT requests
        local config_only
        config_only=$(jq '.config' "$config_file")

        if curl -sf -X PUT \
            -H "Content-Type: application/json" \
            -d "$config_only" \
            "$KAFKA_CONNECT_URL/connectors/$connector_name/config" | jq .; then
            log_info "Connector $connector_name updated successfully"
        else
            log_error "Failed to update connector $connector_name"
            return 1
        fi
    else
        log_info "Creating new connector: $connector_name"

        # Create new connector
        if curl -sf -X POST \
            -H "Content-Type: application/json" \
            -d "@$config_file" \
            "$KAFKA_CONNECT_URL/connectors" | jq .; then
            log_info "Connector $connector_name created successfully"
        else
            log_error "Failed to create connector $connector_name"
            return 1
        fi
    fi

    # Wait a moment for connector to initialize
    sleep 2

    # Check connector status
    log_info "Checking connector status..."
    if check_connector_status "$connector_name"; then
        local status
        status=$(curl -sf "$KAFKA_CONNECT_URL/connectors/$connector_name/status" | jq -r '.connector.state')

        if [ "$status" = "RUNNING" ]; then
            log_info "Connector $connector_name is RUNNING"
            return 0
        else
            log_error "Connector $connector_name is in state: $status"
            log_error "Check logs for details: docker logs cdc-kafka-connect"
            return 1
        fi
    fi

    return 1
}

# Parse command line arguments
ACTION="deploy"
CONFIG_FILE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -u|--url)
            KAFKA_CONNECT_URL="$2"
            shift 2
            ;;
        -d|--delete)
            ACTION="delete"
            shift
            ;;
        -s|--status)
            ACTION="status"
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            CONFIG_FILE="$1"
            shift
            ;;
    esac
done

# Validate arguments
if [ -z "$CONFIG_FILE" ]; then
    log_error "Connector config file is required"
    usage
fi

if [ ! -f "$CONFIG_FILE" ]; then
    log_error "Config file not found: $CONFIG_FILE"
    exit 1
fi

# Wait for Kafka Connect to be ready
if ! wait_for_kafka_connect; then
    exit 1
fi

# Execute action
CONNECTOR_NAME=$(get_connector_name "$CONFIG_FILE")

case $ACTION in
    deploy)
        deploy_connector "$CONFIG_FILE"
        ;;
    delete)
        delete_connector "$CONNECTOR_NAME"
        ;;
    status)
        check_connector_status "$CONNECTOR_NAME"
        ;;
    *)
        log_error "Unknown action: $ACTION"
        exit 1
        ;;
esac
