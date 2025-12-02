#!/bin/bash
#
# scale-connector.sh - Scale Kafka Connect connector task parallelism
#
# This script adjusts the number of tasks for a connector to scale throughput.
# Useful for handling varying workloads and optimizing performance.
#
# Usage:
#   ./scale-connector.sh <connector-name> <tasks-max>
#
# Examples:
#   # Scale sink connector to 5 tasks
#   ./scale-connector.sh postgresql-jdbc-sink 5
#
#   # Scale down to 1 task
#   ./scale-connector.sh postgresql-jdbc-sink 1
#
# Exit codes:
#   0 - Success
#   1 - Invalid arguments
#   2 - Connector not found
#   3 - Failed to update connector

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"

# Print colored output
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Show help message
show_help() {
    cat << EOF
Usage: $0 <connector-name> <tasks-max>

Scale Kafka Connect connector task parallelism.

Arguments:
  connector-name    Name of the connector to scale
  tasks-max         Number of tasks (1-10 recommended)

Options:
  -h, --help        Show this help message
  --url URL         Kafka Connect REST API URL (default: http://localhost:8083)

Examples:
  # Scale PostgreSQL sink connector to 5 parallel tasks
  $0 postgresql-jdbc-sink 5

  # Scale down to single task
  $0 postgresql-jdbc-sink 1

  # Use custom Kafka Connect URL
  $0 --url http://kafka-connect:8083 postgresql-jdbc-sink 3

Notes:
  - Source connectors for SQL Server CDC must remain at tasks.max=1
  - Sink connectors can scale based on topic partitions
  - Recommended: 1 task per CPU core, max 10 tasks
  - Scaling triggers a connector restart
  - Monitor performance after scaling using Grafana dashboards

Environment Variables:
  KAFKA_CONNECT_URL    Kafka Connect REST API URL (default: http://localhost:8083)

EOF
}

# Parse arguments
CONNECTOR_NAME=""
TASKS_MAX=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        --url)
            KAFKA_CONNECT_URL="$2"
            shift 2
            ;;
        *)
            if [ -z "$CONNECTOR_NAME" ]; then
                CONNECTOR_NAME="$1"
            elif [ -z "$TASKS_MAX" ]; then
                TASKS_MAX="$1"
            else
                log_error "Too many arguments"
                show_help
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate arguments
if [ -z "$CONNECTOR_NAME" ] || [ -z "$TASKS_MAX" ]; then
    log_error "Missing required arguments"
    show_help
    exit 1
fi

# Validate tasks.max is a number
if ! [[ "$TASKS_MAX" =~ ^[0-9]+$ ]]; then
    log_error "tasks.max must be a positive integer"
    exit 1
fi

# Validate tasks.max range
if [ "$TASKS_MAX" -lt 1 ] || [ "$TASKS_MAX" -gt 50 ]; then
    log_error "tasks.max must be between 1 and 50"
    exit 1
fi

# Check if connector exists
log_info "Checking if connector '$CONNECTOR_NAME' exists..."

CONNECTOR_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME")

if [ "$CONNECTOR_STATUS" -ne 200 ]; then
    log_error "Connector '$CONNECTOR_NAME' not found"
    log_info "Available connectors:"
    curl -s "$KAFKA_CONNECT_URL/connectors" | jq -r '.[]' || echo "  (failed to fetch connectors)"
    exit 2
fi

log_success "Connector '$CONNECTOR_NAME' found"

# Get current configuration
log_info "Fetching current connector configuration..."

CURRENT_CONFIG=$(curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/config")

if [ $? -ne 0 ]; then
    log_error "Failed to fetch connector configuration"
    exit 3
fi

CURRENT_TASKS=$(echo "$CURRENT_CONFIG" | jq -r '.["tasks.max"]')

log_info "Current tasks.max: $CURRENT_TASKS"
log_info "New tasks.max: $TASKS_MAX"

# Warn if scaling SQL Server source connector
CONNECTOR_CLASS=$(echo "$CURRENT_CONFIG" | jq -r '.["connector.class"]')

if [[ "$CONNECTOR_CLASS" == *"SqlServerConnector"* ]] && [ "$TASKS_MAX" -gt 1 ]; then
    log_warning "SQL Server CDC source connectors should use tasks.max=1"
    log_warning "Multiple tasks can cause duplicate events"
    read -p "Continue anyway? (yes/no): " CONFIRM
    if [[ ! "$CONFIRM" =~ ^[Yy][Ee]?[Ss]?$ ]]; then
        log_info "Aborted by user"
        exit 0
    fi
fi

# Update configuration
log_info "Updating connector configuration..."

# Update the config JSON
UPDATED_CONFIG=$(echo "$CURRENT_CONFIG" | jq --arg tasks "$TASKS_MAX" '.["tasks.max"] = $tasks')

# Apply the updated configuration
RESPONSE=$(curl -s -X PUT \
    -H "Content-Type: application/json" \
    -d "$UPDATED_CONFIG" \
    "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/config")

if [ $? -ne 0 ]; then
    log_error "Failed to update connector configuration"
    exit 3
fi

# Check if update was successful
if echo "$RESPONSE" | jq -e '.error_code' > /dev/null 2>&1; then
    log_error "Failed to update connector:"
    echo "$RESPONSE" | jq '.'
    exit 3
fi

log_success "Connector configuration updated"

# Wait for connector to stabilize
log_info "Waiting for connector to restart..."
sleep 5

# Check connector status
for i in {1..12}; do
    STATUS=$(curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status")
    STATE=$(echo "$STATUS" | jq -r '.connector.state')
    TASKS_COUNT=$(echo "$STATUS" | jq -r '.tasks | length')

    if [ "$STATE" == "RUNNING" ] && [ "$TASKS_COUNT" -eq "$TASKS_MAX" ]; then
        log_success "Connector is RUNNING with $TASKS_COUNT tasks"
        echo ""
        log_info "Task Status:"
        echo "$STATUS" | jq -r '.tasks[] | "  Task \(.id): \(.state)"'
        echo ""
        log_success "Scaling completed successfully!"
        echo ""
        log_info "Monitor performance:"
        echo "  - Grafana: http://localhost:3000/d/kafka-connect"
        echo "  - Connector status: curl $KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status | jq"
        exit 0
    fi

    log_info "Waiting for connector to stabilize... (attempt $i/12)"
    sleep 5
done

# Timeout - check final status
log_warning "Connector did not stabilize within expected time"
log_info "Current status:"
curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq '.'

log_info "Check logs for details:"
echo "  docker logs cdc-kafka-connect --tail 50"

exit 3
