#!/bin/bash
#
# pause-resume.sh - Pause or resume Kafka Connect connectors
#
# This script allows pausing and resuming connectors for maintenance windows,
# schema changes, or troubleshooting without losing state.
#
# Usage:
#   ./pause-resume.sh <action> <connector-name>
#
# Actions:
#   pause    - Pause the connector (stop processing)
#   resume   - Resume the connector (restart processing)
#   status   - Show current connector status
#
# Examples:
#   # Pause connector for maintenance
#   ./pause-resume.sh pause postgresql-jdbc-sink
#
#   # Resume connector after maintenance
#   ./pause-resume.sh resume postgresql-jdbc-sink
#
#   # Check connector status
#   ./pause-resume.sh status postgresql-jdbc-sink
#
# Exit codes:
#   0 - Success
#   1 - Invalid arguments
#   2 - Connector not found
#   3 - Action failed

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
Usage: $0 <action> <connector-name>

Pause or resume Kafka Connect connectors for maintenance.

Actions:
  pause     Pause the connector (stops processing, retains state)
  resume    Resume the connector (restarts processing from last position)
  status    Show current connector status
  restart   Restart the connector (useful for failed connectors)

Arguments:
  connector-name    Name of the connector

Options:
  -h, --help        Show this help message
  --url URL         Kafka Connect REST API URL (default: http://localhost:8083)
  --all             Apply action to all connectors (with confirmation)

Examples:
  # Pause sink connector before PostgreSQL maintenance
  $0 pause postgresql-jdbc-sink

  # Resume after maintenance complete
  $0 resume postgresql-jdbc-sink

  # Check status before resuming
  $0 status postgresql-jdbc-sink

  # Restart a failed connector
  $0 restart sqlserver-cdc-source

  # Pause all connectors (with confirmation)
  $0 pause --all

Use Cases:
  - Database maintenance: Pause before DB upgrade, resume after
  - Schema changes: Pause, alter schema, update connector config, resume
  - Troubleshooting: Pause to investigate issues without losing offsets
  - Planned downtime: Pause during known system maintenance windows

Environment Variables:
  KAFKA_CONNECT_URL    Kafka Connect REST API URL (default: http://localhost:8083)

EOF
}

# Get connector status
get_connector_status() {
    local connector_name=$1
    curl -s "$KAFKA_CONNECT_URL/connectors/$connector_name/status"
}

# Display connector status
display_status() {
    local connector_name=$1

    log_info "Fetching status for '$connector_name'..."

    STATUS=$(get_connector_status "$connector_name")

    if [ $? -ne 0 ]; then
        log_error "Failed to fetch connector status"
        return 1
    fi

    # Check if connector exists
    if echo "$STATUS" | jq -e '.error_code' > /dev/null 2>&1; then
        log_error "Connector '$connector_name' not found"
        return 2
    fi

    # Parse status
    CONNECTOR_STATE=$(echo "$STATUS" | jq -r '.connector.state')
    CONNECTOR_WORKER=$(echo "$STATUS" | jq -r '.connector.worker_id')
    TASKS_COUNT=$(echo "$STATUS" | jq -r '.tasks | length')

    echo ""
    echo "Connector: $connector_name"
    echo "State: $CONNECTOR_STATE"
    echo "Worker: $CONNECTOR_WORKER"
    echo "Tasks: $TASKS_COUNT"
    echo ""
    echo "Task Details:"
    echo "$STATUS" | jq -r '.tasks[] | "  Task \(.id): \(.state) (worker: \(.worker_id))"'
    echo ""

    return 0
}

# Pause connector
pause_connector() {
    local connector_name=$1

    log_info "Pausing connector '$connector_name'..."

    RESPONSE=$(curl -s -X PUT "$KAFKA_CONNECT_URL/connectors/$connector_name/pause")

    if [ $? -ne 0 ]; then
        log_error "Failed to pause connector"
        return 3
    fi

    # Wait for pause to take effect
    log_info "Waiting for connector to pause..."
    sleep 3

    # Verify pause
    for i in {1..10}; do
        STATUS=$(get_connector_status "$connector_name")
        STATE=$(echo "$STATUS" | jq -r '.connector.state')

        if [ "$STATE" == "PAUSED" ]; then
            log_success "Connector '$connector_name' is now PAUSED"
            echo ""
            log_info "Connector is paused. Tasks will not process any new data."
            log_info "Offsets are preserved. Resume when ready with:"
            echo "  $0 resume $connector_name"
            return 0
        fi

        log_info "Waiting for pause state... (attempt $i/10, current: $STATE)"
        sleep 2
    done

    log_warning "Connector pause may still be in progress"
    display_status "$connector_name"
    return 0
}

# Resume connector
resume_connector() {
    local connector_name=$1

    log_info "Resuming connector '$connector_name'..."

    RESPONSE=$(curl -s -X PUT "$KAFKA_CONNECT_URL/connectors/$connector_name/resume")

    if [ $? -ne 0 ]; then
        log_error "Failed to resume connector"
        return 3
    fi

    # Wait for resume to take effect
    log_info "Waiting for connector to resume..."
    sleep 3

    # Verify resume
    for i in {1..10}; do
        STATUS=$(get_connector_status "$connector_name")
        STATE=$(echo "$STATUS" | jq -r '.connector.state')

        if [ "$STATE" == "RUNNING" ]; then
            log_success "Connector '$connector_name' is now RUNNING"
            echo ""
            display_status "$connector_name"
            return 0
        fi

        log_info "Waiting for running state... (attempt $i/10, current: $STATE)"
        sleep 3
    done

    log_warning "Connector may still be starting up"
    display_status "$connector_name"
    return 0
}

# Restart connector
restart_connector() {
    local connector_name=$1

    log_info "Restarting connector '$connector_name'..."

    RESPONSE=$(curl -s -X POST "$KAFKA_CONNECT_URL/connectors/$connector_name/restart")

    if [ $? -ne 0 ]; then
        log_error "Failed to restart connector"
        return 3
    fi

    log_success "Restart command sent"
    log_info "Waiting for connector to restart..."
    sleep 5

    display_status "$connector_name"
    return 0
}

# List all connectors
list_connectors() {
    log_info "Fetching connector list..."
    CONNECTORS=$(curl -s "$KAFKA_CONNECT_URL/connectors")

    if [ $? -ne 0 ]; then
        log_error "Failed to fetch connectors"
        return 1
    fi

    echo "$CONNECTORS" | jq -r '.[]'
}

# Apply action to all connectors
apply_to_all() {
    local action=$1

    log_warning "This will $action ALL connectors"
    read -p "Are you sure? Type 'yes' to confirm: " CONFIRM

    if [ "$CONFIRM" != "yes" ]; then
        log_info "Aborted by user"
        return 0
    fi

    CONNECTORS=$(list_connectors)

    if [ $? -ne 0 ]; then
        log_error "Failed to get connector list"
        return 1
    fi

    echo ""
    log_info "Applying '$action' to all connectors..."
    echo ""

    while IFS= read -r connector; do
        case $action in
            pause)
                pause_connector "$connector"
                ;;
            resume)
                resume_connector "$connector"
                ;;
            restart)
                restart_connector "$connector"
                ;;
        esac
        echo ""
    done <<< "$CONNECTORS"

    log_success "Action '$action' applied to all connectors"
}

# Parse arguments
ACTION=""
CONNECTOR_NAME=""
APPLY_ALL=false

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
        --all)
            APPLY_ALL=true
            shift
            ;;
        pause|resume|status|restart)
            ACTION="$1"
            shift
            ;;
        *)
            if [ -z "$CONNECTOR_NAME" ]; then
                CONNECTOR_NAME="$1"
            else
                log_error "Unknown argument: $1"
                show_help
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate arguments
if [ -z "$ACTION" ]; then
    log_error "Missing action (pause, resume, status, or restart)"
    show_help
    exit 1
fi

if [ "$APPLY_ALL" = false ] && [ -z "$CONNECTOR_NAME" ]; then
    log_error "Missing connector name (or use --all flag)"
    show_help
    exit 1
fi

# Check if Kafka Connect is reachable
if ! curl -s -f "$KAFKA_CONNECT_URL" > /dev/null; then
    log_error "Cannot reach Kafka Connect at $KAFKA_CONNECT_URL"
    log_info "Check if Kafka Connect is running:"
    echo "  docker ps --filter name=kafka-connect"
    exit 3
fi

# Execute action
if [ "$APPLY_ALL" = true ]; then
    apply_to_all "$ACTION"
else
    case $ACTION in
        pause)
            pause_connector "$CONNECTOR_NAME"
            ;;
        resume)
            resume_connector "$CONNECTOR_NAME"
            ;;
        status)
            display_status "$CONNECTOR_NAME"
            ;;
        restart)
            restart_connector "$CONNECTOR_NAME"
            ;;
        *)
            log_error "Unknown action: $ACTION"
            show_help
            exit 1
            ;;
    esac
fi

exit $?
