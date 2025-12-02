#!/bin/bash
# Monitor CDC pipeline status and metrics
# Query connector status, metrics, and health via REST APIs

set -euo pipefail

KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_section() {
    echo -e "${BLUE}=== $* ===${NC}"
}

usage() {
    cat <<EOF
Usage: $0 [OPTIONS] [COMMAND]

Monitor CDC pipeline health, metrics, and alerts.

COMMANDS:
    status          Show connector status (default)
    metrics         Show key metrics from Prometheus
    alerts          Show active alerts
    health          Overall pipeline health check
    lag             Show replication lag
    errors          Show error rates
    dashboards      List available Grafana dashboards

OPTIONS:
    -u, --url URL          Kafka Connect URL (default: $KAFKA_CONNECT_URL)
    -p, --prometheus URL   Prometheus URL (default: $PROMETHEUS_URL)
    -h, --help             Show this help message

EXAMPLES:
    # Show connector status
    $0 status

    # Show metrics
    $0 metrics

    # Show replication lag
    $0 lag

    # Overall health check
    $0 health

EOF
    exit 1
}

query_prometheus() {
    local query="$1"
    curl -s "${PROMETHEUS_URL}/api/v1/query" \
        --data-urlencode "query=${query}" \
        2>/dev/null | jq -r '.data.result[]'
}

show_connector_status() {
    log_section "Connector Status"

    # List all connectors
    local connectors
    connectors=$(curl -s "${KAFKA_CONNECT_URL}/connectors" 2>/dev/null)

    if [ -z "$connectors" ]; then
        log_error "Failed to connect to Kafka Connect at $KAFKA_CONNECT_URL"
        return 1
    fi

    echo "$connectors" | jq -r '.[]' | while read -r connector; do
        echo ""
        echo "Connector: $connector"

        # Get connector status
        local status
        status=$(curl -s "${KAFKA_CONNECT_URL}/connectors/${connector}/status" 2>/dev/null)

        local connector_state
        connector_state=$(echo "$status" | jq -r '.connector.state')

        if [ "$connector_state" = "RUNNING" ]; then
            echo -e "  State: ${GREEN}${connector_state}${NC}"
        else
            echo -e "  State: ${RED}${connector_state}${NC}"
        fi

        # Show task status
        echo "  Tasks:"
        echo "$status" | jq -r '.tasks[] | "    Task \(.id): \(.state) (worker: \(.worker_id))"'

        # Show error if any
        local error_msg
        error_msg=$(echo "$status" | jq -r '.connector.trace // empty')
        if [ -n "$error_msg" ]; then
            echo -e "  ${RED}Error: $error_msg${NC}"
        fi
    done
}

show_metrics() {
    log_section "Key Metrics"

    echo ""
    echo "Source Connector Throughput:"
    query_prometheus 'rate(kafka_connect_source_task_source_record_poll_total[1m])' | \
        jq -r '"\(.metric.connector): \(.value[1]) records/sec"' || echo "  No data"

    echo ""
    echo "Sink Connector Throughput:"
    query_prometheus 'rate(kafka_connect_sink_task_sink_record_send_total[1m])' | \
        jq -r '"\(.metric.connector): \(.value[1]) records/sec"' || echo "  No data"

    echo ""
    echo "Error Rate:"
    query_prometheus 'rate(kafka_connect_task_error_total[5m])' | \
        jq -r '"\(.metric.connector): \(.value[1]) errors/sec"' || echo "  No errors"

    echo ""
    echo "Kafka Topics:"
    query_prometheus 'kafka_log_log_size' | \
        jq -r '"\(.metric.topic): \(.value[1]) messages"' | head -10 || echo "  No data"
}

show_replication_lag() {
    log_section "Replication Lag"

    echo ""
    echo "Records Lag (Source vs Sink):"

    local source_total
    source_total=$(query_prometheus 'kafka_connect_source_task_source_record_poll_total' | \
        jq -r '.value[1]' | head -1)

    local sink_total
    sink_total=$(query_prometheus 'kafka_connect_sink_task_sink_record_send_total' | \
        jq -r '.value[1]' | head -1)

    if [ -n "$source_total" ] && [ -n "$sink_total" ]; then
        local lag=$((source_total - sink_total))
        echo "  Source: $source_total records"
        echo "  Sink: $sink_total records"

        if [ $lag -gt 1000 ]; then
            echo -e "  Lag: ${RED}${lag} records${NC} (HIGH)"
        elif [ $lag -gt 100 ]; then
            echo -e "  Lag: ${YELLOW}${lag} records${NC} (MEDIUM)"
        else
            echo -e "  Lag: ${GREEN}${lag} records${NC} (OK)"
        fi
    else
        echo "  No lag data available (connectors may not be running)"
    fi
}

show_errors() {
    log_section "Error Rates"

    echo ""
    echo "Connector Errors (last 5 minutes):"
    query_prometheus 'increase(kafka_connect_task_error_total[5m])' | \
        jq -r '"\(.metric.connector)/\(.metric.task): \(.value[1]) errors"' || echo "  No errors"

    echo ""
    echo "Failed Connector Tasks:"
    query_prometheus 'kafka_connect_task_status{state="FAILED"}' | \
        jq -r '"\(.metric.connector)/\(.metric.task): FAILED"' || echo "  No failed tasks"
}

show_alerts() {
    log_section "Active Alerts"

    local alerts
    alerts=$(curl -s "${PROMETHEUS_URL}/api/v1/alerts" 2>/dev/null)

    if [ -z "$alerts" ]; then
        log_error "Failed to connect to Prometheus at $PROMETHEUS_URL"
        return 1
    fi

    echo ""
    echo "$alerts" | jq -r '.data.alerts[] | "\(.labels.alertname): \(.state) - \(.annotations.summary // .annotations.description)"' || \
        echo "No active alerts"
}

show_health() {
    log_section "Pipeline Health Check"

    local exit_code=0

    echo ""
    echo "Service Health:"

    # Check Kafka Connect
    if curl -sf "${KAFKA_CONNECT_URL}" > /dev/null 2>&1; then
        echo -e "  Kafka Connect: ${GREEN}UP${NC}"
    else
        echo -e "  Kafka Connect: ${RED}DOWN${NC}"
        exit_code=1
    fi

    # Check Prometheus
    if curl -sf "${PROMETHEUS_URL}/-/healthy" > /dev/null 2>&1; then
        echo -e "  Prometheus: ${GREEN}UP${NC}"
    else
        echo -e "  Prometheus: ${RED}DOWN${NC}"
        exit_code=1
    fi

    # Check Grafana
    if curl -sf "${GRAFANA_URL}/api/health" > /dev/null 2>&1; then
        echo -e "  Grafana: ${GREEN}UP${NC}"
    else
        echo -e "  Grafana: ${RED}DOWN${NC}"
        exit_code=1
    fi

    echo ""
    echo "Connector Health:"

    # Check connector states
    local connectors
    connectors=$(curl -s "${KAFKA_CONNECT_URL}/connectors" 2>/dev/null | jq -r '.[]')

    for connector in $connectors; do
        local state
        state=$(curl -s "${KAFKA_CONNECT_URL}/connectors/${connector}/status" 2>/dev/null | \
            jq -r '.connector.state')

        if [ "$state" = "RUNNING" ]; then
            echo -e "  ${connector}: ${GREEN}${state}${NC}"
        else
            echo -e "  ${connector}: ${RED}${state}${NC}"
            exit_code=1
        fi
    done

    echo ""
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}Overall Status: HEALTHY${NC}"
    else
        echo -e "${RED}Overall Status: UNHEALTHY${NC}"
    fi

    return $exit_code
}

show_dashboards() {
    log_section "Grafana Dashboards"

    echo ""
    echo "Available dashboards:"
    echo "  - CDC Pipeline Overview: ${GRAFANA_URL}/d/cdc-pipeline"
    echo "  - Kafka Connect Metrics: ${GRAFANA_URL}/d/kafka-connect"
    echo ""
    echo "Access Grafana: ${GRAFANA_URL}"
    echo "Default credentials: admin / admin_secure_password"
}

# Parse arguments
COMMAND="status"

while [[ $# -gt 0 ]]; do
    case $1 in
        -u|--url)
            KAFKA_CONNECT_URL="$2"
            shift 2
            ;;
        -p|--prometheus)
            PROMETHEUS_URL="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        status|metrics|alerts|health|lag|errors|dashboards)
            COMMAND="$1"
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Execute command
case $COMMAND in
    status)
        show_connector_status
        ;;
    metrics)
        show_metrics
        ;;
    alerts)
        show_alerts
        ;;
    health)
        show_health
        ;;
    lag)
        show_replication_lag
        ;;
    errors)
        show_errors
        ;;
    dashboards)
        show_dashboards
        ;;
    *)
        log_error "Unknown command: $COMMAND"
        usage
        ;;
esac
