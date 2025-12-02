#!/bin/bash
#
# wait-for-services.sh - Wait for all Docker Compose services to be healthy
#
# This script waits for all services in the CDC pipeline to become healthy
# before proceeding. Useful for CI/CD pipelines and automated testing.
#
# Usage:
#   ./wait-for-services.sh [timeout_seconds]
#
# Exit codes:
#   0 - All services are healthy
#   1 - Timeout reached before all services became healthy
#   2 - Error checking service status

set -e

# Configuration
TIMEOUT=${1:-300}  # Default 5 minutes
CHECK_INTERVAL=5   # Check every 5 seconds
COMPOSE_FILE="$(dirname "$0")/docker-compose.yml"

# Required services and their health check criteria
declare -A SERVICE_CHECKS=(
    ["cdc-sqlserver"]="health"
    ["cdc-postgres"]="health"
    ["cdc-zookeeper"]="health"
    ["cdc-kafka"]="health"
    ["cdc-schema-registry"]="health"
    ["cdc-kafka-connect"]="health"
    ["cdc-vault"]="running"  # Vault in dev mode doesn't have health check
    ["cdc-prometheus"]="health"
    ["cdc-grafana"]="health"
    ["cdc-jaeger"]="running"  # Jaeger may not have health check
)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Check if Docker Compose is available
check_docker_compose() {
    if command -v docker-compose &> /dev/null; then
        COMPOSE_CMD="docker-compose"
    elif docker compose version &> /dev/null; then
        COMPOSE_CMD="docker compose"
    else
        log_error "Docker Compose not found. Please install docker-compose or use Docker Compose V2."
        exit 2
    fi
}

# Get service status
get_service_status() {
    local service_name=$1
    local status

    # Try to get health status
    status=$(docker inspect --format='{{.State.Health.Status}}' "$service_name" 2>/dev/null || echo "none")

    if [ "$status" = "none" ] || [ "$status" = "<no value>" ]; then
        # No health check, check if running
        status=$(docker inspect --format='{{.State.Status}}' "$service_name" 2>/dev/null || echo "not_found")
    fi

    echo "$status"
}

# Check if all services are healthy
check_all_services() {
    local all_healthy=true
    local service_statuses=()

    for service in "${!SERVICE_CHECKS[@]}"; do
        local status=$(get_service_status "$service")
        local expected="${SERVICE_CHECKS[$service]}"

        service_statuses+=("$service:$status:$expected")

        if [ "$expected" = "health" ]; then
            if [ "$status" != "healthy" ]; then
                all_healthy=false
            fi
        elif [ "$expected" = "running" ]; then
            if [ "$status" != "running" ]; then
                all_healthy=false
            fi
        fi
    done

    # Print status table
    if [ "$all_healthy" = false ]; then
        echo ""
        printf "%-25s %-15s %-15s %-10s\n" "SERVICE" "CURRENT" "EXPECTED" "STATUS"
        printf "%-25s %-15s %-15s %-10s\n" "-------" "-------" "--------" "------"

        for entry in "${service_statuses[@]}"; do
            IFS=':' read -r service status expected <<< "$entry"
            local status_symbol

            if [ "$expected" = "health" ] && [ "$status" = "healthy" ]; then
                status_symbol="${GREEN}✓${NC}"
            elif [ "$expected" = "running" ] && [ "$status" = "running" ]; then
                status_symbol="${GREEN}✓${NC}"
            else
                status_symbol="${RED}✗${NC}"
            fi

            printf "%-25s %-15s %-15s %-10s\n" "$service" "$status" "$expected" "$status_symbol"
        done
        echo ""
    fi

    [ "$all_healthy" = true ]
}

# Main waiting loop
wait_for_services() {
    local elapsed=0
    local start_time=$(date +%s)

    log_info "Waiting for services to become healthy (timeout: ${TIMEOUT}s)..."
    echo ""

    while [ $elapsed -lt $TIMEOUT ]; do
        if check_all_services; then
            log_success "All services are healthy!"
            echo ""
            log_info "Service startup completed in ${elapsed}s"
            return 0
        fi

        sleep $CHECK_INTERVAL
        elapsed=$(($(date +%s) - start_time))

        # Print progress
        if [ $((elapsed % 30)) -eq 0 ]; then
            log_info "Still waiting... (${elapsed}s / ${TIMEOUT}s elapsed)"
        fi
    done

    log_error "Timeout reached after ${TIMEOUT}s. Not all services are healthy."
    echo ""
    log_info "Checking individual service logs for errors:"
    echo ""

    # Show logs for unhealthy services
    for service in "${!SERVICE_CHECKS[@]}"; do
        local status=$(get_service_status "$service")
        local expected="${SERVICE_CHECKS[$service]}"

        if [ "$expected" = "health" ] && [ "$status" != "healthy" ]; then
            log_warning "Logs for $service (last 20 lines):"
            docker logs "$service" --tail 20 2>&1 || true
            echo ""
        elif [ "$expected" = "running" ] && [ "$status" != "running" ]; then
            log_warning "Logs for $service (last 20 lines):"
            docker logs "$service" --tail 20 2>&1 || true
            echo ""
        fi
    done

    return 1
}

# Display help
show_help() {
    cat << EOF
Usage: $0 [OPTIONS] [TIMEOUT]

Wait for all Docker Compose services to become healthy.

Arguments:
  TIMEOUT    Maximum time to wait in seconds (default: 300)

Options:
  -h, --help     Show this help message

Examples:
  # Wait up to 5 minutes (default)
  $0

  # Wait up to 10 minutes
  $0 600

  # Use in CI/CD pipeline
  if $0 300; then
    echo "Services ready, running tests..."
    pytest tests/
  else
    echo "Services failed to start"
    exit 1
  fi

Services monitored:
  - SQL Server (cdc-sqlserver)
  - PostgreSQL (cdc-postgres)
  - Zookeeper (cdc-zookeeper)
  - Kafka (cdc-kafka)
  - Schema Registry (cdc-schema-registry)
  - Kafka Connect (cdc-kafka-connect)
  - Vault (cdc-vault)
  - Prometheus (cdc-prometheus)
  - Grafana (cdc-grafana)
  - Jaeger (cdc-jaeger)

EOF
}

# Parse arguments
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_help
    exit 0
fi

# Main execution
main() {
    log_info "CDC Pipeline Service Health Check"
    echo ""

    # Check Docker Compose availability
    check_docker_compose

    # Verify services are started
    log_info "Checking if services are started..."

    local started_services=$(docker ps --filter "name=cdc-" --format "{{.Names}}" | wc -l)

    if [ "$started_services" -eq 0 ]; then
        log_error "No services found. Please start services with:"
        echo "  cd docker && docker-compose up -d"
        exit 2
    fi

    log_info "Found $started_services running services"
    echo ""

    # Wait for services to be healthy
    if wait_for_services; then
        log_success "✓ All services are ready!"
        echo ""
        log_info "You can now:"
        echo "  - Deploy connectors: ./scripts/bash/deploy-connector.sh <config.json>"
        echo "  - Run tests: pytest tests/"
        echo "  - Access Grafana: http://localhost:3000 (admin/admin_secure_password)"
        echo "  - Access Kafka Connect: http://localhost:8083"
        exit 0
    else
        log_error "✗ Service health check failed"
        echo ""
        log_info "Troubleshooting:"
        echo "  - Check logs: docker logs <service-name>"
        echo "  - Restart services: cd docker && docker-compose restart"
        echo "  - Check resources: docker stats"
        exit 1
    fi
}

# Run main function
main
