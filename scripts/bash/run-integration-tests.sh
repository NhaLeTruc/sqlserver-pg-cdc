#!/bin/bash
# Run integration tests for CDC pipeline
# This script sets up the environment and runs pytest integration tests

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
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

# Check if Docker Compose stack is running
check_services() {
    log_info "Checking if Docker Compose stack is running..."

    local services=(
        "cdc-sqlserver"
        "cdc-postgres"
        "cdc-kafka"
        "cdc-schema-registry"
        "cdc-kafka-connect"
        "cdc-vault"
    )

    for service in "${services[@]}"; do
        if ! docker ps --format '{{.Names}}' | grep -q "^${service}$"; then
            log_error "Service $service is not running"
            log_error "Please start the stack: cd docker && docker-compose up -d"
            exit 1
        fi
    done

    log_info "All required services are running"
}

# Wait for services to be healthy
wait_for_services() {
    log_info "Waiting for services to be healthy..."

    # Wait for Kafka Connect
    local max_attempts=60
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if curl -sf http://localhost:8083/connectors > /dev/null 2>&1; then
            log_info "Kafka Connect is ready"
            break
        fi
        log_warn "Waiting for Kafka Connect... (attempt $attempt/$max_attempts)"
        sleep 2
        attempt=$((attempt + 1))
    done

    if [ $attempt -gt $max_attempts ]; then
        log_error "Kafka Connect did not become ready within timeout"
        exit 1
    fi
}

# Install Python dependencies
install_dependencies() {
    log_info "Installing Python dependencies..."

    cd "$PROJECT_ROOT"

    if [ ! -d ".venv" ]; then
        log_info "Creating virtual environment..."
        python3.11 -m venv .venv
    fi

    source .venv/bin/activate

    log_info "Installing dependencies from requirements.txt..."
    pip install -q -r requirements.txt

    log_info "Installing development dependencies..."
    pip install -q pytest pytest-cov jsonschema

    log_info "Dependencies installed"
}

# Set environment variables for tests
set_test_env() {
    export SQLSERVER_HOST="localhost"
    export SQLSERVER_DATABASE="warehouse_source"
    export SQLSERVER_USER="sa"
    export SQLSERVER_PASSWORD="YourStrong!Passw0rd"
    export POSTGRES_HOST="localhost"
    export POSTGRES_PORT="5432"
    export POSTGRES_DB="warehouse_target"
    export POSTGRES_USER="postgres"
    export POSTGRES_PASSWORD="postgres_secure_password"
    export KAFKA_BROKER="localhost:29092"
    export KAFKA_CONNECT_URL="http://localhost:8083"
    export VAULT_ADDR="http://localhost:8200"
    export VAULT_TOKEN="dev-root-token"

    log_info "Environment variables set for tests"
}

# Run contract tests
run_contract_tests() {
    log_info "Running contract tests..."

    cd "$PROJECT_ROOT"
    source .venv/bin/activate

    if pytest tests/contract/ -v --tb=short; then
        log_info "Contract tests passed ✓"
        return 0
    else
        log_error "Contract tests failed ✗"
        return 1
    fi
}

# Run integration tests
run_integration_tests() {
    log_info "Running integration tests..."

    cd "$PROJECT_ROOT"
    source .venv/bin/activate

    # Note: Integration tests require connectors to be deployed
    log_warn "Integration tests require deployed connectors and may take 5-10 minutes"

    if pytest tests/integration/test_replication_flow.py -v --tb=short -s; then
        log_info "Replication flow tests passed ✓"
    else
        log_error "Replication flow tests failed ✗"
        return 1
    fi

    return 0
}

# Run performance tests
run_performance_tests() {
    log_info "Running performance tests..."

    cd "$PROJECT_ROOT"
    source .venv/bin/activate

    log_warn "Performance tests may take 15-30 minutes to complete"

    if pytest tests/integration/test_performance.py -v --tb=short -s; then
        log_info "Performance tests passed ✓"
    else
        log_error "Performance tests failed ✗"
        return 1
    fi

    return 0
}

# Main execution
main() {
    log_info "Starting CDC Pipeline Integration Tests"
    log_info "Project root: $PROJECT_ROOT"
    echo ""

    # Check services
    check_services
    wait_for_services

    # Install dependencies
    install_dependencies

    # Set environment
    set_test_env

    echo ""
    log_info "=== Running Test Suite ==="
    echo ""

    # Run tests in order
    local failed=0

    # Contract tests (fast)
    if ! run_contract_tests; then
        failed=$((failed + 1))
    fi

    echo ""

    # Integration tests (requires connectors)
    if [ "${SKIP_INTEGRATION:-false}" != "true" ]; then
        if ! run_integration_tests; then
            failed=$((failed + 1))
        fi
    else
        log_warn "Skipping integration tests (SKIP_INTEGRATION=true)"
    fi

    echo ""

    # Performance tests (slow)
    if [ "${SKIP_PERFORMANCE:-false}" != "true" ]; then
        if ! run_performance_tests; then
            failed=$((failed + 1))
        fi
    else
        log_warn "Skipping performance tests (SKIP_PERFORMANCE=true)"
    fi

    echo ""
    log_info "=== Test Summary ==="

    if [ $failed -eq 0 ]; then
        log_info "All test suites passed ✓"
        exit 0
    else
        log_error "$failed test suite(s) failed ✗"
        exit 1
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --contract-only)
            export SKIP_INTEGRATION=true
            export SKIP_PERFORMANCE=true
            shift
            ;;
        --skip-performance)
            export SKIP_PERFORMANCE=true
            shift
            ;;
        --help)
            cat <<EOF
Usage: $0 [OPTIONS]

Run integration tests for CDC pipeline.

OPTIONS:
    --contract-only       Run only contract tests (fast)
    --skip-performance    Skip performance tests (saves time)
    --help                Show this help message

EXAMPLES:
    # Run all tests
    $0

    # Run only contract tests
    $0 --contract-only

    # Run contract and integration tests, skip performance
    $0 --skip-performance

EOF
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

main
