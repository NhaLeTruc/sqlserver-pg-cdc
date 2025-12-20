#!/bin/bash
################################################################################
# Diagnostic Collection Script
#
# Collects diagnostic information for troubleshooting:
# - Container status and logs
# - Kafka Connect status
# - Database connectivity
# - Metrics dump
# - System resources
#
# Usage:
#   ./collect-diagnostics.sh [--output-dir DIR]
#
# Output:
#   Creates a tarball with all diagnostic information
################################################################################

set -euo pipefail

# Configuration
OUTPUT_DIR="${1:-/tmp/cdc-diagnostics-$(date +%Y%m%d_%H%M%S)}"
TARBALL="/tmp/cdc-diagnostics-$(date +%Y%m%d_%H%M%S).tar.gz"

echo "================================================"
echo "CDC Pipeline Diagnostic Collection"
echo "================================================"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

################################################################################
# System Information
################################################################################
echo "[1/8] Collecting system information..."
{
    echo "=== System Information ==="
    echo "Timestamp: $(date)"
    echo "Hostname: $(hostname)"
    echo "Kernel: $(uname -a)"
    echo ""
    echo "=== Disk Usage ==="
    df -h
    echo ""
    echo "=== Memory Usage ==="
    free -h
    echo ""
    echo "=== CPU Information ==="
    lscpu | head -20
} > "$OUTPUT_DIR/system-info.txt" 2>&1

################################################################################
# Docker Information
################################################################################
echo "[2/8] Collecting Docker information..."
{
    echo "=== Docker Version ==="
    docker version
    echo ""
    echo "=== Docker Info ==="
    docker info
    echo ""
    echo "=== Running Containers ==="
    docker ps -a
    echo ""
    echo "=== Docker Networks ==="
    docker network ls
    echo ""
    echo "=== Docker Volumes ==="
    docker volume ls
} > "$OUTPUT_DIR/docker-info.txt" 2>&1

################################################################################
# Container Logs
################################################################################
echo "[3/8] Collecting container logs..."
mkdir -p "$OUTPUT_DIR/logs"

for CONTAINER in $(docker ps --format '{{.Names}}'); do
    echo "  - $CONTAINER"
    docker logs --tail 1000 "$CONTAINER" > "$OUTPUT_DIR/logs/${CONTAINER}.log" 2>&1 || true
done

################################################################################
# SQL Server Status
################################################################################
echo "[4/8] Collecting SQL Server status..."
if docker ps --format '{{.Names}}' | grep -q "sqlserver"; then
    {
        echo "=== SQL Server Status ==="
        docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
            -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
            -Q "SELECT @@VERSION" 2>&1 || echo "Failed to connect"

        echo ""
        echo "=== Databases ==="
        docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
            -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
            -Q "SELECT name, state_desc, recovery_model_desc FROM sys.databases" 2>&1 || echo "Failed to query"

        echo ""
        echo "=== CDC Status ==="
        docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
            -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
            -Q "SELECT name, is_cdc_enabled FROM sys.databases" 2>&1 || echo "Failed to query"
    } > "$OUTPUT_DIR/sqlserver-status.txt"
else
    echo "SQL Server container not running" > "$OUTPUT_DIR/sqlserver-status.txt"
fi

################################################################################
# PostgreSQL Status
################################################################################
echo "[5/8] Collecting PostgreSQL status..."
if docker ps --format '{{.Names}}' | grep -q "postgres"; then
    {
        echo "=== PostgreSQL Version ==="
        docker exec postgres psql -U postgres -c "SELECT version()" 2>&1 || echo "Failed to connect"

        echo ""
        echo "=== Databases ==="
        docker exec postgres psql -U postgres -c "\l" 2>&1 || echo "Failed to query"

        echo ""
        echo "=== Active Connections ==="
        docker exec postgres psql -U postgres -c \
            "SELECT datname, count(*) FROM pg_stat_activity GROUP BY datname" 2>&1 || echo "Failed to query"

        echo ""
        echo "=== Database Sizes ==="
        docker exec postgres psql -U postgres -c \
            "SELECT datname, pg_size_pretty(pg_database_size(datname)) FROM pg_database" 2>&1 || echo "Failed to query"
    } > "$OUTPUT_DIR/postgresql-status.txt"
else
    echo "PostgreSQL container not running" > "$OUTPUT_DIR/postgresql-status.txt"
fi

################################################################################
# Kafka Connect Status
################################################################################
echo "[6/8] Collecting Kafka Connect status..."
{
    echo "=== Kafka Connect Status ==="
    curl -s http://localhost:8083/ | jq '.' 2>&1 || echo "Failed to connect to Kafka Connect"

    echo ""
    echo "=== Connectors ==="
    curl -s http://localhost:8083/connectors | jq '.' 2>&1 || echo "Failed to list connectors"

    echo ""
    echo "=== Connector Status ==="
    for CONNECTOR in $(curl -s http://localhost:8083/connectors 2>/dev/null | jq -r '.[]' 2>/dev/null); do
        echo "--- $CONNECTOR ---"
        curl -s "http://localhost:8083/connectors/$CONNECTOR/status" | jq '.' 2>&1 || echo "Failed to get status"
        echo ""
    done
} > "$OUTPUT_DIR/kafka-connect-status.txt"

################################################################################
# Prometheus Metrics
################################################################################
echo "[7/8] Collecting Prometheus metrics..."
{
    echo "=== Reconciliation Metrics ==="
    curl -s http://localhost:9103/metrics 2>&1 || echo "Failed to fetch metrics"

    echo ""
    echo "=== Connector Metrics ==="
    curl -s http://localhost:9104/metrics 2>&1 || echo "Failed to fetch metrics"
} > "$OUTPUT_DIR/prometheus-metrics.txt"

################################################################################
# Reconciliation Reports
################################################################################
echo "[8/8] Collecting recent reconciliation reports..."
mkdir -p "$OUTPUT_DIR/reconciliation-reports"

# Copy recent reports if they exist
if [ -d "./reconciliation_reports" ]; then
    find ./reconciliation_reports -name "*.json" -mtime -7 -exec cp {} "$OUTPUT_DIR/reconciliation-reports/" \; 2>/dev/null || true
fi

################################################################################
# Create Tarball
################################################################################
echo ""
echo "Creating diagnostic tarball..."
tar -czf "$TARBALL" -C "$(dirname "$OUTPUT_DIR")" "$(basename "$OUTPUT_DIR")" 2>&1

# Clean up temporary directory
rm -rf "$OUTPUT_DIR"

echo ""
echo "================================================"
echo "✓ Diagnostic collection complete"
echo "================================================"
echo "Output file: $TARBALL"
echo "Size: $(du -h "$TARBALL" | cut -f1)"
echo ""
echo "To share for support:"
echo "  scp $TARBALL support@example.com:"
echo ""