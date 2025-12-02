# Quickstart Guide: SQL Server to PostgreSQL CDC Pipeline

**Feature**: SQL Server to PostgreSQL CDC Pipeline
**Branch**: 001-sqlserver-pg-cdc
**Last Updated**: 2025-12-02

## Overview

This guide walks you through setting up and operating the CDC pipeline from SQL Server to PostgreSQL. By the end of this guide, you'll have:

- A fully functional CDC pipeline running in Docker Compose
- Real-time replication of changes from SQL Server to PostgreSQL
- Monitoring dashboards showing pipeline health
- Tools for on-demand reconciliation and troubleshooting

**Time to complete**: 30-45 minutes

## Prerequisites

### Required Software
- Docker 24.0+ and Docker Compose 2.0+
- Python 3.11+ (for reconciliation tool)
- Bash 4.0+ or compatible shell
- curl (for REST API calls)
- jq (for JSON processing)

### Verify Prerequisites
```bash
# Check Docker
docker --version  # Should be 24.0+
docker compose version  # Should be 2.0+

# Check Python
python3 --version  # Should be 3.11+

# Check utilities
bash --version
curl --version
jq --version
```

### SQL Server Prerequisites
Your SQL Server instance must have:
- SQL Server 2019+ (or 2017 with CDC enabled)
- SQL Server Agent running (required for CDC cleanup)
- CDC enabled at database level
- Target tables have primary keys defined
- CDC enabled on target tables

**Enable CDC on SQL Server** (if not already done):
```sql
-- Enable CDC at database level
USE warehouse_source;
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on specific tables
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name = N'customers',
  @role_name = NULL,
  @supports_net_changes = 1;

EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name = N'orders',
  @role_name = NULL,
  @supports_net_changes = 1;

-- Verify CDC is enabled
SELECT name, is_cdc_enabled
FROM sys.databases
WHERE name = 'warehouse_source';

SELECT schema_name(t.schema_id), t.name, t.is_tracked_by_cdc
FROM sys.tables t
WHERE t.name IN ('customers', 'orders');
```

## Architecture Overview

```
SQL Server (CDC Tables)
         ↓
    Debezium Connector
         ↓
      Kafka Topics
         ↓
    JDBC Sink Connector
         ↓
     PostgreSQL

Monitoring: Prometheus → Grafana
Tracing: Jaeger
Secrets: HashiCorp Vault
```

## Step 1: Clone and Setup Repository

```bash
# Clone repository (if not already done)
cd /home/bob/WORK/sqlserver-pg-cdc

# Verify project structure
ls -la docker/
ls -la scripts/
ls -la src/
```

Expected directory structure:
```
.
├── docker/
│   ├── docker-compose.yml
│   └── configs/
│       ├── debezium/
│       ├── kafka-connect/
│       ├── prometheus/
│       ├── grafana/
│       └── vault/
├── scripts/
│   ├── bash/
│   └── python/
├── src/
│   └── reconciliation/
└── tests/
```

## Step 2: Configure Environment Variables

Create a `.env` file for local development:

```bash
cat > docker/.env <<EOF
# SQL Server Configuration
SQLSERVER_HOST=sqlserver
SQLSERVER_PORT=1433
SQLSERVER_DB=warehouse_source
SQLSERVER_USER=cdc_reader
SQLSERVER_PASSWORD=ChangeMe123!

# PostgreSQL Configuration
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=warehouse_target
POSTGRES_USER=cdc_writer
POSTGRES_PASSWORD=ChangeMe456!

# Kafka Configuration
KAFKA_BROKER=kafka:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# Kafka Connect Configuration
KAFKA_CONNECT_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_CONNECT_REST_PORT=8083

# Vault Configuration (dev mode)
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=root-token-dev

# Monitoring Configuration
PROMETHEUS_PORT=9090
GRAFANA_PORT=3000
JAEGER_PORT=16686
EOF
```

**IMPORTANT**: For production, replace plaintext passwords with Vault references.

## Step 3: Initialize HashiCorp Vault

Start Vault in dev mode (local only):

```bash
cd docker
docker compose up -d vault

# Wait for Vault to be ready
sleep 5

# Initialize Vault with database credentials
../scripts/bash/vault-init.sh \
  --sqlserver-host sqlserver \
  --sqlserver-port 1433 \
  --sqlserver-user cdc_reader \
  --sqlserver-password "ChangeMe123!" \
  --postgres-host postgres \
  --postgres-port 5432 \
  --postgres-user cdc_writer \
  --postgres-password "ChangeMe456!"

# Verify secrets are stored
docker exec vault vault kv get secret/db
```

Expected output:
```
====== Data ======
Key                  Value
---                  -----
postgres_host        postgres
postgres_password    ChangeMe456!
postgres_port        5432
postgres_user        cdc_writer
sqlserver_host       sqlserver
sqlserver_password   ChangeMe123!
sqlserver_port       1433
sqlserver_user       cdc_reader
```

## Step 4: Start the Full Stack

```bash
cd docker

# Start all services (SQL Server, Kafka, PostgreSQL, monitoring)
docker compose --profile dev up -d

# Check service health
docker compose ps

# Wait for all services to be healthy (may take 2-3 minutes)
watch 'docker compose ps'
```

Expected services:
- `sqlserver` - SQL Server 2019
- `postgres` - PostgreSQL 15
- `zookeeper` - Kafka coordination
- `kafka` - Kafka broker
- `schema-registry` - Confluent Schema Registry
- `kafka-connect` - Kafka Connect workers
- `vault` - HashiCorp Vault
- `prometheus` - Metrics collection
- `grafana` - Dashboards
- `jaeger` - Distributed tracing

## Step 5: Verify Service Health

```bash
# Check Kafka Connect is ready
curl http://localhost:8083/ | jq .

# Check Schema Registry
curl http://localhost:8081/subjects | jq .

# Check Prometheus
curl http://localhost:9090/-/healthy

# Access Grafana (default: admin/admin)
open http://localhost:3000

# Access Jaeger
open http://localhost:16686
```

## Step 6: Deploy Debezium Source Connector

```bash
# Deploy SQL Server CDC source connector
../scripts/bash/deploy-connector.sh \
  --config configs/debezium/sqlserver-source.json \
  --connect-url http://localhost:8083

# Verify connector is running
curl http://localhost:8083/connectors/sqlserver-cdc-source/status | jq .
```

Expected response:
```json
{
  "name": "sqlserver-cdc-source",
  "connector": {
    "state": "RUNNING",
    "worker_id": "kafka-connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "kafka-connect:8083"
    }
  ]
}
```

## Step 7: Verify Kafka Topics Created

```bash
# List Kafka topics
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe CDC topic
docker exec kafka kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic sqlserver.warehouse_source.dbo.customers

# Consume sample events (Ctrl+C to stop)
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic sqlserver.warehouse_source.dbo.customers \
  --from-beginning \
  --max-messages 5
```

## Step 8: Create PostgreSQL Target Tables

```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U cdc_writer -d warehouse_target

# Create target tables (matching SQL Server schema)
CREATE TABLE customers (
  id BIGINT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255),
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT,
  order_date TIMESTAMP,
  total_amount NUMERIC(10,2),
  status VARCHAR(50),
  FOREIGN KEY (customer_id) REFERENCES customers(id)
);

-- Verify tables created
\dt

-- Exit psql
\q
```

## Step 9: Deploy JDBC Sink Connector

```bash
# Deploy PostgreSQL JDBC sink connector
../scripts/bash/deploy-connector.sh \
  --config configs/kafka-connect/postgresql-sink.json \
  --connect-url http://localhost:8083

# Verify connector is running
curl http://localhost:8083/connectors/postgresql-jdbc-sink/status | jq .
```

## Step 10: Test End-to-End Replication

```bash
# Insert test data into SQL Server
docker exec -it sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U SA -P 'YourStrong@Passw0rd' \
  -d warehouse_source

# Inside sqlcmd:
INSERT INTO dbo.customers (id, name, email, created_at, is_active)
VALUES (1, 'Alice Johnson', 'alice@example.com', GETDATE(), 1);

INSERT INTO dbo.customers (id, name, email, created_at, is_active)
VALUES (2, 'Bob Smith', 'bob@example.com', GETDATE(), 1);

UPDATE dbo.customers SET email = 'alice.j@example.com' WHERE id = 1;

GO
QUIT

# Wait 10 seconds for replication
sleep 10

# Verify data in PostgreSQL
docker exec -it postgres psql -U cdc_writer -d warehouse_target \
  -c "SELECT * FROM customers ORDER BY id;"
```

Expected output:
```
 id |     name      |        email        |        created_at         | updated_at | is_active
----+---------------+---------------------+---------------------------+------------+-----------
  1 | Alice Johnson | alice.j@example.com | 2025-12-02 10:30:00.123   |            | t
  2 | Bob Smith     | bob@example.com     | 2025-12-02 10:30:01.456   |            | t
(2 rows)
```

## Step 11: Access Monitoring Dashboards

### Grafana Dashboard

1. Open http://localhost:3000
2. Login: `admin` / `admin` (change on first login)
3. Navigate to **Dashboards** → **CDC Pipeline Overview**

Key metrics to watch:
- Replication lag (seconds)
- Throughput (records/sec)
- Error rate (errors/min)
- Resource usage (CPU, memory)

### Prometheus Queries

Open http://localhost:9090 and try these queries:

```promql
# Replication lag
cdc_replication_lag_seconds{connector="sqlserver-cdc-source"}

# Throughput
rate(kafka_connect_source_task_poll_batch_avg_time_ms[5m])

# Connector task status
kafka_connect_connector_task_status

# Dead letter queue size
kafka_topic_partition_current_offset{topic="dlq-postgresql-sink"}
```

### Jaeger Tracing

1. Open http://localhost:16686
2. Select **Service**: `kafka-connect`
3. Click **Find Traces**
4. Explore distributed traces showing event flow from source to sink

## Step 12: Run Reconciliation

```bash
# Install Python dependencies
cd ../scripts/python
pip install -r requirements.txt

# Run on-demand reconciliation
python reconcile.py \
  --source-table dbo.customers \
  --target-table customers \
  --report-format json \
  --output /tmp/reconcile-report.json

# View report
cat /tmp/reconcile-report.json | jq .
```

Expected report:
```json
{
  "report_id": "abc123-def456-...",
  "generated_at": "2025-12-02T10:35:00Z",
  "tables": [
    {
      "source_table": "dbo.customers",
      "target_table": "customers",
      "source_row_count": 2,
      "target_row_count": 2,
      "row_count_match": true,
      "status": "ok",
      "discrepancies": []
    }
  ],
  "summary": {
    "total_tables": 1,
    "tables_ok": 1,
    "tables_with_discrepancies": 0
  }
}
```

## Common Management Operations

### Pause/Resume Connectors

```bash
# Pause replication
../scripts/bash/pause-resume.sh \
  --connector sqlserver-cdc-source \
  --action pause

# Resume replication
../scripts/bash/pause-resume.sh \
  --connector sqlserver-cdc-source \
  --action resume
```

### Scale Sink Connector

```bash
# Scale to 5 tasks (for higher throughput)
../scripts/bash/scale-connector.sh \
  --connector postgresql-jdbc-sink \
  --tasks 5
```

### Monitor Pipeline Health

```bash
# Get comprehensive health status
../scripts/bash/monitor.sh --format json

# Output:
# {
#   "connectors": [
#     {"name": "sqlserver-cdc-source", "status": "RUNNING", ...},
#     {"name": "postgresql-jdbc-sink", "status": "RUNNING", ...}
#   ],
#   "metrics": {
#     "replication_lag_seconds": 2.5,
#     "throughput_records_per_sec": 1250,
#     "error_rate": 0.0
#   }
# }
```

### Check Dead Letter Queue

```bash
# Check DLQ size
docker exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic dlq-postgresql-sink

# Consume DLQ messages to investigate failures
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dlq-postgresql-sink \
  --from-beginning \
  --property print.key=true
```

### View Connector Logs

```bash
# Tail Kafka Connect logs
docker compose logs -f kafka-connect

# Filter for specific connector
docker compose logs kafka-connect | grep sqlserver-cdc-source

# Check for errors
docker compose logs kafka-connect | grep -i error
```

## Testing Schema Evolution

### Add New Column to SQL Server Table

```bash
# Connect to SQL Server
docker exec -it sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U SA -P 'YourStrong@Passw0rd' \
  -d warehouse_source

# Add nullable column
ALTER TABLE dbo.customers ADD phone VARCHAR(20) NULL;
GO

# Insert data with new column
INSERT INTO dbo.customers (id, name, email, phone, created_at, is_active)
VALUES (3, 'Charlie Brown', 'charlie@example.com', '555-0123', GETDATE(), 1);
GO
QUIT

# Wait 30 seconds for schema detection
sleep 30

# Verify new column in PostgreSQL (should be auto-created if auto.evolve=true)
docker exec -it postgres psql -U cdc_writer -d warehouse_target \
  -c "\d customers"

# Verify new data includes phone number
docker exec -it postgres psql -U cdc_writer -d warehouse_target \
  -c "SELECT * FROM customers WHERE id = 3;"
```

## Troubleshooting

### Connector Fails to Start

**Symptom**: `curl http://localhost:8083/connectors/sqlserver-cdc-source/status` shows `FAILED`

**Solution**:
```bash
# Check connector error message
curl http://localhost:8083/connectors/sqlserver-cdc-source/status | jq '.tasks[0].trace'

# Common issues:
# 1. SQL Server CDC not enabled → Enable CDC on database and tables
# 2. Invalid credentials → Check Vault secrets
# 3. Network connectivity → Verify SQL Server is reachable from Kafka Connect container
```

### Replication Lag Increasing

**Symptom**: Grafana shows lag growing over time

**Solution**:
```bash
# Check if source connector is keeping up
curl http://localhost:8083/connectors/sqlserver-cdc-source/status

# Check if sink connector is falling behind (scale if needed)
../scripts/bash/scale-connector.sh --connector postgresql-jdbc-sink --tasks 5

# Check Kafka lag
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group connect-postgresql-jdbc-sink
```

### Dead Letter Queue Growing

**Symptom**: `dlq-postgresql-sink` topic has growing offset

**Solution**:
```bash
# Consume DLQ to see error details
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dlq-postgresql-sink \
  --from-beginning

# Common errors:
# - Schema mismatch: Manually alter PostgreSQL table to match SQL Server
# - Constraint violation: Check unique indexes, foreign keys
# - Data type mismatch: Adjust type mappings in connector config
```

### Missing Rows in PostgreSQL

**Symptom**: Row counts don't match between source and target

**Solution**:
```bash
# Run reconciliation to identify discrepancies
python ../scripts/python/reconcile.py \
  --source-table dbo.customers \
  --target-table customers \
  --verbose

# Check if connector offset is stale
curl http://localhost:8083/connectors/sqlserver-cdc-source/offsets

# Restart connector to re-sync
curl -X POST http://localhost:8083/connectors/sqlserver-cdc-source/restart
```

## Cleanup

### Stop Pipeline

```bash
cd docker

# Stop all services
docker compose --profile dev down

# Remove volumes (WARNING: deletes all data)
docker compose --profile dev down -v
```

### Reset Pipeline State

```bash
# Delete connector offsets (forces re-snapshot)
curl -X DELETE http://localhost:8083/connectors/sqlserver-cdc-source

# Delete Kafka topics
docker exec kafka kafka-topics.sh --delete \
  --bootstrap-server localhost:9092 \
  --topic sqlserver.warehouse_source.dbo.customers

# Recreate connector with snapshot.mode=initial
```

## Local Development and Testing

### Running the Test Suite

The project includes comprehensive tests for contract validation, integration testing, and end-to-end scenarios.

#### Prerequisites for Testing

```bash
# Install Python dependencies
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Install test dependencies
pip install pytest pytest-cov

# Ensure Docker services are running
cd docker
docker compose up -d

# Wait for all services to be healthy
./wait-for-services.sh 300
```

#### Run All Tests

```bash
# Run complete test suite with coverage
pytest tests/ -v --cov=src --cov-report=html --cov-report=term

# Results will show:
# - Contract tests: Validate connector configurations
# - Integration tests: Test replication, monitoring, schema evolution
# - Unit tests: Test reconciliation tool components
# - E2E tests: Test full pipeline workflow
```

#### Run Specific Test Categories

**Contract Tests** (validate connector configs):
```bash
pytest tests/contract/ -v

# Tests validate:
# - Debezium source connector configuration
# - JDBC sink connector configuration
# - Schema Registry integration
```

**Integration Tests** (test with live services):
```bash
pytest tests/integration/ -v

# Tests validate:
# - INSERT/UPDATE/DELETE replication
# - Monitoring metrics collection
# - Schema evolution handling
# - Error recovery and retry logic
```

**Unit Tests** (test reconciliation tool):
```bash
pytest tests/unit/ -v

# Tests validate:
# - Row count comparison
# - Checksum validation
# - Discrepancy reporting
```

**E2E Tests** (test complete workflows):
```bash
pytest tests/e2e/ -v

# Tests validate:
# - Docker Compose stack startup
# - Service health checks
# - Full reconciliation workflow
# - Failure scenario simulation
```

#### Running Tests in CI/CD

```bash
#!/bin/bash
# ci-test.sh - Example CI/CD test script

set -e

# Start services
cd docker
docker compose up -d

# Wait for services
./wait-for-services.sh 300

# Run tests
pytest tests/ \
  -v \
  --cov=src \
  --cov-report=xml \
  --cov-report=term \
  --junitxml=test-results.xml \
  --maxfail=5

# Generate coverage report
coverage html

# Check coverage threshold (80% required)
coverage report --fail-under=80

# Cleanup
docker compose down -v
```

#### Test Fixtures Available

The test suite provides pytest fixtures for common testing scenarios:

**Database Connections:**
```python
def test_sql_server_query(sqlserver_connection):
    cursor = sqlserver_connection.cursor()
    cursor.execute("SELECT * FROM customers")
    results = cursor.fetchall()
    assert len(results) > 0

def test_postgres_query(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute("SELECT * FROM customers")
    results = cursor.fetchall()
    assert len(results) > 0
```

**Kafka Connect Client:**
```python
def test_deploy_connector(kafka_connect_client):
    # Deploy a test connector
    config = {
        "name": "test-connector",
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "tasks.max": "1",
            # ... configuration
        }
    }

    result = kafka_connect_client.deploy_connector(config)
    assert result["name"] == "test-connector"

    # Verify connector is running
    status = kafka_connect_client.get_connector_status("test-connector")
    assert status["connector"]["state"] == "RUNNING"
```

**Automatic Cleanup:**
```python
def test_with_cleanup(
    sqlserver_cursor,
    postgres_cursor,
    kafka_connect_client,
    cleanup_test_tables,
    cleanup_test_connectors
):
    # Create test data
    sqlserver_cursor.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(50))")

    # Test logic here
    # ...

    # Cleanup happens automatically after test
```

#### Test Configuration

Tests use environment variables for configuration:

```bash
# Default values (can be overridden)
export SQLSERVER_HOST=localhost
export SQLSERVER_DATABASE=warehouse_source
export SQLSERVER_USER=sa
export SQLSERVER_PASSWORD='YourStrong!Passw0rd'

export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=warehouse_target
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres_secure_password

export KAFKA_CONNECT_URL=http://localhost:8083
export VAULT_ADDR=http://localhost:8200
export VAULT_TOKEN=dev-root-token
```

#### Debugging Failed Tests

**View detailed test output:**
```bash
pytest tests/ -vv --tb=long
```

**Run single test:**
```bash
pytest tests/integration/test_replication_flow.py::test_insert_replication -v
```

**Enable debug logging:**
```bash
pytest tests/ -v --log-cli-level=DEBUG
```

**Check service logs during test failure:**
```bash
# Kafka Connect logs
docker logs cdc-kafka-connect --tail 100

# SQL Server logs
docker logs cdc-sqlserver --tail 100

# PostgreSQL logs
docker logs cdc-postgres --tail 100
```

#### Performance Testing

```bash
# Run performance tests
pytest tests/integration/test_performance.py -v

# Test throughput (10K rows/sec requirement)
# Test replication lag (<5 minutes requirement)
# Test reconciliation speed (<10 minutes for 1M rows)
```

#### Code Coverage Requirements

Per NFR-008, all custom code must have 80% test coverage:

```bash
# Generate coverage report
pytest tests/ --cov=src --cov-report=html

# View report
open htmlcov/index.html

# Check coverage by module
coverage report --sort=cover
```

Expected coverage:
- `src/reconciliation/`: 90%+ (unit tests)
- `src/utils/`: 85%+ (integration tests)
- `scripts/python/`: 70%+ (E2E tests)

---

## Next Steps

1. **Production Deployment**: See `docs/operations.md` for production best practices
2. **Advanced Configuration**: Explore Kafka Connect SMTs for custom transformations
3. **Automated Testing**: Set up CI/CD pipeline with pytest integration
4. **CI/CD Integration**: Set up automated connector deployment via GitOps
5. **Scale Out**: Deploy multiple Kafka Connect workers for horizontal scaling

## Reference Links

- [Debezium SQL Server Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html)
- [Confluent JDBC Sink Connector Documentation](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html)
- [Kafka Connect REST API Reference](https://docs.confluent.io/platform/current/connect/references/restapi.html)
- [HashiCorp Vault Documentation](https://developer.hashicorp.com/vault/docs)

## Support

For issues or questions:
1. Check [docs/troubleshooting.md](docs/troubleshooting.md)
2. Review Kafka Connect logs: `docker compose logs kafka-connect`
3. Open GitHub issue with error details and steps to reproduce
