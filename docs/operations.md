# Operations Guide: SQL Server to PostgreSQL CDC Pipeline

This guide provides step-by-step instructions for deploying, operating, and troubleshooting the CDC pipeline.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Connector Deployment](#connector-deployment)
3. [Monitoring and Health Checks](#monitoring-and-health-checks)
4. [Common Operations](#common-operations)
5. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Minimum 8GB RAM and 4 CPU cores available
- Ports available: 1433, 5432, 2181, 9092, 8081, 8083, 8200, 9090, 3000, 16686

### 1. Start the Infrastructure

```bash
# Start all services
cd docker
docker-compose up -d

# Wait for all services to be healthy (takes ~2-3 minutes)
docker-compose ps
```

### 2. Initialize Vault with Credentials

```bash
# Store database credentials in Vault
./scripts/bash/vault-init.sh

# Verify credentials are stored
docker exec cdc-vault vault kv get secret/database
```

### 3. Initialize Databases

```bash
# Initialize SQL Server with sample tables and enable CDC
./scripts/bash/init-sqlserver.sh

# Initialize PostgreSQL with target tables
./scripts/bash/init-postgres.sh
```

### 4. Create Kafka Topics (Optional - auto-created by connectors)

```bash
# Pre-create topics with custom configuration
./scripts/bash/create-topics.sh
```

### 5. Deploy Connectors

```bash
# Deploy Debezium SQL Server source connector
./scripts/bash/deploy-connector.sh docker/configs/debezium/sqlserver-source.json

# Deploy JDBC PostgreSQL sink connector
./scripts/bash/deploy-connector.sh docker/configs/kafka-connect/postgresql-sink.json
```

### 6. Verify Replication

```bash
# Insert test data into SQL Server
docker exec cdc-sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd' \
  -d warehouse_source \
  -Q "INSERT INTO dbo.customers (name, email) VALUES ('Test User', 'test@example.com')"

# Check data in PostgreSQL (wait 5-10 seconds)
docker exec cdc-postgres psql -U postgres -d warehouse_target \
  -c "SELECT * FROM customers WHERE name = 'Test User'"
```

---

## Connector Deployment

### Deploy Debezium Source Connector

The Debezium connector captures CDC events from SQL Server:

```bash
./scripts/bash/deploy-connector.sh docker/configs/debezium/sqlserver-source.json
```

**Configuration file**: `docker/configs/debezium/sqlserver-source.json`

**Key settings**:
- `snapshot.mode`: `initial` (performs initial snapshot, then captures changes)
- `table.include.list`: `dbo.customers,dbo.orders,dbo.line_items`
- `tasks.max`: `1` (SQL Server CDC requires single task)

**Verify deployment**:
```bash
# Check connector status
curl -s http://localhost:8083/connectors/sqlserver-cdc-source/status | jq .

# Should show: "connector": {"state": "RUNNING"}
```

### Deploy JDBC Sink Connector

The JDBC sink connector writes data to PostgreSQL:

```bash
./scripts/bash/deploy-connector.sh docker/configs/kafka-connect/postgresql-sink.json
```

**Configuration file**: `docker/configs/kafka-connect/postgresql-sink.json`

**Key settings**:
- `insert.mode`: `upsert` (idempotent writes)
- `tasks.max`: `3` (parallel writes to PostgreSQL)
- `errors.tolerance`: `all` (failed records go to DLQ)

**Verify deployment**:
```bash
# Check connector status
curl -s http://localhost:8083/connectors/postgresql-jdbc-sink/status | jq .

# Should show: "connector": {"state": "RUNNING"}
```

### Update Connector Configuration

To update an existing connector:

```bash
# Edit configuration file
vim docker/configs/debezium/sqlserver-source.json

# Redeploy (automatically updates if exists)
./scripts/bash/deploy-connector.sh docker/configs/debezium/sqlserver-source.json
```

### Delete Connector

```bash
./scripts/bash/deploy-connector.sh --delete docker/configs/debezium/sqlserver-source.json
```

---

## Monitoring and Health Checks

### Access Monitoring UIs

- **Kafka Connect REST API**: http://localhost:8083
- **Grafana Dashboards**: http://localhost:3000 (admin/admin_secure_password)
- **Prometheus**: http://localhost:9090
- **Jaeger Tracing**: http://localhost:16686
- **Vault UI**: http://localhost:8200 (token: dev-root-token)

### Check Connector Status

```bash
# List all connectors
curl -s http://localhost:8083/connectors | jq .

# Get connector status
curl -s http://localhost:8083/connectors/sqlserver-cdc-source/status | jq .

# Get connector configuration
curl -s http://localhost:8083/connectors/sqlserver-cdc-source/config | jq .

# Get connector tasks
curl -s http://localhost:8083/connectors/sqlserver-cdc-source/tasks | jq .
```

### Monitor Replication Lag

**Via Grafana**:
1. Open http://localhost:3000
2. Navigate to "CDC Pipeline Overview" dashboard
3. Check "Replication Lag (Records)" panel

**Via Prometheus**:
```bash
# Query lag metric
curl -s 'http://localhost:9090/api/v1/query?query=kafka_connect_source_task_source_record_poll_total-kafka_connect_sink_task_sink_record_send_total'
```

### Check Kafka Topics

```bash
# List topics
docker exec cdc-kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
docker exec cdc-kafka kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic sqlserver.warehouse_source.dbo.customers

# Check topic offset lag
docker exec cdc-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group connect-postgresql-jdbc-sink --describe
```

### View Dead Letter Queue

```bash
# Check DLQ topic
docker exec cdc-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic dlq-postgresql-sink \
  --from-beginning \
  --max-messages 10
```

---

## Common Operations

### Pause and Resume Connectors

**Pause connector** (stops reading/writing without deleting state):
```bash
curl -X PUT http://localhost:8083/connectors/sqlserver-cdc-source/pause
```

**Resume connector**:
```bash
curl -X PUT http://localhost:8083/connectors/sqlserver-cdc-source/resume
```

### Restart Failed Connector

```bash
# Restart entire connector
curl -X POST http://localhost:8083/connectors/sqlserver-cdc-source/restart

# Restart specific task
curl -X POST http://localhost:8083/connectors/sqlserver-cdc-source/tasks/0/restart
```

### Scale Sink Connector Tasks

The sink connector can run multiple tasks in parallel for better throughput:

```bash
# Edit config file and change tasks.max
vim docker/configs/kafka-connect/postgresql-sink.json

# Redeploy to apply changes
./scripts/bash/deploy-connector.sh docker/configs/kafka-connect/postgresql-sink.json
```

**Note**: Source connector must remain at `tasks.max=1` for SQL Server CDC.

### Add New Tables to CDC

**1. Enable CDC on SQL Server table**:
```sql
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'new_table',
    @role_name = NULL,
    @supports_net_changes = 1;
```

**2. Update Debezium connector config**:
```bash
# Edit config and add table to table.include.list
vim docker/configs/debezium/sqlserver-source.json

# Redeploy
./scripts/bash/deploy-connector.sh docker/configs/debezium/sqlserver-source.json
```

**3. Create PostgreSQL target table**:
```sql
CREATE TABLE new_table (
    -- Match SQL Server schema
);
```

**4. Update JDBC sink connector**:
```bash
# Edit config and add topic to topics list
vim docker/configs/kafka-connect/postgresql-sink.json

# Redeploy
./scripts/bash/deploy-connector.sh docker/configs/kafka-connect/postgresql-sink.json
```

### Backup and Restore Connector State

Connector state is stored in Kafka topics. To backup:

```bash
# Export connect-offsets topic (contains CDC position)
docker exec cdc-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic connect-offsets \
  --from-beginning \
  --property print.key=true > connector-offsets-backup.json
```

To restore after disaster, replay the offsets topic.

---

## Troubleshooting

### Connector Stuck in FAILED State

**Check logs**:
```bash
docker logs cdc-kafka-connect --tail 100
```

**Common causes**:
1. Database credentials incorrect → Check Vault: `docker exec cdc-vault vault kv get secret/database`
2. SQL Server CDC not enabled → Run `./scripts/bash/init-sqlserver.sh`
3. Network connectivity issues → Verify: `docker exec cdc-kafka-connect ping sqlserver`

**Solution**: Fix the issue and restart:
```bash
curl -X POST http://localhost:8083/connectors/sqlserver-cdc-source/restart
```

### High Replication Lag

**Check metrics**:
```bash
# View lag in Grafana dashboard
open http://localhost:3000/d/cdc-pipeline

# Or query Prometheus
curl 'http://localhost:9090/api/v1/query?query=kafka_connect_source_task_source_record_poll_total'
```

**Common causes**:
1. High insert volume → Scale sink connector tasks
2. Slow PostgreSQL writes → Check PostgreSQL performance
3. Network issues → Check Docker network: `docker network inspect cdc-network`

**Solutions**:
- Increase `tasks.max` for sink connector (up to 10)
- Increase `batch.size` in sink connector config
- Tune PostgreSQL: increase `max_connections`, `shared_buffers`

### Data Not Appearing in PostgreSQL

**1. Check connector status**:
```bash
curl -s http://localhost:8083/connectors/sqlserver-cdc-source/status | jq .
curl -s http://localhost:8083/connectors/postgresql-jdbc-sink/status | jq .
```

**2. Check Kafka topics**:
```bash
# List topics
docker exec cdc-kafka kafka-topics --list --bootstrap-server localhost:9092

# Consume from topic
docker exec cdc-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sqlserver.warehouse_source.dbo.customers \
  --from-beginning --max-messages 5
```

**3. Check Dead Letter Queue**:
```bash
docker exec cdc-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic dlq-postgresql-sink \
  --from-beginning
```

**4. Verify table structure**:
```bash
# PostgreSQL table must match SQL Server schema
docker exec cdc-postgres psql -U postgres -d warehouse_target -c "\d+ customers"
```

### Schema Evolution Issues

When SQL Server table schema changes (add/remove columns):

**1. Check auto.evolve setting** (should be `true`):
```bash
curl -s http://localhost:8083/connectors/postgresql-jdbc-sink/config | jq '.["auto.evolve"]'
```

**2. Check Schema Registry**:
```bash
# List schemas
curl http://localhost:8081/subjects

# Get latest schema version
curl http://localhost:8081/subjects/sqlserver.warehouse_source.dbo.customers-value/versions/latest
```

**3. Manually alter PostgreSQL table** if auto.evolve fails:
```sql
ALTER TABLE customers ADD COLUMN new_column VARCHAR(50);
```

### Performance Issues

**Check resource usage**:
```bash
# Docker stats
docker stats cdc-kafka-connect cdc-kafka cdc-sqlserver cdc-postgres

# Should stay under 4GB memory, 2 CPU cores per container
```

**Tune Kafka Connect JVM**:
Edit `docker/docker-compose.yml`:
```yaml
KAFKA_HEAP_OPTS: "-Xms2G -Xmx4G"
```

**Optimize PostgreSQL**:
```sql
-- Increase connection pool
ALTER SYSTEM SET max_connections = 200;

-- Increase buffer size
ALTER SYSTEM SET shared_buffers = '2GB';

-- Reload config
SELECT pg_reload_conf();
```

### View All Logs

```bash
# Kafka Connect (most important)
docker logs cdc-kafka-connect --tail 100 -f

# Kafka broker
docker logs cdc-kafka --tail 100

# SQL Server
docker logs cdc-sqlserver --tail 100

# PostgreSQL
docker logs cdc-postgres --tail 100

# Schema Registry
docker logs cdc-schema-registry --tail 100
```

---

## Next Steps

- Set up alerting: Configure Alertmanager for Prometheus alerts
- Production hardening: Review [quickstart.md](../specs/001-sqlserver-pg-cdc/quickstart.md)
- Run reconciliation: See reconciliation tool documentation (coming soon)
- Disaster recovery: Document backup and restore procedures
