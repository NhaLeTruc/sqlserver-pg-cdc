# Troubleshooting Guide

## Overview

This guide provides diagnostic procedures and solutions for common issues in the SQL Server to PostgreSQL CDC pipeline.

**Last Updated**: 2025-12-20
**Audience**: Operations, DevOps, Database Administrators

---

## Quick Diagnostic Commands

```bash
# Check all container status
docker ps -a

# Check specific service logs
docker logs kafka-connect --tail 100
docker logs sqlserver --tail 100
docker logs postgres --tail 100

# Collect full diagnostics
./scripts/bash/collect-diagnostics.sh

# Check connector status
curl -s http://localhost:8083/connectors/sqlserver-source-connector/status | jq '.'

# Check Prometheus metrics
curl -s http://localhost:9103/metrics | grep reconciliation

# Run reconciliation check
python -m src.cli.reconcile \
    --source-type sqlserver \
    --source-host sqlserver \
    --source-port 1433 \
    --source-database warehouse_source \
    --source-user sa \
    --source-password "${SQLSERVER_PASSWORD}" \
    --target-type postgresql \
    --target-host postgres \
    --target-port 5432 \
    --target-database warehouse_target \
    --target-user postgres \
    --target-password "${POSTGRES_PASSWORD}" \
    --tables customers,products,orders,inventory,shipments
```

---

## Issue Categories

- [Connector Issues](#connector-issues)
- [Database Issues](#database-issues)
- [Replication Lag](#replication-lag)
- [Data Inconsistency](#data-inconsistency)
- [Performance Issues](#performance-issues)
- [Network Issues](#network-issues)
- [Schema Evolution](#schema-evolution)

---

## Connector Issues

### Issue: Connector Status Shows FAILED

**Symptoms**:
- Connector shows `FAILED` in status
- Error messages in connector logs
- Data not replicating

**Diagnostic Steps**:

1. Check connector status:
```bash
curl -s http://localhost:8083/connectors/sqlserver-source-connector/status | jq '.'
```

2. Check connector logs:
```bash
docker logs kafka-connect --tail 200 | grep -i error
```

3. Check connector configuration:
```bash
curl -s http://localhost:8083/connectors/sqlserver-source-connector/config | jq '.'
```

**Common Causes & Solutions**:

#### Authentication Failure

**Error**: `Login failed for user 'sa'`

**Solution**:
```bash
# Verify credentials
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "SELECT @@VERSION"

# Update connector with correct credentials
curl -X PUT http://localhost:8083/connectors/sqlserver-source-connector/config \
    -H "Content-Type: application/json" \
    -d '{
      "database.user": "sa",
      "database.password": "'"${SQLSERVER_PASSWORD}"'"
    }'

# Restart connector
curl -X POST http://localhost:8083/connectors/sqlserver-source-connector/restart
```

#### CDC Not Enabled

**Error**: `CDC is not enabled on database`

**Solution**:
```bash
# Enable CDC on database
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "EXEC sys.sp_cdc_enable_db" -d warehouse_source

# Enable CDC on tables
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'customers',
        @role_name = NULL" -d warehouse_source

# Verify CDC enabled
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'warehouse_source'"
```

#### Table Not Found

**Error**: `Invalid object name 'dbo.customers'`

**Solution**:
```bash
# Verify table exists
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'customers'" \
    -d warehouse_source

# Update connector config with correct table list
curl -X PUT http://localhost:8083/connectors/sqlserver-source-connector/config \
    -H "Content-Type: application/json" \
    -d '{
      "table.include.list": "dbo.customers,dbo.products,dbo.orders"
    }'
```

#### Kafka Not Reachable

**Error**: `Connection to node -1 could not be established`

**Solution**:
```bash
# Check Kafka container
docker ps | grep kafka

# Restart Kafka if needed
docker-compose restart kafka

# Wait for Kafka to be ready
sleep 30

# Verify Kafka Connect can reach Kafka
docker exec kafka-connect kafka-broker-api-versions --bootstrap-server kafka:9092

# Restart connector
curl -X POST http://localhost:8083/connectors/sqlserver-source-connector/restart
```

---

## Database Issues

### Issue: SQL Server Connection Timeout

**Symptoms**:
- Error: `Login timeout expired`
- Connection attempts fail
- Reconciliation fails with timeout

**Diagnostic Steps**:

1. Check SQL Server container:
```bash
docker ps | grep sqlserver
docker logs sqlserver --tail 50
```

2. Test connection:
```bash
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "SELECT @@VERSION"
```

**Solutions**:

#### Container Not Running

```bash
# Check container status
docker ps -a | grep sqlserver

# Start container if stopped
docker-compose start sqlserver

# Wait for startup
sleep 30

# Verify connection
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "SELECT @@VERSION"
```

#### SQL Server Not Ready

```bash
# Check SQL Server startup logs
docker logs sqlserver | grep -i "SQL Server is now ready"

# If not ready, wait longer (can take 1-2 minutes)
sleep 60

# Retry connection
```

#### Network Issues

```bash
# Check Docker network
docker network inspect sqlserver-pg-cdc_default

# Verify SQL Server port exposed
docker port sqlserver

# Test network connectivity
docker exec kafka-connect ping -c 3 sqlserver
```

### Issue: PostgreSQL Connection Refused

**Symptoms**:
- Error: `Connection refused`
- Cannot connect to PostgreSQL
- Sink connector fails

**Diagnostic Steps**:

1. Check PostgreSQL container:
```bash
docker ps | grep postgres
docker logs postgres --tail 50
```

2. Test connection:
```bash
docker exec postgres psql -U postgres -c "SELECT version()"
```

**Solutions**:

#### Container Not Running

```bash
# Start PostgreSQL
docker-compose start postgres

# Wait for startup
sleep 10

# Verify connection
docker exec postgres psql -U postgres -c "SELECT version()"
```

#### Database Doesn't Exist

```bash
# List databases
docker exec postgres psql -U postgres -c "\l"

# Create database if missing
docker exec postgres psql -U postgres -c "CREATE DATABASE warehouse_target"

# Verify
docker exec postgres psql -U postgres -d warehouse_target -c "SELECT current_database()"
```

#### Permission Issues

```bash
# Check user permissions
docker exec postgres psql -U postgres -c "\du"

# Grant permissions if needed
docker exec postgres psql -U postgres -d warehouse_target \
    -c "GRANT ALL PRIVILEGES ON DATABASE warehouse_target TO postgres"
```

---

## Replication Lag

### Issue: High Replication Lag (>5 minutes)

**Symptoms**:
- Data takes long to appear in target
- Reconciliation shows differences
- Metrics show high lag

**Diagnostic Steps**:

1. Check connector lag:
```bash
curl -s http://localhost:9090/api/v1/query?query=kafka_connect_source_connector_lag | jq '.'
```

2. Check Kafka consumer lag:
```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups
```

3. Check CDC capture job:
```bash
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "SELECT * FROM sys.dm_cdc_log_scan_sessions ORDER BY start_time DESC" \
    -d warehouse_source
```

**Solutions**:

#### CDC Capture Job Stopped

```bash
# Check if CDC jobs are running
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "EXEC sys.sp_cdc_help_jobs" -d warehouse_source

# Start CDC capture job if stopped
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "EXEC sys.sp_cdc_start_job @job_type = N'capture'" -d warehouse_source
```

#### High Transaction Volume

```bash
# Check transaction log size
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "DBCC SQLPERF(LOGSPACE)" -d warehouse_source

# Increase connector parallelism (if using tasks.max > 1)
curl -X PUT http://localhost:8083/connectors/sqlserver-source-connector/config \
    -H "Content-Type: application/json" \
    -d '{
      "tasks.max": "3"
    }'

# Monitor improvement
```

#### Resource Constraints

```bash
# Check container resources
docker stats --no-stream kafka-connect sqlserver postgres

# Increase container resources in docker-compose.yml
# Then restart:
docker-compose down
docker-compose up -d
```

---

## Data Inconsistency

### Issue: Reconciliation Shows Mismatches

**Symptoms**:
- Row count differences
- Checksum mismatches
- Missing or extra rows

**Diagnostic Steps**:

1. Run detailed reconciliation:
```bash
python -m src.cli.reconcile \
    --source-type sqlserver \
    --source-host sqlserver \
    --source-port 1433 \
    --source-database warehouse_source \
    --source-user sa \
    --source-password "${SQLSERVER_PASSWORD}" \
    --target-type postgresql \
    --target-host postgres \
    --target-port 5432 \
    --target-database warehouse_target \
    --target-user postgres \
    --target-password "${POSTGRES_PASSWORD}" \
    --tables customers,products,orders,inventory,shipments \
    --validate-checksums \
    --output /tmp/reconciliation_report.json
```

2. Check connector status:
```bash
curl -s http://localhost:8083/connectors/sqlserver-source-connector/status | jq '.tasks[].state'
```

3. Check for errors:
```bash
docker logs kafka-connect --tail 500 | grep -i error
```

**Solutions**:

#### Missing Rows (Target < Source)

**Root Cause**: Connector failed to replicate some changes

```bash
# Check dead letter queue (if configured)
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic dlq-postgresql-jdbc-sink \
    --from-beginning \
    --max-messages 10

# If DLQ has messages, check error details
# Fix data issues and resync

# Option 1: Delete and recreate connector (triggers snapshot)
curl -X DELETE http://localhost:8083/connectors/sqlserver-source-connector
sleep 10
# Recreate connector with snapshot.mode=initial

# Option 2: Truncate and resync specific table
docker exec postgres psql -U postgres -d warehouse_target \
    -c "TRUNCATE TABLE customers CASCADE"
# Restart connector to trigger snapshot
```

#### Extra Rows (Target > Source)

**Root Cause**: Deletes not propagating or duplicate inserts

```bash
# Check if delete handling is enabled
curl -s http://localhost:8083/connectors/postgres-sink-connector/config | jq '.["delete.enabled"]'

# Enable delete handling if disabled
curl -X PUT http://localhost:8083/connectors/postgres-sink-connector/config \
    -H "Content-Type: application/json" \
    -d '{
      "delete.enabled": "true"
    }'

# Check for duplicate keys
docker exec postgres psql -U postgres -d warehouse_target \
    -c "SELECT customer_id, COUNT(*) FROM customers GROUP BY customer_id HAVING COUNT(*) > 1"

# Remove duplicates if found
docker exec postgres psql -U postgres -d warehouse_target \
    -c "DELETE FROM customers a USING customers b WHERE a.ctid < b.ctid AND a.customer_id = b.customer_id"
```

#### Checksum Mismatch (Same Count, Different Data)

**Root Cause**: Data corruption or update issues

```bash
# Identify specific differences
# Run detailed comparison query on both databases

# SQL Server - get sample data
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "SELECT TOP 10 * FROM dbo.customers ORDER BY customer_id" \
    -d warehouse_source

# PostgreSQL - get sample data
docker exec postgres psql -U postgres -d warehouse_target \
    -c "SELECT * FROM customers ORDER BY customer_id LIMIT 10"

# Compare and identify differences
# If widespread, consider full resync:
docker exec postgres psql -U postgres -d warehouse_target \
    -c "TRUNCATE TABLE customers CASCADE"
curl -X POST http://localhost:8083/connectors/sqlserver-source-connector/restart
```

---

## Performance Issues

### Issue: Slow Reconciliation

**Symptoms**:
- Reconciliation takes >10 minutes
- High CPU/memory usage
- Timeouts during checksum validation

**Diagnostic Steps**:

1. Check table sizes:
```bash
# SQL Server
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "SELECT
        t.NAME AS TableName,
        p.rows AS RowCounts,
        SUM(a.total_pages) * 8 AS TotalSpaceKB
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    WHERE t.is_ms_shipped = 0
    GROUP BY t.Name, p.Rows
    ORDER BY p.Rows DESC" -d warehouse_source

# PostgreSQL
docker exec postgres psql -U postgres -d warehouse_target \
    -c "SELECT
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
        n_live_tup AS row_count
    FROM pg_stat_user_tables
    ORDER BY n_live_tup DESC"
```

2. Monitor resource usage:
```bash
docker stats --no-stream
```

**Solutions**:

#### Use Chunked Checksum for Large Tables

For tables with >100K rows, use chunked checksum:

```python
# In your reconciliation script
from src.reconciliation.compare import calculate_checksum_chunked

# Instead of calculate_checksum, use:
source_checksum = calculate_checksum_chunked(
    source_cursor,
    source_table,
    columns=columns,
    chunk_size=10000  # Adjust based on row size
)
```

#### Skip Checksum Validation

For very large tables, skip checksum validation and only compare row counts:

```bash
python -m src.cli.reconcile \
    --source-type sqlserver \
    --source-host sqlserver \
    ... \
    --tables customers,products,orders \
    # Don't use --validate-checksums flag
```

#### Increase Resources

```yaml
# In docker-compose.yml
services:
  sqlserver:
    deploy:
      resources:
        limits:
          cpus: '4.0'
          memory: 8G
        reservations:
          cpus: '2.0'
          memory: 4G
```

#### Run Reconciliation Off-Hours

```bash
# Schedule reconciliation during low-traffic periods
# Using cron:
# 0 2 * * * cd /path/to/project && python -m src.cli.reconcile ...
```

---

## Network Issues

### Issue: Connection Refused Between Services

**Symptoms**:
- Services can't reach each other
- Connection refused errors
- Timeout errors

**Diagnostic Steps**:

1. Check Docker network:
```bash
docker network ls
docker network inspect sqlserver-pg-cdc_default
```

2. Test connectivity:
```bash
# From kafka-connect to sqlserver
docker exec kafka-connect ping -c 3 sqlserver

# From kafka-connect to postgres
docker exec kafka-connect ping -c 3 postgres

# From kafka-connect to kafka
docker exec kafka-connect nc -zv kafka 9092
```

**Solutions**:

#### Services on Different Networks

```bash
# Check which networks each container is on
docker inspect sqlserver | jq '.[].NetworkSettings.Networks'
docker inspect postgres | jq '.[].NetworkSettings.Networks'
docker inspect kafka-connect | jq '.[].NetworkSettings.Networks'

# If on different networks, recreate with docker-compose
docker-compose down
docker-compose up -d
```

#### DNS Resolution Issues

```bash
# Check DNS resolution
docker exec kafka-connect nslookup sqlserver
docker exec kafka-connect nslookup postgres

# If DNS fails, use IP addresses in connector config
# Get container IPs:
docker inspect sqlserver | jq '.[].NetworkSettings.Networks[].IPAddress'
docker inspect postgres | jq '.[].NetworkSettings.Networks[].IPAddress'
```

#### Firewall Rules

```bash
# Check if host firewall is blocking ports
sudo iptables -L -n | grep -E "(1433|5432|8083|9092)"

# Allow required ports (adjust for your firewall)
sudo ufw allow 1433/tcp  # SQL Server
sudo ufw allow 5432/tcp  # PostgreSQL
sudo ufw allow 8083/tcp  # Kafka Connect
sudo ufw allow 9092/tcp  # Kafka
```

---

## Schema Evolution

### Issue: Schema Change Not Propagating

**Symptoms**:
- New columns not appearing in target
- ALTER statements not captured
- Schema mismatch errors

**Diagnostic Steps**:

1. Check schema change history:
```bash
# Check Kafka schema changes topic
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic schema-changes.warehouse \
    --from-beginning \
    --max-messages 20
```

2. Compare schemas:
```bash
# SQL Server schema
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'customers'
        ORDER BY ORDINAL_POSITION" \
    -d warehouse_source

# PostgreSQL schema
docker exec postgres psql -U postgres -d warehouse_target \
    -c "SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = 'customers'
        ORDER BY ordinal_position"
```

**Solutions**:

#### Enable Auto Schema Evolution

```bash
# Check if auto.evolve is enabled
curl -s http://localhost:8083/connectors/postgres-sink-connector/config | jq '.["auto.evolve"]'

# Enable auto.evolve
curl -X PUT http://localhost:8083/connectors/postgres-sink-connector/config \
    -H "Content-Type: application/json" \
    -d '{
      "auto.evolve": "true"
    }'
```

#### Manual Schema Update

```bash
# Add column to PostgreSQL manually
docker exec postgres psql -U postgres -d warehouse_target \
    -c "ALTER TABLE customers ADD COLUMN new_column VARCHAR(255)"

# Restart connector to pick up changes
curl -X POST http://localhost:8083/connectors/postgres-sink-connector/restart
```

#### Recreate Table with Snapshot

```bash
# Drop and recreate table (data loss!)
docker exec postgres psql -U postgres -d warehouse_target \
    -c "DROP TABLE customers CASCADE"

# Restart connector to trigger snapshot
curl -X POST http://localhost:8083/connectors/sqlserver-source-connector/restart
```

---

## Common Error Messages

### SQL Server Errors

| Error Code | Message | Solution |
|------------|---------|----------|
| HYT00 | Login timeout expired | Check SQL Server is running, verify network connectivity |
| 28000 | Login failed for user | Verify credentials, check user permissions |
| 08001 | Cannot open database | Check database exists, verify user has access |
| 42S02 | Invalid object name | Table doesn't exist, verify table name and schema |

### PostgreSQL Errors

| Error Code | Message | Solution |
|------------|---------|----------|
| 08006 | Connection failure | Check PostgreSQL is running, verify network |
| 28P01 | Password authentication failed | Verify credentials in connector config |
| 3D000 | Database does not exist | Create database or fix database name |
| 42P01 | Relation does not exist | Create table or fix table name |

### Kafka Connect Errors

| Error | Message | Solution |
|-------|---------|----------|
| ConnectException | Could not connect to Kafka | Check Kafka is running, verify bootstrap servers |
| ConfigException | Invalid configuration | Review connector config, fix invalid properties |
| DataException | Schema mismatch | Enable auto.evolve or manually update schema |
| RetriableException | Temporary failure | Wait for retry, check logs for root cause |

---

## Escalation Procedures

### When to Escalate

Escalate to senior team members when:
- Data loss suspected
- Recovery procedures failing
- Performance degradation >50%
- Multiple systems affected
- Root cause unknown after 2 hours

### Escalation Contacts

- **Level 1**: Database Operations Team (`#database-ops` Slack)
- **Level 2**: Infrastructure Team Lead
- **Level 3**: Platform Engineering Manager
- **Emergency**: PagerDuty `cdc-pipeline-alerts`

### Information to Provide

When escalating, include:
1. Diagnostic tarball from `collect-diagnostics.sh`
2. Timeline of issue and actions taken
3. Impact assessment (tables affected, data loss, downtime)
4. Recent changes or deployments
5. Error messages and stack traces
6. Current status of all services

---

## Preventive Measures

### Daily Checks

- Review reconciliation reports in `./reconciliation_reports/`
- Check Prometheus metrics for anomalies
- Verify all connectors are in `RUNNING` state
- Monitor disk space on backup volumes

### Weekly Maintenance

- Review connector logs for warnings
- Check database growth trends
- Verify backup success
- Test restore procedure (sample)

### Monthly Reviews

- Review incident logs and root causes
- Update troubleshooting documentation
- Performance trend analysis
- Capacity planning review

---

## Additional Resources

- [Disaster Recovery Runbook](disaster-recovery.md)
- [Debezium SQL Server Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html)
- [Confluent JDBC Sink Connector Documentation](https://docs.confluent.io/kafka-connect-jdbc/current/sink-connector/index.html)
- [SQL Server CDC Documentation](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server)

---

## Revision History

| Date       | Version | Changes                          | Author |
|------------|---------|----------------------------------|--------|
| 2025-12-20 | 1.0     | Initial troubleshooting guide    | System |