# Operations Runbook

## Overview

This runbook provides standard operating procedures for daily operations, maintenance tasks, and routine procedures for the SQL Server to PostgreSQL CDC pipeline.

**Last Updated**: 2025-12-20
**Audience**: Operations Team, DevOps Engineers

---

## Table of Contents

- [Daily Operations](#daily-operations)
- [Connector Management](#connector-management)
- [Monitoring & Alerts](#monitoring--alerts)
- [Scheduled Tasks](#scheduled-tasks)
- [Maintenance Windows](#maintenance-windows)
- [Change Management](#change-management)

---

## Daily Operations

### Morning Health Check (10 minutes)

Run this checklist every morning to ensure system health:

#### 1. Check Service Status

```bash
# Verify all containers running
docker ps

# Expected containers:
# - sqlserver
# - postgres
# - zookeeper
# - kafka
# - schema-registry
# - kafka-connect
```

**Action**: If any container is down, check logs and restart:
```bash
docker logs <container_name> --tail 50
docker-compose restart <container_name>
```

#### 2. Check Connector Status

```bash
# List all connectors
curl -s http://localhost:8083/connectors | jq '.'

# Check each connector status
curl -s http://localhost:8083/connectors/sqlserver-source-connector/status | jq '.connector.state'
curl -s http://localhost:8083/connectors/postgres-sink-connector/status | jq '.connector.state'
```

**Expected**: Both should show `"RUNNING"`

**Action**: If `FAILED`, check troubleshooting guide and restart:
```bash
curl -X POST http://localhost:8083/connectors/<connector-name>/restart
```

#### 3. Review Reconciliation Reports

```bash
# List recent reports
ls -lht ./reconciliation_reports/ | head -10

# View latest report
cat ./reconciliation_reports/reconciliation_report_$(date +%Y%m%d)*.json | jq '.summary'
```

**Expected**: All tables show `"MATCH"`

**Action**: If mismatches, investigate using [Troubleshooting Guide](troubleshooting.md#data-inconsistency)

#### 4. Check Metrics

```bash
# Check reconciliation metrics
curl -s http://localhost:9103/metrics | grep reconciliation_row_count_mismatch_total

# Check connector metrics
curl -s http://localhost:9104/metrics | grep connector_state
```

**Expected**: No mismatches, connectors in state 1 (running)

#### 5. Check Disk Space

```bash
# Check Docker volumes
df -h | grep docker

# Check backup directory
df -h /var/backups/cdc
du -sh /var/backups/cdc/*
```

**Expected**: <80% usage

**Action**: If >80%, clean up old backups:
```bash
find /var/backups/cdc -type f -mtime +7 -delete
```

### Health Check Summary

Create daily health check report:

```bash
#!/bin/bash
# Daily health check summary
echo "=== Daily Health Check $(date) ==="
echo ""
echo "1. Container Status:"
docker ps --format "table {{.Names}}\t{{.Status}}"
echo ""
echo "2. Connector Status:"
curl -s http://localhost:8083/connectors/sqlserver-source-connector/status | jq -r '"Source: " + .connector.state'
curl -s http://localhost:8083/connectors/postgres-sink-connector/status | jq -r '"Sink: " + .connector.state'
echo ""
echo "3. Latest Reconciliation:"
cat ./reconciliation_reports/reconciliation_report_$(date +%Y%m%d)*.json 2>/dev/null | jq '.summary' || echo "No report today"
echo ""
echo "4. Disk Space:"
df -h | grep -E "(Filesystem|docker|backups)"
```

---

## Connector Management

### Deploy New Connector

#### Prerequisites
- Connector configuration JSON file
- Database connectivity verified
- Kafka topics created (if pre-creating)

#### Procedure

1. **Validate Configuration**

Create connector config file (e.g., `new-connector.json`):
```json
{
  "name": "new-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "database.hostname": "sqlserver",
    "database.port": "1433",
    "database.user": "sa",
    "database.password": "${SQLSERVER_PASSWORD}",
    "database.dbname": "new_database",
    "database.server.name": "sqlserver",
    "table.include.list": "dbo.table1,dbo.table2",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "schema-changes.new_database"
  }
}
```

2. **Deploy Connector**

```bash
# Deploy connector
curl -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
    -d @new-connector.json

# Verify deployment
curl -s http://localhost:8083/connectors/new-source-connector/status | jq '.'
```

3. **Monitor Initial Snapshot**

```bash
# Watch connector logs
docker logs -f kafka-connect | grep new-source-connector

# Check progress
curl -s http://localhost:8083/connectors/new-source-connector/status | jq '.tasks[].state'
```

4. **Validate Data Replication**

```bash
# Wait for snapshot to complete (check logs)
# Then verify data in target

# Count rows
docker exec postgres psql -U postgres -d target_database \
    -c "SELECT count(*) FROM table1"
```

### Update Connector Configuration

```bash
# Get current config
curl -s http://localhost:8083/connectors/sqlserver-source-connector/config | jq '.' > current-config.json

# Edit config file
# Modify current-config.json

# Update connector
curl -X PUT http://localhost:8083/connectors/sqlserver-source-connector/config \
    -H "Content-Type: application/json" \
    -d @current-config.json

# Verify update
curl -s http://localhost:8083/connectors/sqlserver-source-connector/status | jq '.'
```

### Pause Connector

Use when you need to stop replication temporarily (e.g., during maintenance):

```bash
# Pause connector
curl -X PUT http://localhost:8083/connectors/sqlserver-source-connector/pause

# Verify paused
curl -s http://localhost:8083/connectors/sqlserver-source-connector/status | jq '.connector.state'
# Should show "PAUSED"

# Resume when ready
curl -X PUT http://localhost:8083/connectors/sqlserver-source-connector/resume
```

### Delete Connector

```bash
# Delete connector
curl -X DELETE http://localhost:8083/connectors/sqlserver-source-connector

# Verify deletion
curl -s http://localhost:8083/connectors | jq '.'
```

**Note**: Deleting a connector does not delete Kafka topics or data.

---

## Monitoring & Alerts

### Prometheus Metrics

Access Prometheus at `http://localhost:9090`

#### Key Metrics to Monitor

**Reconciliation Metrics** (Port 9103):
```
# Row count mismatches
reconciliation_row_count_mismatch_total

# Checksum mismatches
reconciliation_checksum_mismatch_total

# Reconciliation runs
reconciliation_runs_total{status="success"}
reconciliation_runs_total{status="failure"}

# Processing rate
reconciliation_rows_per_second
```

**Connector Metrics** (Port 9104):
```
# Connector state (1=running, 0=other)
connector_state{connector="sqlserver-source-connector"}

# Deployment status
connector_deployments_total{status="success"}
connector_deployments_total{status="failure"}
```

### Grafana Dashboards

Access Grafana at `http://localhost:3000`

Default credentials: `admin` / `admin`

#### Pre-configured Dashboards

1. **CDC Pipeline Overview**
   - Connector status
   - Replication lag
   - Throughput metrics
   - Error rates

2. **Database Health**
   - Connection pool status
   - Query performance
   - Resource utilization

3. **Reconciliation Dashboard**
   - Match/mismatch trends
   - Table-level discrepancies
   - Processing time

### Alert Rules

Configure these alert rules in Prometheus:

#### High Replication Lag
```yaml
- alert: HighReplicationLag
  expr: kafka_connect_source_connector_lag > 10000
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "High replication lag detected"
    description: "Connector {{ $labels.connector }} has lag of {{ $value }} records"
```

#### Connector Down
```yaml
- alert: ConnectorDown
  expr: connector_state == 0
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "Connector is not running"
    description: "Connector {{ $labels.connector }} is in failed state"
```

#### Data Mismatch
```yaml
- alert: DataMismatch
  expr: increase(reconciliation_row_count_mismatch_total[1h]) > 0
  labels:
    severity: warning
  annotations:
    summary: "Data mismatch detected"
    description: "Table {{ $labels.table }} has row count mismatch"
```

### On-Call Response

When alerts fire:

1. **Acknowledge alert** in PagerDuty
2. **Run diagnostics**: `./scripts/bash/collect-diagnostics.sh`
3. **Check status**: Review health check commands
4. **Investigate**: Follow [Troubleshooting Guide](troubleshooting.md)
5. **Escalate**: If not resolved in 30 minutes
6. **Document**: Record incident and resolution

---

## Scheduled Tasks

### Automated Tasks (Cron)

Configure these in crontab:

```cron
# Daily backup at 02:00 UTC
0 2 * * * cd /path/to/project && ./scripts/bash/backup-databases.sh --s3-bucket cdc-backups >> /var/log/cdc-backup.log 2>&1

# Hourly reconciliation
0 * * * * cd /path/to/project && python -m src.cli.reconcile --source-type sqlserver --source-host sqlserver --source-port 1433 --source-database warehouse_source --source-user sa --source-password "${SQLSERVER_PASSWORD}" --target-type postgresql --target-host postgres --target-port 5432 --target-database warehouse_target --target-user postgres --target-password "${POSTGRES_PASSWORD}" --tables customers,products,orders,inventory,shipments >> /var/log/cdc-reconcile.log 2>&1

# Weekly diagnostic collection (Sunday 01:00 UTC)
0 1 * * 0 cd /path/to/project && ./scripts/bash/collect-diagnostics.sh >> /var/log/cdc-diagnostics.log 2>&1

# Monthly cleanup of old reports (1st of month, 03:00 UTC)
0 3 1 * * find /path/to/project/reconciliation_reports -type f -mtime +90 -delete

# Daily metrics cleanup (keep 30 days)
0 4 * * * find /var/lib/prometheus/data -type f -mtime +30 -delete
```

### Manual Weekly Tasks

Every Monday morning:

1. **Review Last Week's Metrics**
```bash
# Check error trends
curl -s 'http://localhost:9090/api/v1/query?query=increase(reconciliation_runs_total{status="failure"}[7d])' | jq '.'

# Check replication lag trends
curl -s 'http://localhost:9090/api/v1/query?query=avg_over_time(kafka_connect_source_connector_lag[7d])' | jq '.'
```

2. **Review Backup Status**
```bash
# Check backup success
grep -E "(✓|✗)" /var/log/cdc-backup.log | tail -20

# Verify S3 uploads
aws s3 ls s3://cdc-backups/$(date -d "7 days ago" +%Y/%m/%d)/ --recursive
```

3. **Capacity Planning**
```bash
# Check database growth
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "EXEC sp_spaceused" -d warehouse_source

# Check Kafka topic sizes
docker exec kafka kafka-log-dirs --bootstrap-server localhost:9092 --describe
```

### Manual Monthly Tasks

First Monday of each month:

1. **Security Updates**
```bash
# Pull latest Docker images
docker-compose pull

# Review changelog for breaking changes
# Schedule maintenance window for updates
```

2. **Performance Review**
```bash
# Export Grafana dashboard data
# Analyze trends
# Identify optimization opportunities
```

3. **Disaster Recovery Test**
```bash
# Schedule quarterly DR drill
# Follow disaster-recovery.md procedures
# Document RTO/RPO actual vs target
```

---

## Maintenance Windows

### Planned Maintenance Procedure

#### 1. Pre-Maintenance (1 week before)

- [ ] Announce maintenance window to stakeholders
- [ ] Schedule downtime (recommend: Sunday 02:00-06:00 UTC)
- [ ] Review change plan and rollback procedures
- [ ] Run full backup
- [ ] Test changes in staging environment

#### 2. Maintenance Window Start

**15 minutes before window**:

```bash
# 1. Run final reconciliation
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
    --validate-checksums

# 2. Collect diagnostics (baseline)
./scripts/bash/collect-diagnostics.sh

# 3. Create backup
./scripts/bash/backup-databases.sh --s3-bucket cdc-backups
```

**At window start**:

```bash
# 1. Pause connectors
curl -X PUT http://localhost:8083/connectors/sqlserver-source-connector/pause
curl -X PUT http://localhost:8083/connectors/postgres-sink-connector/pause

# 2. Verify paused
curl -s http://localhost:8083/connectors/sqlserver-source-connector/status | jq '.connector.state'
curl -s http://localhost:8083/connectors/postgres-sink-connector/status | jq '.connector.state'

# 3. Stop services (if needed for updates)
docker-compose down
```

#### 3. Perform Maintenance

Execute planned changes:

**Example: Update Docker Images**
```bash
# Pull new images
docker-compose pull

# Review changes
docker images | grep -E "(debezium|confluent)"

# Start services
docker-compose up -d

# Wait for services to be ready
sleep 60
```

**Example: Update Connector Configuration**
```bash
# Update connector config
curl -X PUT http://localhost:8083/connectors/sqlserver-source-connector/config \
    -H "Content-Type: application/json" \
    -d @updated-config.json
```

#### 4. Post-Maintenance Validation

```bash
# 1. Verify all containers running
docker ps

# 2. Resume connectors
curl -X PUT http://localhost:8083/connectors/sqlserver-source-connector/resume
curl -X PUT http://localhost:8083/connectors/postgres-sink-connector/resume

# 3. Check connector status
curl -s http://localhost:8083/connectors/sqlserver-source-connector/status | jq '.connector.state'
curl -s http://localhost:8083/connectors/postgres-sink-connector/status | jq '.connector.state'

# 4. Test replication
# Insert test record in SQL Server
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "INSERT INTO dbo.customers (customer_id, name, email) VALUES (99999, 'Test', 'test@example.com')" \
    -d warehouse_source

# Wait 30 seconds
sleep 30

# Verify in PostgreSQL
docker exec postgres psql -U postgres -d warehouse_target \
    -c "SELECT * FROM customers WHERE customer_id = 99999"

# Cleanup test data
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "DELETE FROM dbo.customers WHERE customer_id = 99999" \
    -d warehouse_source

# 5. Run reconciliation
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

# 6. Monitor for 30 minutes
# Check logs, metrics, alerts
```

#### 5. Rollback Procedure (if needed)

```bash
# 1. Stop services
docker-compose down

# 2. Restore from backup
./scripts/bash/restore-databases.sh --timestamp <backup-timestamp>

# 3. Revert Docker images (if updated)
# Edit docker-compose.yml to specify previous image versions
docker-compose up -d

# 4. Restore previous connector configurations
curl -X PUT http://localhost:8083/connectors/sqlserver-source-connector/config \
    -H "Content-Type: application/json" \
    -d @previous-config.json

# 5. Validate
# Run post-maintenance validation steps
```

#### 6. Post-Maintenance Report

Document:
- Start and end times
- Changes made
- Validation results
- Issues encountered
- Actual downtime
- Lessons learned

---

## Change Management

### Change Request Process

#### 1. Submit Change Request

**For low-risk changes** (config updates, adding tables):
- Create ticket in issue tracker
- Describe change and rationale
- Get approval from team lead

**For high-risk changes** (major upgrades, schema changes):
- Create formal change request
- Include rollback plan
- Get approval from change advisory board
- Schedule maintenance window

#### 2. Change Categories

**Low Risk** (< 5 min downtime):
- Adding new tables to replication
- Updating connector parallelism
- Adjusting reconciliation schedule
- Minor config changes

**Medium Risk** (5-30 min downtime):
- Updating connector configurations
- Schema changes (with auto.evolve)
- Adding new connectors
- Resource adjustments

**High Risk** (>30 min downtime):
- Major version upgrades
- Database migrations
- Network changes
- Disaster recovery failover

#### 3. Testing Requirements

All changes must be tested in staging before production:

```bash
# Set up staging environment
docker-compose -f docker-compose.staging.yml up -d

# Apply changes
# ... execute change procedures ...

# Run validation tests
pytest tests/integration/

# Run reconciliation
python -m src.cli.reconcile ... # staging credentials

# If successful, proceed to production
```

---

## Emergency Procedures

### Complete System Failure

Follow [Disaster Recovery Runbook](disaster-recovery.md#scenario-1-complete-data-loss)

### Data Loss Suspected

1. **Immediately pause replication**:
```bash
curl -X PUT http://localhost:8083/connectors/sqlserver-source-connector/pause
curl -X PUT http://localhost:8083/connectors/postgres-sink-connector/pause
```

2. **Collect evidence**:
```bash
./scripts/bash/collect-diagnostics.sh
```

3. **Escalate** to senior team immediately

4. **Do not make changes** until approved by senior team

### Security Incident

1. **Isolate affected systems**:
```bash
docker-compose down
```

2. **Preserve evidence**:
```bash
# Collect logs
for container in $(docker ps -a --format '{{.Names}}'); do
    docker logs $container > /tmp/incident-$container.log 2>&1
done

# Collect diagnostics
./scripts/bash/collect-diagnostics.sh
```

3. **Report** to security team immediately

4. **Follow** security incident response procedures

---

## Contacts & Escalation

### Team Contacts

- **Database Operations**: `#database-ops` Slack
- **Infrastructure Team**: `#infrastructure` Slack
- **On-Call Engineer**: PagerDuty `cdc-pipeline-alerts`

### Escalation Path

1. **Level 1**: On-call engineer (0-30 min)
2. **Level 2**: Team lead (30-60 min)
3. **Level 3**: Infrastructure manager (60+ min)
4. **Emergency**: CTO (data loss, security incident)

---

## Revision History

| Date       | Version | Changes                    | Author |
|------------|---------|----------------------------|--------|
| 2025-12-20 | 1.0     | Initial operations runbook | System |
