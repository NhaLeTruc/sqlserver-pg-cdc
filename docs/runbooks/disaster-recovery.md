# Disaster Recovery Runbook

## Overview

This runbook provides step-by-step procedures for recovering the SQL Server to PostgreSQL CDC pipeline from various failure scenarios.

**Last Updated**: 2025-12-20
**Owner**: Database Operations Team
**Review Frequency**: Quarterly

## Recovery Time/Point Objectives

- **Recovery Time Objective (RTO)**: 4 hours
- **Recovery Point Objective (RPO)**: 24 hours (daily backups)
- **Backup Frequency**: Daily at 02:00 UTC
- **Backup Retention**: 7 days local, 30 days S3

## Prerequisites

- Access to production infrastructure
- Docker and Docker Compose installed
- AWS CLI configured (for S3 restores)
- Database credentials available
- Network access to database hosts

## Backup Locations

- **Local Backups**: `/var/backups/cdc/`
  - SQL Server: `/var/backups/cdc/sqlserver/`
  - PostgreSQL: `/var/backups/cdc/postgresql/`
  - Kafka Connect: `/var/backups/cdc/kafka-connect/`

- **Off-site Backups**: `s3://cdc-backups/YYYY/MM/DD/`
  - Retention: 30 days
  - Storage Class: STANDARD_IA

## Scenario 1: Complete Data Loss

**Symptoms**:
- All databases corrupted or inaccessible
- Data inconsistency across all tables
- Hardware failure requiring full rebuild

**Recovery Procedure**:

### Step 1: Collect Diagnostics (5 minutes)

```bash
cd /home/bob/WORK/sqlserver-pg-cdc
./scripts/bash/collect-diagnostics.sh
```

**Output**: `/tmp/cdc-diagnostics-TIMESTAMP.tar.gz`

**Actions**:
- Archive diagnostic tarball for post-incident review
- Check for obvious errors in logs before proceeding

### Step 2: Stop All Services (2 minutes)

```bash
docker-compose down
```

**Verification**:
```bash
docker ps
# Should show no CDC-related containers running
```

### Step 3: Identify Backup to Restore (5 minutes)

List available backups:
```bash
ls -lh /var/backups/cdc/sqlserver/
ls -lh /var/backups/cdc/postgresql/
```

Identify most recent backup timestamp (format: `YYYYMMDD_HHMMSS`).

**Example**:
```
warehouse_source_20251220_020000.bak
warehouse_target_20251220_020000.sql.gz
```

Backup timestamp: `20251220_020000`

### Step 4: Restore All Databases (30-60 minutes)

Using the automated restore script:

```bash
./scripts/bash/restore-databases.sh --timestamp 20251220_020000
```

**What this does**:
1. Verifies backup files exist
2. Restores all SQL Server databases
3. Restores all PostgreSQL databases
4. Verifies table counts match pre-backup state
5. Logs all operations to `/var/backups/cdc/restore_TIMESTAMP.log`

**Monitor Progress**:
```bash
tail -f /var/backups/cdc/restore_*.log
```

**Expected Output**:
```
[2025-12-20 14:30:00] Starting restore process
[2025-12-20 14:30:05] Restoring SQL Server database: warehouse_source
[2025-12-20 14:35:20]   ✓ Restore completed: warehouse_source
[2025-12-20 14:35:25] Restoring PostgreSQL database: warehouse_target
[2025-12-20 14:40:30]   ✓ Restore completed: warehouse_target
[2025-12-20 14:40:35] === Verification Results ===
[2025-12-20 14:40:40]   ✓ warehouse_source: 5 tables verified
[2025-12-20 14:40:45]   ✓ warehouse_target: 5 tables verified
[2025-12-20 14:40:50] ✓ Restore completed successfully
```

### Step 5: Start Services (5 minutes)

```bash
docker-compose up -d
```

**Wait for Services**:
```bash
# Wait 60 seconds for services to initialize
sleep 60

# Verify all containers running
docker ps
```

**Expected Containers**:
- sqlserver
- postgres
- zookeeper
- kafka
- schema-registry
- kafka-connect

### Step 6: Verify Database Connectivity (5 minutes)

**SQL Server**:
```bash
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "SELECT name, state_desc FROM sys.databases WHERE name = 'warehouse_source'"
```

**Expected Output**:
```
name              state_desc
warehouse_source  ONLINE
```

**PostgreSQL**:
```bash
docker exec postgres psql -U postgres -d warehouse_target \
    -c "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'"
```

**Expected Output**:
```
 count
-------
     5
```

### Step 7: Recreate CDC Configuration (10 minutes)

**Enable CDC on SQL Server**:
```bash
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "EXEC sys.sp_cdc_enable_db" -d warehouse_source

docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'customers',
        @role_name = NULL" -d warehouse_source
```

**Repeat for each table**: `products`, `orders`, `inventory`, `shipments`

**Verify CDC Enabled**:
```bash
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'warehouse_source'"
```

**Expected**: `is_cdc_enabled = 1`

### Step 8: Recreate Kafka Connectors (15 minutes)

**Source Connector** (SQL Server to Kafka):
```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "sqlserver-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "database.hostname": "sqlserver",
    "database.port": "1433",
    "database.user": "sa",
    "database.password": "'"${SQLSERVER_PASSWORD}"'",
    "database.dbname": "warehouse_source",
    "database.server.name": "sqlserver",
    "table.include.list": "dbo.customers,dbo.products,dbo.orders,dbo.inventory,dbo.shipments",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "schema-changes.warehouse"
  }
}'
```

**Sink Connector** (Kafka to PostgreSQL):
```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "postgres-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "connection.url": "jdbc:postgresql://postgres:5432/warehouse_target",
    "connection.user": "postgres",
    "connection.password": "'"${POSTGRES_PASSWORD}"'",
    "topics": "sqlserver.dbo.customers,sqlserver.dbo.products,sqlserver.dbo.orders,sqlserver.dbo.inventory,sqlserver.dbo.shipments",
    "auto.create": "false",
    "auto.evolve": "true",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "delete.enabled": "true"
  }
}'
```

**Verify Connectors**:
```bash
curl -s http://localhost:8083/connectors/sqlserver-source-connector/status | jq '.'
curl -s http://localhost:8083/connectors/postgres-sink-connector/status | jq '.'
```

**Expected**: Both connectors show `"state": "RUNNING"`

### Step 9: Run Reconciliation Check (15 minutes)

Wait 5 minutes for initial CDC sync, then run reconciliation:

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
    --tables customers,products,orders,inventory,shipments
```

**Expected Output**:
```
┌────────────┬──────────────┬──────────────┬────────┬─────────────┐
│ Table      │ Source Count │ Target Count │ Match  │ Difference  │
├────────────┼──────────────┼──────────────┼────────┼─────────────┤
│ customers  │ 10000        │ 10000        │ ✓ MATCH│ 0           │
│ products   │ 5000         │ 5000         │ ✓ MATCH│ 0           │
│ orders     │ 25000        │ 25000        │ ✓ MATCH│ 0           │
│ inventory  │ 5000         │ 5000         │ ✓ MATCH│ 0           │
│ shipments  │ 15000        │ 15000        │ ✓ MATCH│ 0           │
└────────────┴──────────────┴──────────────┴────────┴─────────────┘
```

**If Mismatches Occur**:
- Wait 10 more minutes for CDC lag to resolve
- Re-run reconciliation
- If still mismatched, proceed to "Scenario 3: Data Inconsistency"

### Step 10: Resume Monitoring (5 minutes)

```bash
# Check Prometheus metrics
curl -s http://localhost:9103/metrics | grep reconciliation

# Verify scheduled reconciliation
python -m src.cli.schedule status
```

**Expected**:
- Metrics available on port 9103
- Scheduled reconciliation job running

### Step 11: Document Recovery

Record the following in incident log:

- Recovery start time
- Recovery completion time
- Backup timestamp used
- Data loss window (time between backup and incident)
- Any issues encountered
- Verification results
- Total downtime

**Incident Report Template**:
```markdown
## Incident Recovery Report

**Date**: YYYY-MM-DD
**Incident**: Complete data loss
**Recovery Start**: HH:MM UTC
**Recovery Complete**: HH:MM UTC
**Total Downtime**: X hours

**Backup Used**: YYYYMMDD_HHMMSS
**Data Loss Window**: X hours (last backup to incident)

**Verification Results**:
- All tables: MATCH/MISMATCH
- Connector status: RUNNING/FAILED
- Reconciliation: PASS/FAIL

**Issues Encountered**:
- None / [list issues]

**Post-Incident Actions**:
- [Action items]
```

---

## Scenario 2: Single Database Corruption

**Symptoms**:
- One database (source or target) corrupted
- Other databases functioning normally
- Specific error messages indicating database corruption

**Recovery Procedure**:

### Step 1: Identify Corrupted Database

Check database status:

**SQL Server**:
```bash
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "SELECT name, state_desc FROM sys.databases"
```

**PostgreSQL**:
```bash
docker exec postgres psql -U postgres -c "\l"
```

**Look for**: `SUSPECT`, `RECOVERY_PENDING`, or connection errors

### Step 2: Stop Connectors (2 minutes)

Pause CDC to prevent further issues:

```bash
curl -X PUT http://localhost:8083/connectors/sqlserver-source-connector/pause
curl -X PUT http://localhost:8083/connectors/postgres-sink-connector/pause
```

### Step 3: Restore Specific Database (15-30 minutes)

**For SQL Server database `warehouse_source`**:
```bash
./scripts/bash/restore-databases.sh \
    --timestamp 20251220_020000 \
    --database warehouse_source \
    --type sqlserver
```

**For PostgreSQL database `warehouse_target`**:
```bash
./scripts/bash/restore-databases.sh \
    --timestamp 20251220_020000 \
    --database warehouse_target \
    --type postgresql
```

### Step 4: Re-enable CDC (SQL Server only, 5 minutes)

If you restored the SQL Server database:

```bash
# Enable CDC on database
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "EXEC sys.sp_cdc_enable_db" -d warehouse_source

# Enable CDC on each table
for TABLE in customers products orders inventory shipments; do
    docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
        -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
        -Q "EXEC sys.sp_cdc_enable_table
            @source_schema = N'dbo',
            @source_name = N'$TABLE',
            @role_name = NULL" -d warehouse_source
done
```

### Step 5: Resume Connectors (2 minutes)

```bash
curl -X PUT http://localhost:8083/connectors/sqlserver-source-connector/resume
curl -X PUT http://localhost:8083/connectors/postgres-sink-connector/resume
```

### Step 6: Verify and Reconcile (10 minutes)

Run reconciliation to verify consistency:

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
    --tables customers,products,orders,inventory,shipments
```

---

## Scenario 3: Data Inconsistency (No Corruption)

**Symptoms**:
- Reconciliation reports mismatches
- No database corruption
- Connector running but data drift detected

**Recovery Procedure**:

### Step 1: Collect Current State (5 minutes)

```bash
# Run diagnostic collection
./scripts/bash/collect-diagnostics.sh

# Run detailed reconciliation with checksums
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
```

### Step 2: Identify Affected Tables

Review reconciliation output to identify which tables have mismatches:

```
┌────────────┬──────────────┬──────────────┬────────┬─────────────┐
│ Table      │ Source Count │ Target Count │ Match  │ Difference  │
├────────────┼──────────────┼──────────────┼────────┼─────────────┤
│ customers  │ 10000        │ 10000        │ ✓ MATCH│ 0           │
│ products   │ 5000         │ 4998         │ ✗ MISMATCH│ -2       │
│ orders     │ 25000        │ 25000        │ ✓ MATCH│ 0           │
└────────────┴──────────────┴──────────────┴────────┴─────────────┘
```

In this example, `products` table has a mismatch.

### Step 3: Determine Root Cause

Check connector status and logs:

```bash
# Check connector status
curl -s http://localhost:8083/connectors/sqlserver-source-connector/status | jq '.'

# Check connector logs
docker logs kafka-connect --tail 100

# Check for CDC capture issues
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "SELECT * FROM sys.dm_cdc_errors" -d warehouse_source
```

**Common causes**:
- Connector was paused/stopped
- CDC capture job stopped
- Network partition
- Schema evolution issue

### Step 4: Resync Affected Tables

**Option A: Snapshot Resync (Recommended)**

Delete and recreate connector to trigger snapshot:

```bash
# Delete connector
curl -X DELETE http://localhost:8083/connectors/sqlserver-source-connector

# Wait 10 seconds
sleep 10

# Recreate connector with snapshot mode
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "sqlserver-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "database.hostname": "sqlserver",
    "database.port": "1433",
    "database.user": "sa",
    "database.password": "'"${SQLSERVER_PASSWORD}"'",
    "database.dbname": "warehouse_source",
    "database.server.name": "sqlserver",
    "table.include.list": "dbo.products",
    "snapshot.mode": "initial",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "schema-changes.warehouse"
  }
}'
```

**Option B: Manual Resync**

For small tables, truncate and resync:

```bash
# Truncate target table
docker exec postgres psql -U postgres -d warehouse_target \
    -c "TRUNCATE TABLE products CASCADE"

# Trigger snapshot via connector restart
curl -X POST http://localhost:8083/connectors/sqlserver-source-connector/restart
```

### Step 5: Verify Resync (10 minutes)

Wait 5 minutes, then verify:

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
    --tables products \
    --validate-checksums
```

**Expected**: `✓ MATCH` for all reconciled tables

---

## Scenario 4: Kafka Connect Failure

**Symptoms**:
- Kafka Connect container down
- Connectors not responding
- Port 8083 unreachable

**Recovery Procedure**:

### Step 1: Check Container Status (1 minute)

```bash
docker ps -a | grep kafka-connect
```

**Check logs**:
```bash
docker logs kafka-connect --tail 100
```

### Step 2: Restart Kafka Connect (2 minutes)

```bash
docker-compose restart kafka-connect
```

**Wait for startup** (60 seconds):
```bash
sleep 60
```

### Step 3: Verify Service (2 minutes)

```bash
curl -s http://localhost:8083/ | jq '.'
```

**Expected Output**:
```json
{
  "version": "7.3.0",
  "commit": "...",
  "kafka_cluster_id": "..."
}
```

### Step 4: Check Connector Configuration (5 minutes)

List connectors:
```bash
curl -s http://localhost:8083/connectors | jq '.'
```

**If no connectors exist**, restore from backup:

```bash
# Find most recent Kafka Connect backup
LATEST_BACKUP=$(ls -t /var/backups/cdc/kafka-connect/connectors_*.json | head -1)

# Review connector list
cat "$LATEST_BACKUP"

# Recreate connectors manually (see Step 8 in Scenario 1)
```

### Step 5: Verify CDC Flow (10 minutes)

Make a test change in SQL Server:

```bash
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "INSERT INTO dbo.customers (customer_id, name, email)
        VALUES (99999, 'Test Customer', 'test@example.com')" \
    -d warehouse_source
```

Wait 30 seconds, then check PostgreSQL:

```bash
docker exec postgres psql -U postgres -d warehouse_target \
    -c "SELECT * FROM customers WHERE customer_id = 99999"
```

**Expected**: Record should exist in PostgreSQL

Clean up test data:

```bash
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
    -Q "DELETE FROM dbo.customers WHERE customer_id = 99999" \
    -d warehouse_source
```

---

## Scenario 5: Recovery from S3 Backups

**Symptoms**:
- Local backups unavailable or corrupted
- Need to restore from off-site S3 backups

**Recovery Procedure**:

### Step 1: List S3 Backups (2 minutes)

```bash
aws s3 ls s3://cdc-backups/ --recursive | grep "warehouse_source\|warehouse_target"
```

**Example Output**:
```
2025/12/19/sqlserver/warehouse_source_20251219_020000.bak
2025/12/19/postgresql/warehouse_target_20251219_020000.sql.gz
2025/12/20/sqlserver/warehouse_source_20251220_020000.bak
2025/12/20/postgresql/warehouse_target_20251220_020000.sql.gz
```

### Step 2: Download Backups (5-15 minutes)

```bash
# Create local backup directory
mkdir -p /var/backups/cdc/sqlserver
mkdir -p /var/backups/cdc/postgresql

# Download SQL Server backups
aws s3 cp s3://cdc-backups/2025/12/20/sqlserver/ \
    /var/backups/cdc/sqlserver/ --recursive

# Download PostgreSQL backups
aws s3 cp s3://cdc-backups/2025/12/20/postgresql/ \
    /var/backups/cdc/postgresql/ --recursive
```

### Step 3: Verify Downloaded Backups (2 minutes)

```bash
ls -lh /var/backups/cdc/sqlserver/
ls -lh /var/backups/cdc/postgresql/
```

Check file integrity:
```bash
file /var/backups/cdc/sqlserver/warehouse_source_20251220_020000.bak
file /var/backups/cdc/postgresql/warehouse_target_20251220_020000.sql.gz
```

### Step 4: Proceed with Standard Restore

Follow **Scenario 1: Complete Data Loss**, starting from Step 4.

---

## Preventive Maintenance

### Daily Tasks

**Automated** (via cron):
- Run backup script at 02:00 UTC: `./scripts/bash/backup-databases.sh --s3-bucket cdc-backups`
- Run reconciliation at 06:00 UTC: `python -m src.cli.reconcile ...`

**Manual verification**:
- Check backup success: `tail -20 /var/backups/cdc/backup_*.log`
- Review reconciliation reports: `ls -lh ./reconciliation_reports/`

### Weekly Tasks

- Review diagnostic metrics: `curl -s http://localhost:9103/metrics`
- Check disk space: `df -h /var/backups/cdc`
- Verify S3 backups uploaded: `aws s3 ls s3://cdc-backups/$(date +%Y/%m/%d)/`

### Monthly Tasks

- Review incident reports and recovery times
- Update RTO/RPO targets based on actual performance
- Validate backup retention policies
- Test restore procedure (see Quarterly DR Drill)

### Quarterly Tasks

**Disaster Recovery Drill** (schedule 4 hours):

1. Announce maintenance window
2. Collect diagnostics (baseline)
3. Perform complete data loss recovery (Scenario 1)
4. Measure actual RTO/RPO
5. Document lessons learned
6. Update runbook with improvements

**Success Criteria**:
- Recovery completed within RTO (4 hours)
- All reconciliation checks pass
- Connectors running and processing changes
- Zero data loss within RPO window

---

## Troubleshooting

### Issue: Restore Script Fails

**Symptoms**: `restore-databases.sh` exits with error

**Diagnostic Steps**:
1. Check log file: `tail -50 /var/backups/cdc/restore_*.log`
2. Verify backup files exist: `ls -lh /var/backups/cdc/*/`
3. Check disk space: `df -h /var/backups`
4. Verify container is running: `docker ps | grep sqlserver`

**Common Solutions**:
- Insufficient disk space: Free up space, then retry
- Backup file corrupted: Use previous day's backup
- Container not running: Start container, then retry
- Permissions issue: Check file ownership and chmod +x

### Issue: Connector Won't Start

**Symptoms**: Connector shows `FAILED` state

**Diagnostic Steps**:
```bash
curl -s http://localhost:8083/connectors/sqlserver-source-connector/status | jq '.connector.trace'
```

**Common Solutions**:
- Database not accessible: Check database connectivity
- Invalid credentials: Update connector config with correct credentials
- Schema mismatch: Verify table schemas match connector expectations
- CDC not enabled: Re-enable CDC on SQL Server

### Issue: Reconciliation Shows Persistent Mismatches

**Symptoms**: After restore, reconciliation still shows differences

**Diagnostic Steps**:
1. Check CDC lag: Look for delays in change capture
2. Verify connector is running: `curl -s http://localhost:8083/connectors/sqlserver-source-connector/status`
3. Check for CDC errors: Query `sys.dm_cdc_errors` on SQL Server
4. Review connector logs: `docker logs kafka-connect --tail 200`

**Common Solutions**:
- Wait longer: CDC can have 5-10 minute lag
- Restart connector: Trigger snapshot resync
- Manual resync: Truncate target table and resync
- Check for schema drift: Verify column definitions match

---

## Contact Information

**Primary On-call**: Database Operations Team
**Escalation**: Infrastructure Team Lead
**Vendor Support**: Debezium Community Forum, Confluent Support

**Emergency Contacts**:
- Database Ops Slack: `#database-ops`
- Infrastructure Slack: `#infrastructure`
- PagerDuty: `cdc-pipeline-alerts`

---

## Revision History

| Date       | Version | Changes                          | Author |
|------------|---------|----------------------------------|--------|
| 2025-12-20 | 1.0     | Initial runbook creation         | System |

---

## Appendix A: Quick Reference Commands

### Check Service Health
```bash
# All containers
docker ps

# Database connectivity
docker exec sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SQLSERVER_PASSWORD" -Q "SELECT @@VERSION"
docker exec postgres psql -U postgres -c "SELECT version()"

# Kafka Connect
curl -s http://localhost:8083/ | jq '.'

# Connector status
curl -s http://localhost:8083/connectors/sqlserver-source-connector/status | jq '.connector.state'
```

### Backup Commands
```bash
# Manual backup
./scripts/bash/backup-databases.sh

# Backup with S3 upload
./scripts/bash/backup-databases.sh --s3-bucket cdc-backups

# List backups
ls -lh /var/backups/cdc/sqlserver/
ls -lh /var/backups/cdc/postgresql/
```

### Restore Commands
```bash
# Restore all from timestamp
./scripts/bash/restore-databases.sh --timestamp 20251220_020000

# Restore specific database
./scripts/bash/restore-databases.sh --timestamp 20251220_020000 --database warehouse_source --type sqlserver
```

### Reconciliation Commands
```bash
# Basic reconciliation
python -m src.cli.reconcile --source-type sqlserver --source-host sqlserver --source-port 1433 --source-database warehouse_source --source-user sa --source-password "$SQLSERVER_PASSWORD" --target-type postgresql --target-host postgres --target-port 5432 --target-database warehouse_target --target-user postgres --target-password "$POSTGRES_PASSWORD" --tables customers,products,orders,inventory,shipments

# With checksum validation
python -m src.cli.reconcile ... --validate-checksums
```

### Diagnostic Commands
```bash
# Collect diagnostics
./scripts/bash/collect-diagnostics.sh

# Check metrics
curl -s http://localhost:9103/metrics | grep reconciliation

# Check logs
docker logs kafka-connect --tail 100
docker logs sqlserver --tail 100
docker logs postgres --tail 100
```

---

## Appendix B: Recovery Time Matrix

| Scenario                    | Estimated Time | Complexity | Risk  |
|-----------------------------|----------------|------------|-------|
| Complete Data Loss          | 2-4 hours      | High       | High  |
| Single Database Corruption  | 30-60 minutes  | Medium     | Medium|
| Data Inconsistency          | 15-30 minutes  | Low        | Low   |
| Kafka Connect Failure       | 10-20 minutes  | Low        | Low   |
| S3 Restore                  | 3-5 hours      | High       | Medium|

**Notes**:
- Times assume local backups available (add 1-2 hours for S3 download)
- Times assume no complications or errors
- First-time execution may take 2x estimated time
- Practice runs improve execution time by 30-50%