# Schema Evolution Operational Runbook

## Overview

This runbook provides step-by-step procedures for manually handling schema evolution scenarios in the SQL Server to PostgreSQL CDC pipeline. Use this guide when automated schema evolution is insufficient or when planning complex schema changes.

**Last Updated**: 2025-12-21
**Audience**: Database administrators, DevOps engineers, Data platform operators
**Prerequisites**: Access to SQL Server, PostgreSQL, Kafka Connect cluster, monitoring tools

---

## Table of Contents

1. [Quick Reference](#quick-reference)
2. [Pre-Change Checklist](#pre-change-checklist)
3. [Common Scenarios](#common-scenarios)
4. [Emergency Procedures](#emergency-procedures)
5. [Monitoring and Validation](#monitoring-and-validation)
6. [Rollback Procedures](#rollback-procedures)
7. [Troubleshooting](#troubleshooting)
8. [Appendix](#appendix)

---

## Quick Reference

### Schema Change Decision Matrix

| Change Type | Auto-Handled? | Action Required | Downtime? | Risk Level |
|------------|---------------|-----------------|-----------|------------|
| ADD COLUMN (nullable) | ✅ Yes | Monitor only | No | Low |
| ADD COLUMN (NOT NULL with default) | ✅ Yes | Monitor only | No | Low |
| ADD COLUMN (NOT NULL, no default) | ❌ No | Manual intervention | Yes | Medium |
| DROP COLUMN | ❌ No | Manual DROP in PostgreSQL | No | Medium |
| RENAME COLUMN | ❌ No | Manual RENAME in PostgreSQL | No | Medium |
| ALTER TYPE (compatible) | ✅ Yes | Monitor only | No | Low |
| ALTER TYPE (incompatible) | ❌ No | Pause pipeline, migrate data | Yes | High |
| ADD PRIMARY KEY | ⚠️ Partial | Verify replication | No | Medium |
| DROP PRIMARY KEY | ❌ No | High-risk operation | Yes | Critical |
| ADD INDEX | ✅ Yes | N/A (not replicated) | No | Low |
| ADD CONSTRAINT (CHECK/FK) | ⚠️ Partial | Recreate in PostgreSQL | No | Medium |

### Key Configuration Settings

```properties
# Source Connector (debezium/sqlserver-source.json)
include.schema.changes=true                                    # Enable schema change tracking
schema.history.internal.kafka.topic=schema-changes.warehouse_source
provide.transaction.metadata=true

# Sink Connector (kafka-connect/postgresql-sink.json)
auto.create=false                                              # Prevent auto table creation
auto.evolve=true                                               # Enable automatic column addition
errors.deadletterqueue.topic.name=dlq-postgresql-sink         # DLQ for failed messages
```

### Emergency Contacts

| Component | Monitoring URL | Log Location |
|-----------|---------------|--------------|
| Debezium Source | http://localhost:8083/connectors/sqlserver-source/status | `/logs/debezium-source.log` |
| PostgreSQL Sink | http://localhost:8083/connectors/postgresql-sink/status | `/logs/postgresql-sink.log` |
| Schema Registry | http://localhost:8081 | `/logs/schema-registry.log` |
| Kafka Topics | `schema-changes.warehouse_source`, `dlq-postgresql-sink` | N/A |

---

## Pre-Change Checklist

### Planning Phase

- [ ] **Document the Change**
  - Create change ticket with rationale, scope, and timeline
  - Identify affected tables and downstream dependencies
  - Estimate row count for affected tables (use `src/reconciliation/compare.py --row-count`)

- [ ] **Assess Impact**
  - Review [Schema Change Decision Matrix](#schema-change-decision-matrix)
  - Determine if change is auto-handled or requires manual intervention
  - Calculate lag tolerance (max acceptable replication delay)

- [ ] **Validate in Non-Production**
  - Execute change in DEV environment first
  - Monitor Kafka Connect logs for errors
  - Verify data appears correctly in PostgreSQL replica
  - Run reconciliation: `python src/reconciliation/cli.py reconcile --table <table_name> --checksum`

- [ ] **Prepare Rollback Plan**
  - Document rollback SQL for both databases
  - Identify point-in-time recovery options
  - Test rollback procedure in DEV

### Pre-Execution

- [ ] **Communication**
  - Notify stakeholders of change window (even if no downtime expected)
  - Update status page / operations channel

- [ ] **Backup**
  - PostgreSQL: `pg_dump -h localhost -U postgres -d warehouse_target -t <table_name> > backup_$(date +%Y%m%d_%H%M%S).sql`
  - SQL Server: Create snapshot or full backup

- [ ] **Baseline Metrics**
  ```bash
  # Record current lag
  ./scripts/bash/monitor.sh --component lag

  # Record row counts
  python src/reconciliation/cli.py reconcile --table <table_name> --row-count-only

  # Check DLQ is empty
  kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic dlq-postgresql-sink --from-beginning --timeout-ms 5000
  ```

- [ ] **Pause Sink Connector (if high-risk change)**
  ```bash
  ./scripts/bash/pause-resume.sh --connector postgresql-sink --action pause
  ```

---

## Common Scenarios

### Scenario 1: Add Nullable Column

**Complexity**: Low
**Auto-Handled**: Yes
**Downtime**: None
**Estimated Duration**: 5-10 minutes

#### Procedure

1. **Execute DDL in SQL Server**
   ```sql
   USE warehouse_source;
   ALTER TABLE dbo.customers
   ADD email_secondary VARCHAR(255) NULL;
   ```

2. **Monitor Schema Change Event**
   ```bash
   # Watch schema change topic
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic schema-changes.warehouse_source --from-beginning

   # Wait for event matching table name
   # Expected: Schema change event with operation=ADD COLUMN
   ```

3. **Verify Auto-Evolution in PostgreSQL**
   ```bash
   # Wait 30-60 seconds for JDBC Sink to process
   psql -h localhost -U postgres -d warehouse_target -c "\d+ customers"

   # Verify column appears with correct type
   # Expected: email_secondary | character varying(255) |
   ```

4. **Validate Data Flow**
   ```sql
   -- Insert test row in SQL Server
   INSERT INTO dbo.customers (customer_id, name, email_secondary)
   VALUES (999999, 'Test User', 'test@example.com');

   -- Verify in PostgreSQL (wait ~5 seconds)
   SELECT customer_id, name, email_secondary
   FROM customers
   WHERE customer_id = 999999;

   -- Cleanup
   DELETE FROM dbo.customers WHERE customer_id = 999999;
   ```

5. **Post-Change Verification**
   - Check sink connector status: `curl http://localhost:8083/connectors/postgresql-sink/status | jq`
   - Verify no DLQ messages
   - Run reconciliation on affected table

#### Rollback

```sql
-- PostgreSQL (if needed)
ALTER TABLE customers DROP COLUMN email_secondary;

-- SQL Server
ALTER TABLE dbo.customers DROP COLUMN email_secondary;
```

---

### Scenario 2: Add NOT NULL Column with Default

**Complexity**: Low-Medium
**Auto-Handled**: Yes (with caveats)
**Downtime**: None
**Estimated Duration**: 10-20 minutes

#### Procedure

1. **Execute DDL in SQL Server**
   ```sql
   ALTER TABLE dbo.orders
   ADD processing_status VARCHAR(50) NOT NULL DEFAULT 'pending';
   ```

2. **Monitor for Errors**
   ```bash
   # Watch sink connector logs
   docker logs -f kafka-connect | grep -i "processing_status\|error\|exception"

   # Monitor DLQ for issues
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic dlq-postgresql-sink --timeout-ms 10000
   ```

3. **Verify Column in PostgreSQL**
   ```bash
   psql -h localhost -U postgres -d warehouse_target -c "\d+ orders"

   # Check default value applied
   # Expected: processing_status | character varying(50) | not null default 'pending'::character varying
   ```

4. **Handle Existing NULL Records (if any routed to DLQ)**
   ```bash
   # If DLQ has messages, investigate
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic dlq-postgresql-sink --from-beginning --max-messages 10 \
     --property print.headers=true

   # Manually backfill if needed
   psql -h localhost -U postgres -d warehouse_target -c \
     "UPDATE orders SET processing_status = 'pending' WHERE processing_status IS NULL;"
   ```

#### Warnings

- **Existing Rows**: SQL Server applies default to existing rows. PostgreSQL receives UPDATE events for ALL rows if SQL Server backfills.
- **Large Tables**: For tables with millions of rows, SQL Server's backfill can cause replication lag. Consider adding column as nullable first, then applying NOT NULL constraint separately.

#### Rollback

```sql
-- PostgreSQL
ALTER TABLE orders ALTER COLUMN processing_status DROP NOT NULL;
ALTER TABLE orders ALTER COLUMN processing_status DROP DEFAULT;
ALTER TABLE orders DROP COLUMN processing_status;

-- SQL Server
ALTER TABLE dbo.orders DROP COLUMN processing_status;
```

---

### Scenario 3: Drop Column

**Complexity**: Medium
**Auto-Handled**: No (safety feature)
**Downtime**: None
**Estimated Duration**: 15-30 minutes

#### Why Manual?

The JDBC Sink connector **does not automatically drop columns** to prevent accidental data loss. Dropped columns in SQL Server result in the column being ignored in PostgreSQL (old data preserved).

#### Procedure

1. **Verify Column is Safe to Drop**
   ```sql
   -- Check for downstream dependencies in PostgreSQL
   SELECT
     n.nspname AS schema_name,
     c.relname AS table_name,
     a.attname AS column_name,
     pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
   FROM pg_catalog.pg_attribute a
   JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
   JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
   WHERE a.attname = 'deprecated_field'
     AND NOT a.attisdropped
     AND a.attnum > 0;

   -- Check views, functions, triggers using the column
   SELECT DISTINCT
     schemaname,
     viewname
   FROM pg_views
   WHERE definition LIKE '%deprecated_field%';
   ```

2. **Drop Column in SQL Server**
   ```sql
   ALTER TABLE dbo.products DROP COLUMN deprecated_field;
   ```

3. **Monitor Replication**
   ```bash
   # Verify no errors in sink connector
   curl http://localhost:8083/connectors/postgresql-sink/status | jq '.tasks[0].state'
   # Expected: "RUNNING"

   # Check recent logs
   docker logs kafka-connect --tail 50 | grep -i error
   ```

4. **Drop Column in PostgreSQL**
   ```sql
   -- After confirming replication is stable (wait 5-10 minutes)
   \c warehouse_target
   ALTER TABLE products DROP COLUMN deprecated_field;
   ```

5. **Verify Table Structure Match**
   ```bash
   # Compare schemas
   psql -h localhost -U postgres -d warehouse_target -c "\d+ products" > /tmp/pg_schema.txt

   # Manually compare with SQL Server schema
   # Ensure both have identical columns (except PostgreSQL-specific like __deleted)
   ```

#### Rollback

```sql
-- Add column back (if data still exists in PostgreSQL)
-- SQL Server
ALTER TABLE dbo.products ADD deprecated_field VARCHAR(100) NULL;

-- PostgreSQL (if already dropped)
ALTER TABLE products ADD COLUMN deprecated_field VARCHAR(100);
-- Note: Historical data lost if column was dropped in PostgreSQL
```

---

### Scenario 4: Rename Column

**Complexity**: Medium
**Auto-Handled**: No
**Downtime**: Depends on approach
**Estimated Duration**: 30-60 minutes

#### Challenge

Debezium treats RENAME as DROP + ADD. This requires careful coordination to prevent data loss.

#### Approach A: Zero-Downtime (Recommended)

Use shadow column technique:

1. **Add New Column in SQL Server**
   ```sql
   -- Add new column (nullable initially)
   ALTER TABLE dbo.customers ADD email_address VARCHAR(255) NULL;

   -- Copy data from old column
   UPDATE dbo.customers SET email_address = email_column WHERE email_column IS NOT NULL;
   ```

2. **Wait for Replication**
   ```bash
   # Monitor lag
   ./scripts/bash/monitor.sh --component lag

   # Verify new column in PostgreSQL
   psql -h localhost -U postgres -d warehouse_target -c "\d+ customers"
   ```

3. **Update Application Logic**
   - Deploy application changes to write to `email_address` instead of `email_column`
   - Keep reading from both columns during transition (fallback logic)

4. **Monitor and Soak**
   - Run for 24-48 hours
   - Verify all writes go to new column
   - Run reconciliation to ensure data consistency

5. **Drop Old Column**
   ```sql
   -- SQL Server
   ALTER TABLE dbo.customers DROP COLUMN email_column;

   -- PostgreSQL (after verification)
   ALTER TABLE customers DROP COLUMN email_column;
   ```

#### Approach B: Maintenance Window

Use when zero-downtime is not required:

1. **Pause Sink Connector**
   ```bash
   ./scripts/bash/pause-resume.sh --connector postgresql-sink --action pause
   ```

2. **Rename Column in Both Databases**
   ```sql
   -- SQL Server
   EXEC sp_rename 'dbo.customers.email_column', 'email_address', 'COLUMN';

   -- PostgreSQL
   ALTER TABLE customers RENAME COLUMN email_column TO email_address;
   ```

3. **Resume Sink Connector**
   ```bash
   ./scripts/bash/pause-resume.sh --connector postgresql-sink --action resume
   ```

4. **Verify Replication**
   ```bash
   # Insert test row
   INSERT INTO dbo.customers (customer_id, email_address)
   VALUES (999998, 'rename-test@example.com');

   # Check PostgreSQL
   SELECT * FROM customers WHERE customer_id = 999998;
   ```

#### Rollback

```sql
-- Approach A: Drop new column, application rollback
ALTER TABLE dbo.customers DROP COLUMN email_address;
ALTER TABLE customers DROP COLUMN email_address;

-- Approach B: Rename back
EXEC sp_rename 'dbo.customers.email_address', 'email_column', 'COLUMN';
ALTER TABLE customers RENAME COLUMN email_address TO email_column;
```

---

### Scenario 5: Incompatible Type Change

**Complexity**: High
**Auto-Handled**: No (routes to DLQ)
**Downtime**: Yes (recommended)
**Estimated Duration**: 1-4 hours (depends on table size)

#### Example

Changing `phone_number` from `VARCHAR(20)` to `BIGINT` (assuming numeric-only values).

#### Procedure

1. **Pause Sink Connector**
   ```bash
   ./scripts/bash/pause-resume.sh --connector postgresql-sink --action pause

   # Verify paused
   curl http://localhost:8083/connectors/postgresql-sink/status | jq '.connector.state'
   # Expected: "PAUSED"
   ```

2. **Alter Column in SQL Server**
   ```sql
   -- Backup first!
   SELECT * INTO dbo.customers_backup FROM dbo.customers;

   -- Alter type
   ALTER TABLE dbo.customers ALTER COLUMN phone_number BIGINT;
   ```

3. **Alter Column in PostgreSQL**
   ```sql
   -- Attempt direct conversion
   ALTER TABLE customers ALTER COLUMN phone_number TYPE BIGINT USING phone_number::BIGINT;

   -- If conversion fails, data cleanup required:
   -- UPDATE customers SET phone_number = regexp_replace(phone_number, '[^0-9]', '', 'g')
   -- WHERE phone_number IS NOT NULL;
   ```

4. **Resume Sink Connector**
   ```bash
   ./scripts/bash/pause-resume.sh --connector postgresql-sink --action resume
   ```

5. **Monitor for DLQ Messages**
   ```bash
   # Watch DLQ for 10 minutes
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic dlq-postgresql-sink --timeout-ms 600000

   # If messages appear, investigate error headers
   # Common issues: unconverted data, null handling
   ```

6. **Reconcile Data**
   ```bash
   # Full checksum reconciliation
   python src/reconciliation/cli.py reconcile \
     --table customers \
     --checksum \
     --chunk-size 10000

   # Review report
   cat reports/reconciliation_customers_*.json
   ```

#### Alternative: Shadow Column Migration

For extremely large tables where downtime is unacceptable:

1. Add new column with target type
2. Backfill data using batched updates
3. Switch application to use new column
4. Drop old column after soak period

(See Approach A in Scenario 4 for detailed steps)

#### Rollback

```sql
-- SQL Server
ALTER TABLE dbo.customers ALTER COLUMN phone_number VARCHAR(20);

-- PostgreSQL
ALTER TABLE customers ALTER COLUMN phone_number TYPE VARCHAR(20);

-- Resume connector if paused
./scripts/bash/pause-resume.sh --connector postgresql-sink --action resume
```

---

### Scenario 6: Add Primary Key

**Complexity**: Medium-High
**Auto-Handled**: Partial (key metadata may not replicate)
**Downtime**: Recommended for large tables
**Estimated Duration**: 30 minutes - 2 hours

#### Considerations

- Debezium replicates data changes, not DDL constraints
- Primary keys must be recreated manually in PostgreSQL
- Affects reconciliation tool's sorting and deduplication

#### Procedure

1. **Add Primary Key in SQL Server**
   ```sql
   -- Ensure column is unique and NOT NULL first
   ALTER TABLE dbo.transactions ALTER COLUMN transaction_id BIGINT NOT NULL;

   -- Add primary key
   ALTER TABLE dbo.transactions
   ADD CONSTRAINT PK_transactions PRIMARY KEY CLUSTERED (transaction_id);
   ```

2. **Monitor Replication**
   ```bash
   # Check for errors (PK violations if duplicates exist)
   docker logs kafka-connect --tail 100 | grep -i "duplicate\|unique\|primary"
   ```

3. **Add Primary Key in PostgreSQL**
   ```sql
   -- Verify uniqueness first
   SELECT transaction_id, COUNT(*)
   FROM transactions
   GROUP BY transaction_id
   HAVING COUNT(*) > 1;

   -- If no duplicates, add PK
   ALTER TABLE transactions ADD PRIMARY KEY (transaction_id);
   ```

4. **Update Reconciliation Configuration**
   ```bash
   # Re-run reconciliation to verify PK detection
   python src/reconciliation/cli.py reconcile --table transactions --row-count-only

   # Check logs for PK detection
   # Expected: "Primary key detected: ['transaction_id']"
   ```

#### Warnings

- **Duplicate Data**: If duplicates exist in PostgreSQL from pre-PK era, constraint will fail
- **Performance**: Adding PK on large tables locks the table in PostgreSQL (use `CONCURRENTLY` option if supported)

#### Rollback

```sql
-- SQL Server
ALTER TABLE dbo.transactions DROP CONSTRAINT PK_transactions;

-- PostgreSQL
ALTER TABLE transactions DROP CONSTRAINT transactions_pkey;
```

---

### Scenario 7: Complex Multi-Step Migration

**Complexity**: Very High
**Auto-Handled**: No
**Downtime**: Required
**Estimated Duration**: 4-24 hours (depends on scope)

#### Example

Splitting `customer_name` (VARCHAR) into `first_name` and `last_name`.

#### Procedure

1. **Planning Phase** (1-2 weeks before execution)
   - Document migration plan with rollback steps
   - Write data transformation scripts
   - Test in DEV and STAGING environments
   - Schedule maintenance window

2. **Pre-Migration Setup**
   ```sql
   -- SQL Server: Add new columns
   ALTER TABLE dbo.customers ADD first_name VARCHAR(100) NULL;
   ALTER TABLE dbo.customers ADD last_name VARCHAR(100) NULL;

   -- Wait for replication to PostgreSQL (~5 min)
   ```

3. **Pause Replication**
   ```bash
   # Stop both connectors to prevent mid-migration conflicts
   ./scripts/bash/pause-resume.sh --connector sqlserver-source --action pause
   ./scripts/bash/pause-resume.sh --connector postgresql-sink --action pause
   ```

4. **Data Migration in SQL Server**
   ```sql
   -- Split names (example logic, adjust for data quality)
   UPDATE dbo.customers
   SET
     first_name = CASE
       WHEN CHARINDEX(' ', customer_name) > 0
       THEN LEFT(customer_name, CHARINDEX(' ', customer_name) - 1)
       ELSE customer_name
     END,
     last_name = CASE
       WHEN CHARINDEX(' ', customer_name) > 0
       THEN SUBSTRING(customer_name, CHARINDEX(' ', customer_name) + 1, LEN(customer_name))
       ELSE ''
     END
   WHERE first_name IS NULL OR last_name IS NULL;

   -- Verify results
   SELECT customer_name, first_name, last_name FROM dbo.customers SAMPLE(100);
   ```

5. **Apply Same Migration in PostgreSQL**
   ```sql
   -- Replicate exact transformation logic
   UPDATE customers
   SET
     first_name = CASE
       WHEN position(' ' in customer_name) > 0
       THEN substring(customer_name from 1 for position(' ' in customer_name) - 1)
       ELSE customer_name
     END,
     last_name = CASE
       WHEN position(' ' in customer_name) > 0
       THEN substring(customer_name from position(' ' in customer_name) + 1)
       ELSE ''
     END
   WHERE first_name IS NULL OR last_name IS NULL;
   ```

6. **Reconcile Data**
   ```bash
   # Verify row counts match
   python src/reconciliation/cli.py reconcile --table customers --row-count-only

   # Checksum validation
   python src/reconciliation/cli.py reconcile \
     --table customers \
     --checksum \
     --chunk-size 5000
   ```

7. **Resume Replication**
   ```bash
   ./scripts/bash/pause-resume.sh --connector sqlserver-source --action resume
   ./scripts/bash/pause-resume.sh --connector postgresql-sink --action resume

   # Monitor lag
   ./scripts/bash/monitor.sh --component lag
   ```

8. **Post-Migration Cleanup** (after soak period)
   ```sql
   -- Drop old column (see Scenario 3)
   ALTER TABLE dbo.customers DROP COLUMN customer_name;
   ALTER TABLE customers DROP COLUMN customer_name;
   ```

#### Rollback

```sql
-- If caught before cleanup
-- Repopulate customer_name from first_name + last_name
UPDATE dbo.customers
SET customer_name = CONCAT(first_name, ' ', last_name)
WHERE customer_name IS NULL;

UPDATE customers
SET customer_name = first_name || ' ' || last_name
WHERE customer_name IS NULL;

-- Drop new columns
ALTER TABLE dbo.customers DROP COLUMN first_name;
ALTER TABLE dbo.customers DROP COLUMN last_name;
ALTER TABLE customers DROP COLUMN first_name;
ALTER TABLE customers DROP COLUMN last_name;
```

---

## Emergency Procedures

### Emergency 1: Sink Connector in FAILED State

**Symptoms**: Connector status shows `"state": "FAILED"`, data not replicating

#### Immediate Actions

1. **Check Connector Status**
   ```bash
   curl http://localhost:8083/connectors/postgresql-sink/status | jq

   # Note the error message in .tasks[0].trace
   ```

2. **Review Recent Schema Changes**
   ```bash
   # Check last 10 schema change events
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic schema-changes.warehouse_source \
     --from-beginning --max-messages 10 --timeout-ms 5000
   ```

3. **Identify Root Cause**
   - **Error: "Column does not exist"**: Type mismatch or missing column in PostgreSQL
   - **Error: "Value too long"**: VARCHAR size mismatch
   - **Error: "Violates not-null constraint"**: NULL value for NOT NULL column

4. **Temporary Fix: Restart Connector**
   ```bash
   curl -X POST http://localhost:8083/connectors/postgresql-sink/restart

   # If task fails again immediately, deeper fix needed
   ```

5. **Drain DLQ**
   ```bash
   # Consume and log all DLQ messages
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic dlq-postgresql-sink \
     --from-beginning > /tmp/dlq_messages_$(date +%Y%m%d_%H%M%S).log

   # Analyze for patterns
   grep -i "exception\|error" /tmp/dlq_messages_*.log
   ```

6. **Apply Schema Fix**
   ```sql
   -- Example: Add missing column
   ALTER TABLE customers ADD COLUMN missing_field VARCHAR(255);

   -- Example: Increase column size
   ALTER TABLE orders ALTER COLUMN notes TYPE TEXT;
   ```

7. **Resume Connector**
   ```bash
   curl -X PUT http://localhost:8083/connectors/postgresql-sink/resume
   ```

#### Escalation

If issue persists >15 minutes:
- Review [Troubleshooting](#troubleshooting) section
- Check `/docs/troubleshooting.md` for known issues
- Contact platform team with connector logs and DLQ samples

---

### Emergency 2: Massive Replication Lag

**Symptoms**: PostgreSQL replica hours/days behind SQL Server

#### Immediate Actions

1. **Quantify Lag**
   ```bash
   ./scripts/bash/monitor.sh --component lag

   # Check Kafka consumer lag
   kafka-consumer-groups --bootstrap-server localhost:9092 \
     --describe --group postgresql-sink-connector
   ```

2. **Identify Cause**
   - **Recent Schema Change?**: Large table backfill from adding NOT NULL column
   - **High Volume of Updates?**: Bulk data migration in progress
   - **Slow PostgreSQL Writes?**: Check disk I/O, locks, or long-running queries

3. **Check PostgreSQL Performance**
   ```sql
   -- Long-running queries
   SELECT pid, now() - pg_stat_activity.query_start AS duration, query
   FROM pg_stat_activity
   WHERE state = 'active'
   ORDER BY duration DESC;

   -- Table locks
   SELECT * FROM pg_locks WHERE granted = false;
   ```

4. **Increase Sink Connector Throughput** (temporary)
   ```bash
   # Update connector config (requires pause/resume)
   ./scripts/bash/pause-resume.sh --connector postgresql-sink --action pause

   # Edit config: increase batch.size from 1000 to 5000
   # Edit generated config file and redeploy
   vim docker/configs/kafka-connect/postgresql-sink.json
   ./scripts/bash/deploy-connector.sh --connector postgresql-sink

   ./scripts/bash/pause-resume.sh --connector postgresql-sink --action resume
   ```

5. **Monitor Progress**
   ```bash
   # Watch lag decrease
   watch -n 10 './scripts/bash/monitor.sh --component lag'
   ```

#### Escalation

If lag >6 hours:
- Consider snapshot + incremental catch-up strategy
- Evaluate if recent schema change should be rolled back
- Review PostgreSQL resource limits (connections, memory)

---

### Emergency 3: Data Mismatch Detected

**Symptoms**: Reconciliation reports row count or checksum differences

#### Immediate Actions

1. **Run Detailed Reconciliation**
   ```bash
   python src/reconciliation/cli.py reconcile \
     --table <affected_table> \
     --checksum \
     --chunk-size 1000 \
     --verbose
   ```

2. **Identify Mismatch Pattern**
   ```bash
   # Review reconciliation report
   cat reports/reconciliation_<table>_*.json | jq

   # Common patterns:
   # - Row count off by small amount: Missing recent transactions (lag)
   # - Checksum mismatch in chunks: Data corruption or type conversion issue
   # - Large row count difference: Replication stuck or filtered
   ```

3. **Check for Filtering**
   ```bash
   # Verify no table/column filters in source connector
   curl http://localhost:8083/connectors/sqlserver-source/config | jq '.["table.include.list"]'
   curl http://localhost:8083/connectors/sqlserver-source/config | jq '.["column.exclude.list"]'
   ```

4. **Compare Sample Rows**
   ```sql
   -- SQL Server: Get first 10 rows
   SELECT TOP 10 * FROM dbo.<table> ORDER BY <primary_key>;

   -- PostgreSQL: Get same rows
   SELECT * FROM <table> ORDER BY <primary_key> LIMIT 10;

   -- Look for type conversion issues (e.g., DATETIME precision, DECIMAL rounding)
   ```

5. **If Data Loss Suspected**
   ```bash
   # STOP WRITES IMMEDIATELY
   ./scripts/bash/pause-resume.sh --connector sqlserver-source --action pause
   ./scripts/bash/pause-resume.sh --connector postgresql-sink --action pause

   # Preserve evidence
   pg_dump -h localhost -U postgres -d warehouse_target -t <table> > evidence_$(date +%Y%m%d_%H%M%S).sql

   # Contact DBA team immediately
   ```

#### Root Cause Analysis

- Review schema change history (last 7 days)
- Check DLQ for dropped messages
- Analyze connector logs for errors during mismatch period
- Verify Kafka topic retention hasn't deleted events

---

## Monitoring and Validation

### Real-Time Monitoring

#### Connector Health

```bash
# All connectors status
./scripts/bash/monitor.sh --component connectors

# Specific connector
curl http://localhost:8083/connectors/postgresql-sink/status | jq

# Expected healthy state:
# {
#   "name": "postgresql-sink",
#   "connector": { "state": "RUNNING" },
#   "tasks": [{ "state": "RUNNING", "worker_id": "connect:8083" }]
# }
```

#### Replication Lag

```bash
# Overall lag
./scripts/bash/monitor.sh --component lag

# Detailed Kafka consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group postgresql-sink-connector

# Expected output:
# TOPIC                PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# sqlserver.warehouse  0          12345           12345           0
```

#### Schema Change Events

```bash
# Tail schema change topic
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic schema-changes.warehouse_source \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true

# Filter for specific table
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic schema-changes.warehouse_source \
  --from-beginning | grep -i "customers"
```

#### Dead Letter Queue

```bash
# Check for errors
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic dlq-postgresql-sink \
  --from-beginning \
  --timeout-ms 5000 \
  --property print.headers=true

# Count messages in DLQ
kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic dlq-postgresql-sink \
  --time -1
```

### Post-Change Validation

#### Schema Consistency Check

```bash
# PostgreSQL: Get table schema
psql -h localhost -U postgres -d warehouse_target -c "\d+ <table_name>"

# Compare with SQL Server
# Manual comparison or use schema comparison tool
```

#### Data Reconciliation

```bash
# Quick row count check
python src/reconciliation/cli.py reconcile --table <table_name> --row-count-only

# Full checksum validation (use for high-risk changes)
python src/reconciliation/cli.py reconcile \
  --table <table_name> \
  --checksum \
  --chunk-size 10000 \
  --report-format json

# Review report
cat reports/reconciliation_<table>_*.json | jq '.summary'
```

#### End-to-End Validation

```sql
-- Insert test record in SQL Server
INSERT INTO dbo.<table> (<columns>) VALUES (<test_data>);

-- Wait 5-10 seconds for replication

-- Query PostgreSQL
SELECT * FROM <table> WHERE <primary_key> = <test_value>;

-- Verify data matches exactly

-- Cleanup
DELETE FROM dbo.<table> WHERE <primary_key> = <test_value>;
```

---

## Rollback Procedures

### General Rollback Principles

1. **Stop Writes First**: Pause connectors before rolling back schema
2. **Preserve Data**: Take PostgreSQL snapshot before destructive operations
3. **Rollback Both Sides**: Undo changes in both SQL Server and PostgreSQL
4. **Verify Consistency**: Run reconciliation after rollback

### Rollback Decision Tree

```
Schema change deployed?
│
├─ No issues observed within 1 hour
│  └─ Proceed with monitoring (no rollback needed)
│
├─ Minor issues (DLQ messages, retryable errors)
│  ├─ Fix schema issue in PostgreSQL only
│  └─ Resume connector
│
├─ Major issues (connector FAILED, data loss risk)
│  ├─ Pause connectors immediately
│  ├─ Rollback schema in PostgreSQL
│  ├─ Rollback schema in SQL Server
│  └─ Resume connectors
│
└─ Critical issues (data corruption confirmed)
   ├─ STOP ALL WRITES (pause connectors)
   ├─ Restore PostgreSQL from backup
   ├─ Rollback SQL Server schema
   ├─ Replay Kafka events from offset
   └─ Resume connectors after validation
```

### Quick Rollback Commands

```bash
# 1. Pause replication
./scripts/bash/pause-resume.sh --connector sqlserver-source --action pause
./scripts/bash/pause-resume.sh --connector postgresql-sink --action pause

# 2. Rollback schema (use scenario-specific SQL from above)

# 3. Verify schema consistency
psql -h localhost -U postgres -d warehouse_target -c "\d+ <table>"

# 4. Resume replication
./scripts/bash/pause-resume.sh --connector sqlserver-source --action resume
./scripts/bash/pause-resume.sh --connector postgresql-sink --action resume

# 5. Reconcile data
python src/reconciliation/cli.py reconcile --table <table> --checksum
```

### Point-in-Time Recovery

If rollback is insufficient and data restoration required:

```bash
# 1. Identify recovery point (timestamp before schema change)
RECOVERY_TIMESTAMP="2025-12-21 14:30:00"

# 2. Restore PostgreSQL table from backup
pg_restore -h localhost -U postgres -d warehouse_target \
  --table=<table_name> \
  --clean \
  backup_file.dump

# 3. Replay Kafka events from specific offset
# (Requires Kafka Connect offset reset - advanced operation)

# 4. Monitor replication until caught up
./scripts/bash/monitor.sh --component lag
```

---

## Troubleshooting

### Issue: Schema Change Not Detected

**Symptoms**: DDL executed in SQL Server but PostgreSQL unchanged

#### Diagnostic Steps

1. **Verify CDC Enabled on Table**
   ```sql
   -- SQL Server
   SELECT name, is_tracked_by_cdc
   FROM sys.tables
   WHERE name = '<table_name>';

   -- Expected: is_tracked_by_cdc = 1
   ```

2. **Check Source Connector Config**
   ```bash
   curl http://localhost:8083/connectors/sqlserver-source/config | jq '.["include.schema.changes"]'
   # Expected: "true"
   ```

3. **Review Schema Change Topic**
   ```bash
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic schema-changes.warehouse_source \
     --from-beginning \
     --timeout-ms 10000

   # If empty, schema changes not being captured
   ```

#### Resolution

```bash
# Restart source connector to refresh schema metadata
curl -X POST http://localhost:8083/connectors/sqlserver-source/restart

# Wait 30 seconds, then verify schema change captured
```

---

### Issue: Column Type Mismatch

**Symptoms**: DLQ messages with "Cannot convert value" errors

#### Diagnostic Steps

1. **Identify Mismatched Column**
   ```bash
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic dlq-postgresql-sink \
     --from-beginning --max-messages 1 \
     --property print.headers=true | grep "column"
   ```

2. **Compare Types**
   ```sql
   -- SQL Server
   SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
   FROM INFORMATION_SCHEMA.COLUMNS
   WHERE TABLE_NAME = '<table>' AND COLUMN_NAME = '<column>';

   -- PostgreSQL
   SELECT column_name, data_type, character_maximum_length
   FROM information_schema.columns
   WHERE table_name = '<table>' AND column_name = '<column>';
   ```

#### Resolution

```sql
-- Option 1: Widen PostgreSQL column
ALTER TABLE <table> ALTER COLUMN <column> TYPE <wider_type>;

-- Option 2: Add explicit type conversion in sink connector
-- (Requires custom SMT - advanced configuration)
```

---

### Issue: Replication Stuck After Schema Change

**Symptoms**: Lag increasing, connector RUNNING but no progress

#### Diagnostic Steps

1. **Check Connector Logs**
   ```bash
   docker logs kafka-connect --tail 200 | grep -A 10 -B 10 "ERROR\|WARN"
   ```

2. **Verify Kafka Consumer Position**
   ```bash
   kafka-consumer-groups --bootstrap-server localhost:9092 \
     --describe --group postgresql-sink-connector

   # If LAG not decreasing, consumer stuck
   ```

3. **Check PostgreSQL Locks**
   ```sql
   SELECT
     l.pid,
     l.mode,
     l.granted,
     a.query
   FROM pg_locks l
   JOIN pg_stat_activity a ON l.pid = a.pid
   WHERE l.relation = '<table>'::regclass;
   ```

#### Resolution

```bash
# 1. Pause connector
./scripts/bash/pause-resume.sh --connector postgresql-sink --action pause

# 2. Kill blocking PostgreSQL queries (if any)
# (Identify PIDs from query above)
psql -h localhost -U postgres -d warehouse_target -c "SELECT pg_terminate_backend(<pid>);"

# 3. Resume connector
./scripts/bash/pause-resume.sh --connector postgresql-sink --action resume
```

---

### Issue: Data Type Precision Loss

**Symptoms**: Reconciliation shows checksum mismatch but row counts match

#### Common Causes

- **DECIMAL/NUMERIC**: Precision/scale mismatch
- **DATETIME**: Millisecond vs microsecond precision
- **FLOAT/REAL**: Floating-point rounding differences

#### Diagnostic Steps

```sql
-- Find rows with checksum differences (example for DECIMAL)
SELECT
  sqlserver_value,
  postgresql_value,
  sqlserver_value - postgresql_value AS difference
FROM (
  -- Compare sample rows from both databases
  -- (Requires manual query construction)
) comparison
WHERE ABS(sqlserver_value - postgresql_value) > 0.0001;
```

#### Resolution

```sql
-- Align PostgreSQL column definition with SQL Server
ALTER TABLE <table> ALTER COLUMN <column> TYPE NUMERIC(18, 6);

-- Or adjust reconciliation tolerance in compare.py
# (Code change required)
```

---

## Appendix

### A. Configuration Reference

#### Source Connector (debezium/sqlserver-source.json)

```json
{
  "name": "sqlserver-source",
  "config": {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "database.hostname": "${SQL_SERVER_HOST}",
    "database.port": "1433",
    "database.user": "${SQL_SERVER_USER}",
    "database.password": "${SQL_SERVER_PASSWORD}",
    "database.dbname": "warehouse_source",
    "database.server.name": "sqlserver",
    "table.include.list": "dbo.customers,dbo.orders,dbo.products",
    "database.history.kafka.topic": "schema-changes.warehouse_source",
    "database.history.kafka.bootstrap.servers": "kafka:9092",

    // Schema change tracking
    "include.schema.changes": "true",
    "provide.transaction.metadata": "true",

    // CDC configuration
    "snapshot.mode": "initial",
    "decimal.handling.mode": "precise",
    "time.precision.mode": "adaptive",

    // Performance tuning
    "max.batch.size": "2048",
    "max.queue.size": "8192",
    "poll.interval.ms": "1000"
  }
}
```

#### Sink Connector (kafka-connect/postgresql-sink.json)

```json
{
  "name": "postgresql-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "connection.url": "jdbc:postgresql://postgres:5432/warehouse_target",
    "connection.user": "${POSTGRES_USER}",
    "connection.password": "${POSTGRES_PASSWORD}",
    "topics.regex": "sqlserver\\.warehouse_source\\.dbo\\.(.*)",

    // Schema evolution settings
    "auto.create": "false",
    "auto.evolve": "true",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "delete.enabled": "true",

    // Error handling
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq-postgresql-sink",
    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.context.headers.enable": "true",

    // Transforms
    "transforms": "unwrap,route",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "true",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "sqlserver\\.warehouse_source\\.dbo\\.(.*)",
    "transforms.route.replacement": "$1"
  }
}
```

### B. SQL Server CDC Commands

```sql
-- Enable CDC on database (one-time setup)
USE warehouse_source;
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on table
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name = N'customers',
  @role_name = NULL,
  @supports_net_changes = 1;

-- Check CDC status
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'warehouse_source';
SELECT name, is_tracked_by_cdc FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo');

-- View CDC change tables
SELECT * FROM cdc.change_tables;

-- Disable CDC (if needed)
EXEC sys.sp_cdc_disable_table
  @source_schema = N'dbo',
  @source_name = N'customers',
  @capture_instance = N'dbo_customers';
```

### C. PostgreSQL Schema Introspection

```sql
-- List all tables
SELECT tablename FROM pg_tables WHERE schemaname = 'public';

-- Describe table structure
\d+ <table_name>

-- Get column details
SELECT
  column_name,
  data_type,
  character_maximum_length,
  is_nullable,
  column_default
FROM information_schema.columns
WHERE table_name = '<table_name>'
ORDER BY ordinal_position;

-- Find primary keys
SELECT
  tc.table_name,
  kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_name = '<table_name>';

-- Check constraints
SELECT
  conname AS constraint_name,
  contype AS constraint_type,
  pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid = '<table_name>'::regclass;
```

### D. Kafka Topic Management

```bash
# List all topics
kafka-topics --bootstrap-server localhost:9092 --list

# Describe schema change topic
kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic schema-changes.warehouse_source

# Get offset range
kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic sqlserver.warehouse_source.dbo.customers

# Reset consumer group offset (DANGER: data replay)
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group postgresql-sink-connector \
  --reset-offsets --to-earliest \
  --topic sqlserver.warehouse_source.dbo.customers \
  --execute
```

### E. Reconciliation Tool Usage

```bash
# Basic usage
python src/reconciliation/cli.py reconcile --table <table_name>

# Row count only (fast)
python src/reconciliation/cli.py reconcile --table <table_name> --row-count-only

# With checksum validation
python src/reconciliation/cli.py reconcile \
  --table <table_name> \
  --checksum \
  --chunk-size 10000

# Multiple tables
python src/reconciliation/cli.py reconcile \
  --tables customers,orders,products \
  --checksum

# Custom database connections
python src/reconciliation/cli.py reconcile \
  --table <table_name> \
  --source-host localhost \
  --source-port 1433 \
  --source-db warehouse_source \
  --target-host localhost \
  --target-port 5432 \
  --target-db warehouse_target

# Generate report
python src/reconciliation/cli.py reconcile \
  --table <table_name> \
  --checksum \
  --report-format json \
  --output-dir ./reports
```

### F. Useful Monitoring Queries

#### Replication Lag Estimation

```sql
-- SQL Server: Get latest LSN
SELECT MAX(sys.fn_cdc_get_max_lsn()) AS max_lsn FROM sys.change_tables;

-- PostgreSQL: Query latest replicated timestamp (if using transaction metadata)
SELECT MAX(event_timestamp) FROM <table_name>;

-- Time difference indicates lag
```

#### DLQ Analysis

```bash
# Count DLQ messages by error type
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic dlq-postgresql-sink \
  --from-beginning \
  --property print.headers=true \
  --timeout-ms 30000 | \
  grep "__connect.errors.exception.message" | \
  sort | uniq -c | sort -rn
```

#### Connector Throughput

```bash
# Kafka Connect metrics (requires JMX enabled)
curl -s http://localhost:8083/metrics | grep "sink-record-send-rate"

# Approximate from Kafka lag
watch -n 5 'kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group postgresql-sink-connector | grep LAG'
```

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-12-21 | Claude | Initial creation based on codebase analysis |

## Related Documentation

- [Schema Evolution Guide](/docs/schema-evolution.md) - Detailed reference documentation
- [Operations Manual](/docs/operations.md) - General operational procedures
- [Troubleshooting Guide](/docs/troubleshooting.md) - Issue resolution procedures
- [Architecture Overview](/docs/architecture.md) - System design and components

## Feedback

For corrections or improvements to this runbook, please submit issues or pull requests to the repository.