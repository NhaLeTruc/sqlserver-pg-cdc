# Troubleshooting Guide

This guide provides solutions to common issues in the SQL Server to PostgreSQL CDC pipeline.

## Table of Contents

- [Connector Issues](#connector-issues)
- [Replication Lag](#replication-lag)
- [Schema Evolution](#schema-evolution)
- [Performance Issues](#performance-issues)
- [Database Connection Issues](#database-connection-issues)
- [Kafka Issues](#kafka-issues)
- [Vault Issues](#vault-issues)
- [Monitoring Issues](#monitoring-issues)
- [Docker Issues](#docker-issues)

## Connector Issues

### Connector Fails to Start

**Symptoms:**
- Connector shows `FAILED` state
- Error in Kafka Connect logs

**Common Causes and Solutions:**

1. **Invalid Configuration**
   ```bash
   # Check connector status
   ./scripts/bash/pause-resume.sh status sqlserver-cdc-source

   # Validate configuration
   curl -X PUT http://localhost:8083/connector-plugins/io.debezium.connector.sqlserver.SqlServerConnector/config/validate \
     -H "Content-Type: application/json" \
     -d @docker/configs/kafka-connect/sqlserver-source.json
   ```

2. **Missing Permissions**
   - SQL Server: Ensure CDC is enabled and user has `db_owner` role
   ```sql
   -- Enable CDC on database
   EXEC sys.sp_cdc_enable_db;

   -- Enable CDC on table
   EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo',
     @source_name = N'Customers',
     @role_name = NULL;

   -- Grant permissions
   ALTER SERVER ROLE [dbcreator] ADD MEMBER [cdc_user];
   ```

3. **Database Not Reachable**
   ```bash
   # Test SQL Server connection from Kafka Connect container
   docker exec -it cdc-kafka-connect bash
   curl -v telnet://sqlserver:1433

   # Test PostgreSQL connection
   docker exec -it cdc-kafka-connect bash
   curl -v telnet://postgres:5432
   ```

### Connector Tasks Fail Repeatedly

**Symptoms:**
- Tasks show `FAILED` state and restart continuously
- High CPU usage in Kafka Connect

**Solutions:**

1. **Check Error Logs**
   ```bash
   docker logs cdc-kafka-connect --tail 100 | grep ERROR
   ```

2. **Review Retry Configuration**
   - Edit connector config to adjust retry limits:
   ```json
   {
     "errors.retry.timeout": "600000",
     "errors.retry.delay.max.ms": "60000",
     "errors.tolerance": "all",
     "errors.deadletterqueue.topic.name": "dlq-postgresql-sink"
   }
   ```

3. **Restart Connector**
   ```bash
   ./scripts/bash/pause-resume.sh restart sqlserver-cdc-source
   ```

### Connector Stops Processing Changes

**Symptoms:**
- Connector shows `RUNNING` state but no data flows
- Consumer lag increases

**Solutions:**

1. **Check Offset Storage**
   ```bash
   # Check connector offsets
   docker exec -it cdc-kafka-connect kafka-console-consumer \
     --bootstrap-server kafka:9092 \
     --topic connect-offsets \
     --from-beginning \
     --property print.key=true | grep sqlserver-cdc-source
   ```

2. **Verify SQL Server CDC**
   ```sql
   -- Check if CDC is enabled
   SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'SourceDB';

   -- Check CDC capture instance
   SELECT * FROM cdc.change_tables;

   -- Check CDC cleanup job
   EXEC sys.sp_cdc_help_jobs;
   ```

3. **Check for Long-Running Transactions**
   ```sql
   -- Find blocking transactions
   SELECT
     session_id,
     transaction_id,
     transaction_begin_time,
     DATEDIFF(SECOND, transaction_begin_time, GETDATE()) AS duration_seconds
   FROM sys.dm_tran_session_transactions t
   JOIN sys.dm_tran_active_transactions a ON t.transaction_id = a.transaction_id
   WHERE DATEDIFF(SECOND, transaction_begin_time, GETDATE()) > 300
   ORDER BY duration_seconds DESC;
   ```

## Replication Lag

### High Replication Lag

**Symptoms:**
- Data appears in PostgreSQL minutes/hours after SQL Server changes
- Prometheus alert: `ReplicationLagHigh`

**Diagnosis:**

1. **Check Consumer Lag**
   ```bash
   docker exec -it cdc-kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --describe \
     --group connect-postgresql-jdbc-sink
   ```

2. **Check Kafka Topic Lag**
   ```bash
   # View topic details
   docker exec -it cdc-kafka kafka-topics \
     --bootstrap-server localhost:9092 \
     --describe \
     --topic sqlserver.dbo.Customers
   ```

**Solutions:**

1. **Scale Connector Tasks**
   ```bash
   # Increase parallelism (be careful with SQL Server source)
   ./scripts/bash/scale-connector.sh postgresql-jdbc-sink 4
   ```

2. **Increase Kafka Partitions**
   ```bash
   docker exec -it cdc-kafka kafka-topics \
     --bootstrap-server localhost:9092 \
     --alter \
     --topic sqlserver.dbo.Customers \
     --partitions 6
   ```

3. **Optimize PostgreSQL Sink**
   - Update connector config:
   ```json
   {
     "batch.size": "3000",
     "connection.attempts": "5",
     "connection.backoff.ms": "10000"
   }
   ```

4. **Check SQL Server CDC Cleanup**
   ```sql
   -- Adjust CDC retention (default 3 days)
   EXEC sys.sp_cdc_change_job
     @job_type = N'cleanup',
     @retention = 10080;  -- 7 days in minutes
   ```

### Sudden Spike in Lag

**Symptoms:**
- Lag suddenly increases from seconds to minutes/hours
- Normal processing before spike

**Diagnosis:**

1. **Check for Large Transactions**
   ```bash
   # Check message sizes in Kafka
   docker exec -it cdc-kafka kafka-run-class kafka.tools.GetOffsetShell \
     --broker-list localhost:9092 \
     --topic sqlserver.dbo.Customers \
     --time -1
   ```

2. **Check Database Performance**
   ```sql
   -- SQL Server: Check wait statistics
   SELECT TOP 10
     wait_type,
     wait_time_ms,
     waiting_tasks_count
   FROM sys.dm_os_wait_stats
   ORDER BY wait_time_ms DESC;
   ```

**Solutions:**

1. **Pause and Resume**
   ```bash
   # Pause to let system catch up
   ./scripts/bash/pause-resume.sh pause postgresql-jdbc-sink

   # Wait for Kafka to process backlog
   sleep 60

   # Resume
   ./scripts/bash/pause-resume.sh resume postgresql-jdbc-sink
   ```

2. **Split Large Transactions**
   - Process large updates in smaller batches
   - Use SQL Server CDC with transaction splitting

## Schema Evolution

### Schema Mismatch Between Source and Sink

**Symptoms:**
- Connector fails with schema validation errors
- Dead letter queue receives messages

**Solutions:**

1. **Check Schema Registry**
   ```bash
   # List all schemas
   curl http://localhost:8081/subjects

   # Get latest schema version
   curl http://localhost:8081/subjects/sqlserver.dbo.Customers-value/versions/latest
   ```

2. **Update PostgreSQL Schema**
   ```sql
   -- Add missing column
   ALTER TABLE public.customers ADD COLUMN new_column VARCHAR(255);
   ```

3. **Evolve Schema Safely**
   - Use backward-compatible changes (add nullable columns)
   - Pause connector before schema changes:
   ```bash
   ./scripts/bash/pause-resume.sh pause sqlserver-cdc-source
   ./scripts/bash/pause-resume.sh pause postgresql-jdbc-sink

   # Apply schema changes...

   ./scripts/bash/pause-resume.sh resume sqlserver-cdc-source
   ./scripts/bash/pause-resume.sh resume postgresql-jdbc-sink
   ```

### Incompatible Data Type Changes

**Symptoms:**
- Type conversion errors in connector logs
- Data truncation warnings

**Solutions:**

1. **Map Data Types in Connector Config**
   ```json
   {
     "transforms": "castTypes",
     "transforms.castTypes.type": "org.apache.kafka.connect.transforms.Cast$Value",
     "transforms.castTypes.spec": "price:float64,quantity:int32"
   }
   ```

2. **Use Custom SMT (Single Message Transform)**
   - Create custom transformation for complex type conversions
   - Deploy JAR to Kafka Connect plugins directory

### Column Added/Removed in Source

**Symptoms:**
- Connector continues running but new column not synced
- Extra column in PostgreSQL not updated

**Solutions:**

1. **Restart Connector** (for new columns)
   ```bash
   ./scripts/bash/pause-resume.sh restart sqlserver-cdc-source
   ```

2. **Update Schema Registry**
   ```bash
   # Delete and re-register schema (careful - breaks compatibility)
   curl -X DELETE http://localhost:8081/subjects/sqlserver.dbo.Customers-value
   ```

3. **Clean PostgreSQL Table** (for removed columns)
   ```sql
   ALTER TABLE public.customers DROP COLUMN old_column;
   ```

## Performance Issues

### High CPU Usage

**Symptoms:**
- Kafka Connect container CPU > 80%
- Slow message processing

**Diagnosis:**

1. **Check Connector Metrics**
   ```bash
   curl http://localhost:8083/connectors/sqlserver-cdc-source/status | jq
   ```

2. **Check JVM Metrics**
   ```bash
   # Access JMX metrics (if enabled)
   docker exec -it cdc-kafka-connect bash
   jconsole localhost:9999
   ```

**Solutions:**

1. **Increase JVM Heap**
   - Edit `docker-compose.yml`:
   ```yaml
   environment:
     KAFKA_HEAP_OPTS: "-Xms4g -Xmx4g"
   ```

2. **Reduce Connector Tasks**
   ```bash
   ./scripts/bash/scale-connector.sh postgresql-jdbc-sink 2
   ```

3. **Optimize Batch Size**
   ```json
   {
     "consumer.max.poll.records": "1000",
     "batch.size": "2000"
   }
   ```

### High Memory Usage

**Symptoms:**
- OOM errors in Kafka Connect
- Container restarts frequently

**Solutions:**

1. **Monitor Memory**
   ```bash
   docker stats cdc-kafka-connect
   ```

2. **Reduce Buffer Sizes**
   ```yaml
   environment:
     CONNECT_PRODUCER_BUFFER_MEMORY: "67108864"  # 64MB
     CONNECT_CONSUMER_MAX_PARTITION_FETCH_BYTES: "1048576"  # 1MB
   ```

3. **Increase Container Memory Limit**
   ```yaml
   services:
     kafka-connect:
       deploy:
         resources:
           limits:
             memory: 8g
   ```

### Slow PostgreSQL Inserts

**Symptoms:**
- High lag on sink connector
- PostgreSQL CPU high during writes

**Diagnosis:**

1. **Check PostgreSQL Performance**
   ```sql
   -- Check slow queries
   SELECT query, calls, mean_exec_time, max_exec_time
   FROM pg_stat_statements
   WHERE mean_exec_time > 100
   ORDER BY mean_exec_time DESC
   LIMIT 10;
   ```

2. **Check Locks**
   ```sql
   SELECT
     l.pid,
     l.mode,
     l.granted,
     a.query
   FROM pg_locks l
   JOIN pg_stat_activity a ON l.pid = a.pid
   WHERE NOT l.granted;
   ```

**Solutions:**

1. **Add Indexes**
   ```sql
   -- Add index on frequently queried columns
   CREATE INDEX CONCURRENTLY idx_customers_email ON public.customers(email);
   ```

2. **Tune PostgreSQL**
   ```sql
   -- Increase checkpoint timeout
   ALTER SYSTEM SET checkpoint_timeout = '15min';

   -- Increase shared buffers
   ALTER SYSTEM SET shared_buffers = '2GB';

   -- Apply changes
   SELECT pg_reload_conf();
   ```

3. **Use Connection Pooling**
   - Configure PgBouncer for connection pooling
   - Update connector to use pooler

## Database Connection Issues

### Cannot Connect to SQL Server

**Symptoms:**
- Connection timeout errors
- Authentication failures

**Solutions:**

1. **Verify SQL Server is Running**
   ```bash
   docker ps | grep cdc-sqlserver
   docker logs cdc-sqlserver --tail 50
   ```

2. **Test Connection**
   ```bash
   docker exec -it cdc-kafka-connect bash
   telnet sqlserver 1433
   ```

3. **Check Credentials in Vault**
   ```bash
   # Read credentials from Vault
   docker exec -it cdc-vault vault kv get secret/sqlserver/cdc-user
   ```

4. **Verify SQL Server Authentication**
   ```sql
   -- Enable mixed mode authentication (if needed)
   EXEC xp_instance_regwrite
     N'HKEY_LOCAL_MACHINE',
     N'Software\Microsoft\MSSQLServer\MSSQLServer',
     N'LoginMode',
     REG_DWORD,
     2;
   ```

### Cannot Connect to PostgreSQL

**Symptoms:**
- `FATAL: password authentication failed`
- Connection refused errors

**Solutions:**

1. **Check pg_hba.conf**
   ```bash
   docker exec -it cdc-postgres cat /var/lib/postgresql/data/pg_hba.conf
   ```

2. **Test Connection**
   ```bash
   docker exec -it cdc-kafka-connect bash
   psql -h postgres -U postgres -d target_db
   ```

3. **Check PostgreSQL Logs**
   ```bash
   docker logs cdc-postgres --tail 100 | grep ERROR
   ```

### Connection Pool Exhaustion

**Symptoms:**
- `Connection pool exhausted` errors
- Connector tasks fail intermittently

**Solutions:**

1. **Increase Connection Pool Size**
   ```json
   {
     "connection.pool.size": "10",
     "connection.attempts": "5"
   }
   ```

2. **Check for Connection Leaks**
   ```sql
   -- PostgreSQL: Check active connections
   SELECT
     datname,
     usename,
     COUNT(*)
   FROM pg_stat_activity
   GROUP BY datname, usename;
   ```

## Kafka Issues

### Kafka Broker Not Available

**Symptoms:**
- `Connection to node -1 could not be established`
- Connectors cannot produce/consume messages

**Solutions:**

1. **Check Kafka Broker Health**
   ```bash
   docker logs cdc-kafka --tail 100
   ```

2. **Verify Zookeeper Connection**
   ```bash
   docker exec -it cdc-zookeeper zkCli.sh
   ls /brokers/ids
   ```

3. **Restart Kafka**
   ```bash
   docker-compose -f docker/docker-compose.yml restart kafka
   ```

### Topic Not Found

**Symptoms:**
- Connector fails with `Topic does not exist` error

**Solutions:**

1. **List Topics**
   ```bash
   docker exec -it cdc-kafka kafka-topics \
     --bootstrap-server localhost:9092 \
     --list
   ```

2. **Create Topic Manually**
   ```bash
   docker exec -it cdc-kafka kafka-topics \
     --bootstrap-server localhost:9092 \
     --create \
     --topic sqlserver.dbo.Customers \
     --partitions 3 \
     --replication-factor 1
   ```

3. **Enable Auto Topic Creation**
   ```yaml
   environment:
     KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
   ```

### Messages in Dead Letter Queue

**Symptoms:**
- `dlq-postgresql-sink` topic contains messages
- Some records not appearing in PostgreSQL

**Diagnosis:**

1. **Read DLQ Messages**
   ```bash
   docker exec -it cdc-kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic dlq-postgresql-sink \
     --from-beginning \
     --property print.key=true \
     --property print.headers=true
   ```

2. **Check Error Headers**
   - Look for `__connect.errors.exception.class.name`
   - Look for `__connect.errors.exception.message`

**Solutions:**

1. **Fix Data Issues**
   - Correct constraint violations
   - Handle NULL values properly
   - Fix data type mismatches

2. **Replay DLQ Messages**
   ```bash
   # Export DLQ messages
   docker exec -it cdc-kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic dlq-postgresql-sink \
     --from-beginning > dlq_messages.txt

   # Fix and republish to original topic
   # (requires custom script)
   ```

## Vault Issues

### Cannot Retrieve Credentials

**Symptoms:**
- Connector fails with authentication error
- `permission denied` in Vault logs

**Solutions:**

1. **Check Vault Status**
   ```bash
   docker exec -it cdc-vault vault status
   ```

2. **Verify Token**
   ```bash
   export VAULT_TOKEN="dev-root-token"
   docker exec -e VAULT_TOKEN=$VAULT_TOKEN cdc-vault vault token lookup
   ```

3. **Check Secret Path**
   ```bash
   docker exec -e VAULT_TOKEN=dev-root-token cdc-vault \
     vault kv list secret/sqlserver/
   ```

4. **Grant Policy Access**
   ```bash
   docker exec -e VAULT_TOKEN=dev-root-token cdc-vault vault policy write cdc-reader - <<EOF
   path "secret/data/sqlserver/*" {
     capabilities = ["read"]
   }
   path "secret/data/postgresql/*" {
     capabilities = ["read"]
   }
   EOF
   ```

### Vault Sealed

**Symptoms:**
- `Vault is sealed` error
- Connectors cannot start

**Solutions:**

1. **Unseal Vault** (Production)
   ```bash
   docker exec -it cdc-vault vault operator unseal <key1>
   docker exec -it cdc-vault vault operator unseal <key2>
   docker exec -it cdc-vault vault operator unseal <key3>
   ```

2. **Development Mode**
   - In dev mode, Vault auto-unseals
   - Check if container restarted:
   ```bash
   docker-compose -f docker/docker-compose.yml restart vault
   ```

## Monitoring Issues

### Grafana Dashboard Shows No Data

**Symptoms:**
- Grafana dashboards empty
- Metrics not appearing

**Solutions:**

1. **Check Prometheus Targets**
   - Open: http://localhost:9090/targets
   - Verify all targets are `UP`

2. **Verify Metrics Exposed**
   ```bash
   # Check Kafka Connect metrics
   curl http://localhost:8083/metrics

   # Check Prometheus scraping
   curl http://localhost:9090/api/v1/query?query=up
   ```

3. **Check Grafana Data Source**
   - Open: http://localhost:3000/datasources
   - Test connection to Prometheus

4. **Import Dashboard**
   ```bash
   # Re-import dashboard
   curl -X POST http://admin:admin_secure_password@localhost:3000/api/dashboards/import \
     -H "Content-Type: application/json" \
     -d @docker/configs/grafana/dashboards/kafka-connect.json
   ```

### Alerts Not Firing

**Symptoms:**
- Known issues not triggering alerts
- Alert rules show as `Inactive`

**Solutions:**

1. **Check Alert Rules**
   - Open: http://localhost:9090/alerts
   - Verify rules are loaded

2. **Test Alert Query**
   ```promql
   # Test replication lag alert
   kafka_connect_sink_record_lag_max > 10000
   ```

3. **Check Alertmanager**
   ```bash
   docker logs cdc-prometheus | grep alertmanager
   ```

## Docker Issues

### Services Not Starting

**Symptoms:**
- `docker-compose up` hangs
- Services show as `unhealthy`

**Solutions:**

1. **Check Service Logs**
   ```bash
   docker-compose -f docker/docker-compose.yml logs -f
   ```

2. **Check Resource Usage**
   ```bash
   docker stats
   ```

3. **Verify Dependencies**
   ```bash
   # Wait for services to be healthy
   ./docker/wait-for-services.sh 600
   ```

4. **Clean and Restart**
   ```bash
   docker-compose -f docker/docker-compose.yml down -v
   docker system prune -f
   docker-compose -f docker/docker-compose.yml up -d
   ```

### Port Conflicts

**Symptoms:**
- `port is already allocated` error
- Cannot start services

**Solutions:**

1. **Check Port Usage**
   ```bash
   sudo netstat -tlnp | grep -E ':(1433|5432|9092|8083)'
   ```

2. **Change Ports in docker-compose.yml**
   ```yaml
   services:
     postgres:
       ports:
         - "15432:5432"  # Use alternate port
   ```

### Disk Space Issues

**Symptoms:**
- `no space left on device`
- Services crash randomly

**Solutions:**

1. **Check Disk Usage**
   ```bash
   df -h
   docker system df
   ```

2. **Clean Docker Resources**
   ```bash
   # Remove unused containers
   docker container prune -f

   # Remove unused images
   docker image prune -a -f

   # Remove unused volumes
   docker volume prune -f
   ```

3. **Configure Log Rotation**
   ```yaml
   services:
     kafka:
       logging:
         driver: "json-file"
         options:
           max-size: "100m"
           max-file: "3"
   ```

## General Troubleshooting Tips

### Enable Debug Logging

1. **Kafka Connect**
   ```bash
   # Update log level via REST API
   curl -X PUT http://localhost:8083/admin/loggers/io.debezium \
     -H "Content-Type: application/json" \
     -d '{"level": "DEBUG"}'
   ```

2. **Connector-Specific**
   ```json
   {
     "log4j.logger.io.debezium": "DEBUG",
     "log4j.logger.org.apache.kafka.connect": "DEBUG"
   }
   ```

### Collect Diagnostic Information

```bash
# Create diagnostic bundle
mkdir -p diagnostics
docker-compose -f docker/docker-compose.yml logs > diagnostics/compose-logs.txt
docker ps -a > diagnostics/containers.txt
docker stats --no-stream > diagnostics/resources.txt

# Connector status
curl http://localhost:8083/connectors | jq > diagnostics/connectors.json

# Kafka topics
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list > diagnostics/topics.txt

# Prometheus metrics
curl http://localhost:9090/api/v1/query?query=up > diagnostics/prometheus-targets.json

# Compress bundle
tar -czf diagnostics-$(date +%Y%m%d-%H%M%S).tar.gz diagnostics/
```

### Reset and Start Fresh

```bash
# Complete reset (WARNING: DELETES ALL DATA)
docker-compose -f docker/docker-compose.yml down -v
docker volume prune -f
docker network prune -f

# Start fresh
docker-compose -f docker/docker-compose.yml up -d
./docker/wait-for-services.sh 600

# Redeploy connectors
./scripts/bash/deploy-connector.sh docker/configs/kafka-connect/sqlserver-source.json
./scripts/bash/deploy-connector.sh docker/configs/kafka-connect/postgresql-sink.json
```

## Getting Help

If you encounter issues not covered in this guide:

1. **Check Documentation**
   - [Architecture Documentation](architecture.md)
   - [Operations Guide](operations.md)
   - [Quickstart Guide](../specs/001-sqlserver-pg-cdc/quickstart.md)

2. **Review Logs**
   - Enable debug logging
   - Collect diagnostic information
   - Look for ERROR and WARN messages

3. **Check Component Documentation**
   - [Debezium Documentation](https://debezium.io/documentation/)
   - [Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)
   - [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/)

4. **Community Resources**
   - Debezium mailing list and Slack
   - Kafka users mailing list
   - Stack Overflow (tags: debezium, kafka-connect, cdc)
