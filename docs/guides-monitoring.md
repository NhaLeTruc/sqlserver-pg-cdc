# Monitoring Guide: CDC Pipeline Observability

Complete guide for monitoring the SQL Server to PostgreSQL CDC pipeline using Prometheus, Grafana, and Jaeger.

## Table of Contents

1. [Monitoring Stack Overview](#monitoring-stack-overview)
2. [Using the Monitoring Script](#using-the-monitoring-script)
3. [Prometheus Metrics](#prometheus-metrics)
4. [Grafana Dashboards](#grafana-dashboards)
5. [Alert Configuration](#alert-configuration)
6. [Jaeger Distributed Tracing](#jaeger-distributed-tracing)
7. [Custom Metrics](#custom-metrics)

---

## Monitoring Stack Overview

The CDC pipeline includes comprehensive observability:

- **Prometheus** (`:9090`): Metrics collection and alerting
- **Grafana** (`:3000`): Visualization dashboards
- **Jaeger** (`:16686`): Distributed tracing
- **PostgreSQL Exporter** (`:9187`): Database metrics
- **JMX Exporters**: Kafka and Kafka Connect metrics

### Architecture

```
┌─────────────┐     ┌──────────────┐
│   Kafka     │────▶│  Prometheus  │
│ (JMX:9101)  │     │   (:9090)    │
└─────────────┘     └──────┬───────┘
                           │
┌─────────────┐            │     ┌──────────────┐
│KafkaConnect │────────────┼────▶│   Grafana    │
│ (JMX:9102)  │            │     │   (:3000)    │
└─────────────┘            │     └──────────────┘
                           │
┌─────────────┐            │
│ PostgreSQL  │────────────┘
│  Exporter   │
│  (:9187)    │
└─────────────┘
```

---

## Using the Monitoring Script

The `monitor.sh` script provides a unified interface for checking pipeline health.

### Quick Status Check

```bash
# Overall health
./scripts/bash/monitor.sh health

# Connector status
./scripts/bash/monitor.sh status

# Replication lag
./scripts/bash/monitor.sh lag

# Error rates
./scripts/bash/monitor.sh errors
```

### Available Commands

```bash
monitor.sh [COMMAND]

Commands:
  status      - Show connector status (default)
  metrics     - Show key metrics from Prometheus
  alerts      - Show active alerts
  health      - Overall pipeline health check
  lag         - Show replication lag
  errors      - Show error rates
  dashboards  - List available Grafana dashboards
```

### Examples

```bash
# Check if all services are up
./scripts/bash/monitor.sh health

# View current metrics
./scripts/bash/monitor.sh metrics

# Check for active alerts
./scripts/bash/monitor.sh alerts

# Monitor lag in real-time
watch -n 5 './scripts/bash/monitor.sh lag'
```

---

## Prometheus Metrics

### Accessing Prometheus

- **URL**: http://localhost:9090
- **Query Editor**: http://localhost:9090/graph
- **Targets**: http://localhost:9090/targets
- **Alerts**: http://localhost:9090/alerts

### Key Metrics to Monitor

#### Kafka Connect Metrics

**Connector Status**:
```promql
kafka_connect_connector_status
```

**Source Connector Throughput**:
```promql
rate(kafka_connect_source_task_source_record_poll_total[1m])
```

**Sink Connector Throughput**:
```promql
rate(kafka_connect_sink_task_sink_record_send_total[1m])
```

**Replication Lag (Records)**:
```promql
kafka_connect_source_task_source_record_poll_total
- kafka_connect_sink_task_sink_record_send_total
```

**Error Rate**:
```promql
rate(kafka_connect_task_error_total[5m])
```

#### Kafka Broker Metrics

**Topic Size**:
```promql
kafka_log_log_size
```

**Broker Leader Count**:
```promql
kafka_server_replicamanager_leadercount
```

**Under-Replicated Partitions**:
```promql
kafka_server_replicamanager_underreplicatedpartitions
```

#### PostgreSQL Metrics

**Table Sizes**:
```promql
pg_stat_user_tables_n_live_tup
```

**Insert/Update/Delete Rates**:
```promql
rate(pg_stat_user_tables_n_tup_ins[5m])
rate(pg_stat_user_tables_n_tup_upd[5m])
rate(pg_stat_user_tables_n_tup_del[5m])
```

**Dead Tuples (needs VACUUM)**:
```promql
pg_stat_user_tables_n_dead_tup
```

### Query Examples

**Find slow queries**:
```bash
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=rate(kafka_connect_source_task_source_record_poll_total[1m])' \
  | jq .
```

**Check connector state**:
```bash
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=kafka_connect_connector_status' \
  | jq '.data.result[] | {connector: .metric.connector, state: .value[1]}'
```

---

## Grafana Dashboards

### Accessing Grafana

- **URL**: http://localhost:3000
- **Default Credentials**: `admin` / `admin_secure_password`

### Available Dashboards

#### 1. CDC Pipeline Overview

**Location**: http://localhost:3000/d/cdc-pipeline

**Panels**:
- Replication Lag (Records)
- Throughput (Records/sec)
- Error Rate
- Connector Status
- Dead Letter Queue Size
- Kafka Topics Table
- Resource Usage

**Use Cases**:
- Overall health monitoring
- Performance tracking
- Capacity planning

#### 2. Kafka Connect Metrics

**Location**: http://localhost:3000/d/kafka-connect

**Panels**:
- Source Connector - Records Polled
- Sink Connector - Records Written
- Task Status
- Connector Errors
- Offset Commit Time
- Batch Size Distribution
- Task Metrics Table

**Use Cases**:
- Connector performance tuning
- Task distribution analysis
- Error investigation

### Creating Custom Dashboards

1. Go to http://localhost:3000
2. Click **+** → **Dashboard**
3. Click **Add visualization**
4. Select **Prometheus** as data source
5. Enter PromQL query
6. Configure visualization settings
7. Save dashboard

### Importing Dashboards

The pre-configured dashboards are located in:
- `docker/configs/grafana/dashboards/cdc-pipeline.json`
- `docker/configs/grafana/dashboards/kafka-connect.json`

To manually import:
1. Go to **Dashboards** → **Import**
2. Click **Upload JSON file**
3. Select the JSON file
4. Click **Import**

---

## Alert Configuration

### Alert Rules

All alert rules are defined in `docker/configs/prometheus/alert-rules.yml`.

#### Critical Alerts

**HighReplicationLag**:
- **Threshold**: >1000 records lag for 5 minutes
- **Severity**: warning
- **Action**: Check connector performance, scale if needed

**CriticalReplicationLag**:
- **Threshold**: >10000 records lag for 2 minutes
- **Severity**: critical
- **Action**: Immediate investigation required

**ConnectorDown**:
- **Threshold**: Connector not RUNNING for 5 minutes
- **Severity**: critical
- **Action**: Check logs, restart connector

**HighErrorRate**:
- **Threshold**: >5% error rate for 5 minutes
- **Severity**: warning
- **Action**: Check DLQ, investigate errors

**CriticalErrorRate**:
- **Threshold**: >20% error rate for 2 minutes
- **Severity**: critical
- **Action**: Pause connector, investigate

#### Viewing Alerts

**Via Prometheus**:
```bash
# Check active alerts
curl -s http://localhost:9090/api/v1/alerts | jq '.data.alerts[]'

# Check alert rules
curl -s http://localhost:9090/api/v1/rules | jq '.data.groups[].rules[] | select(.type=="alerting")'
```

**Via monitor.sh**:
```bash
./scripts/bash/monitor.sh alerts
```

### Testing Alerts

**Simulate high lag**:
```bash
# Pause sink connector (lag will grow)
curl -X PUT http://localhost:8083/connectors/postgresql-jdbc-sink/pause

# Wait 5 minutes and check alerts
./scripts/bash/monitor.sh alerts

# Resume connector
curl -X PUT http://localhost:8083/connectors/postgresql-jdbc-sink/resume
```

**Simulate high error rate**:
```bash
# Drop PostgreSQL table to cause errors
docker exec cdc-postgres psql -U postgres -d warehouse_target -c "DROP TABLE customers"

# Insert data into SQL Server (will cause errors)
docker exec cdc-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' \
  -d warehouse_source -Q "INSERT INTO dbo.customers (name, email) VALUES ('Test', 'test@example.com')"

# Check DLQ and alerts
./scripts/bash/monitor.sh errors
```

---

## Jaeger Distributed Tracing

### Accessing Jaeger

- **URL**: http://localhost:16686
- **API**: http://localhost:16686/api

### Use Cases

1. **Trace end-to-end replication**: Follow a single row from SQL Server → Kafka → PostgreSQL
2. **Identify bottlenecks**: Find slow operations in the pipeline
3. **Debug errors**: View trace context for failed operations

### Viewing Traces

1. Open http://localhost:16686
2. Select **Service**: `cdc-pipeline` or `kafka-connect`
3. Select **Operation**: e.g., `poll`, `commit`, `put`
4. Click **Find Traces**
5. Click on a trace to see details

### Trace Context

Each trace includes:
- **Span ID**: Unique identifier
- **Trace ID**: Groups related spans
- **Duration**: Operation timing
- **Tags**: Metadata (connector, task, table, etc.)
- **Logs**: Events during operation

---

## Custom Metrics

### Adding Custom Metrics

If you build custom tooling (e.g., reconciliation tool), expose metrics on port `:9103`:

```python
from prometheus_client import start_http_server, Counter, Gauge

# Define metrics
rows_compared = Counter('reconciliation_rows_compared_total', 'Total rows compared')
rows_mismatch = Counter('reconciliation_rows_mismatch_total', 'Rows with mismatches')
lag_seconds = Gauge('reconciliation_lag_seconds', 'Data lag in seconds')

# Start metrics server
start_http_server(9103)

# Update metrics
rows_compared.inc()
rows_mismatch.inc()
lag_seconds.set(45.2)
```

Prometheus will automatically scrape `http://host.docker.internal:9103/metrics`.

### Querying Custom Metrics

```promql
# Check reconciliation lag
reconciliation_lag_seconds

# Rate of rows compared
rate(reconciliation_rows_compared_total[5m])

# Percentage of mismatches
(rate(reconciliation_rows_mismatch_total[5m]) / rate(reconciliation_rows_compared_total[5m])) * 100
```

---

## Monitoring Best Practices

### Daily Checks

```bash
# Morning health check
./scripts/bash/monitor.sh health

# Check replication lag
./scripts/bash/monitor.sh lag

# Review any alerts
./scripts/bash/monitor.sh alerts
```

### Weekly Reviews

1. **Review Grafana dashboards**
   - Check for trends in lag
   - Verify throughput meets SLA
   - Review error patterns

2. **Check resource usage**
   - Memory consumption trends
   - CPU usage patterns
   - Disk space on Kafka/databases

3. **Review DLQ**
   - Check for accumulated errors
   - Investigate recurring failures
   - Clean up resolved issues

### Monthly Tasks

1. **Capacity planning**
   - Project future volume growth
   - Plan scaling if needed
   - Review retention policies

2. **Alert tuning**
   - Adjust thresholds based on observed patterns
   - Add new alerts for edge cases
   - Remove noisy alerts

3. **Performance optimization**
   - Review slow queries
   - Optimize connector configurations
   - Tune batch sizes

---

## Troubleshooting Monitoring Issues

### Prometheus Not Scraping Metrics

**Check targets**:
```bash
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job, health, lastError}'
```

**Common issues**:
- Service not running: `docker ps | grep cdc-`
- Port not accessible: Check firewall/network
- JMX not enabled: Verify KAFKA_JMX_PORT in docker-compose.yml

### Grafana Dashboard Shows No Data

**Check datasource**:
```bash
curl -s http://localhost:3000/api/datasources \
  -u admin:admin_secure_password | jq .
```

**Verify Prometheus connection**:
1. Go to **Configuration** → **Data Sources**
2. Click **Prometheus**
3. Click **Test** button
4. Should show "Data source is working"

### Alerts Not Firing

**Check alert rules loaded**:
```bash
curl -s http://localhost:9090/api/v1/rules | jq '.data.groups[].name'
```

**Verify alert evaluation**:
```bash
# Check if alert expression returns data
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=kafka_connect_source_task_source_record_poll_total - kafka_connect_sink_task_sink_record_send_total > 1000'
```

**Common issues**:
- Alert rules file not mounted correctly
- PromQL syntax error in rule
- Metric labels don't match rule selectors

---

## Additional Resources

- [Prometheus Query Documentation](https://prometheus.io/docs/prometheus/latest/querying/basics/)
- [Grafana Dashboard Best Practices](https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/best-practices/)
- [Kafka Connect Monitoring Guide](https://docs.confluent.io/platform/current/connect/monitoring.html)
- [Debezium Monitoring](https://debezium.io/documentation/reference/stable/operations/monitoring.html)
