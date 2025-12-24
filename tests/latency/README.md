# CDC Pipeline Latency Tests

This directory contains tests and reports for measuring end-to-end replication latency in the CDC pipeline.

## Overview

**Purpose:** Measure the time it takes for changes (INSERT, UPDATE, DELETE) to replicate from SQL Server through Kafka to PostgreSQL.

**Importance:**
- Validates NFR-002 (replication lag < 5 minutes p95)
- Provides baseline for performance monitoring
- Helps identify bottlenecks and regressions

## Running Latency Tests

### Prerequisites

```bash
# Ensure all Docker services are running
make status

# Verify connectors are deployed
curl http://localhost:8083/connectors
```

### Execute Test

**Using Make (Recommended):**
```bash
# From project root
make test-latency
```

**Or run directly:**
```bash
# From project root
.venv/bin/python tests/latency/measure_cdc_latency.py
```

### Expected Output

```
CDC Pipeline Latency Measurement
============================================================

--- Iteration 1/3 ---
=== Testing INSERT Latency ===
✓ INSERT Latency: 3544ms

=== Testing UPDATE Latency ===
✓ UPDATE Latency: 4946ms

=== Testing DELETE Latency ===
✓ DELETE Latency: 5051ms

[... iterations 2 and 3 ...]

============================================================
SUMMARY
============================================================
INSERT: avg=3436ms, min=2930ms, max=4344ms
UPDATE: avg=4945ms, min=4940ms, max=4949ms
DELETE: avg=4982ms, min=4947ms, max=5051ms
```

## Test Script Details

### `measure_cdc_latency.py`

**What it does:**
1. Connects to both SQL Server and PostgreSQL
2. For each operation type (INSERT, UPDATE, DELETE):
   - Executes the operation on SQL Server
   - Records timestamp
   - Polls PostgreSQL every 100ms until change appears
   - Calculates latency
3. Runs 3 iterations per operation type
4. Reports statistics (min, max, average)

**Key Features:**
- Uses `customers` table (must exist and have CDC enabled)
- Generates unique test data to avoid conflicts
- Polls at 100ms intervals for accuracy
- 30-second timeout per operation
- Handles soft deletes (`__deleted` column)

**Dependencies:**
- `pyodbc` - SQL Server connection
- `psycopg2` - PostgreSQL connection
- Active CDC pipeline with connectors running

## Baseline Performance

**Current baseline (as of 2025-12-24):**

| Operation | Expected Latency |
|-----------|------------------|
| INSERT | 3-4 seconds |
| UPDATE | 4-5 seconds |
| DELETE | 4-5 seconds |

**Alert if:**
- Any operation > 10 seconds
- p95 > 30 seconds
- Frequent timeouts (> 30s)

## Reports

### Latest Report
📄 **[20251224_latency.md](./20251224_latency.md)** - Comprehensive analysis with:
- Detailed latency breakdown by component
- Configuration analysis
- Optimization scenarios
- Monitoring recommendations

### Report Format

When creating new reports, use the format:
- Filename: `YYYYMMDD_latency.md`
- Include: test results, configuration, analysis, recommendations

## Interpreting Results

### Normal Behavior
- INSERT slightly faster than UPDATE/DELETE (30-40% difference)
- UPDATE and DELETE have similar latency
- Low variance between iterations (< 1 second)

### Performance Issues

**Symptoms:**
- Latency > 10 seconds consistently
- High variance (std dev > 2 seconds)
- Timeouts

**Investigation Steps:**

1. **Check connector health:**
   ```bash
   curl http://localhost:8083/connectors/sqlserver-cdc-source/status
   curl http://localhost:8083/connectors/postgresql-jdbc-sink/status
   ```

2. **Check Kafka lag:**
   ```bash
   docker exec cdc-kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group connect-postgresql-jdbc-sink \
     --describe
   ```

3. **Review connector logs:**
   ```bash
   docker logs cdc-kafka-connect --tail 100
   ```

4. **Check database performance:**
   - SQL Server: Query DMVs for CDC job status
   - PostgreSQL: Check `pg_stat_statements` for slow queries

## Configuration Impact

### Current Settings (Balanced)
```json
Debezium:
  poll.interval.ms: 500
  max.batch.size: 2048

PostgreSQL Sink:
  batch.size: 3000
  insert.mode: upsert
```

**Result:** 3-5 second latency, 10K+ rows/sec throughput

### Low-Latency Settings
```json
Debezium:
  poll.interval.ms: 100
  max.batch.size: 100

PostgreSQL Sink:
  batch.size: 100
```

**Result:** 1-2 second latency, ~3K rows/sec throughput ⚠️

### High-Throughput Settings
```json
Debezium:
  poll.interval.ms: 1000
  max.batch.size: 5000

PostgreSQL Sink:
  batch.size: 5000
```

**Result:** 10-20 second latency, 20K+ rows/sec throughput

## Monitoring Integration

### Prometheus Metrics

Key metrics to collect:
- `debezium.metrics.MilliSecondsBehindSource`
- `kafka.connect:type=sink-task-metrics`
- Custom latency metrics from this test

### Grafana Dashboards

Recommended panels:
- Latency trend over time (line graph)
- Operation type breakdown (bar chart)
- p95/p99 percentiles (stat panel)
- Alert status (threshold visualization)

### Alerting Rules

Example Prometheus alert:
```yaml
- alert: CDCLatencyHigh
  expr: cdc_latency_p95_seconds > 30
  for: 5m
  annotations:
    summary: "CDC replication latency is high"
    description: "p95 latency is {{ $value }}s, exceeding 30s threshold"
```

## Automation

### Weekly Scheduled Test

Add to CI/CD or cron:
```bash
#!/bin/bash
# Run weekly latency test and save results

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_FILE="tests/latency/results_${TIMESTAMP}.txt"

.venv/bin/python tests/latency/measure_cdc_latency.py > "$REPORT_FILE"

# Check if latency exceeds threshold
if grep -q "avg=.*[1-9][0-9][0-9][0-9][0-9]ms" "$REPORT_FILE"; then
  echo "WARNING: Latency exceeds 10 seconds"
  # Send alert
fi
```

### Integration with Performance Tests

The latency test complements the performance tests:
- Performance tests: Measure throughput and sustained load
- Latency tests: Measure single-operation timing

Both are needed for complete performance picture.

## Troubleshooting

### Common Issues

**1. Connection failures**
```
Error: Unable to connect to SQL Server
```
**Fix:** Ensure Docker services are running: `make start`

**2. Table not found**
```
Error: relation "customers" does not exist
```
**Fix:** Run database initialization: `make init-dbs`

**3. CDC not enabled**
```
Error: No CDC data found
```
**Fix:** Check CDC status:
```sql
SELECT is_cdc_enabled FROM sys.databases WHERE name = 'warehouse_source'
```

**4. Timeouts**
```
✗ INSERT did not replicate within 30s
```
**Fix:** Check connector status and logs:
```bash
make connector-status
make logs LOGS=kafka-connect
```

## Best Practices

1. **Run during maintenance windows** - Avoid production load
2. **Establish baseline first** - Run multiple times to get consistent results
3. **Document changes** - Keep reports when configuration changes
4. **Trend analysis** - Compare over time to detect degradation
5. **Coordinate with load tests** - Run before/after performance tests

## Contributing

When adding new latency tests:
1. Follow naming convention: `test_<operation>_latency.py`
2. Include detailed docstrings
3. Generate report in same format
4. Update this README with new test details

## Support

**Issues:** See main project README for support channels
**Documentation:** Full analysis in `20251224_latency.md`
**Configuration:** See `docker/configs/runtime/` for connector settings
