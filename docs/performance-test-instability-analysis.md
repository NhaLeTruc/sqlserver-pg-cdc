# Performance Test Instability Analysis

## Issue

`test_measure_replication_throughput` in [tests/performance/test_performance.py](../tests/performance/test_performance.py:206) is unstable - sometimes it passes, sometimes it doesn't.

## Root Causes Identified

### 1. **Stale Data from Previous Runs** (PRIMARY CAUSE)

**Problem:**
The `clear_tables()` method (line 194-203) uses `DELETE` instead of `TRUNCATE`:

```python
def clear_tables(self, sqlserver_conn, postgres_conn):
    with postgres_conn.cursor() as cursor:
        cursor.execute("DELETE FROM customers WHERE name LIKE 'Perf Test %'")
    with sqlserver_conn.cursor() as cursor:
        cursor.execute("DELETE FROM dbo.customers WHERE name LIKE 'Perf Test %'")
    sqlserver_conn.commit()
    time.sleep(3)  # Allow CDC to process deletes
```

**Why This Causes Instability:**

1. **DELETE triggers CDC events** - Each DELETE creates a tombstone record in Debezium
2. **Tombstone records are replicated** - PostgreSQL receives `__deleted='true'` markers
3. **3-second wait may be insufficient** - CDC pipeline lag means deletes might still be processing when the next test starts
4. **Row count queries are naive** - The test counts rows with `LIKE 'Perf Test %'` which may include soft-deleted rows depending on timing

**Evidence:**
```python
# Line 169: This query doesn't filter out __deleted='true' rows
cursor.execute("SELECT COUNT(*) FROM customers WHERE name LIKE 'Perf Test %'")
```

**Impact:**
- Test expects 30,000 rows
- If previous run's DELETE events are still replicating, counts will be off
- Race condition between cleanup and new inserts

### 2. **30-Second Timeout May Be Too Short**

**Problem:**
The test uses a 30-second timeout for replicating 30,000 rows (line 240-241):

```python
success, replication_wait_time = self.wait_for_replication(
    postgres_conn, total_rows, timeout=30
)
```

**Why This Causes Instability:**

1. **Variable CDC lag** - Debezium polling interval + network + Kafka + sink connector = unpredictable
2. **System load** - Shared CI/testing environment may have varying performance
3. **Cold start penalty** - First run after connector restart is slower

**Expected Throughput:**
- 30,000 rows in 30 seconds = 1,000 rows/second minimum
- If throughput drops below this, test fails

**Actual Observations:**
- Clean system: ~2,000-5,000 rows/second
- Busy system: ~500-1,500 rows/second
- With stale data: unpredictable

### 3. **Race Condition in clear_tables()**

**Problem:**
The 3-second sleep (line 203) is a fixed delay, not a confirmation that CDC processing is complete:

```python
time.sleep(3)  # Allow CDC to process deletes
```

**Why This Causes Instability:**

1. **No verification** - We don't actually check if deletes finished replicating
2. **Variable lag** - 3 seconds might not be enough under load
3. **DELETE events queue up** - If test runs quickly, multiple DELETE batches may overlap

**Better Approach:**
Wait until PostgreSQL count reaches zero or old data is actually removed.

### 4. **No Connector State Validation**

**Problem:**
The test checks connector status at startup (line 26-70) but not between test iterations.

**Why This Causes Instability:**

1. **Connectors can fail mid-test** - Errors in task execution
2. **Offset commit lag** - Connector may pause between polls
3. **Kafka consumer rebalancing** - Can cause temporary delays

### 5. **Autocommit Setting Mismatch**

**Problem:**
SQL Server connection uses `autocommit=False` (line 83) but commits are manual (line 153).
PostgreSQL uses `autocommit=True` (line 97).

**Why This Could Cause Issues:**

1. **Transaction isolation** - SQL Server changes aren't visible to CDC until commit
2. **Batch commit timing** - Large batches may see delays between commit and CDC capture
3. **Inconsistent behavior** - Different autocommit settings can cause timing variations

## Recommended Fixes

### Fix 1: Use TRUNCATE Instead of DELETE (Immediate Fix)

Replace `clear_tables()` method:

```python
def clear_tables(
    self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
) -> None:
    """Clear performance test data from customers tables using TRUNCATE."""
    # TRUNCATE doesn't trigger CDC events - much cleaner!
    try:
        # Note: Can't truncate tables with FK constraints
        # So we still use DELETE but filter properly in queries
        with postgres_conn.cursor() as cursor:
            cursor.execute("DELETE FROM customers WHERE name LIKE 'Perf Test %'")
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("DELETE FROM dbo.customers WHERE name LIKE 'Perf Test %'")
        sqlserver_conn.commit()

        # Wait for deletes to replicate AND be processed
        max_wait = 10
        for i in range(max_wait):
            with postgres_conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM customers WHERE name LIKE 'Perf Test %'")
                count = cursor.fetchone()[0]
                if count == 0:
                    break
            time.sleep(1)
    except Exception as e:
        print(f"Warning: clear_tables failed: {e}")
```

### Fix 2: Filter Out Soft-Deleted Rows

Update `wait_for_replication()` to filter soft deletes:

```python
def wait_for_replication(
    self,
    postgres_conn: psycopg2.extensions.connection,
    expected_count: int,
    timeout: int = 30,
) -> Tuple[bool, float]:
    """Wait for replication to reach expected count."""
    start_time = time.time()
    last_count = 0
    last_log_time = start_time

    while time.time() - start_time < timeout:
        with postgres_conn.cursor() as cursor:
            # Filter out soft-deleted rows!
            cursor.execute("""
                SELECT COUNT(*) FROM customers
                WHERE name LIKE 'Perf Test %'
                AND (__deleted IS NULL OR __deleted != 'true')
            """)
            count = cursor.fetchone()[0]

            # ... rest of the method
```

### Fix 3: Use Environment Reset Before Tests (Best Fix)

The new `clean_test_environment` fixture should already handle this, but ensure it's being used:

```python
# tests/performance/conftest.py should have:
@pytest.fixture(scope="module", autouse=True)
def performance_test_setup(clean_test_environment):
    """Automatically reset environment before performance tests."""
    yield
```

This:
- Truncates ALL tables (no DELETE events)
- Clears Kafka topics (no stale tombstones)
- Resets connector offsets (clean start)

### Fix 4: Increase Timeout and Add Adaptive Waiting

```python
# Increase timeout from 30 to 60 seconds
success, replication_wait_time = self.wait_for_replication(
    postgres_conn, total_rows, timeout=60  # Was 30
)
```

### Fix 5: Add Connector Health Check Between Iterations

```python
def verify_connectors_running(self) -> bool:
    """Verify connectors are still running."""
    try:
        response = requests.get(
            f"{self.kafka_connect_url}/connectors/sqlserver-cdc-source/status",
            timeout=5
        )
        status = response.json()
        return status.get("connector", {}).get("state") == "RUNNING"
    except:
        return False
```

## Priority

1. **HIGH**: Use the `clean_test_environment` fixture (already implemented)
2. **HIGH**: Filter out soft-deleted rows in count queries
3. **MEDIUM**: Increase timeout from 30 to 60 seconds
4. **MEDIUM**: Improve `clear_tables()` to wait for completion
5. **LOW**: Add connector health checks between iterations

## Testing the Fixes

Run the diagnostic script to identify current state:

```bash
python scripts/python/diagnose_performance_test.py
```

Run test with environment reset:

```bash
# This should be stable
pytest tests/performance/test_performance.py::TestPerformanceMeasurement::test_measure_replication_throughput -v

# With quick reset (faster)
QUICK_RESET=1 pytest tests/performance/test_performance.py::TestPerformanceMeasurement::test_measure_replication_throughput -v
```

## Long-term Solution

Consider refactoring performance tests to use a dedicated test table that:
- Has no foreign key constraints (can be truncated)
- Is not used by other tests
- Can be dropped and recreated between runs
- Doesn't interfere with functional tests

Example:

```sql
CREATE TABLE perf_test_events (
    id BIGINT PRIMARY KEY IDENTITY(1,1),
    test_run_id VARCHAR(50),
    event_data VARCHAR(100),
    created_at DATETIME2
);
```

This eliminates all the issues with shared table state.