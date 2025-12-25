# Performance Test Instability - Fix Summary

## Problem Statement

`test_measure_replication_throughput` in [tests/performance/test_performance.py](../tests/performance/test_performance.py) was unstable - sometimes passing, sometimes failing.

## Root Cause

**PRIMARY ISSUE**: The test was counting soft-deleted rows (tombstone records) from previous test runs.

### How It Happened

1. Test uses `DELETE` to clear data: `DELETE FROM customers WHERE name LIKE 'Perf Test %'`
2. DELETE triggers CDC events that create tombstone records in Debezium
3. PostgreSQL receives these as rows with `__deleted='true'`
4. Test's count query was naive: `SELECT COUNT(*) FROM customers WHERE name LIKE 'Perf Test %'`
5. This counted BOTH active rows AND soft-deleted rows
6. Race condition: If previous test's DELETE events were still replicating, counts would be off

### Visual Example

```
First Test Run:
  SQL Server: INSERT 30,000 rows → PostgreSQL: 30,000 rows (✓ Pass)
  Cleanup: DELETE 30,000 rows → PostgreSQL: 30,000 tombstones (__deleted='true')

Second Test Run (BEFORE DELETE events finish):
  SQL Server: INSERT 30,000 rows
  PostgreSQL count query: "SELECT COUNT(*) WHERE name LIKE 'Perf Test %'"
  Result: 30,000 (new) + 25,000 (still-replicating tombstones) = 55,000 (✗ Fail!)
```

## Solution Implemented

### Fix 1: Filter Soft-Deleted Rows in Queries

Updated `wait_for_replication()` method to filter out tombstones:

```python
# BEFORE (line 169)
cursor.execute("SELECT COUNT(*) FROM customers WHERE name LIKE 'Perf Test %'")

# AFTER (lines 174-178)
cursor.execute("""
    SELECT COUNT(*) FROM customers
    WHERE name LIKE 'Perf Test %'
    AND (__deleted IS NULL OR __deleted != 'true')
""")
```

Also updated the final count verification (lines 283-289) with the same filter.

### Fix 2: Leverage clean_test_environment Fixture

The test automatically uses the `clean_test_environment` fixture (via [tests/performance/conftest.py](../tests/performance/conftest.py)) which:
- Truncates ALL tables (no DELETE events)
- Clears Kafka topics (no stale tombstones)
- Resets connector offsets (clean start)

This is the BEST fix because it completely eliminates stale data.

## Files Modified

1. **[tests/performance/test_performance.py](../tests/performance/test_performance.py)**
   - Lines 156-205: Updated `wait_for_replication()` to filter soft deletes
   - Lines 282-290: Updated final count query to filter soft deletes

## Files Created

1. **[scripts/python/diagnose_performance_test.py](../scripts/python/diagnose_performance_test.py)**
   - Diagnostic tool to identify stale data and other issues
   - Can be run before tests to check environment state

2. **[docs/performance-test-instability-analysis.md](../docs/performance-test-instability-analysis.md)**
   - Detailed analysis of all 5 root causes
   - Comprehensive fix recommendations
   - Troubleshooting guide

## Testing the Fix

### Run with automatic environment reset (recommended):

```bash
# Uses clean_test_environment fixture automatically
pytest tests/performance/test_performance.py::TestPerformanceMeasurement::test_measure_replication_throughput -v

# With quick reset (faster)
QUICK_RESET=1 pytest tests/performance/test_performance.py::TestPerformanceMeasurement::test_measure_replication_throughput -v
```

### Diagnose issues before running:

```bash
python scripts/python/diagnose_performance_test.py
```

### Manual environment reset:

```bash
make reset-test-env
```

## Expected Behavior Now

✅ **STABLE**: Test should pass consistently when run with `clean_test_environment` fixture
✅ **ISOLATED**: Each test run starts with clean tables (no stale data)
✅ **ACCURATE**: Count queries filter out tombstones correctly

## Additional Improvements Made

While fixing the primary issue, we also:

1. **Added environment reset feature** - Comprehensive cleanup before tests
2. **Created diagnostic tools** - Help identify issues quickly
3. **Documented all root causes** - Complete analysis in [performance-test-instability-analysis.md](../docs/performance-test-instability-analysis.md)

## Remaining Considerations

The test still uses `clear_tables()` which creates DELETE events. While we now filter these correctly, you could further improve by:

1. Using a dedicated test table with no FK constraints (can be TRUNCATE'd)
2. Increasing timeout from 30s to 60s for loaded systems
3. Adding connector health checks between test iterations

These are documented in the analysis but not critical for stability.

## Verification

To verify the fix works:

```bash
# Run test 5 times in succession
for i in {1..5}; do
  echo "=== Run $i ==="
  pytest tests/performance/test_performance.py::TestPerformanceMeasurement::test_measure_replication_throughput -v
done
```

All 5 runs should pass consistently.
