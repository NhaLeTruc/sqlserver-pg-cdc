# Test Suite Cleanup Implementation Plan

## Executive Summary

This plan removes **100% of useless tests and mock abuses** from the test suite through a phased approach:
- **Phase 1**: Delete pure useless tests (logging verification, mock-only tests, framework behavior tests)
- **Phase 2**: Delete all Prometheus metrics tests (testing library behavior, not application logic)
- **Phase 3**: Refactor mock-heavy tests to integration tests using real database fixtures
- **Phase 4**: Verify changes and run full test suite

**Estimated Impact:**
- ~1,200 lines of useless tests deleted
- ~800 lines of mock-heavy tests refactored to ~400 lines of integration tests
- Net reduction: ~1,600 lines
- Improved test reliability and maintainability

---

## Phase 1: Delete Pure Useless Tests

### 1.1 tests/unit/test_scheduler.py (1,215 lines → ~650 lines)

**DELETE (565 lines):**

#### Logging-only tests (delete all):
- Lines 176-197: `test_add_interval_job_logs_addition` - Only verifies logging
- Lines 341-358: `test_add_cron_job_logs_addition` - Only verifies logging
- Lines 406-424: `test_remove_job_logs_removal` - Only verifies logging
- Lines 531-548: `test_start_logs_starting_message` - Only verifies logging
- Lines 616-628: `test_stop_logs_stopped_message` - Only verifies logging

#### Mock-only tests (testing APScheduler library):
- Lines 26-33: `test_scheduler_initializes_with_blocking_scheduler` - Tests framework initialization
- Lines 35-42: `test_scheduler_has_empty_jobs_list` - Trivial assertion
- Lines 44-51: `test_scheduler_creates_blocking_scheduler_instance` - Tests mock, not code
- Lines 82-98: `test_add_interval_job_creates_interval_trigger` - Tests APScheduler behavior
- Lines 100-127: `test_add_interval_job_stores_job_reference` - Tests list append
- Lines 199-223: `test_add_interval_job_passes_kwargs_to_function` - Tests APScheduler
- Lines 360-385: `test_add_cron_job_creates_cron_trigger` - Tests APScheduler behavior
- Lines 426-450: `test_remove_job_from_scheduler` - Tests APScheduler
- Lines 550-574: `test_start_calls_scheduler_start` - Only verifies mock.start() called
- Lines 630-653: `test_stop_calls_scheduler_shutdown` - Only verifies mock.shutdown() called

#### Over-mocked job wrapper tests (90% mock setup):
- Lines 655-726: `test_reconcile_job_wrapper_successful_execution` - 7 patches, tests nothing
- Lines 728-799: `test_reconcile_job_wrapper_handles_reconciliation_failure` - 7 patches
- Lines 801-872: `test_reconcile_job_wrapper_handles_report_generation_failure` - 7 patches
- Lines 874-945: `test_reconcile_job_wrapper_handles_export_failure` - 7 patches
- Lines 897-989: `test_reconcile_job_wrapper_creates_output_directory` - 92 lines of setup
- Lines 991-1062: `test_reconcile_job_wrapper_logs_exceptions` - Only tests logging
- Lines 1064-1135: `test_reconcile_job_wrapper_uses_correct_timestamp_format` - Tests string formatting
- Lines 1137-1215: `test_reconcile_job_wrapper_closes_connections_on_exception` - Mock inception

**KEEP (650 lines):**
- Integration-style tests that actually test scheduler logic with minimal mocking
- Tests that verify job parameters are correctly passed
- Tests for actual scheduling behavior (not logging)

**Action:** Delete specified line ranges, keep remaining tests

---

### 1.2 tests/unit/test_logging_config.py (892 lines → ~500 lines)

**DELETE (392 lines):**

#### Tests that verify JSON structure only:
- Lines 232-235: `test_format_includes_process_info` - Just checks dict keys
- Lines 237-240: `test_format_includes_thread_info` - Just checks dict keys
- Lines 242-249: `test_format_includes_exception_info` - Just checks dict keys
- Lines 251-264: `test_format_includes_custom_fields` - Just checks dict keys

#### Tests that verify logging framework behavior:
- Lines 266-289: `test_handlers_configured_correctly` - Tests logging library
- Lines 291-314: `test_console_handler_uses_json_formatter` - Tests logging library
- Lines 316-339: `test_file_handler_uses_json_formatter` - Tests logging library
- Lines 341-365: `test_log_level_from_environment_variable` - Tests env var parsing (trivial)

#### Logging-only verification tests:
- Lines 450-473: `test_structured_logging_logs_with_extra_fields` - Only verifies logging happened
- Lines 475-498: `test_get_logger_returns_configured_logger` - Tests logging.getLogger()
- Lines 500-523: `test_logger_propagates_to_root` - Tests logging library propagation

**KEEP (500 lines):**
- Tests for actual logging configuration logic
- Tests for custom formatters that add business logic
- Tests for error handling in logging setup

**Action:** Delete specified sections

---

### 1.3 tests/unit/test_cli.py (974 lines → ~550 lines)

**DELETE (424 lines):**

#### Tests that verify argparse behavior:
- Lines 909-975: `test_argparse_configuration` - Tests argparse library, not application logic
- Lines 50-85: `test_cmd_run_parses_arguments_correctly` - Tests argparse
- Lines 87-120: `test_cmd_run_validates_required_arguments` - Tests argparse

#### Logging-only tests:
- Lines 675-710: `test_cmd_run_logs_vault_usage` - Only verifies logging
- Lines 712-747: `test_cmd_run_logs_connection_details` - Only verifies logging

#### Over-mocked tests (8+ patches):
- Lines 260-335: `test_cmd_run_basic_success` - 8 patches, tests mock orchestration
- Lines 337-412: `test_cmd_run_handles_database_connection_failure` - 8 patches
- Lines 414-489: `test_cmd_run_handles_reconciliation_failure` - 8 patches
- Lines 491-566: `test_cmd_run_handles_report_generation_failure` - 8 patches

**KEEP (550 lines):**
- Tests for actual CLI command logic
- Tests for error handling with minimal mocking
- Integration-style tests that use real components

**Action:** Delete specified sections, refactor over-mocked tests in Phase 3

---

### 1.4 tests/unit/test_vault_client.py (883 lines → ~650 lines)

**DELETE (233 lines):**

#### Logging verification tests:
- Lines 339-366: `test_get_secret_logs_retrieval` - Only verifies logging
- Lines 368-395: `test_store_secret_logs_storage` - Only verifies logging
- Lines 560-587: `test_delete_secret_logs_deletion` - Only verifies logging

#### Tests coupled to implementation details:
- Lines 175-202: `test_get_secret_adds_data_to_path` - Tests internal URL construction
- Lines 204-231: `test_get_secret_handles_kv_v1_format` - Tests internal path transformation
- Lines 397-424: `test_store_secret_uses_post_method` - Tests HTTP method choice (trivial)

#### Tests that verify requests library behavior:
- Lines 589-616: `test_requests_timeout_configuration` - Tests requests library
- Lines 618-645: `test_requests_headers_configuration` - Tests requests library

**KEEP (650 lines):**
- Tests for actual Vault client logic
- Tests for error handling
- Tests for secret retrieval/storage behavior

**Action:** Delete specified sections

---

## Phase 2: Delete All Prometheus Metrics Tests

### 2.1 tests/unit/test_metrics.py (930 lines → DELETE ENTIRE FILE)

**RATIONALE:** This file tests Prometheus library behavior, not application logic.

**Tests in this file:**
- 46 test functions
- All test Prometheus Counter/Gauge/Histogram increment/set behavior
- None test actual business logic
- Examples:
  - `test_record_reconciliation_run_success` - Verifies counter incremented
  - `test_reconciliation_duration_histogram_records_value` - Tests histogram.observe()
  - `test_connector_status_gauge_sets_value` - Tests gauge.set()

**Action:** DELETE ENTIRE FILE (930 lines)

---

### 2.2 Delete metrics tests from other files

**tests/unit/test_reconcile.py:**
- Lines 1150-1230: Metrics-related tests (80 lines) - DELETE

**tests/unit/test_transform.py:**
- Lines 740-820: Metrics-related tests (80 lines) - DELETE

**tests/unit/test_parallel.py:**
- Lines 442-535: Metrics-related tests (93 lines) - DELETE

**Total metrics tests deleted:** 930 + 80 + 80 + 93 = **1,183 lines**

---

## Phase 3: Refactor Mock-Heavy Tests to Integration Tests

### 3.1 tests/unit/test_db_pool.py → tests/integration/test_db_pool_integration.py

**Current state:** 824 lines, 470 lines with heavy database mocking

**Problems:**
- Mocks `pyodbc.connect()` and `psycopg2.connect()`
- Tests mock context managers with `__enter__` / `__exit__`
- Mock inception: mocks returning mocks returning mocks

**Solution:** Create new integration test file using real database fixtures

**New file:** `tests/integration/test_db_pool_integration.py` (~350 lines)

```python
"""
Integration tests for database connection pooling.

Tests use REAL SQL Server and PostgreSQL databases from conftest.py fixtures.
"""

import pytest
import time
from threading import Thread
from src.utils.db_pool import (
    initialize_pools,
    get_postgres_pool,
    get_sqlserver_pool,
    close_pools,
    PoolExhaustedError,
    PoolClosedError,
)


class TestPostgresConnectionPool:
    """Test PostgreSQL connection pool with real database."""

    @pytest.fixture(autouse=True)
    def setup_pool(self, postgres_connection_params):
        """Initialize pool before each test, cleanup after."""
        initialize_pools(
            postgres_config=postgres_connection_params,
            postgres_pool_size=3,
            postgres_max_overflow=2,
        )
        yield
        close_pools()

    def test_acquire_and_release_connection(self):
        """Test acquiring and releasing connections from pool."""
        pool = get_postgres_pool()

        # Acquire connection
        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1

        # Verify connection returned to pool
        assert pool.size() == pool.pool_size

    def test_pool_exhaustion_with_real_connections(self):
        """Test pool exhaustion behavior with real connections."""
        pool = get_postgres_pool()

        # Acquire all connections
        contexts = [pool.acquire() for _ in range(3)]
        connections = [ctx.__enter__() for ctx in contexts]

        # Attempting to acquire beyond pool_size should raise
        with pytest.raises(PoolExhaustedError):
            with pool.acquire(timeout=1):
                pass

        # Release connections
        for ctx in contexts:
            ctx.__exit__(None, None, None)

    def test_connection_health_check_with_real_database(self):
        """Test that unhealthy connections are replaced."""
        pool = get_postgres_pool()

        # Acquire and forcibly close a connection
        with pool.acquire() as conn:
            cursor = conn.cursor()
            # Force connection closed
            conn.close()

        # Next acquire should detect unhealthy connection and create new one
        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1

    def test_concurrent_access_with_real_connections(self):
        """Test multiple threads accessing pool concurrently."""
        pool = get_postgres_pool()
        results = []
        errors = []

        def worker():
            try:
                with pool.acquire() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT pg_backend_pid()")
                    pid = cursor.fetchone()[0]
                    results.append(pid)
                    time.sleep(0.1)
            except Exception as e:
                errors.append(e)

        threads = [Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 5
        # At least some connections should be reused
        assert len(set(results)) <= 5


class TestSQLServerConnectionPool:
    """Test SQL Server connection pool with real database."""

    @pytest.fixture(autouse=True)
    def setup_pool(self, sqlserver_connection_string):
        """Initialize pool before each test, cleanup after."""
        initialize_pools(
            sqlserver_config={'connection_string': sqlserver_connection_string},
            sqlserver_pool_size=3,
            sqlserver_max_overflow=2,
        )
        yield
        close_pools()

    def test_acquire_and_release_connection(self):
        """Test acquiring and releasing SQL Server connections."""
        pool = get_sqlserver_pool()

        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1

    def test_connection_string_parsing(self):
        """Test that connection string is correctly used."""
        pool = get_sqlserver_pool()

        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            assert "Microsoft SQL Server" in version


class TestPoolManagement:
    """Test global pool management functions."""

    def test_initialize_and_close_pools(
        self, postgres_connection_params, sqlserver_connection_string
    ):
        """Test pool initialization and cleanup."""
        initialize_pools(
            postgres_config=postgres_connection_params,
            sqlserver_config={'connection_string': sqlserver_connection_string},
        )

        # Verify pools are accessible
        postgres_pool = get_postgres_pool()
        sqlserver_pool = get_sqlserver_pool()
        assert postgres_pool is not None
        assert sqlserver_pool is not None

        # Close pools
        close_pools()

        # Attempting to get pools after closing should raise
        with pytest.raises(PoolClosedError):
            get_postgres_pool()
```

**Action:**
1. Create new file `tests/integration/test_db_pool_integration.py`
2. DELETE old `tests/unit/test_db_pool.py` (824 lines)
3. Net change: 824 deleted, ~350 added = **474 line reduction**

---

### 3.2 tests/unit/test_scheduler.py - Refactor reconcile_job_wrapper tests

**Current state:** Lines 655-1215 (560 lines) with 7+ patches per test

**Solution:** Create integration test for actual reconciliation job execution

**New file:** `tests/integration/test_scheduler_jobs.py` (~200 lines)

```python
"""
Integration tests for reconciliation scheduler jobs.

Tests use real databases and minimal mocking.
"""

import pytest
import json
from pathlib import Path
from datetime import datetime
from src.reconciliation.scheduler.jobs import reconcile_job_wrapper


class TestReconcileJobWrapper:
    """Test reconcile_job_wrapper with real components."""

    @pytest.fixture(autouse=True)
    def setup_test_table(
        self, sqlserver_connection, postgres_connection, cleanup_test_tables
    ):
        """Create test table with sample data."""
        # Create table in SQL Server
        sqlserver_cursor = sqlserver_connection.cursor()
        sqlserver_cursor.execute("""
            CREATE TABLE test_scheduler_table (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                value DECIMAL(10,2)
            )
        """)
        sqlserver_cursor.execute("""
            INSERT INTO test_scheduler_table VALUES
            (1, 'Item 1', 100.50),
            (2, 'Item 2', 200.75)
        """)
        sqlserver_connection.commit()

        # Create matching table in PostgreSQL
        postgres_cursor = postgres_connection.cursor()
        postgres_cursor.execute("""
            CREATE TABLE test_scheduler_table (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                value DECIMAL(10,2)
            )
        """)
        postgres_cursor.execute("""
            INSERT INTO test_scheduler_table VALUES
            (1, 'Item 1', 100.50),
            (2, 'Item 2', 200.75)
        """)
        postgres_connection.commit()

    def test_successful_job_execution(self, tmp_path):
        """Test that reconcile_job_wrapper executes successfully."""
        output_dir = tmp_path / "reports"

        reconcile_job_wrapper(
            source_table="test_scheduler_table",
            target_table="test_scheduler_table",
            source_schema="dbo",
            target_schema="public",
            output_dir=str(output_dir),
        )

        # Verify report was created
        report_files = list(output_dir.glob("*.json"))
        assert len(report_files) > 0

        # Verify report content
        with open(report_files[0]) as f:
            report = json.load(f)

        assert report['table_name'] == 'test_scheduler_table'
        assert report['status'] == 'SUCCESS'
        assert report['rows_compared'] == 2
        assert report['discrepancies'] == 0

    def test_job_with_discrepancies(
        self, sqlserver_connection, postgres_connection, tmp_path
    ):
        """Test job execution when discrepancies exist."""
        # Modify data in PostgreSQL to create discrepancy
        postgres_cursor = postgres_connection.cursor()
        postgres_cursor.execute("""
            UPDATE test_scheduler_table
            SET value = 999.99
            WHERE id = 1
        """)
        postgres_connection.commit()

        output_dir = tmp_path / "reports"

        reconcile_job_wrapper(
            source_table="test_scheduler_table",
            target_table="test_scheduler_table",
            output_dir=str(output_dir),
        )

        # Verify report shows discrepancies
        report_files = list(output_dir.glob("*.json"))
        with open(report_files[0]) as f:
            report = json.load(f)

        assert report['discrepancies'] > 0
```

**Action:**
1. Create `tests/integration/test_scheduler_jobs.py`
2. DELETE lines 655-1215 from `tests/unit/test_scheduler.py` (560 lines)
3. Net change: 560 deleted, ~200 added = **360 line reduction**

---

### 3.3 tests/unit/test_cli.py - Refactor over-mocked tests

**Current state:** Lines 260-566 (306 lines) with 8 patches each

**Solution:** Create E2E tests that execute actual CLI commands

**New file:** `tests/e2e/test_cli_commands.py` (~150 lines)

```python
"""
E2E tests for CLI commands.

Tests execute actual CLI commands via subprocess.
"""

import pytest
import subprocess
import sys
import json
from pathlib import Path


class TestCLIRun:
    """Test 'reconcile run' command."""

    @pytest.fixture(autouse=True)
    def setup_test_table(
        self, sqlserver_connection, postgres_connection, cleanup_test_tables
    ):
        """Create test table with sample data."""
        # Create matching data in both databases
        for conn in [sqlserver_connection, postgres_connection]:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE test_cli_table (
                    id INT PRIMARY KEY,
                    name VARCHAR(100)
                )
            """)
            cursor.execute("""
                INSERT INTO test_cli_table VALUES
                (1, 'Test Record')
            """)
            conn.commit()

    def test_basic_run_command(self, tmp_path):
        """Test basic reconciliation via CLI."""
        output_file = tmp_path / "report.json"

        result = subprocess.run(
            [
                sys.executable, "-m", "src.reconciliation.cli",
                "run",
                "--source-table", "test_cli_table",
                "--target-table", "test_cli_table",
                "--output", str(output_file),
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert output_file.exists()

        with open(output_file) as f:
            report = json.load(f)

        assert report['status'] == 'SUCCESS'

    def test_run_command_with_discrepancies(
        self, sqlserver_connection, tmp_path
    ):
        """Test CLI correctly reports discrepancies."""
        # Modify SQL Server data to create discrepancy
        cursor = sqlserver_connection.cursor()
        cursor.execute("""
            UPDATE test_cli_table
            SET name = 'Modified'
            WHERE id = 1
        """)
        sqlserver_connection.commit()

        output_file = tmp_path / "report.json"

        result = subprocess.run(
            [
                sys.executable, "-m", "src.reconciliation.cli",
                "run",
                "--source-table", "test_cli_table",
                "--target-table", "test_cli_table",
                "--output", str(output_file),
            ],
            capture_output=True,
            text=True,
        )

        # CLI should exit with non-zero for discrepancies
        assert result.returncode != 0

        with open(output_file) as f:
            report = json.load(f)

        assert report['discrepancies'] > 0
```

**Action:**
1. Create `tests/e2e/test_cli_commands.py`
2. DELETE lines 260-566 from `tests/unit/test_cli.py` (306 lines)
3. Net change: 306 deleted, ~150 added = **156 line reduction**

---

## Phase 4: Verification and Cleanup

### 4.1 Run test suite and verify

```bash
# Run all tests
pytest tests/

# Run only integration tests
pytest tests/integration/

# Run with coverage to ensure no regression
pytest --cov=src --cov-report=html

# Verify no useless patterns remain
grep -r "mock_logger.*assert_called" tests/unit/
grep -r "@patch.*@patch.*@patch.*@patch.*@patch" tests/unit/
```

### 4.2 Update documentation

**Files to update:**
- `README.md` - Update test instructions if needed
- `docs/testing.md` - Document new integration test patterns (if exists)

### 4.3 Final cleanup

- Remove unused imports from test files
- Run `ruff check tests/` to ensure code quality
- Run `ruff format tests/` to format code

---

## Summary of Changes

### Files to DELETE entirely:
1. `tests/unit/test_metrics.py` (930 lines)

### Files with MAJOR deletions:
1. `tests/unit/test_scheduler.py`: 1,215 → 650 lines (**565 lines deleted**)
2. `tests/unit/test_logging_config.py`: 892 → 500 lines (**392 lines deleted**)
3. `tests/unit/test_cli.py`: 974 → 550 lines (**424 lines deleted**)
4. `tests/unit/test_vault_client.py`: 883 → 650 lines (**233 lines deleted**)
5. `tests/unit/test_db_pool.py`: 824 → 0 lines (**DELETED**)
6. `tests/unit/test_reconcile.py`: Delete 80 lines (metrics tests)
7. `tests/unit/test_transform.py`: Delete 80 lines (metrics tests)
8. `tests/unit/test_parallel.py`: Delete 93 lines (metrics tests)

### New FILES to CREATE:
1. `tests/integration/test_db_pool_integration.py` (~350 lines)
2. `tests/integration/test_scheduler_jobs.py` (~200 lines)
3. `tests/e2e/test_cli_commands.py` (~150 lines)

### Net Impact:
- **Lines deleted:** 930 + 565 + 392 + 424 + 233 + 824 + 80 + 80 + 93 = **3,621 lines**
- **Lines added:** 350 + 200 + 150 = **700 lines**
- **Net reduction:** **2,921 lines** (~35% of test suite)

### Quality Improvements:
- ✅ Zero tests verifying logging only
- ✅ Zero tests verifying framework behavior
- ✅ Zero tests with 5+ mock patches
- ✅ Zero Prometheus metrics tests
- ✅ All database tests use real connections
- ✅ All CLI tests execute actual commands
- ✅ Integration tests follow established patterns

---

## Risk Assessment

### Low Risk:
- Deleting logging verification tests (no business logic)
- Deleting metrics tests (testing library behavior)
- Deleting framework behavior tests (testing argparse, APScheduler)

### Medium Risk:
- Refactoring mock-heavy tests to integration tests
- Requires real database setup (already exists in conftest.py)
- May uncover actual bugs in production code

### Mitigation:
- Run full test suite after each phase
- Verify coverage doesn't drop for legitimate code paths
- Keep git history for rollback if needed

---

## Implementation Order

1. **Phase 1** (Low Risk): Delete useless tests from test_scheduler.py, test_logging_config.py, test_cli.py, test_vault_client.py
2. **Phase 2** (Low Risk): Delete all metrics tests
3. **Phase 3** (Medium Risk): Create integration tests, delete mock-heavy tests
4. **Phase 4** (Verification): Run tests, check coverage, update docs

**Estimated Time:** 4-6 hours for complete implementation

---

## Success Criteria

- ✅ All useless tests removed (0 logging-only tests, 0 mock verification tests)
- ✅ All metrics tests removed (0 Prometheus library tests)
- ✅ All mock-heavy tests refactored (0 tests with 5+ patches)
- ✅ Full test suite passes
- ✅ Test coverage maintained or improved for production code
- ✅ Integration tests use real database fixtures
- ✅ Code follows project style guidelines (ruff check passes)
