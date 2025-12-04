# Implementation Plan: 100% Unit Test Coverage for src/

**Project:** sqlserver-pg-cdc
**Goal:** Achieve 100% unit test coverage for all modules in `src/` directory
**Current Coverage:** Partial (only `compare.py` and `report.py` have unit tests)

## Executive Summary

This plan outlines the comprehensive approach to increase unittest coverage from the current partial coverage to 100% for all source code in the `src/` directory. The project currently has 98 tests across integration, e2e, contract, and performance test categories, but only ~21 unit tests covering 2 of the 6 source modules.

## Current State Analysis

### Existing Source Modules (6 modules total)

```
src/
├── reconciliation/
│   ├── __init__.py          (19 lines)  ⚠️  NOT TESTED
│   ├── compare.py           (230 lines) ✓  PARTIALLY TESTED (unit tests exist)
│   ├── report.py            (369 lines) ✓  PARTIALLY TESTED (unit tests exist)
│   └── scheduler.py         (271 lines) ⚠️  NOT TESTED
└── utils/
    ├── __init__.py          (10 lines)  ⚠️  NOT TESTED
    ├── vault_client.py      (215 lines) ⚠️  MINIMALLY TESTED (only e2e)
    ├── metrics.py           (653 lines) ⚠️  MINIMALLY TESTED (integration only)
    └── logging_config.py    (471 lines) ⚠️  NOT TESTED
```

### Current Test Coverage

**Unit Tests:** `tests/unit/test_reconcile.py` (21 tests)
- ✓ `src/reconciliation/compare.py` - Row count and checksum functions
- ✓ `src/reconciliation/report.py` - Report generation and formatting

**Coverage Gaps:**
1. **scheduler.py** (271 lines) - 0% unit test coverage
2. **vault_client.py** (215 lines) - 0% unit test coverage
3. **metrics.py** (653 lines) - 0% unit test coverage
4. **logging_config.py** (471 lines) - 0% unit test coverage
5. **__init__.py** files - Not tested (low priority, minimal logic)
6. **compare.py** - Edge cases and error paths may be missing
7. **report.py** - Edge cases and error paths may be missing

## Implementation Plan

### Phase 1: Enhance Existing Unit Tests (Week 1)

#### Task 1.1: Audit and Improve `compare.py` Coverage
**File:** `tests/unit/test_reconcile.py` (TestRowCountComparison, TestChecksumValidation)

**Missing Coverage:**
- Error handling paths (database exceptions, invalid cursors)
- Edge cases:
  - Empty tables (0 rows)
  - Very large tables (performance boundaries)
  - NULL-heavy data in checksum calculations
  - Special characters and encoding issues
  - Connection timeout scenarios
- `reconcile_table()` function comprehensive testing:
  - Both checksum enabled/disabled paths
  - Exception handling and error propagation
  - Return value structure validation

**Estimated New Tests:** 10-15 tests

**Dependencies:**
- Mock database cursors (using `unittest.mock.Mock`)
- Test fixtures for cursor responses

---

#### Task 1.2: Audit and Improve `report.py` Coverage
**File:** `tests/unit/test_reconcile.py` (TestDiscrepancyReporting)

**Missing Coverage:**
- `_calculate_severity()` edge cases:
  - Zero source count (division by zero)
  - Negative differences (should never happen, but defensive)
  - Boundary values (0%, 1%, 5%, 10%, 50%, 100%+)
- `_generate_recommendations()` comprehensive testing:
  - All severity levels
  - Multiple discrepancies
  - Edge case messaging
- `format_report_console()` testing:
  - Various report structures
  - Empty reports
  - Very long table names
  - Special characters in table names
- `export_report_csv()` comprehensive testing:
  - File I/O error handling
  - Path validation
  - CSV format validation
  - Special characters in data
- `export_report_json()` error handling:
  - Invalid paths
  - Permission errors
  - JSON serialization edge cases

**Estimated New Tests:** 12-18 tests

**Dependencies:**
- Temporary file/directory fixtures
- Mock file system operations for error scenarios

---

### Phase 2: New Unit Tests for Untested Modules (Weeks 2-3)

#### Task 2.1: Create `tests/unit/test_scheduler.py`
**Target:** `src/reconciliation/scheduler.py` (271 lines)

**Test Classes:**

##### `TestReconciliationSchedulerInit`
- Test scheduler initialization
- Test default BlockingScheduler creation
- Test jobs list initialization

**Estimated Tests:** 3

##### `TestIntervalJobs`
- Test `add_interval_job()` with various intervals
- Test job registration and storage
- Test `replace_existing=True` behavior
- Test invalid intervals (negative, zero)
- Test job_id uniqueness

**Estimated Tests:** 6-8

##### `TestCronJobs`
- Test `add_cron_job()` with valid cron expressions:
  - "0 */6 * * *" (every 6 hours)
  - "0 0 * * *" (daily at midnight)
  - "*/30 * * * *" (every 30 minutes)
- Test invalid cron expressions:
  - Too few parts (< 5)
  - Too many parts (> 5)
  - Invalid characters
- Test cron trigger creation and parsing

**Estimated Tests:** 8-10

##### `TestJobManagement`
- Test `remove_job()` functionality
- Test `list_jobs()` return structure
- Test listing with no jobs
- Test listing with multiple jobs
- Test next_run_time formatting

**Estimated Tests:** 5-7

##### `TestSchedulerLifecycle`
- Test `start()` method (mock BlockingScheduler.start)
- Test KeyboardInterrupt handling
- Test SystemExit handling
- Test `stop()` method
- Test shutdown behavior

**Estimated Tests:** 5

##### `TestReconcileJobWrapper`
- Test successful reconciliation job execution
- Test database connection creation:
  - SQL Server connection string formatting
  - PostgreSQL connection parameters
- Test table iteration and reconciliation
- Test error handling per table (continue on error)
- Test report generation and export
- Test output directory creation
- Test timestamp formatting
- Test connection cleanup (close cursors and connections)
- Test exception propagation on fatal errors

**Estimated Tests:** 12-15

**Total for scheduler.py:** ~40-48 tests

**Mocking Strategy:**
- Mock `BlockingScheduler` from apscheduler
- Mock `pyodbc.connect` and `psycopg2.connect`
- Mock `reconcile_table`, `generate_report`, `export_report_json`
- Mock `Path.mkdir` for directory creation
- Use `freezegun` or similar for timestamp testing

---

#### Task 2.2: Create `tests/unit/test_vault_client.py`
**Target:** `src/utils/vault_client.py` (215 lines)

**Test Classes:**

##### `TestVaultClientInit`
- Test initialization with explicit parameters
- Test initialization from environment variables
- Test missing vault_addr raises ValueError
- Test missing vault_token raises ValueError
- Test trailing slash removal from vault_addr
- Test namespace header inclusion (Vault Enterprise)
- Test headers dictionary structure

**Estimated Tests:** 7-9

##### `TestGetSecret`
- Test successful secret retrieval
- Test KV v2 path transformation:
  - "secret/database/sqlserver" → "secret/data/database/sqlserver"
  - Path already containing "/data/"
  - Single-level paths
- Test 404 response handling
- Test 403 (permission denied) handling
- Test connection timeout
- Test invalid JSON response
- Test missing "data" key in response
- Test request headers inclusion

**Estimated Tests:** 10-12

##### `TestGetDatabaseCredentials`
- Test SQL Server credential retrieval
- Test PostgreSQL credential retrieval
- Test credential validation (required fields):
  - host/server
  - database
  - username
  - password
- Test missing required fields raises appropriate errors
- Test additional optional fields preservation

**Estimated Tests:** 8-10

##### `TestHealthCheck`
- Test successful health check
- Test unhealthy Vault response
- Test connection errors
- Test timeout handling

**Estimated Tests:** 4-5

##### `TestGetCredentialsFromVault` (utility function)
- Test convenience function delegation
- Test parameter passing
- Test return value structure

**Estimated Tests:** 3

**Total for vault_client.py:** ~32-39 tests

**Mocking Strategy:**
- Mock `requests.get` and `requests.post`
- Mock `os.getenv` for environment variable tests
- Use `responses` library for HTTP mocking
- Create fixture Vault response payloads

---

#### Task 2.3: Create `tests/unit/test_metrics.py`
**Target:** `src/utils/metrics.py` (653 lines)

**Test Classes:**

##### `TestMetricsPublisher`
- Test initialization with default port (9091)
- Test initialization with custom port
- Test custom registry support
- Test `start()` method:
  - Successful start
  - Port already in use handling
  - Multiple start() calls (idempotent)
- Test `is_started()` state tracking

**Estimated Tests:** 7-9

##### `TestReconciliationMetrics`
- Test metrics initialization
- Test counter creation (reconciliation_runs_total)
- Test gauge creation (row_count_mismatch)
- Test histogram creation (reconciliation_duration_seconds)
- Test `record_reconciliation_run()`:
  - Success case
  - Failure case
  - Duration recording
  - Table name labeling
- Test `record_row_count_mismatch()`:
  - Mismatch recording
  - Source/target count labels
- Test `record_checksum_mismatch()`:
  - Mismatch detection
  - Table labeling
- Test `record_performance()`:
  - Rows per second calculation
  - Histogram bucketing

**Estimated Tests:** 15-18

##### `TestConnectorMetrics`
- Test metrics initialization
- Test connector deployment counter
- Test connector status gauge
- Test operation recording
- Test state transitions

**Estimated Tests:** 6-8

##### `TestVaultMetrics`
- Test credential retrieval counter
- Test health check gauge
- Test error tracking

**Estimated Tests:** 4-5

##### `TestApplicationInfo`
- Test application metadata exposure
- Test uptime calculation
- Test version information

**Estimated Tests:** 3-4

##### `TestInitializeMetrics` (utility function)
- Test all metrics initialized correctly
- Test MetricsPublisher started
- Test return value structure

**Estimated Tests:** 3

**Total for metrics.py:** ~38-47 tests

**Mocking Strategy:**
- Mock `start_http_server` from prometheus_client
- Use Prometheus `CollectorRegistry` for isolated testing
- Mock time functions for uptime/duration testing
- Assert metric values using `.collect()` method

---

#### Task 2.4: Create `tests/unit/test_logging_config.py`
**Target:** `src/utils/logging_config.py` (471 lines)

**Test Classes:**

##### `TestJSONFormatter`
- Test initialization parameters
- Test `format()` basic structure:
  - level, logger, message, app fields
  - timestamp inclusion
  - hostname inclusion
- Test source location formatting
- Test process/thread info inclusion
- Test exception info formatting:
  - Type, message, traceback
- Test extra context inclusion
- Test field skipping logic
- Test JSON serialization of complex objects (using default=str)

**Estimated Tests:** 10-12

##### `TestConsoleFormatter`
- Test initialization with colors enabled/disabled
- Test `format()` basic output
- Test color code injection for each level:
  - DEBUG (cyan)
  - INFO (green)
  - WARNING (yellow)
  - ERROR (red)
  - CRITICAL (magenta)
- Test color disabling for non-TTY
- Test extra context appending
- Test timestamp formatting

**Estimated Tests:** 8-10

##### `TestSetupLogging`
- Test basic setup (console only)
- Test file logging with rotation:
  - File creation
  - Directory creation
  - RotatingFileHandler configuration
- Test log level setting
- Test JSON format selection
- Test console output toggle
- Test handler clearing (avoid duplicates)
- Test third-party library log level adjustment:
  - urllib3, requests, kafka, hvac set to WARNING
- Test max_bytes and backup_count parameters

**Estimated Tests:** 12-15

##### `TestGetLogger`
- Test logger retrieval
- Test logger name assignment
- Test logger instance caching

**Estimated Tests:** 3

##### `TestContextLogger`
- Test initialization with context
- Test `debug()`, `info()`, `warning()`, `error()`, `critical()` methods
- Test context merging with kwargs
- Test `update_context()` functionality
- Test `get_context()` returns copy
- Test exception info passing (exc_info parameter)
- Test persistent context across multiple log calls

**Estimated Tests:** 10-12

##### `TestConfigureFromEnv`
- Test environment variable parsing:
  - LOG_LEVEL
  - LOG_FILE
  - LOG_JSON
  - LOG_CONSOLE
- Test default values
- Test boolean parsing ("true", "1", "yes")
- Test delegation to `setup_logging()`

**Estimated Tests:** 6-8

**Total for logging_config.py:** ~49-60 tests

**Mocking Strategy:**
- Mock `logging.handlers.RotatingFileHandler`
- Mock `os.makedirs` for directory creation tests
- Mock `os.getenv` for environment variable tests
- Mock `sys.stderr.isatty()` for TTY detection
- Mock `os.uname()` for hostname testing
- Use `io.StringIO` for capturing log output
- Use temporary directories for file logging tests (or mock file operations)

---

### Phase 3: Test __init__.py Files (Week 4)

#### Task 3.1: Create `tests/unit/test_init_files.py`
**Targets:**
- `src/reconciliation/__init__.py` (19 lines)
- `src/utils/__init__.py` (10 lines)

**Test Classes:**

##### `TestReconciliationInit`
- Test `__version__` attribute
- Test `__all__` attribute contents
- Test module imports (ensure no import errors)

**Estimated Tests:** 3

##### `TestUtilsInit`
- Test `__version__` attribute
- Test `__all__` attribute contents
- Test module imports (ensure no import errors)

**Estimated Tests:** 3

**Total for __init__.py files:** ~6 tests

**Note:** These files have minimal logic, so testing is primarily for completeness and import validation.

---

### Phase 4: Coverage Validation and Gap Analysis (Week 4)

#### Task 4.1: Run Coverage Analysis
- Run pytest with coverage: `pytest tests/unit/ --cov=src --cov-report=html --cov-report=term-missing`
- Generate HTML coverage report for detailed line-by-line analysis
- Identify any remaining untested lines or branches

#### Task 4.2: Address Coverage Gaps
- Review coverage report for lines marked as "not covered"
- Add targeted tests for:
  - Exception handlers
  - Edge cases
  - Conditional branches
  - Error paths
- Aim for 100% line coverage and >95% branch coverage

#### Task 4.3: Refactor for Testability (if needed)
- Identify any tightly-coupled code that's difficult to test
- Extract dependencies to enable mocking
- Add dependency injection where appropriate
- Ensure all external dependencies (DB, HTTP, file I/O) are mockable

---

## Testing Strategy & Best Practices

### General Principles
1. **Isolation:** Unit tests must not depend on external services (databases, Vault, Prometheus)
2. **Speed:** All unit tests should run in <5 seconds total
3. **Determinism:** Tests must be repeatable and not flaky
4. **Clarity:** Test names should clearly describe what is being tested
5. **Coverage:** Aim for 100% line coverage and >95% branch coverage

### Mocking Strategy
- **Database Operations:** Mock `pyodbc` and `psycopg2` connections and cursors
- **HTTP Requests:** Mock `requests.get/post` using `unittest.mock` or `responses` library
- **File I/O:** Mock `Path` operations and file handlers
- **Time:** Use `freezegun` for timestamp-dependent tests
- **Prometheus:** Use isolated `CollectorRegistry` instances
- **Logging:** Capture log output with `caplog` (pytest) or `io.StringIO`

### Test Organization
```
tests/unit/
├── test_reconcile.py              # Existing (enhance)
├── test_scheduler.py              # New
├── test_vault_client.py           # New
├── test_metrics.py                # New
├── test_logging_config.py         # New
└── test_init_files.py             # New
```

### Test Naming Convention
- Class names: `Test<ModuleName><Component>`
  - Example: `TestReconciliationSchedulerInit`, `TestVaultClientGetSecret`
- Method names: `test_<functionality>_<scenario>`
  - Example: `test_add_interval_job_with_valid_params`, `test_get_secret_when_vault_returns_404`

### Fixtures and Utilities
Create shared fixtures in `tests/unit/conftest.py`:
- Mock database cursors with realistic responses
- Mock Vault HTTP responses
- Temporary directories for file tests
- Prometheus registry fixtures
- Logger capture fixtures

---

## Dependencies & Tools

### Required Libraries (already in pyproject.toml)
- `pytest>=7.4.3` - Test framework
- `pytest-cov>=4.1.0` - Coverage reporting
- `pytest-asyncio>=0.21.1` - Async support (if needed)

### Additional Recommended Libraries
- `pytest-mock>=3.12.0` - Enhanced mocking utilities
- `freezegun>=1.4.0` - Time freezing for timestamp tests
- `responses>=0.24.0` - HTTP request mocking

### Coverage Configuration (already in pyproject.toml)
```toml
[tool.pytest.ini_options]
addopts = [
    "--verbose",
    "--cov=src",
    "--cov-report=term-missing",
    "--cov-report=html",
    "--cov-fail-under=80",  # Increase to 100 after implementation
]
```

---

## Success Criteria

### Quantitative Goals
- ✓ 100% line coverage for all `src/**/*.py` files
- ✓ >95% branch coverage for all modules
- ✓ All tests pass in CI/CD pipeline
- ✓ Unit test suite runs in <10 seconds
- ✓ Zero flaky tests (100% pass rate over 10 runs)

### Qualitative Goals
- ✓ All edge cases identified and tested
- ✓ All error paths validated
- ✓ Tests serve as documentation for module behavior
- ✓ Mocking strategy is consistent and maintainable
- ✓ Tests are isolated and don't depend on test execution order

---

## Estimated Effort Summary

| Phase | Tasks | Est. Tests | Est. Time |
|-------|-------|-----------|----------|
| **Phase 1: Enhance Existing** | Tasks 1.1-1.2 | 22-33 tests | 3-4 days |
| **Phase 2: New Modules** | Tasks 2.1-2.4 | 169-194 tests | 10-12 days |
| **Phase 3: __init__ Files** | Task 3.1 | 6 tests | 0.5 days |
| **Phase 4: Validation** | Tasks 4.1-4.3 | Variable | 2-3 days |
| **Total** | 11 tasks | ~200-235 tests | **15-20 days** |

**Note:** This estimate assumes full-time dedicated work. Adjust timeline based on available resources.

---

## Risks & Mitigations

### Risk 1: Complex Mocking Requirements
**Impact:** High
**Likelihood:** Medium
**Mitigation:**
- Start with simpler modules (vault_client, __init__ files)
- Build reusable mock fixtures early
- Document mocking patterns in `tests/unit/conftest.py`

### Risk 2: Tightly Coupled Code
**Impact:** Medium
**Likelihood:** Medium
**Mitigation:**
- Identify coupling during Phase 1 audit
- Refactor for testability if necessary (with stakeholder approval)
- Use monkey patching as last resort

### Risk 3: Time Constraints
**Impact:** Medium
**Likelihood:** Low
**Mitigation:**
- Prioritize high-value modules first (scheduler, vault_client)
- Deliver in incremental PRs (one module at a time)
- Adjust scope if needed (e.g., target 95% coverage instead of 100%)

---

## Delivery Plan

### Incremental Delivery (Recommended)
1. **PR 1:** Enhanced coverage for `compare.py` and `report.py` (Phase 1)
2. **PR 2:** New tests for `scheduler.py` (Task 2.1)
3. **PR 3:** New tests for `vault_client.py` (Task 2.2)
4. **PR 4:** New tests for `metrics.py` (Task 2.3)
5. **PR 5:** New tests for `logging_config.py` (Task 2.4)
6. **PR 6:** Tests for `__init__.py` files + final validation (Phases 3-4)

### CI/CD Integration
- Run unit tests on every commit
- Enforce minimum coverage threshold (start at 80%, increase to 100%)
- Block PRs that decrease coverage
- Generate coverage badges for README

---

## Appendix: Test File Templates

### Template: Basic Test Structure
```python
"""Unit tests for <module_name>"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.<package>.<module> import <ClassOrFunction>


class Test<ClassName><Component>:
    """Tests for <ClassName>.<component>"""

    def test_<functionality>_<scenario>(self):
        """Test that <functionality> <expected_behavior> when <scenario>"""
        # Arrange
        # ... setup test data and mocks

        # Act
        # ... call the function/method under test

        # Assert
        # ... verify expected outcomes
```

### Template: Fixture Example
```python
@pytest.fixture
def mock_database_cursor():
    """Fixture providing a mock database cursor"""
    cursor = Mock()
    cursor.fetchone.return_value = (100,)  # Example row count
    cursor.fetchall.return_value = [
        (1, "data1"),
        (2, "data2"),
    ]
    return cursor
```

---

## Next Steps

1. **Review and Approve Plan:** Stakeholder review of this implementation plan
2. **Setup Environment:** Ensure dev environment has all required dependencies
3. **Create Tracking Board:** Create GitHub issues/JIRA tickets for each task
4. **Begin Phase 1:** Start with enhancing existing unit tests
5. **Iterate:** Deliver incrementally with regular coverage reports

---

**Document Version:** 1.0
**Last Updated:** 2025-12-04
**Owner:** Development Team
**Status:** Draft - Awaiting Approval
