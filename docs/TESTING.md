# Testing Guide

## Overview

This guide provides comprehensive documentation for testing the SQL Server to PostgreSQL CDC pipeline, including test structure, best practices, and guidelines for writing new tests.

**Last Updated**: 2025-12-20
**Test Coverage**: 88% (exceeds 80% requirement)

---

## Table of Contents

- [Test Structure](#test-structure)
- [Running Tests](#running-tests)
- [Writing Tests](#writing-tests)
- [Test Categories](#test-categories)
- [Best Practices](#best-practices)
- [Continuous Integration](#continuous-integration)

---

## Test Structure

The project follows a multi-layered testing strategy:

```
tests/
├── unit/                  # Unit tests (353 tests)
│   ├── test_cli.py
│   ├── test_reconcile.py
│   ├── test_scheduler.py
│   ├── test_metrics.py
│   ├── test_logging_config.py
│   ├── test_retry.py
│   ├── test_sql_injection.py
│   └── test_init_files.py
├── contract/              # Contract tests
│   ├── test_debezium_schema.py
│   └── test_jdbc_sink_schema.py
├── integration/           # Integration tests
│   ├── test_replication_flow.py
│   ├── test_replication_independent.py
│   ├── test_schema_evolution.py
│   ├── test_error_recovery.py
│   └── test_monitoring.py
├── e2e/                   # End-to-end tests
│   └── test_reconciliation.py
├── performance/           # Performance benchmarks
│   └── test_performance.py
├── chaos/                 # Chaos engineering tests
│   └── test_chaos_recovery.py
└── conftest.py           # Shared pytest fixtures
```

### Test Coverage by Component

| Component | Coverage | Tests |
|-----------|----------|-------|
| `src/reconciliation/compare.py` | 60% | 32 SQL injection + retry tests |
| `src/reconciliation/report.py` | 100% | Report generation tests |
| `src/reconciliation/cli.py` | 98% | CLI tests |
| `src/utils/retry.py` | 89% | 26 retry logic tests |
| `src/utils/metrics.py` | 91% | Metrics tests |
| `src/utils/logging_config.py` | 85% | Logging tests |
| `src/utils/vault_client.py` | 93% | Vault client tests |
| **Overall** | **88%** | **353 unit tests** |

---

## Running Tests

### Quick Start

```bash
# Run all unit tests
source .venv/bin/activate
pytest tests/unit/ -v

# Run with coverage report
pytest tests/unit/ --cov=src --cov-report=html --cov-report=term

# Run specific test file
pytest tests/unit/test_retry.py -v

# Run specific test class
pytest tests/unit/test_retry.py::TestRetryWithBackoff -v

# Run specific test
pytest tests/unit/test_retry.py::TestRetryWithBackoff::test_success_on_first_attempt -v
```

### Running by Category

```bash
# Unit tests only (fast, no external dependencies)
pytest tests/unit/ -v

# Contract tests (validates JSON schemas)
pytest tests/contract/ -v

# Integration tests (requires running services)
pytest tests/integration/ -v

# E2E tests (full pipeline validation)
pytest tests/e2e/ -v

# Performance tests
pytest tests/performance/ -v

# Chaos tests (resilience validation)
pytest tests/chaos/ -v
```

### Using Markers

```bash
# Run all integration tests
pytest -m integration -v

# Run all contract tests
pytest -m contract -v

# Run slow tests
pytest -m slow -v

# Skip slow tests
pytest -m "not slow" -v
```

### Coverage Reports

```bash
# Generate HTML coverage report
pytest tests/unit/ --cov=src --cov-report=html
# Open htmlcov/index.html in browser

# Generate terminal coverage report
pytest tests/unit/ --cov=src --cov-report=term

# Generate XML report (for CI/CD)
pytest tests/unit/ --cov=src --cov-report=xml

# Set minimum coverage threshold
pytest tests/unit/ --cov=src --cov-fail-under=80
```

---

## Writing Tests

### Unit Test Template

```python
"""
Unit tests for [module name]

Tests [specific functionality] including:
- [test scenario 1]
- [test scenario 2]
- [test scenario 3]
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from src.module import function_under_test


class TestFunctionName:
    """Test [function name] functionality"""

    def test_success_case(self):
        """Test successful execution"""
        # Arrange
        input_data = "test"
        expected_output = "expected"

        # Act
        result = function_under_test(input_data)

        # Assert
        assert result == expected_output

    def test_error_case(self):
        """Test error handling"""
        # Arrange
        invalid_input = None

        # Act & Assert
        with pytest.raises(ValueError, match="Expected error message"):
            function_under_test(invalid_input)

    @patch('src.module.external_dependency')
    def test_with_mock(self, mock_dependency):
        """Test with mocked dependency"""
        # Arrange
        mock_dependency.return_value = "mocked"

        # Act
        result = function_under_test()

        # Assert
        assert result == "mocked"
        mock_dependency.assert_called_once()
```

### Integration Test Template

```python
"""
Integration tests for [component name]

Tests component interactions with:
- [dependency 1]
- [dependency 2]
"""

import pytest


@pytest.mark.integration
class TestComponentIntegration:
    """Test [component] integration"""

    @pytest.fixture
    def setup_environment(self):
        """Set up test environment"""
        # Setup code
        yield
        # Teardown code

    def test_integration_scenario(self, setup_environment):
        """Test integration scenario"""
        # Arrange
        # Setup dependencies

        # Act
        # Execute integration scenario

        # Assert
        # Verify expected interactions
```

---

## Test Categories

### 1. Unit Tests

**Purpose**: Test individual components in isolation

**Characteristics**:
- Fast execution (<1 second per test)
- No external dependencies
- Use mocks for dependencies
- Test specific functions/methods

**Example**:
```python
def test_compare_row_counts_match():
    """Test row count comparison when counts match"""
    result = compare_row_counts(
        table_name="customers",
        source_count=100,
        target_count=100
    )

    assert result["match"] is True
    assert result["difference"] == 0
    assert result["status"] == "MATCH"
```

### 2. Contract Tests

**Purpose**: Validate connector configurations against schemas

**Characteristics**:
- Validates JSON configuration files
- Checks required fields
- Verifies schema compliance
- Fast execution

**Example**:
```python
def test_config_validates_against_schema():
    """Test connector config validates against JSON schema"""
    with open('connectors/debezium-source.json') as f:
        config = json.load(f)

    with open('schemas/debezium-connector-schema.json') as f:
        schema = json.load(f)

    validate(instance=config, schema=schema)
```

### 3. Integration Tests

**Purpose**: Test component interactions

**Characteristics**:
- Require running services (Docker)
- Test database connectivity
- Validate Kafka messaging
- Test end-to-end flows

**Prerequisites**:
```bash
# Start services
docker-compose up -d

# Wait for services to be ready
sleep 60

# Run integration tests
pytest tests/integration/ -v
```

**Example**:
```python
@pytest.mark.integration
def test_insert_replication():
    """Test INSERT operation replication"""
    # Insert into SQL Server
    source_cursor.execute(
        "INSERT INTO customers (id, name) VALUES (1, 'Test')"
    )

    # Wait for replication
    time.sleep(5)

    # Verify in PostgreSQL
    target_cursor.execute(
        "SELECT * FROM customers WHERE id = 1"
    )
    result = target_cursor.fetchone()

    assert result is not None
    assert result[1] == 'Test'
```

### 4. E2E Tests

**Purpose**: Validate complete pipeline flows

**Characteristics**:
- Test full pipeline (SQL Server → Kafka → PostgreSQL)
- Validate reconciliation tools
- Test real-world scenarios
- Slower execution

**Example**:
```python
def test_reconciliation_detects_mismatch():
    """Test reconciliation detects row count mismatch"""
    # Create mismatch by deleting from target
    target_cursor.execute("DELETE FROM customers WHERE id = 1")

    # Run reconciliation
    result = reconcile_table(
        source_cursor, target_cursor,
        "dbo.customers", "customers"
    )

    assert result["match"] is False
    assert result["difference"] == -1
```

### 5. Performance Tests

**Purpose**: Benchmark throughput and latency

**Characteristics**:
- Measure processing rates
- Monitor resource usage
- Test under load
- Establish baselines

**Example**:
```python
@pytest.mark.performance
def test_throughput_10k_rows_per_second():
    """Test pipeline can handle 10k rows/second"""
    start_time = time.time()

    # Insert 100k rows
    for i in range(100000):
        source_cursor.execute(
            "INSERT INTO test_table (id, data) VALUES (?, ?)",
            (i, f"data_{i}")
        )

    # Wait for replication
    wait_for_replication()

    elapsed = time.time() - start_time
    throughput = 100000 / elapsed

    assert throughput >= 10000, f"Throughput {throughput:.0f} < 10000 rows/sec"
```

### 6. Chaos Tests

**Purpose**: Validate resilience and recovery

**Characteristics**:
- Simulate failures
- Test recovery procedures
- Validate error handling
- Ensure data consistency

**Example**:
```python
@pytest.mark.chaos
def test_connector_recovers_from_network_partition():
    """Test connector recovers after network partition"""
    # Pause connector (simulate network failure)
    pause_connector("sqlserver-source-connector")

    # Insert data while connector is down
    source_cursor.execute("INSERT INTO customers ...")

    # Resume connector
    resume_connector("sqlserver-source-connector")

    # Verify data replicated after recovery
    wait_for_replication()
    assert_data_consistent()
```

---

## Best Practices

### 1. Test Naming

Use descriptive test names that explain what is being tested:

```python
# Good
def test_row_count_comparison_detects_missing_rows():
    ...

def test_retry_decorator_handles_connection_timeout():
    ...

# Bad
def test_compare():
    ...

def test_retry():
    ...
```

### 2. Arrange-Act-Assert Pattern

Structure tests clearly:

```python
def test_calculate_checksum_with_null_values():
    # Arrange - Set up test data
    mock_cursor = Mock()
    mock_cursor.__iter__ = Mock(return_value=iter([
        [1, "test", None],
        [2, None, "data"]
    ]))

    # Act - Execute function under test
    result = calculate_checksum(mock_cursor, "test_table")

    # Assert - Verify expected behavior
    assert isinstance(result, str)
    assert len(result) == 64  # SHA256 hex length
```

### 3. Use Fixtures for Setup

```python
@pytest.fixture
def mock_database_cursor():
    """Provide a mock database cursor"""
    cursor = Mock()
    cursor.fetchone.return_value = [100]
    cursor.description = [('id',), ('name',)]
    return cursor


def test_with_fixture(mock_database_cursor):
    """Test using fixture"""
    result = get_row_count(mock_database_cursor, "customers")
    assert result == 100
```

### 4. Test Edge Cases

```python
def test_row_count_with_zero_rows():
    """Test row count comparison with zero rows"""
    result = compare_row_counts("empty_table", 0, 0)
    assert result["match"] is True

def test_row_count_invalid_negative():
    """Test row count rejects negative counts"""
    with pytest.raises(ValueError, match="cannot be negative"):
        compare_row_counts("table", -1, 100)
```

### 5. Mock External Dependencies

```python
@patch('src.reconciliation.compare.retry_database_operation')
def test_with_mocked_retry(mock_retry):
    """Test with mocked retry decorator"""
    mock_retry.return_value = lambda func: func

    result = get_row_count(mock_cursor, "customers")

    assert result == 100
```

### 6. Test Error Handling

```python
def test_handles_database_connection_error():
    """Test graceful handling of connection errors"""
    mock_cursor = Mock()
    mock_cursor.execute.side_effect = ConnectionError("Connection failed")

    with pytest.raises(ConnectionError, match="Connection failed"):
        get_row_count(mock_cursor, "customers")
```

### 7. Parametrize Similar Tests

```python
@pytest.mark.parametrize("source_count,target_count,expected_match", [
    (100, 100, True),
    (100, 99, False),
    (0, 0, True),
    (50, 100, False),
])
def test_row_count_comparison(source_count, target_count, expected_match):
    """Test row count comparison with various inputs"""
    result = compare_row_counts("table", source_count, target_count)
    assert result["match"] == expected_match
```

---

## Continuous Integration

### GitHub Actions Workflow

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v2

    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.11'

    - name: Install dependencies
      run: |
        python -m venv .venv
        source .venv/bin/activate
        pip install -r requirements.txt

    - name: Run unit tests
      run: |
        source .venv/bin/activate
        pytest tests/unit/ --cov=src --cov-report=xml --cov-fail-under=80

    - name: Upload coverage
      uses: codecov/codecov-action@v2
      with:
        file: ./coverage.xml
```

### Pre-commit Hook

```bash
#!/bin/bash
# .git/hooks/pre-commit

# Run unit tests before commit
source .venv/bin/activate
pytest tests/unit/ -q

if [ $? -ne 0 ]; then
    echo "Unit tests failed. Commit aborted."
    exit 1
fi
```

---

## Test Data Management

### Creating Test Data

```python
@pytest.fixture
def sample_customers():
    """Provide sample customer data"""
    return [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
        {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
    ]


def test_with_sample_data(sample_customers):
    """Test with sample data fixture"""
    result = process_customers(sample_customers)
    assert len(result) == 3
```

### Cleaning Up Test Data

```python
@pytest.fixture
def clean_database():
    """Ensure clean database for each test"""
    # Setup
    cursor.execute("DELETE FROM customers WHERE id >= 90000")

    yield

    # Teardown
    cursor.execute("DELETE FROM customers WHERE id >= 90000")
```

---

## Debugging Tests

### Running Single Test with Debug

```bash
# Run single test with verbose output
pytest tests/unit/test_retry.py::TestRetryWithBackoff::test_success_on_first_attempt -vv

# Run with print statements visible
pytest tests/unit/test_retry.py -s

# Run with Python debugger
pytest tests/unit/test_retry.py --pdb
```

### Adding Debug Output

```python
def test_with_debug_output():
    """Test with debug output"""
    import logging
    logging.basicConfig(level=logging.DEBUG)

    result = function_under_test()

    # Debug output
    print(f"Result: {result}")
    assert result is not None
```

---

## Common Testing Patterns

### Testing SQL Injection Protection

```python
def test_reject_sql_injection_in_table_name():
    """Test SQL injection rejection"""
    malicious_inputs = [
        "customers; DROP TABLE users--",
        "customers' OR '1'='1",
        "customers/**/UNION/**/SELECT",
    ]

    for malicious_input in malicious_inputs:
        with pytest.raises(ValueError, match="Invalid identifier format"):
            get_row_count(mock_cursor, malicious_input)
```

### Testing Retry Logic

```python
def test_retry_on_transient_error():
    """Test retry on transient connection error"""
    mock_func = Mock(side_effect=[
        ConnectionError("Timeout"),
        ConnectionError("Timeout"),
        "success"  # Third attempt succeeds
    ])

    @retry_with_backoff(max_retries=3, base_delay=0.1)
    def wrapped():
        return mock_func()

    result = wrapped()

    assert result == "success"
    assert mock_func.call_count == 3
```

### Testing Async Operations

```python
@pytest.mark.asyncio
async def test_async_operation():
    """Test async operation"""
    result = await async_function()
    assert result is not None
```

---

## Test Metrics

### Current Test Statistics

- **Total Tests**: 428 (unit + integration + e2e + contract + performance + chaos)
- **Unit Tests**: 353 (82% of total)
- **Passing**: 383 (89%)
- **Skipped**: 8 (2%)
- **Failed**: 18 (4%) - Integration tests requiring running services
- **Errors**: 18 (4%) - Database connection timeouts
- **Coverage**: 88.44%

### Coverage Goals

| Component | Current | Target |
|-----------|---------|--------|
| Core Logic | 88% | 90% |
| Error Handling | 85% | 90% |
| Edge Cases | 80% | 85% |
| Integration Paths | 75% | 80% |

---

## Resources

- [pytest Documentation](https://docs.pytest.org/)
- [unittest.mock Documentation](https://docs.python.org/3/library/unittest.mock.html)
- [pytest-cov Plugin](https://pytest-cov.readthedocs.io/)
- [Testing Best Practices](https://docs.python-guide.org/writing/tests/)

---

## Revision History

| Date       | Version | Changes                | Author |
|------------|---------|------------------------|--------|
| 2025-12-20 | 1.0     | Initial testing guide  | System |