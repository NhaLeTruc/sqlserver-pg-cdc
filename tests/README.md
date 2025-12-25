# SQL Server to PostgreSQL CDC Pipeline - Test Suite

Comprehensive test suite for validating the CDC pipeline's correctness, reliability, and performance.

## Test Directory Structure

```
tests/
├── property/              # Property-based tests (Hypothesis)
│   ├── test_reconciliation_properties.py
│   └── test_data_integrity_properties.py
├── unit/                  # Unit tests
│   ├── test_cli.py
│   ├── test_logging_config.py
│   ├── test_sql_injection.py
│   └── test_vault_client.py
├── integration/           # Integration tests
│   ├── test_replication_flow.py
│   ├── test_schema_evolution.py
│   ├── test_error_recovery.py
│   └── test_monitoring.py
├── e2e/                   # End-to-end tests
│   └── test_reconciliation.py
├── contract/              # Contract tests
│   ├── test_debezium_schema.py
│   └── test_jdbc_sink_schema.py
├── load/                  # Load and performance tests
│   ├── locustfile.py
│   ├── reconciliation_load_test.py
│   └── database_load_test.py
├── performance/           # Performance benchmark tests
│   └── test_performance.py
├── chaos/                 # Chaos engineering tests
│   └── test_chaos_recovery.py
└── conftest.py           # Pytest configuration and fixtures
```

## Test Categories

### 1. Property-Based Tests (`property/`)

Automatically generate test cases to validate properties and invariants.

**What they test:**
- Mathematical properties (symmetry, associativity, etc.)
- Invariants that should always hold
- Edge cases and boundary conditions
- SQL injection safety
- Checksum consistency

**Run with:**
```bash
make test-property
pytest tests/property/ -v
```

**Example:**
```python
@given(source_count=st.integers(min_value=0, max_value=10000000))
def test_row_count_properties(source_count: int):
    result = compare_row_counts("table", source_count, source_count)
    assert result['match'] is True
    assert result['difference'] == 0
```

### 2. Unit Tests (`unit/`)

Test individual functions and modules in isolation.

**What they test:**
- Function-level correctness
- Edge cases and error handling
- Input validation
- Security (SQL injection, etc.)

**Run with:**
```bash
make test-unit
pytest tests/unit/ -v
```

### 3. Integration Tests (`integration/`)

Test interactions between components.

**What they test:**
- Replication flow end-to-end
- Schema evolution handling
- Error recovery mechanisms
- Monitoring and observability

**Run with:**
```bash
make test-integration
pytest tests/integration/ -v
```

### 4. End-to-End Tests (`e2e/`)

Test complete workflows from source to target.

**What they test:**
- Full CDC pipeline
- Reconciliation workflows
- Multi-table scenarios

**Run with:**
```bash
make test-e2e
pytest tests/e2e/ -v
```

### 5. Contract Tests (`contract/`)

Validate data format contracts between components.

**What they test:**
- Debezium message schema
- JDBC sink payload format
- API contracts

**Run with:**
```bash
make test-contract
pytest tests/contract/ -v
```

### 6. Load Tests (`load/`)

Validate performance and scalability under load.

**What they test:**
- Kafka Connect API throughput
- Reconciliation performance
- Connection pool behavior
- Scalability limits

**Run with:**
```bash
# Headless mode
make test-load

# Web UI mode
make load-test-ui
```

**Tools:** Locust

### 7. Performance Tests (`performance/`)

Benchmark and regression testing for performance.

**What they test:**
- Query performance
- Checksum calculation speed
- Memory usage
- Performance regression

**Run with:**
```bash
pytest tests/performance/ -v
```

### 8. Chaos Tests (`chaos/`)

Validate resilience under failure conditions.

**What they test:**
- Network failures
- Database failures
- Kafka failures
- Recovery mechanisms

**Run with:**
```bash
pytest tests/chaos/ -v
```

## Running Tests

### Quick Start

```bash
# Install dependencies
pip install -e ".[dev]"

# Run all tests
make test-all

# Run specific category
make test-unit
make test-integration
make test-property
make test-load
```

### Individual Test Files

```bash
# Run specific test file
pytest tests/unit/test_cli.py -v

# Run specific test function
pytest tests/unit/test_cli.py::test_specific_function -v

# Run with markers
pytest -m "integration" -v
pytest -m "slow" -v
```

### With Coverage

```bash
# Generate coverage report
pytest --cov=src --cov-report=html

# View in browser
open htmlcov/index.html
```

### Advanced Testing

#### Mutation Testing

Validate test suite effectiveness by introducing bugs:

```bash
# Run mutation tests
make mutation-test

# View results
make mutation-results

# Generate HTML report
make mutation-html
```

Target mutation score: **>80%**

#### Property-Based Testing Profiles

```bash
# Dev mode (fast, 20 examples)
pytest tests/property/ --hypothesis-profile=dev

# CI mode (balanced, 100 examples)
pytest tests/property/ --hypothesis-profile=ci

# Thorough mode (comprehensive, 500 examples)
pytest tests/property/ --hypothesis-profile=thorough
```

## Test Configuration

### pytest.ini Settings

```ini
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
markers = [
    "e2e: End-to-end tests requiring real database instances",
    "vault: Tests requiring HashiCorp Vault",
    "slow: Tests that take significant time to run",
    "integration: Integration tests",
    "contract: Contract tests",
    "performance: Performance tests",
]
```

### Markers

Use markers to selectively run tests:

```bash
# Run only integration tests
pytest -m integration

# Skip slow tests
pytest -m "not slow"

# Run e2e and contract tests
pytest -m "e2e or contract"
```

### Fixtures

Common fixtures in `conftest.py`:

- `project_root`: Project root directory
- `specs_dir`: Specifications directory
- `contracts_dir`: Contract specifications

## Performance Benchmarks

### Target Metrics

| Test Category | Target Time | Target Pass Rate |
|--------------|-------------|------------------|
| Unit Tests | <5 seconds | 100% |
| Property Tests | <30 seconds (dev) | 100% |
| Integration Tests | <2 minutes | >95% |
| Load Tests | 2-5 minutes | <1% failures |
| Mutation Tests | 10-30 minutes | >80% score |

### Current Metrics

Run `make test-all` to see current metrics.

## Continuous Integration

Tests are automatically run on:

- Pull requests
- Pushes to main branch
- Nightly builds

See `.github/workflows/` for CI configuration.

## Troubleshooting

### Tests Failing Locally

1. **Check dependencies:**
   ```bash
   pip install -e ".[dev]"
   ```

2. **Verify services running:**
   ```bash
   make verify-services
   ```

3. **Check database state:**
   ```bash
   make db-count
   ```

### Slow Tests

```bash
# Profile slow tests
pytest --durations=10

# Skip slow tests
pytest -m "not slow"
```

### Flaky Tests

```bash
# Run test multiple times
pytest --count=10 tests/path/to/test.py

# Run with different random seeds
pytest --hypothesis-seed=12345
```

## Writing New Tests

### Unit Test Template

```python
def test_function_name():
    """Test description."""
    # Arrange
    input_data = ...

    # Act
    result = function_under_test(input_data)

    # Assert
    assert result == expected
```

### Property Test Template

```python
from hypothesis import given, strategies as st

@given(value=st.integers(min_value=0, max_value=1000))
def test_property_name(value: int):
    """Property description."""
    result = function_under_test(value)

    # Assert property
    assert property_holds(result)
```

### Load Test Template

```python
from locust import HttpUser, task, between

class MyUser(HttpUser):
    wait_time = between(1, 3)

    @task
    def my_task(self):
        self.client.get("/endpoint")
```

## Contributing

When adding new tests:

1. Follow existing patterns and structure
2. Add appropriate markers
3. Include docstrings
4. Update this README if adding new category
5. Ensure tests pass locally before PR

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [Hypothesis documentation](https://hypothesis.readthedocs.io/)
- [Locust documentation](https://docs.locust.io/)
- [mutmut documentation](https://mutmut.readthedocs.io/)

## License

MIT
