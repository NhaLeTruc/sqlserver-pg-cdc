# Test Environment Reset

This document describes the test environment reset feature that ensures tests run with a clean, predictable state.

## Overview

The test environment reset feature provides automated cleanup of:
- **Database tables** (truncates all tables in SQL Server and PostgreSQL)
- **Kafka topics** (deletes and recreates CDC topics)
- **Connector offsets** (resets CDC connectors to start from clean state)

This ensures that performance and e2e tests are not affected by stale data from previous test runs.

## Usage

### Manual Reset

Reset the test environment manually using Make targets:

```bash
# Full reset (truncate tables, clear Kafka, redeploy connectors)
make reset-test-env

# Quick reset (skip connector restart for faster execution)
make reset-test-env-quick
```

Or directly using the bash script:

```bash
# Full reset
./scripts/bash/reset-test-environment.sh

# Quick reset
./scripts/bash/reset-test-environment.sh --quick
```

Or using Python:

```bash
# Full reset
python scripts/python/reset_test_environment.py

# Quick reset
python scripts/python/reset_test_environment.py --quick
```

### Automatic Reset (Pytest)

The reset feature is automatically triggered before performance and e2e tests via pytest fixtures.

#### Performance Tests

```bash
# Runs with automatic environment reset
pytest tests/performance/

# Skip reset (faster but may have stale data)
SKIP_RESET=1 pytest tests/performance/

# Quick reset (skip connector restart)
QUICK_RESET=1 pytest tests/performance/
```

#### E2E Tests

```bash
# Runs with automatic environment reset
pytest tests/e2e/

# Skip reset (not recommended)
SKIP_RESET=1 pytest tests/e2e/
```

## What Gets Reset?

### 1. SQL Server Tables (Truncated)

All tables in the `dbo` schema are truncated:
- `customers`
- `orders`
- `line_items`
- Any other user tables

Foreign key constraints are temporarily disabled during truncation.

### 2. PostgreSQL Tables (Truncated)

All tables in the `public` schema are truncated:
- `customers`
- `orders`
- `line_items`
- `test_customers`
- Any other user tables

Tables are truncated with `CASCADE` to handle foreign key relationships.

### 3. Kafka Topics (Deleted)

All CDC-related Kafka topics are deleted:
- Topics matching pattern: `sqlserver.*` or `warehouse_source.*`
- These topics are automatically recreated by Debezium when connectors restart

### 4. Connector Offsets (Reset)

In full mode (not quick mode):
- All Kafka connectors are paused
- Connectors are deleted (which resets their offsets)
- Connectors are redeployed via `deploy-with-vault.sh`

This ensures CDC starts capturing from a clean state.

## Reset Modes

### Full Reset (Default)

Includes all cleanup steps above. Recommended for most cases.

```bash
make reset-test-env
```

**Duration:** ~20-30 seconds

### Quick Reset

Skips connector restart. Faster but doesn't reset connector offsets.

```bash
make reset-test-env-quick
```

**Duration:** ~5-10 seconds

**Use when:**
- Running tests multiple times in quick succession
- Connector offsets don't need to be reset
- Speed is more important than absolute cleanliness

## Environment Variables

Control reset behavior with environment variables:

```bash
# Skip reset entirely (not recommended for performance/e2e tests)
SKIP_RESET=1 pytest tests/performance/

# Use quick mode (skip connector restart)
QUICK_RESET=1 pytest tests/performance/
```

## Integration with Tests

### Performance Tests

Performance tests automatically trigger environment reset via the `clean_test_environment` fixture:

```python
# tests/performance/conftest.py
@pytest.fixture(scope="module", autouse=True)
def performance_test_setup(clean_test_environment):
    """Automatically reset environment before performance tests."""
    yield
```

### E2E Tests

E2E tests also automatically trigger environment reset:

```python
# tests/e2e/conftest.py
@pytest.fixture(scope="module", autouse=True)
def e2e_test_setup(clean_test_environment):
    """Automatically reset environment before e2e tests."""
    yield
```

## Implementation Details

### Bash Script

Location: `scripts/bash/reset-test-environment.sh`

Key features:
- Service health checks before reset
- Foreign key constraint management
- Graceful error handling
- Colored output for better visibility

### Python Helper

Location: `scripts/python/reset_test_environment.py`

Provides programmatic access to reset functionality:

```python
from scripts.python.reset_test_environment import reset_environment

# Full reset
reset_environment(quick=False, verbose=True)

# Quick reset
reset_environment(quick=True)
```

### Pytest Fixture

Location: `tests/conftest.py`

The `clean_test_environment` fixture provides session-scoped reset for tests.

## Troubleshooting

### Reset Script Fails

If the reset script fails:

1. Check that all services are running:
   ```bash
   make status
   ```

2. Verify services are healthy:
   ```bash
   docker ps
   ```

3. Try full Docker environment reset:
   ```bash
   FULL_RESET=1 pytest tests/performance/
   ```

### Connector Deployment Fails

If connectors fail to redeploy after reset:

1. Manually redeploy connectors:
   ```bash
   make deploy
   ```

2. Check connector status:
   ```bash
   make connector-status
   ```

### Stale Data Persists

If tests still show stale data after reset:

1. Use full reset instead of quick reset
2. Verify tables were actually truncated:
   ```bash
   make db-count
   ```

3. Check Kafka topics were deleted:
   ```bash
   make kafka-topics
   ```

## Best Practices

1. **Always use reset for performance tests** - Ensures consistent, accurate measurements
2. **Use full reset for critical tests** - Don't skip connector restart for important test runs
3. **Use quick reset during development** - Faster iterations when developing tests
4. **Don't disable reset in CI/CD** - Always run with clean state in automated environments

## Future Enhancements

Potential improvements:
- [ ] Selective table reset (reset only specific tables)
- [ ] Schema registry reset (clear registered schemas)
- [ ] Metrics/monitoring reset (clear Prometheus data)
- [ ] Parallel reset operations (speed improvements)