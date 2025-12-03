# End-to-End Tests

These tests verify the complete reconciliation tool workflow with real database instances.

## Prerequisites

The e2e tests require:
- SQL Server (localhost:1433)
- PostgreSQL (localhost:5432)
- HashiCorp Vault (localhost:8200) - for vault-specific tests

## Running E2E Tests

### 1. Start Infrastructure

```bash
cd docker
docker-compose up -d
```

### 2. Run Tests

```bash
# Run all e2e tests (they will skip if infrastructure is not available)
pytest tests/e2e/ -v --no-cov

# Run specific test
pytest tests/e2e/test_reconciliation.py::TestReconciliationE2E::test_reconcile_tool_basic_execution -v --no-cov
```

## Test Markers

- `@pytest.mark.e2e` - Requires database infrastructure
- `@pytest.mark.vault` - Requires HashiCorp Vault
- `@pytest.mark.slow` - Long-running tests (>1 minute)

## Default Behavior

By default, all e2e tests are **skipped** if the required infrastructure is not available. This allows the main test suite to run without failures in CI/CD environments where databases may not be present.

To enable e2e tests in CI, ensure the infrastructure is available and pass the `--no-cov` flag to pytest.
