# SQL-server to Postgres CDC Pipeline

## Quickstart

```bash
make quickstart

docker ps --format "table {{.Names}}\t{{.Status}}"

# Simple insert and select test
make test

make stop

make clean
```


## Tests

```bash
# Run unit, contract, integration, e2e
make test-lite

.venv/bin/pytest tests/ -v

.venv/bin/pytest tests/ -v --cov=src --cov-report=html --cov-report=term

.venv/bin/pytest tests/contract/ -v

.venv/bin/pytest tests/unit/ -v

.venv/bin/pytest tests/integration/ -v

.venv/bin/pytest tests/e2e/ -v

.venv/bin/pytest tests/performance/ -v

# Integration tests only
.venv/bin/pytest -m integration -v

# Contract tests only
.venv/bin/pytest -m contract -v

# Performance tests only
.venv/bin/pytest -m performance -v

# Slow tests only
.venv/bin/pytest -m slow -v

# Generate JUnit XML (for CI/CD)
.venv/bin/pytest tests/ --junitxml=test-results.xml
```

## Lintings

```bash
make lint
```
