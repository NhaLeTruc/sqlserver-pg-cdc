# SQL-server to Postgres CDC Pipeline

## Quickstart

```bash
make quickstart

make stop

make clean

# Simple insert and select test
make test
```


## Tests

```bash
pytest tests/ -v

pytest tests/ -v --cov=src --cov-report=html --cov-report=term

pytest tests/contract/ -v

pytest tests/integration/ -v

pytest tests/unit/ -v

pytest tests/e2e/ -v

# Integration tests only
pytest -m integration -v

# Contract tests only
pytest -m contract -v

# Performance tests only
pytest -m performance -v

# Slow tests only
pytest -m slow -v

# Generate JUnit XML (for CI/CD)
pytest tests/ --junitxml=test-results.xml
```

## Lintings

```bash
make lint
```
