# sqlserver-pg-cdc Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-01-10

## Active Technologies

- Python 3.11+ for custom tooling (reconciliation, transformation, utilities)
- Bash for operational scripts
- Docker Compose for local development
- Kafka, Debezium, PostgreSQL, SQL Server for CDC pipeline

## Project Structure

```text
src/
├── reconciliation/       # Data reconciliation tools
│   ├── compare/         # Comparison logic
│   ├── report/          # Report generation
│   ├── scheduler/       # Scheduled reconciliation
│   ├── row_level/       # Row-by-row comparison
│   └── parallel/        # Parallel execution
├── transformation/       # Data transformation framework
│   └── transformers/    # PII masking, hashing, conversions
└── utils/               # Shared utilities
    ├── db_pool/         # Database connection pooling
    ├── metrics/         # Prometheus metrics
    ├── query_optimizer/ # Query analysis and optimization
    ├── logging/         # Structured logging
    ├── tracing/         # Distributed tracing (OpenTelemetry)
    └── vault_client.py  # HashiCorp Vault integration

tests/
├── unit/                # Unit tests
├── integration/         # Integration tests
├── contract/            # Contract tests
├── e2e/                 # End-to-end tests
├── property/            # Property-based tests
├── performance/         # Performance tests
└── chaos/               # Chaos engineering tests
```

## Commands

```bash
# Testing
pytest                                  # Run all tests
pytest tests/unit/                      # Run unit tests only
pytest --cov=src --cov-report=html      # Run with coverage

# Linting and formatting
ruff check .                            # Check code quality
ruff format .                           # Format code

# Type checking
mypy src/                               # Type check source code

# CDC Pipeline
make quickstart                         # Start pipeline with defaults
make start                              # Start all services
make init                               # Initialize databases and Vault
make deploy                             # Deploy connectors
make test                               # Test replication
```

## Code Style

- **Python 3.11+**: Follow PEP 8, use type hints, keep modules 100-250 lines
- **Modular Design**: Each file has a single responsibility
- **Backward Compatibility**: All refactorings maintain backward compatibility via `__init__.py` re-exports
- **Testing**: Comprehensive test coverage (unit, integration, property-based)
- **Documentation**: All modules have docstrings, see `docs/module-structure.md`

## Module Import Guidelines

Import from top-level modules for clarity and backward compatibility:

```python
# Database pooling
from src.utils.db_pool import initialize_pools, get_postgres_pool

# Metrics
from src.utils.metrics import initialize_metrics

# Logging
from src.utils.logging import setup_logging, get_logger

# Tracing
from src.utils.tracing import initialize_tracing, trace_operation

# Transformations
from transformation.transformers import create_pii_pipeline
```

See `docs/module-structure.md` for detailed module documentation and usage examples.

## Recent Changes

- **2026-01-10**: Major refactoring - split monolithic files into modular subpackages
  - `db_pool.py` → `db_pool/` module (4 files)
  - `metrics.py` → `metrics/` module (4 files)
  - `query_optimizer.py` → `query_optimizer/` module (3 files)
  - `logging_config.py` → `logging/` module (3 files)
  - `tracing.py` + `tracing_integration.py` → `tracing/` module (5 files)
  - `transform.py` → `transformers/` module (4 files)
- **2025-12-02**: Added Python 3.11 for custom tooling (reconciliation, management scripts)

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
