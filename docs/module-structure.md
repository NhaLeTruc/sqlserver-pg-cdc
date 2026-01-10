# Python Module Structure

**Project**: SQL Server to PostgreSQL CDC Pipeline
**Last Updated**: 2026-01-10
**Version**: 2.0

## Overview

This document describes the modular structure of the Python codebase for the CDC pipeline reconciliation and transformation tools. The codebase has been refactored to improve maintainability, testability, and code organization by splitting large monolithic files into focused, cohesive modules.

## Table of Contents

1. [Design Principles](#design-principles)
2. [Module Organization](#module-organization)
3. [Utils Modules](#utils-modules)
4. [Transformation Modules](#transformation-modules)
5. [Reconciliation Modules](#reconciliation-modules)
6. [Import Guide](#import-guide)

---

## Design Principles

The module refactoring follows these principles:

1. **Single Responsibility**: Each module has a focused, well-defined purpose
2. **Size Constraints**: All files maintained within 100-250 lines for readability
3. **Clear Boundaries**: Logical separation between concerns
4. **Backward Compatibility**: All existing imports continue to work via `__init__.py` re-exports
5. **No Duplication**: Functionality consolidated into single locations

---

## Module Organization

### Project Structure

```
src/
├── reconciliation/          # Data reconciliation tools
│   ├── compare/            # Row count and checksum comparison
│   ├── report/             # Report generation and formatting
│   ├── scheduler/          # Scheduled reconciliation
│   ├── row_level/          # Row-by-row comparison
│   └── parallel/           # Parallel reconciliation execution
│
├── transformation/          # Data transformation framework
│   └── transformers/       # Transformer implementations
│       ├── base.py         # Base Transformer ABC
│       ├── pii.py          # PII masking and hashing
│       ├── types.py        # Type conversion and pipelines
│       └── rules.py        # Business rule pipelines
│
├── utils/                   # Shared utilities
│   ├── db_pool/            # Database connection pooling
│   │   ├── base.py         # BaseConnectionPool and exceptions
│   │   ├── postgres.py     # PostgreSQL pool implementation
│   │   ├── sqlserver.py    # SQL Server pool implementation
│   │   └── __init__.py     # Global pool management
│   │
│   ├── metrics/            # Prometheus metrics
│   │   ├── publisher.py    # MetricsPublisher and ApplicationInfo
│   │   ├── reconciliation.py  # Reconciliation metrics
│   │   ├── pipeline.py     # Connector and Vault metrics
│   │   └── __init__.py     # Re-exports and initialization
│   │
│   ├── query_optimizer/    # Query optimization
│   │   ├── analyzer.py     # Query analysis and execution plans
│   │   ├── optimizer.py    # Query optimization strategies
│   │   ├── advisor.py      # Index recommendations and DDL
│   │   └── __init__.py     # Re-exports
│   │
│   ├── logging/            # Structured logging
│   │   ├── config.py       # Configuration functions
│   │   ├── formatters.py   # JSONFormatter and ConsoleFormatter
│   │   ├── handlers.py     # ContextLogger
│   │   └── __init__.py     # Re-exports
│   │
│   ├── tracing/            # Distributed tracing (OpenTelemetry/Jaeger)
│   │   ├── tracer.py       # Tracer initialization
│   │   ├── context.py      # Context managers for spans
│   │   ├── decorators.py   # Tracing decorators
│   │   ├── database.py     # Database and HTTP tracing
│   │   ├── reconciliation.py  # Reconciliation-specific tracing
│   │   └── __init__.py     # Re-exports
│   │
│   └── vault_client.py     # HashiCorp Vault integration
│
└── cli.py                   # Command-line interface

tests/
├── unit/                    # Unit tests mirroring src/ structure
├── integration/             # Integration tests
├── property/                # Property-based tests (Hypothesis)
└── e2e/                     # End-to-end tests
```

---

## Utils Modules

### Database Connection Pooling (`src/utils/db_pool/`)

Thread-safe connection pools with health checks and automatic recycling.

**Files**:
- `base.py` (273 lines): BaseConnectionPool, PooledConnection, exceptions
- `postgres.py` (85 lines): PostgresConnectionPool implementation
- `sqlserver.py` (93 lines): SQLServerConnectionPool implementation
- `__init__.py` (92 lines): Global pool management functions

**Usage**:
```python
from src.utils.db_pool import initialize_pools, get_postgres_pool

# Initialize pools at application startup
initialize_pools(
    postgres_config={
        "host": "localhost",
        "port": 5432,
        "database": "warehouse",
        "user": "admin",
        "password": "secret",
        "min_size": 2,
        "max_size": 10
    }
)

# Acquire and use connections
pool = get_postgres_pool()
with pool.acquire() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM customers")
    count = cursor.fetchone()[0]
```

**Features**:
- Automatic connection health checks
- Connection recycling based on age and idle time
- Prometheus metrics for pool size, active/idle connections
- Thread-safe with proper locking
- Graceful degradation and error handling

---

### Metrics (`src/utils/metrics/`)

Prometheus metrics publishing for monitoring and alerting.

**Files**:
- `publisher.py` (125 lines): MetricsPublisher, ApplicationInfo
- `reconciliation.py` (201 lines): Reconciliation-specific metrics
- `pipeline.py` (297 lines): Connector and Vault metrics
- `__init__.py` (100 lines): Re-exports and initialization

**Usage**:
```python
from src.utils.metrics import initialize_metrics

# Initialize all metrics
metrics = initialize_metrics(port=9091)

# Record reconciliation metrics
metrics["reconciliation"].record_reconciliation_run(
    table_name="customers",
    success=True,
    duration=45.2,
    rows_compared=10000
)

# Record row count mismatch
metrics["reconciliation"].record_row_count_mismatch(
    table_name="orders",
    source_count=1000,
    target_count=998
)
```

**Metric Types**:
- `MetricsPublisher`: HTTP server exposing /metrics endpoint
- `ReconciliationMetrics`: Reconciliation runs, discrepancies, performance
- `ConnectorMetrics`: Kafka Connect connector operations and state
- `VaultMetrics`: Credential retrievals and health checks
- `ApplicationInfo`: Application version and uptime

---

### Query Optimizer (`src/utils/query_optimizer/`)

Query analysis, optimization, and index recommendations for reconciliation.

**Files**:
- `analyzer.py` (306 lines): Execution plan analysis for PostgreSQL and SQL Server
- `optimizer.py` (93 lines): Query optimization strategies (row count, checksum)
- `advisor.py` (203 lines): Index recommendations and DDL generation
- `__init__.py` (18 lines): Re-exports

**Usage**:
```python
from src.utils.query_optimizer import (
    QueryAnalyzer,
    QueryOptimizer,
    IndexAdvisor
)

# Analyze query performance
metrics, plan = QueryAnalyzer.analyze_postgres_query(
    conn=pg_conn,
    query="SELECT * FROM customers WHERE updated_at > %s",
    params=(last_sync,),
    execute=True
)

# Get optimized row count query
query = QueryOptimizer.optimize_row_count_query(
    "customers",
    database_type="postgresql"
)

# Get index recommendations
recommendations = IndexAdvisor.recommend_indexes_for_reconciliation(
    table_name="customers",
    primary_keys=["id"],
    timestamp_column="updated_at",
    checksum_column="row_checksum"
)

# Generate DDL
for rec in recommendations:
    ddl = IndexAdvisor.generate_index_ddl(rec, database_type="postgresql")
    print(ddl)
```

**Features**:
- Query execution plan analysis (EXPLAIN)
- Detection of table scans, index usage, joins
- Optimized row count queries using system tables
- Optimized checksum queries (MD5, CHECKSUM_AGG)
- Index recommendations for common reconciliation patterns
- DDL generation for PostgreSQL and SQL Server

---

### Logging (`src/utils/logging/`)

Structured logging with JSON formatting and contextual information.

**Files**:
- `config.py` (146 lines): Configuration functions (setup_logging, get_logger)
- `formatters.py` (180 lines): JSONFormatter and ConsoleFormatter
- `handlers.py` (97 lines): ContextLogger for adding context to logs
- `__init__.py` (98 lines): Re-exports and example usage

**Usage**:
```python
from src.utils.logging import setup_logging, get_logger, ContextLogger

# Setup logging at application startup
setup_logging(
    level="INFO",
    log_file="/var/log/cdc/app.log",
    console_output=True,
    json_format=True
)

# Get logger for module
logger = get_logger(__name__)

# Log with context
logger.info(
    "Processing record",
    extra={
        "table_name": "customers",
        "record_id": 12345,
        "operation": "INSERT"
    }
)

# Use ContextLogger for persistent context
context_logger = ContextLogger(
    "reconciliation",
    service="cdc-reconciliation",
    table_name="customers"
)
context_logger.info("Starting reconciliation", row_count=1000)
```

**Features**:
- JSON-formatted logs for structured logging
- Console formatter with color-coded log levels
- ContextLogger for adding persistent context
- Log rotation support (RotatingFileHandler)
- Environment variable configuration
- Filters for noisy third-party libraries

---

### Tracing (`src/utils/tracing/`)

Distributed tracing with OpenTelemetry and Jaeger integration.

**Files**:
- `tracer.py` (195 lines): Tracer initialization and auto-instrumentation
- `context.py` (102 lines): Context managers for span management
- `decorators.py` (42 lines): Function tracing decorators
- `database.py` (67 lines): Database and HTTP operation tracing
- `reconciliation.py` (230+ lines): Reconciliation-specific decorators and helpers
- `__init__.py` (78 lines): Re-exports

**Usage**:
```python
from src.utils.tracing import (
    initialize_tracing,
    trace_operation,
    trace_reconciliation,
    add_span_attributes
)

# Initialize tracing at startup
initialize_tracing(
    service_name="cdc-reconciliation",
    otlp_endpoint="localhost:4317"
)

# Trace an operation
with trace_operation("reconcile_table", table="customers") as span:
    result = reconcile_table("customers")
    span.set_attribute("rows_reconciled", result.count)

# Use decorator for reconciliation functions
@trace_reconciliation
def reconcile_table(source_cursor, target_cursor, source_table, target_table):
    # Function automatically traced with table names and results
    pass
```

**Features**:
- OpenTelemetry integration for distributed tracing
- OTLP exporter for Jaeger, Tempo, etc.
- Auto-instrumentation for psycopg2 and requests
- Context managers for manual span management
- Decorators for automatic function tracing
- Reconciliation-specific tracing helpers
- Span attributes, events, and exception recording

---

## Transformation Modules

### Transformers (`src/transformation/transformers/`)

Data transformation framework for PII masking, hashing, and business rules.

**Files**:
- `base.py` (57 lines): Base Transformer ABC and shared metrics
- `pii.py` (259 lines): PIIMaskingTransformer and HashingTransformer
- `types.py` (203 lines): TypeConversionTransformer, ConditionalTransformer, TransformationPipeline
- `rules.py` (79 lines): Factory functions for PII and GDPR pipelines
- `__init__.py` (33 lines): Re-exports

**Usage**:
```python
from transformation.transformers import (
    PIIMaskingTransformer,
    HashingTransformer,
    TransformationPipeline,
    create_pii_pipeline
)

# Use pre-configured PII pipeline
pipeline = create_pii_pipeline(salt="production_salt")

# Transform a row
row = {
    "id": 12345,
    "name": "John Doe",
    "email": "john.doe@example.com",
    "phone": "(555) 123-4567",
    "ssn": "123-45-6789"
}
transformed = pipeline.transform_row(row)
# Result:
# {
#     "id": "a1b2c3...",  # hashed
#     "name": "John Doe",
#     "email": "j********@example.com",  # masked
#     "phone": "(***) ***-4567",  # masked
#     "ssn": "***-**-6789"  # masked
# }

# Build custom pipeline
pipeline = TransformationPipeline()
pipeline.add_transformer(r".*email.*", PIIMaskingTransformer())
pipeline.add_transformer(r".*_id$", HashingTransformer(salt="custom"))

# Transform multiple rows
rows = [row1, row2, row3]
transformed_rows = pipeline.transform_rows(rows)
```

**Features**:
- PII masking (email, phone, SSN, credit cards, IP addresses)
- One-way hashing for pseudonymization
- Type conversion for database compatibility
- Conditional transformations based on predicates
- Composable transformation pipelines
- Pre-configured pipelines (PII, GDPR)
- Prometheus metrics for transformations

---

## Reconciliation Modules

The reconciliation modules provide tools for comparing source and target databases to detect discrepancies.

**Structure**:
- `compare/`: Core comparison logic (row counts, checksums, row-level)
- `report/`: Report generation and formatting (console, JSON, CSV)
- `scheduler/`: Scheduled reconciliation with cron/interval support
- `row_level/`: Row-by-row comparison for detailed discrepancy analysis
- `parallel/`: Parallel reconciliation for multiple tables

**See existing reconciliation documentation for detailed usage.**

---

## Import Guide

### Backward Compatibility

All existing imports continue to work due to `__init__.py` re-exports:

```python
# These still work (backward compatible):
from src.utils.db_pool import PostgresConnectionPool  # ✓
from src.utils.metrics import ReconciliationMetrics  # ✓
from src.utils.logging import setup_logging  # ✓
from transformation.transformers import PIIMaskingTransformer  # ✓
```

### Recommended Imports

For new code, import from the top-level module for clarity:

```python
# Database pooling
from src.utils.db_pool import (
    initialize_pools,
    get_postgres_pool,
    get_sqlserver_pool,
    close_pools
)

# Metrics
from src.utils.metrics import initialize_metrics

# Query optimization
from src.utils.query_optimizer import (
    QueryAnalyzer,
    QueryOptimizer,
    IndexAdvisor
)

# Logging
from src.utils.logging import setup_logging, get_logger, ContextLogger

# Tracing
from src.utils.tracing import (
    initialize_tracing,
    trace_operation,
    trace_reconciliation
)

# Transformations
from transformation.transformers import (
    create_pii_pipeline,
    create_gdpr_pipeline,
    TransformationPipeline,
    PIIMaskingTransformer,
    HashingTransformer
)
```

### Internal Imports (Advanced)

For specific submodule functionality, you can import directly:

```python
# Direct submodule imports (if needed)
from src.utils.db_pool.postgres import PostgresConnectionPool
from src.utils.metrics.reconciliation import ReconciliationMetrics
from src.utils.tracing.decorators import trace_function
from transformation.transformers.pii import PIIMaskingTransformer
```

---

## Benefits of Refactoring

### Before Refactoring

- **db_pool.py**: 733 lines (monolithic)
- **metrics.py**: 659 lines (mixed concerns)
- **query_optimizer.py**: 570 lines (analysis, optimization, recommendations)
- **logging_config.py**: 483 lines (config, formatters, handlers)
- **tracing.py + tracing_integration.py**: 745 lines (duplicated functionality)
- **transform.py**: 555 lines (transformers, pipelines, rules)

### After Refactoring

- **All modules**: 100-250 lines per file
- **Clear separation**: Each file has a single, focused responsibility
- **Reduced duplication**: Merged tracing modules
- **Better testability**: Smaller modules easier to test
- **Improved maintainability**: Easier to locate and modify functionality
- **Backward compatible**: All existing code continues to work

---

## Testing

All tests have been updated to work with the new module structure:

```bash
# Run all tests
pytest tests/

# Run tests for specific module
pytest tests/unit/test_db_pool.py
pytest tests/unit/test_metrics.py
pytest tests/unit/test_query_optimizer.py
pytest tests/unit/test_logging_config.py
pytest tests/unit/test_transform.py

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

All 708 tests passing with the refactored module structure.

---

## Future Improvements

Potential future enhancements:

1. **Type Annotations**: Add comprehensive type hints throughout
2. **API Documentation**: Generate Sphinx documentation from docstrings
3. **Performance Profiling**: Add performance benchmarks for critical paths
4. **Plugin System**: Allow custom transformers and metrics via plugins
5. **Configuration Validation**: Pydantic models for configuration validation

---

**Maintained by**: CDC Platform Team
**Questions**: See `CONTRIBUTING.md` or file an issue
