# SQL Server to PostgreSQL CDC Pipeline

A real-time Change Data Capture (CDC) solution for replicating data from Microsoft SQL Server to PostgreSQL databases. This project provides a robust, production-ready pipeline that captures changes from SQL Server using Debezium and streams them to PostgreSQL with full schema synchronization and data transformation support.

## Overview

This CDC pipeline bridges the gap between SQL Server and PostgreSQL ecosystems, enabling:

- **Real-time Data Replication**: Capture and replicate INSERT, UPDATE, and DELETE operations as they occur
- **Schema Synchronization**: Automatically sync table schemas from SQL Server to PostgreSQL with appropriate type mappings
- **Change Data Capture**: Leverage Debezium's powerful CDC capabilities for SQL Server
- **Kafka-based Streaming**: Use Apache Kafka as a reliable message broker for change events
- **Transaction Consistency**: Maintain data integrity across the replication pipeline
- **Monitoring & Observability**: Built-in health checks and monitoring capabilities

## Architecture

The pipeline consists of the following components:

- **SQL Server Source**: Microsoft SQL Server database with CDC enabled
- **Debezium Connector**: Captures changes from SQL Server transaction logs
- **Apache Kafka**: Message broker for streaming change events
- **PostgreSQL Sink**: Target PostgreSQL database for replicated data
- **Python Management Tools**: Custom scripts for reconciliation, monitoring, and pipeline management

## Features

- Automatic schema creation and migration in PostgreSQL
- Type mapping between SQL Server and PostgreSQL data types
- Support for complex data types and schemas
- Configurable replication filters and transformations
- Comprehensive test coverage (unit, integration, contract, E2E, performance)
- Docker-based development environment
- Production-ready error handling and retry mechanisms
- **SQL injection protection** with database-native identifier quoting
- **Automated retry logic** with exponential backoff for transient failures
- **Chunked processing** for large tables to prevent memory exhaustion
- **Automated backup and restore** with S3 support
- **Disaster recovery procedures** with documented RTO/RPO
- **Comprehensive monitoring** with Prometheus metrics and Grafana dashboards
- **Operational runbooks** for troubleshooting and daily operations

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Make

## Quickstart

### Basic Setup (Default Configuration)

```bash
# Initialize the environment
make quickstart

# Verify all services are running
docker ps --format "table {{.Names}}\t{{.Status}}"

# Simple insert and select test
make test

make stop

make clean
```

### Customized Setup (Recommended)

For production use or to customize data flow parameters (batch sizes, parallelism, polling intervals):

```bash
# 1. Copy and customize configuration
cp .env.example .env
vi .env  # Edit parameters as needed (see Configuration section below)

# 2. Start services
make start

# 3. Initialize databases and Vault
make init

# 4. Generate connector configs and deploy (auto-generates from .env)
make deploy

# 5. Verify connectors are running
make verify-connectors

# 6. Test replication
make test
```

### Configuration

The pipeline supports parameterized configuration for data flow tuning. Key configurable parameters include:

**Debezium Source Connector:**

- `DEBEZIUM_MAX_BATCH_SIZE` - Batch size for change capture (default: 2048)
- `DEBEZIUM_MAX_QUEUE_SIZE` - Internal queue size (default: 8192)
- `DEBEZIUM_POLL_INTERVAL_MS` - Transaction log polling interval (default: 500ms)
- `DEBEZIUM_TASKS_MAX` - **Must be 1** for SQL Server CDC (see [configuration docs](docs/configs-connectors.md#why-tasksmax1-is-mandatory))

**PostgreSQL Sink Connector:**

- `SINK_BATCH_SIZE` - Batch size for writes (default: 3000)
- `SINK_TASKS_MAX` - Number of parallel tasks (default: 3)
- `SINK_CONNECTION_POOL_SIZE` - JDBC connection pool size (default: 10)

See [**Connector Configuration Guide**](docs/configs-connectors.md) for:

- Complete parameter reference
- Performance tuning matrices for different scenarios (low latency, high throughput, bursty workloads)
- Advanced usage and troubleshooting

### What Happens During Setup

Once the environment is set up, the CDC pipeline will automatically:

1. Enable CDC on configured SQL Server tables
2. Start capturing changes via Debezium
3. Stream changes through Kafka
4. Apply changes to PostgreSQL with schema sync

## Tests

```bash
# Run unit, contract, property, integration, e2e, performance tests
make test-lite

# Run all tests
.venv/bin/pytest tests/ -v

# Run with coverage
.venv/bin/pytest tests/ -v --cov=src --cov-report=html --cov-report=term

# Run specific test categories
.venv/bin/pytest tests/unit/ -v           # Unit tests
.venv/bin/pytest tests/contract/ -v       # Contract tests
.venv/bin/pytest tests/property/ -v       # Property-based tests
.venv/bin/pytest tests/integration/ -v    # Integration tests
.venv/bin/pytest tests/e2e/ -v            # End-to-end tests
.venv/bin/pytest tests/performance/ -v    # Performance benchmarks
.venv/bin/pytest tests/latency/ -v        # Latency measurements
.venv/bin/pytest tests/load/ -v           # Load/stress tests
```

## Linting

```bash
make lint
```

## Project Structure

```bash
sqlserver-pg-cdc/
├── src/                           # Source code for CDC pipeline components
│   ├── reconciliation/            # Data reconciliation subsystem
│   │   ├── cli/                  # CLI commands and argument parsing
│   │   ├── compare/              # Row count and checksum comparison
│   │   ├── report/               # Report generation modules
│   │   ├── scheduler/            # Scheduled reconciliation jobs
│   │   ├── row_level/            # Row-by-row comparison and repair
│   │   ├── parallel/             # Parallel execution engine
│   │   └── incremental/          # Incremental reconciliation with state
│   ├── transformation/            # Data transformation framework
│   │   └── transformers/         # PII masking, hashing, type conversion
│   └── utils/                     # Shared utilities
│       ├── db_pool/              # Database connection pooling
│       ├── metrics/              # Prometheus metrics publisher
│       ├── query_optimizer/      # Query analysis and recommendations
│       ├── logging/              # Structured logging framework
│       ├── tracing/              # Distributed tracing (OpenTelemetry)
│       ├── vault_client.py       # HashiCorp Vault integration
│       ├── retry.py              # Retry logic with exponential backoff
│       └── sql_safety.py         # SQL injection protection utilities
├── tests/                         # Comprehensive test suite
│   ├── unit/                     # Unit tests for individual components
│   ├── contract/                 # Contract tests for interface validation
│   ├── integration/              # Integration tests for component interactions
│   ├── e2e/                      # End-to-end pipeline validation tests
│   ├── performance/              # Performance benchmarking tests
│   ├── property/                 # Property-based tests (Hypothesis)
│   ├── latency/                  # Latency measurement tests
│   ├── load/                     # Load and stress tests (Locust)
│   └── conftest.py               # Pytest configuration and fixtures
├── scripts/                       # Operational and utility scripts
│   ├── bash/                     # Shell scripts for pipeline operations
│   ├── python/                   # Python management scripts
│   └── sql/                      # Database initialization scripts
├── docker/                        # Docker configuration
│   ├── docker-compose.yml        # Main services configuration
│   ├── docker-compose.test.yml   # Test environment configuration
│   ├── docker-compose.logging.yml # Logging stack (Loki/Promtail)
│   └── configs/                  # Service configurations
│       ├── debezium/             # Debezium connector configs
│       ├── kafka-connect/        # JDBC sink connector configs
│       ├── prometheus/           # Prometheus and alert rules
│       ├── grafana/              # Grafana dashboards
│       ├── vault/                # Vault policies and configuration
│       └── templates/            # Config templates for generation
├── configs/                       # Additional service configurations
│   ├── grafana/                  # Grafana dashboards and provisioning
│   ├── loki/                     # Loki log aggregation config
│   └── promtail/                 # Promtail log collection config
├── kafka-connect-transforms/      # Custom Kafka Connect SMTs (Maven)
│   ├── pom.xml                   # Maven build configuration
│   └── src/                      # Java source for AddDeletedField SMT
├── docs/                          # Documentation
│   ├── development-architecture.md # System architecture overview
│   ├── development-notes.md      # Development guidelines
│   ├── configs-connectors.md     # Connector configuration reference
│   ├── configs-vault.md          # Vault setup and usage
│   ├── guides-monitoring.md      # Monitoring and alerting guide
│   ├── guides-schema-evolution.md # Schema evolution handling
│   ├── guides-testing.md         # Testing strategy guide
│   ├── troubleshooting-part01.md # Troubleshooting guide (part 1)
│   ├── troubleshooting-part02.md # Troubleshooting guide (part 2)
│   └── runbooks/                 # Operational runbooks
│       ├── disaster-recovery.md  # DR procedures and drills
│       ├── operations.md         # Daily operations guide
│       ├── quickstart.md         # Getting started guide
│       ├── quickstart-vault.md   # Vault quickstart
│       └── schema-evolutions.md  # Schema change procedures
├── .github/workflows/             # CI/CD configuration
│   ├── ci.yml                    # Main CI pipeline
│   ├── docker-build.yml          # Docker image builds
│   └── security-scan.yml         # Security scanning
├── Makefile                       # Build and task automation
└── README.md                      # This file
```

### Key Components

**Reconciliation Module** (`src/reconciliation/`)

- `cli/`: Command-line interface with argument parsing and credential handling
- `compare/`: Row count comparison and checksum validation between source and target databases
- `report/`: Report generation with multiple output formats (JSON, CSV, console)
- `scheduler/`: APScheduler-based automation for periodic reconciliation jobs
- `row_level/`: Row-by-row comparison with automatic repair capabilities
- `parallel/`: Parallel execution engine for high-performance reconciliation
- `incremental/`: Incremental reconciliation with state persistence for efficient updates

**Transformation Module** (`src/transformation/`)

- `transformers/`: Data transformation pipeline with PII masking, hashing, and type conversions
  - `pii.py`: PII detection and masking transformers
  - `types.py`: Type conversion transformers for SQL Server to PostgreSQL mappings
  - `rules.py`: Configurable transformation rules engine

**Utilities Module** (`src/utils/`)

- `db_pool/`: Thread-safe database connection pooling with health checks and automatic recycling for PostgreSQL and SQL Server
- `metrics/`: Prometheus metrics publisher for reconciliation, connector operations, and Vault health monitoring
- `query_optimizer/`: Query analysis, optimization, and index recommendations for reconciliation operations
- `logging/`: Structured JSON logging with console and file formatters, contextual logging support
- `tracing/`: Distributed tracing with OpenTelemetry and Jaeger integration for end-to-end visibility
- `vault_client.py`: Secure credential management using HashiCorp Vault KV v2 secrets engine
- `retry.py`: Configurable retry logic with exponential backoff for transient failures
- `sql_safety.py`: SQL injection protection with database-native identifier quoting

### Operational Scripts

**Bash Scripts** (`scripts/bash/`)

The project includes comprehensive operational scripts for managing the CDC pipeline infrastructure:

- `init-sqlserver.sh`: Initialize SQL Server with sample tables and enable CDC on configured tables for change capture
- `init-postgres.sh`: Initialize PostgreSQL target database with tables matching Debezium CDC schema, including BIGINT timestamps and `__deleted` column for soft delete tracking
- `vault-init.sh`: Initialize HashiCorp Vault with KV v2 secrets engine and store database credentials securely
- `vault-helpers.sh`: Reusable Vault helper functions for fetching secrets, validating credentials, and exporting to environment variables
- `create-topics.sh`: Pre-create Kafka topics with custom configuration (partitions, replication factor, retention, compression)
- `verify-topics.sh`: Verify Kafka topics are created correctly and display topic configuration details
- `generate-connector-configs.sh`: Generate Kafka Connect connector configurations with Vault secret placeholders for secure credential injection
- `deploy-connector.sh`: Deploy Kafka Connect connectors (Debezium source, JDBC sink) via REST API with support for create, update, delete, and status operations
- `deploy-with-vault.sh`: Deploy connectors with credentials fetched from Vault and substituted into connector configurations at runtime
- `monitor.sh`: Monitor CDC pipeline status, connector health, and metrics via Kafka Connect REST API, Prometheus, and Grafana
- `pause-resume.sh`: Pause or resume connectors for maintenance windows, schema changes, or troubleshooting without losing connector state
- `scale-connector.sh`: Adjust connector task parallelism (scale up/down) to optimize throughput and handle varying workloads
- `run-integration-tests.sh`: Run integration tests with proper environment setup, service health checks, and cleanup
- `backup-databases.sh`: Automated backup script with compression, retention policy, and S3 upload support
- `restore-databases.sh`: Automated restore script with point-in-time recovery and validation
- `collect-diagnostics.sh`: Collect diagnostic information (logs, metrics, status) for troubleshooting
- `reset-test-environment.sh`: Reset test environment to clean state for reproducible testing
- `setup-pre-commit.sh`: Configure pre-commit hooks for code quality enforcement

**Python Scripts** (`scripts/python/`)

- `reconcile.py`: CLI tool for on-demand and scheduled data reconciliation between SQL Server and PostgreSQL
  - Supports row count comparison, checksum validation, and discrepancy reporting
  - Vault integration for secure credential retrieval
  - Scheduled mode for periodic reconciliation jobs with configurable intervals
  - Multiple output formats (JSON, CSV, console)
  - Comprehensive logging and metrics integration
- `analyze_query_performance.py`: Query performance analysis and optimization recommendations
- `diagnose_performance_test.py`: Performance test diagnostics and bottleneck identification
- `reset_test_environment.py`: Reset test databases to clean state

**SQL Scripts** (`scripts/sql/`)

- `create_reconciliation_indexes.sql`: Create optimized indexes for reconciliation queries

### Custom Kafka Connect Transforms

**kafka-connect-transforms/** This directory contains custom Single Message Transforms (SMTs) for Kafka Connect that extend Debezium's CDC capabilities:

**`AddDeletedField.java`** - Custom SMT for soft delete support

- Inspects CDC records from Debezium and adds a `__deleted` field to track record deletion state
- For DELETE operations: sets `__deleted = "true"`
- For INSERT/UPDATE operations: sets `__deleted = "false"`
- Enables soft delete pattern in PostgreSQL instead of hard deletes, preserving historical data
- Configurable field name and values
- Integrates seamlessly with Debezium's `ExtractNewRecordState` transform

This transform is essential for maintaining audit trails and supporting temporal queries on replicated data. It works by examining the operation type (`op` field) from Debezium's CDC events and adding a boolean-like field that PostgreSQL can use to filter active vs. deleted records.

**Why this matters:** Without this transform, DELETE operations would remove rows from PostgreSQL. With it, deleted rows remain queryable with `WHERE __deleted = 'false'`, enabling point-in-time queries and historical analysis.

## Testing Strategy

The project includes multiple test layers to ensure reliability and robustness:

- **Unit Tests** (`tests/unit/`): Test individual components in isolation, validating core logic in reconciliation, vault client, logging, and metrics modules
- **Contract Tests** (`tests/contract/`): Verify interfaces between components, ensuring API contracts remain stable across changes
- **Property Tests** (`tests/property/`): Property-based testing with Hypothesis for edge cases, SQL injection safety, and data invariants
- **Integration Tests** (`tests/integration/`): Test component interactions including database connectivity, Kafka messaging, and end-to-end replication flows
- **E2E Tests** (`tests/e2e/`): Full pipeline validation from SQL Server CDC capture through Kafka streaming to PostgreSQL sink
- **Performance Tests** (`tests/performance/`): Benchmark throughput and latency under various load conditions
- **Latency Tests** (`tests/latency/`): Measure and validate end-to-end latency requirements
- **Load Tests** (`tests/load/`): Stress testing and scalability validation using Locust

See [tests/README.md](tests/README.md) for detailed testing documentation and execution instructions.

> **Note**: _Performance and Load tests are under active development to establish comprehensive baseline metrics and load scenarios_

## CI/CD

The project includes GitHub Actions workflows for continuous integration and deployment:

- **ci.yml**: Main CI pipeline
  - Runs on pull requests and pushes to main
  - Executes unit, property, contract, and integration tests
  - Performs code linting and type checking
  - Generates code coverage reports

- **docker-build.yml**: Docker image builds
  - Builds and pushes container images
  - Tags images based on branch and version

- **security-scan.yml**: Security scanning
  - Dependency vulnerability scanning
  - Container image security scanning
  - Code security analysis

## Contributing

When contributing to this project:

1. Ensure all tests pass with `make test-lite`
2. Run linting with `make lint`
3. Add appropriate tests for new features
4. Follow the existing code style and conventions
5. Use pre-commit hooks: `./scripts/bash/setup-pre-commit.sh`

## License

MIT License - See [pyproject.toml](pyproject.toml) for details.

## Operations & Runbooks

Comprehensive operational documentation is available in the `docs/runbooks/` directory:

- [Disaster Recovery Runbook](docs/runbooks/disaster-recovery.md) - Complete data loss scenarios, restore procedures, and quarterly DR drills
- [Operations Runbook](docs/runbooks/operations.md) - Daily operations, maintenance windows, and change management
- [Quickstart Guide](docs/runbooks/quickstart.md) - Getting started with the CDC pipeline
- [Vault Quickstart](docs/runbooks/quickstart-vault.md) - HashiCorp Vault setup and integration
- [Schema Evolution Guide](docs/runbooks/schema-evolutions.md) - Handling schema changes in production

Additional documentation in `docs/`:

- [Connector Configuration](docs/configs-connectors.md) - Complete connector parameter reference
- [Vault Configuration](docs/configs-vault.md) - Vault setup and secret management
- [Monitoring Guide](docs/guides-monitoring.md) - Prometheus metrics and Grafana dashboards
- [Testing Guide](docs/guides-testing.md) - Test strategy and execution
- [Troubleshooting Part 1](docs/troubleshooting-part01.md) - Common issues and solutions
- [Troubleshooting Part 2](docs/troubleshooting-part02.md) - Advanced debugging

### Quick Operations Commands

```bash
# Daily backup (with S3 upload)
./scripts/bash/backup-databases.sh --s3-bucket cdc-backups

# Restore from backup
./scripts/bash/restore-databases.sh --timestamp 20251220_020000

# Collect diagnostics for troubleshooting
./scripts/bash/collect-diagnostics.sh

# Run reconciliation check
python -m src.cli.reconcile \
    --source-type sqlserver \
    --source-host sqlserver \
    --source-port 1433 \
    --source-database warehouse_source \
    --source-user sa \
    --source-password "${SQLSERVER_PASSWORD}" \
    --target-type postgresql \
    --target-host postgres \
    --target-port 5432 \
    --target-database warehouse_target \
    --target-user postgres \
    --target-password "${POSTGRES_PASSWORD}" \
    --tables customers,products,orders,inventory,shipments \
    --validate-checksums
```

## Support

For issues, questions, or contributions, please refer to the project's issue tracker.
