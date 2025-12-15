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

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Make

## Quickstart

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

Once the environment is set up, the CDC pipeline will automatically:

1. Enable CDC on configured SQL Server tables
2. Start capturing changes via Debezium
3. Stream changes through Kafka
4. Apply changes to PostgreSQL with schema sync

## Tests

```bash
# Run unit, contract, integration, e2e
make test-lite

.venv/bin/pytest tests/ -v

.venv/bin/pytest tests/ -v --cov=src --cov-report=html --cov-report=term

.venv/bin/pytest tests/contract/ -v

.venv/bin/pytest tests/unit/ -v

.venv/bin/pytest tests/integration/ -v

.venv/bin/pytest tests/integration/test_replication_flow.py -v --no-cov

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

## Linting

```bash
make lint
```

## Project Structure

```bash
sqlserver-pg-cdc/
├── src/                           # Source code for CDC pipeline components
│   ├── reconciliation/            # Data reconciliation subsystem
│   │   ├── compare.py            # Row count and checksum comparison logic
│   │   ├── report.py             # Report generation (human and machine-readable)
│   │   └── scheduler.py          # Cron-like scheduling for automated reconciliation
│   └── utils/                     # Shared utilities
│       ├── vault_client.py       # HashiCorp Vault integration for secrets management
│       ├── logging_config.py     # Structured JSON logging with correlation IDs
│       └── metrics.py            # Prometheus metrics publisher for monitoring
├── tests/                         # Comprehensive test suite
│   ├── unit/                     # Unit tests for individual components
│   ├── contract/                 # Contract tests for interface validation
│   ├── integration/              # Integration tests for component interactions
│   ├── e2e/                      # End-to-end pipeline validation tests
│   ├── performance/              # Performance benchmarking tests
│   └── chaos/                    # Chaos engineering and recovery tests
├── docker-compose.yml            # Docker services configuration
├── Makefile                      # Build and task automation
└── README.md                     # This file
```

### Key Components

**Reconciliation Module** (`src/reconciliation/`)

- `compare.py`: Compares row counts and checksums between SQL Server source and PostgreSQL target to verify data integrity
- `report.py`: Generates reconciliation reports with status summaries, discrepancy details, and actionable recommendations
- `scheduler.py`: Provides APScheduler-based automation for periodic reconciliation jobs with interval and cron triggers

**Utilities Module** (`src/utils/`)

- `vault_client.py`: Secure credential management using HashiCorp Vault KV v2 secrets engine
- `logging_config.py`: Structured JSON logging with contextual information for observability and monitoring integration
- `metrics.py`: Prometheus metrics exporter for tracking pipeline health, reconciliation runs, and data quality metrics

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

**Python Scripts** (`scripts/python/`)

- `reconcile.py`: CLI tool for on-demand and scheduled data reconciliation between SQL Server and PostgreSQL
  - Supports row count comparison, checksum validation, and discrepancy reporting
  - Vault integration for secure credential retrieval
  - Scheduled mode for periodic reconciliation jobs with configurable intervals
  - Multiple output formats (JSON, CSV, console)
  - Comprehensive logging and metrics integration

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

- **Unit Tests**: Test individual components in isolation, validating core logic in reconciliation, vault client, logging, and metrics modules
- **Contract Tests**: Verify interfaces between components, ensuring API contracts remain stable across changes
- **Integration Tests**: Test component interactions including database connectivity, Kafka messaging, and end-to-end replication flows
- **E2E Tests**: Full pipeline validation from SQL Server CDC capture through Kafka streaming to PostgreSQL sink
- **Chaos Tests**: Validate system resilience and recovery behavior under failure conditions such as:
  - Network partitions and connectivity failures
  - Database service interruptions
  - Kafka broker failures
  - Container crashes and restarts
  - Resource exhaustion scenarios
- **Performance Tests**: Benchmark throughput and latency under various load conditions

> **Note**: _Performance and Chaos tests are still under active development and require further work to establish comprehensive baseline metrics and load scenarios_

## Contributing

When contributing to this project:

1. Ensure all tests pass with `make test-lite`
2. Run linting with `make lint`
3. Add appropriate tests for new features
4. Follow the existing code style and conventions

## License

[Add your license information here]

## Support

For issues, questions, or contributions, please refer to the project's issue tracker.
