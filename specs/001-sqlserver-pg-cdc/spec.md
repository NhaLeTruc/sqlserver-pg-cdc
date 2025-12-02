# Feature Specification: SQL Server to PostgreSQL CDC Pipeline

**Feature Branch**: `001-sqlserver-pg-cdc`
**Created**: 2025-12-02
**Status**: Draft
**Input**: User description: "Create a change data capture pipeline from a SQL Server to a Postgres data-warehouse. The pipeline MUST has the following qualities: 1. Locally testable. Its docker compose environment MUST enables e2e, and integration tests locally. 2. Communities supported. It MUST utilize free open-sourced softwares, and minimum amount of custom code. 3. Observable. There MUST BE proper logs management systems and monitoring infrastructures. 4. Strictly Tested. Tests MUST be written first before implementation for all of its components. 5. Robust. There MUST be proper reconciliation mechanism, error handling, retry strategies, and stale events handling for the cdc pipeline. 6. Flexible. There MUST be proper handlings of schema evolutions, and dirty data. 7. Secured. There MUST be a proper safe-guards against SQL injection and other commom security vulnerabilities."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Real-Time Data Replication (Priority: P1)

As a data engineer, I need the pipeline to automatically capture and replicate all changes (inserts, updates, deletes) from SQL Server tables to PostgreSQL in near real-time, so that the data warehouse stays current for analytics and reporting.

**Why this priority**: This is the core value proposition. Without reliable data replication, the entire CDC pipeline fails its primary purpose. All other features depend on this working correctly.

**Independent Test**: Can be fully tested by inserting, updating, and deleting rows in SQL Server source tables, then verifying the same changes appear in PostgreSQL target tables within the acceptable latency window (e.g., <5 minutes). Delivers immediate value by enabling downstream analytics on synchronized data.

**Acceptance Scenarios**:

1. **Given** SQL Server has 3 tables enabled for CDC, **When** a new row is inserted into any table, **Then** the same row appears in the corresponding PostgreSQL table within 5 minutes
2. **Given** an existing row in SQL Server, **When** the row is updated, **Then** the updated values are reflected in PostgreSQL within 5 minutes
3. **Given** an existing row in SQL Server, **When** the row is deleted, **Then** the row is removed (or soft-deleted based on configuration) in PostgreSQL within 5 minutes
4. **Given** multiple changes occur in a single SQL Server transaction, **When** the transaction commits, **Then** all changes are applied atomically in PostgreSQL (all succeed or all fail)
5. **Given** the pipeline is processing changes, **When** 10,000 rows are bulk inserted into SQL Server, **Then** all 10,000 rows appear in PostgreSQL within 15 minutes

---

### User Story 2 - Pipeline Health Monitoring and Alerting (Priority: P2)

As a platform engineer, I need comprehensive visibility into pipeline health (throughput, lag, errors, resource usage), so that I can proactively identify and resolve issues before they impact data quality or availability.

**Why this priority**: Operational visibility is critical for production readiness. Without monitoring, silent failures can corrupt data or cause extended outages before detection. This is the second priority because basic replication must work first, but monitoring enables confident production deployment.

**Independent Test**: Can be tested by deploying the monitoring stack (metrics, logs, dashboards), generating various pipeline scenarios (normal operation, errors, high load), and verifying that metrics update correctly, logs are searchable, and alerts fire when thresholds are breached. Delivers value by reducing MTTR (Mean Time To Recovery) for pipeline incidents.

**Acceptance Scenarios**:

1. **Given** the pipeline is running, **When** I access the monitoring dashboard, **Then** I see current metrics for throughput (rows/sec), replication lag (seconds behind source), error count, and resource usage (CPU, memory)
2. **Given** the SQL Server source becomes unavailable, **When** 5 minutes pass, **Then** an alert is triggered notifying operators of source connection failure
3. **Given** the pipeline encounters data transformation errors, **When** error rate exceeds 5%, **Then** an alert is triggered with details about the affected tables and error types
4. **Given** replication lag increases, **When** lag exceeds 10 minutes, **Then** a warning alert is triggered to investigate potential performance issues
5. **Given** a pipeline component fails, **When** the failure occurs, **Then** structured logs are written with full context (timestamp, component, error message, stack trace) and are searchable via log aggregation system

---

### User Story 3 - Schema Evolution Handling (Priority: P3)

As a data engineer, I need the pipeline to detect and handle schema changes in SQL Server (new columns, dropped columns, type changes), so that downstream data warehouse operations continue without manual intervention when source schemas evolve.

**Why this priority**: Schema evolution is inevitable in production systems. Without automated handling, every schema change requires manual pipeline updates and potential downtime. This is third priority because the pipeline must first replicate data reliably (P1) and be observable (P2) before adding schema flexibility.

**Independent Test**: Can be tested by enabling CDC on a SQL Server table, replicating data, then altering the table schema (add column, drop column, change type), and verifying the pipeline either automatically applies changes to PostgreSQL or logs clear instructions for manual intervention. Delivers value by reducing operational toil and preventing schema-related downtime.

**Acceptance Scenarios**:

1. **Given** a table is actively replicating, **When** a new nullable column is added to the SQL Server table, **Then** the pipeline detects the schema change and either automatically adds the column to PostgreSQL or creates a migration task with clear instructions
2. **Given** a table is actively replicating, **When** a column is dropped from SQL Server, **Then** the pipeline detects the change and either archives the column in PostgreSQL (soft delete) or marks it for manual review before deletion
3. **Given** a table is actively replicating, **When** a column data type changes in SQL Server, **Then** the pipeline detects the incompatible change, pauses replication for that table, and alerts operators with a recommended migration path
4. **Given** the pipeline detects a schema mismatch, **When** the mismatch is unresolvable automatically, **Then** affected records are routed to a dead letter queue for manual review, and replication continues for other tables
5. **Given** schema evolution handling is enabled, **When** viewing the monitoring dashboard, **Then** all detected schema changes are logged with timestamps, affected tables, and handling status (auto-applied, pending review, failed)

---

### User Story 4 - Error Recovery and Data Reconciliation (Priority: P4)

As a data engineer, I need automated retry mechanisms for transient failures and reconciliation tools to verify source-target consistency, so that temporary issues don't cause permanent data loss and I can validate data integrity over time.

**Why this priority**: Production systems encounter transient failures (network blips, temporary resource exhaustion). Robust error handling prevents data loss and reduces operational burden. Reconciliation provides confidence in long-term data integrity. This is lower priority because it builds on top of working replication (P1-P3).

**Independent Test**: Can be tested by simulating various failure scenarios (network interruption, PostgreSQL connection loss, out-of-memory), verifying the pipeline retries with exponential backoff and eventually succeeds, then running reconciliation to confirm row counts and checksums match between source and target. Delivers value by improving pipeline reliability and data trustworthiness.

**Acceptance Scenarios**:

1. **Given** the pipeline is replicating data, **When** PostgreSQL becomes temporarily unavailable, **Then** the pipeline retries with exponential backoff (starting at 1 second, max 60 seconds) for up to 10 minutes before alerting
2. **Given** a transient network error occurs, **When** the pipeline retries, **Then** changes are applied idempotently (no duplicate rows or double-updates)
3. **Given** a record fails validation or transformation, **When** the error is non-transient (e.g., data type mismatch), **Then** the record is moved to a dead letter queue with full error context, and replication continues for other records
4. **Given** the pipeline has been running for 24 hours, **When** I execute the reconciliation tool, **Then** it compares row counts and checksums between SQL Server and PostgreSQL for all replicated tables and reports any discrepancies
5. **Given** reconciliation detects missing or inconsistent rows, **When** the report is generated, **Then** it includes table names, primary keys of affected rows, and suggested remediation actions (re-sync, investigate, ignore)
6. **Given** the pipeline detects a stale event (timestamp older than 24 hours), **When** processing the event, **Then** it logs a warning, applies the change with a staleness flag, and increments a staleness metric for monitoring

---

### User Story 5 - Local Development and Testing Environment (Priority: P5)

As a developer, I need a Docker Compose environment that spins up the entire CDC pipeline stack (SQL Server, PostgreSQL, pipeline components, monitoring), so that I can run end-to-end and integration tests locally without requiring cloud infrastructure.

**Why this priority**: Local testability accelerates development velocity and enables TDD (tests written before implementation). This is lower priority because it's an enabler for development workflow rather than direct user value, but it's essential for the constitution's TDD requirement.

**Independent Test**: Can be tested by running `docker-compose up`, waiting for all services to be healthy, executing the test suite (unit, integration, e2e), and verifying all tests pass. Delivers value by enabling rapid feedback loops and reducing CI/CD costs.

**Acceptance Scenarios**:

1. **Given** Docker and Docker Compose are installed, **When** I run `docker-compose up`, **Then** all services (SQL Server, PostgreSQL, CDC pipeline, monitoring stack) start successfully and health checks pass within 2 minutes
2. **Given** the Docker Compose environment is running, **When** I execute integration tests, **Then** tests can create SQL Server tables, enable CDC, generate test data, and verify replication to PostgreSQL
3. **Given** the local environment is running, **When** I execute end-to-end tests, **Then** tests can simulate complete user scenarios (replication, schema changes, failures, reconciliation) and verify expected outcomes
4. **Given** the Docker Compose environment is running, **When** I access the monitoring dashboard at localhost, **Then** I see live metrics and logs from the local pipeline
5. **Given** I want to test a specific failure scenario, **When** I use Docker Compose to stop a service (e.g., `docker-compose stop postgres`), **Then** the pipeline handles the failure gracefully and resumes when the service restarts

---

### Edge Cases

- What happens when the pipeline encounters a very large transaction (e.g., 1 million row bulk insert)? Does it batch the changes appropriately to avoid memory exhaustion?
- How does the system handle SQL Server CDC cleanup? If CDC retention is too short, changes might be missed before replication.
- What happens when PostgreSQL target table has additional constraints (unique indexes, foreign keys) that don't exist in SQL Server? Do failed inserts go to dead letter queue?
- How does the pipeline handle clock skew between SQL Server and PostgreSQL servers?
- What happens if the same table is modified in both SQL Server and PostgreSQL (bidirectional conflict)? Does the pipeline detect and warn about this unsupported scenario?
- How does the system handle special data types (XML, JSON, binary, spatial data) during replication?
- What happens when disk space runs out on the PostgreSQL server? Does the pipeline pause gracefully and alert?
- How are edge cases around character encodings (UTF-8, Latin1, etc.) handled during replication?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST capture all insert, update, and delete operations from SQL Server tables enabled for Change Data Capture
- **FR-002**: System MUST replicate captured changes to corresponding PostgreSQL tables with eventual consistency (target lag <5 minutes under normal load)
- **FR-003**: System MUST preserve transactional boundaries from SQL Server when replicating to PostgreSQL (changes within a single SQL Server transaction are applied atomically in PostgreSQL)
- **FR-004**: System MUST handle schema evolution by detecting column additions, deletions, and type changes in SQL Server tables
- **FR-005**: System MUST implement idempotent operations to allow safe retries without creating duplicate records or double-applying updates
- **FR-006**: System MUST implement exponential backoff retry logic for transient failures (network, connection, temporary resource exhaustion) with configurable max retries and backoff parameters
- **FR-007**: System MUST route records that fail validation or transformation to a dead letter queue with full error context (original record, error message, timestamp, stack trace)
- **FR-008**: System MUST detect and flag stale events (events with timestamps older than a configurable threshold, default 24 hours) and log warnings
- **FR-009**: System MUST emit structured logs in JSON format with context (timestamp, component, transaction ID, table name, primary key, operation type)
- **FR-010**: System MUST expose metrics for monitoring: throughput (rows/sec), replication lag (seconds), error rate (errors/min), resource usage (CPU %, memory %)
- **FR-011**: System MUST expose health check endpoints that report component status (healthy, degraded, unhealthy)
- **FR-012**: System MUST provide a reconciliation tool that compares row counts and checksums between SQL Server source tables and PostgreSQL target tables
- **FR-013**: System MUST support configuration of which SQL Server tables to replicate via a configuration file or environment variables
- **FR-014**: System MUST validate data at system boundaries (SQL Server extraction and PostgreSQL loading) to detect data corruption early
- **FR-015**: System MUST use parameterized queries or prepared statements for all database operations to prevent SQL injection vulnerabilities
- **FR-016**: System MUST store database credentials securely (environment variables, secrets management) and never log credentials in plain text
- **FR-017**: System MUST provide a Docker Compose environment that includes SQL Server, PostgreSQL, CDC pipeline components, and monitoring stack for local development and testing
- **FR-018**: System MUST support local execution of end-to-end tests that verify complete replication scenarios without requiring external infrastructure
- **FR-019**: System MUST implement checkpointing to track replication progress and enable resumability after pipeline restarts
- **FR-020**: System MUST handle NULL values correctly during replication (preserving NULL vs. empty string distinctions)
- **FR-021**: System MUST support configuration of replication lag alert thresholds and error rate alert thresholds
- **FR-022**: System MUST gracefully handle SQL Server or PostgreSQL downtime by pausing replication and resuming when connectivity is restored

### Non-Functional Requirements

- **NFR-001**: Pipeline MUST achieve throughput of at least 10,000 rows per second under normal load for typical OLTP workload patterns
- **NFR-002**: Pipeline MUST maintain replication lag below 5 minutes under normal conditions (95th percentile)
- **NFR-003**: System MUST run on standard Docker-compatible infrastructure with maximum 4GB memory and 2 CPU cores per pipeline instance
- **NFR-004**: Pipeline MUST support horizontal scaling by partitioning tables across multiple pipeline instances
- **NFR-005**: All pipeline components MUST use open-source software with permissive licenses (Apache 2.0, MIT, BSD)
- **NFR-006**: Custom code MUST be minimized; prefer configuration and integration of existing tools over custom development
- **NFR-007**: System MUST provide clear error messages with actionable remediation steps for common failure scenarios
- **NFR-008**: All code MUST have automated tests written before implementation (TDD) with minimum 80% code coverage
- **NFR-009**: Monitoring dashboards MUST be accessible via web browser with no authentication required in local development environment
- **NFR-010**: System MUST support running in air-gapped environments (all dependencies available as Docker images)

### Key Entities

- **Source Table**: A SQL Server table enabled for Change Data Capture, containing business data to be replicated. Key attributes: schema name, table name, primary key columns, CDC enable timestamp.

- **Target Table**: A PostgreSQL table receiving replicated data from a SQL Server source table. Key attributes: schema name, table name, primary key columns, last replicated timestamp, row count.

- **Change Event**: Represents a single insert, update, or delete operation captured from SQL Server CDC. Key attributes: event timestamp, operation type (INSERT/UPDATE/DELETE), table name, primary key values, before/after column values, transaction ID.

- **Dead Letter Record**: A change event that failed processing and was routed to a quarantine area for manual review. Key attributes: original change event, error message, error timestamp, retry count, resolution status (pending/resolved/ignored).

- **Replication Checkpoint**: Tracks the last successfully processed change event for each source table, enabling resumability after restarts. Key attributes: table name, last processed event timestamp, last processed LSN (Log Sequence Number), checkpoint timestamp.

- **Schema Mapping**: Defines how a SQL Server table schema maps to a PostgreSQL table schema, including data type conversions. Key attributes: source table, target table, column mappings, type conversions, transformation rules.

- **Reconciliation Report**: Output of comparing source and target data for consistency. Key attributes: table name, source row count, target row count, row count delta, checksum match status, discrepancies (list of mismatched primary keys), generation timestamp.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Pipeline successfully replicates 100% of insert, update, and delete operations from SQL Server to PostgreSQL with zero data loss under normal operating conditions
- **SC-002**: Replication lag remains below 5 minutes for 95% of operations during business hours (8am-6pm) under typical load (up to 1,000 transactions/minute)
- **SC-003**: Pipeline handles transient failures (network interruptions, temporary database unavailability) by automatically retrying and recovering without operator intervention in 90% of cases
- **SC-004**: Reconciliation tool detects data inconsistencies (missing rows, checksum mismatches) with 100% accuracy when run on demand
- **SC-005**: Monitoring dashboard provides visibility into pipeline health within 60 seconds of any state change (throughput changes, errors, lag increases)
- **SC-006**: Operators can diagnose root cause of pipeline failures within 10 minutes using structured logs and monitoring metrics
- **SC-007**: Developer can spin up complete local testing environment (Docker Compose) and run end-to-end tests in under 5 minutes
- **SC-008**: Pipeline handles schema evolution events (column additions/removals) by detecting changes within 5 minutes and either auto-applying or alerting for manual intervention
- **SC-009**: Pipeline processes 1 million row bulk inserts within 30 minutes without running out of memory or crashing
- **SC-010**: Zero SQL injection vulnerabilities detected during security testing using standard tools (SQLMap, manual code review)
- **SC-011**: All pipeline components achieve 80% or higher automated test coverage as measured by code coverage tools
- **SC-012**: Pipeline resource utilization remains below 70% CPU and 80% memory under peak load conditions

## Assumptions

1. **SQL Server CDC is already enabled**: We assume the SQL Server instance has Change Data Capture enabled at the database level, and target tables have CDC enabled. The pipeline does not need to enable CDC itself.

2. **Network connectivity**: We assume stable network connectivity between the pipeline and both SQL Server and PostgreSQL with typical latency (<50ms). The pipeline handles transient failures but is not designed for high-latency or unstable networks.

3. **Schema compatibility**: We assume SQL Server and PostgreSQL schemas are initially compatible (matching column names and compatible data types). The pipeline will not perform complex schema transformations beyond standard type mappings.

4. **Primary keys exist**: We assume all replicated tables have primary keys defined in both SQL Server and PostgreSQL. The pipeline requires primary keys to correlate records and handle updates/deletes correctly.

5. **Single-directional replication**: We assume data flows only from SQL Server to PostgreSQL. Bidirectional replication or conflict resolution for concurrent modifications is out of scope.

6. **PostgreSQL target is dedicated**: We assume the PostgreSQL data warehouse is primarily for analytics and not subject to high-frequency writes from other sources that might conflict with CDC replication.

7. **Monitoring stack is pre-configured**: We assume standard monitoring tools (e.g., Prometheus, Grafana, ELK stack) are available and configured. The pipeline will emit metrics and logs in standard formats.

8. **Docker environment for local testing**: We assume developers have Docker and Docker Compose installed for local testing. Production deployment strategy is out of scope for this specification.

9. **Standard data types**: We assume most data types are standard (integers, strings, dates, decimals). Handling of exotic types (XML, spatial, custom UDTs) will be documented as limitations if not supported.

10. **Reasonable transaction sizes**: We assume typical transactions contain hundreds to thousands of rows, not tens of millions. Extremely large transactions may require special handling or batching.

## Constraints

1. **Open-source software only**: All components must use free, open-source software with permissive licenses. No proprietary or commercial tools allowed.

2. **Minimal custom code**: Prefer configuration and orchestration of existing CDC tools (e.g., Debezium, Kafka Connect, pglogical) over writing custom CDC logic from scratch.

3. **Docker-based deployment**: The entire stack must be deployable via Docker Compose for local development and testing.

4. **TDD mandatory**: All code must follow test-driven development. Tests written and approved before implementation. This is non-negotiable per project constitution.

5. **No cloud-specific services**: The pipeline must be cloud-agnostic and runnable on-premises or in any cloud environment. Avoid dependencies on AWS RDS, Azure SQL, GCP-specific features.

6. **Security-first**: All database operations must use parameterized queries. Credentials must be externalized to environment variables or secrets management. No hardcoded credentials.

7. **Resource limits**: The pipeline should operate within modest resource constraints (4GB RAM, 2 CPUs per instance) to keep infrastructure costs low.

8. **Observability built-in**: Logging, metrics, and monitoring are mandatory components from day one, not added later.

## Out of Scope

The following are explicitly **not** included in this feature:

1. **Bidirectional replication**: Changes made directly in PostgreSQL will not be replicated back to SQL Server
2. **Historical data backfill**: Initial bulk load of existing SQL Server data into PostgreSQL before CDC starts (may be addressed in a separate feature)
3. **Complex data transformations**: The pipeline performs type mapping and basic transformations, but complex business logic (aggregations, joins, denormalization) should be handled by downstream ETL processes
4. **Production deployment automation**: CI/CD pipelines, Kubernetes manifests, Terraform scripts for production deployment are out of scope
5. **Multi-tenancy**: Supporting multiple isolated SQL Server → PostgreSQL replication pipelines in a single deployment
6. **Performance tuning for exotic workloads**: The pipeline targets typical OLTP workloads; extreme edge cases (10M row transactions, 1000 tables) may require separate optimization
7. **Authentication and authorization**: The local Docker environment has no authentication. Production security hardening is out of scope for this spec.
8. **Data masking or PII redaction**: Sensitive data handling and compliance features (GDPR, HIPAA) are not included
9. **Custom alerting integrations**: The pipeline emits standard metrics and logs, but integration with specific alerting platforms (PagerDuty, Slack) is out of scope
10. **Zero-downtime upgrades**: Upgrading pipeline components may require brief downtime; rolling updates are not required
