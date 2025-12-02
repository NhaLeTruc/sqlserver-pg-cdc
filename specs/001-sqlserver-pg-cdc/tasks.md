# Tasks: SQL Server to PostgreSQL CDC Pipeline

**Input**: Design documents from `/specs/001-sqlserver-pg-cdc/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are MANDATORY per NFR-008 (TDD with 80% coverage). All custom code must follow Red-Green-Refactor cycle.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Infrastructure**: `docker/`, `docker/configs/`
- **Scripts**: `scripts/bash/`, `scripts/python/`
- **Source**: `src/reconciliation/`, `src/utils/`
- **Tests**: `tests/contract/`, `tests/integration/`, `tests/e2e/`
- **Docs**: `docs/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [X] T001 Create project directory structure per implementation plan (docker/, scripts/, src/, tests/, docs/)
- [X] T002 Initialize Python project with pyproject.toml and requirements.txt for Python 3.11
- [X] T003 [P] Create .gitignore for Python, Docker, and IDE files
- [X] T004 [P] Create docker/.env.example template with environment variable documentation

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

### Docker Compose Infrastructure

- [X] T005 Create docker/docker-compose.yml with SQL Server 2019 service definition
- [X] T006 Add PostgreSQL 15 service to docker/docker-compose.yml
- [X] T007 Add Zookeeper service to docker/docker-compose.yml (required for Kafka)
- [X] T008 Add Kafka 3.6+ broker service to docker/docker-compose.yml
- [X] T009 Add Confluent Schema Registry 7.5+ service to docker/docker-compose.yml
- [X] T010 Add Kafka Connect 3.6+ workers service to docker/docker-compose.yml with Debezium and JDBC connectors
- [X] T011 Add HashiCorp Vault 1.15+ service to docker/docker-compose.yml (dev mode for local)
- [X] T012 Add Prometheus 2.48+ service to docker/docker-compose.yml
- [X] T013 Add Grafana 10.2+ service to docker/docker-compose.yml
- [X] T014 Add Jaeger 1.51+ service to docker/docker-compose.yml for distributed tracing
- [X] T015 Create docker/docker-compose.test.yml for Testcontainers-based testing environment

### Connector Configurations

- [X] T016 Create docker/configs/debezium/sqlserver-source.json from contract specification
- [X] T017 Create docker/configs/kafka-connect/postgresql-sink.json from contract specification
- [X] T018 [P] Create Kafka topic creation scripts based on contracts/kafka-topics.yaml

### Monitoring Configurations

- [X] T019 [P] Create docker/configs/prometheus/prometheus.yml with scrape configs for Kafka Connect JMX
- [X] T020 [P] Create docker/configs/prometheus/alert-rules.yml with replication lag, error rate, and downtime alerts
- [X] T021 [P] Create docker/configs/grafana/dashboards/cdc-pipeline.json for pipeline overview
- [X] T022 [P] Create docker/configs/grafana/dashboards/kafka-connect.json for connector metrics
- [X] T023 [P] Create docker/configs/jaeger/jaeger-config.yml for trace collection

### Vault Configuration

- [X] T024 [P] Create docker/configs/vault/config.hcl with KV v2 secrets engine and audit logging
- [X] T025 [P] Create docker/configs/vault/policies/kafka-connect-policy.hcl for read-only DB secret access

### Foundational Scripts

- [X] T026 Create scripts/bash/vault-init.sh to initialize Vault with database credentials
- [ ] T027 Verify full Docker Compose stack starts successfully (all services healthy within 2 minutes)

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Real-Time Data Replication (Priority: P1) 🎯 MVP

**Goal**: Capture and replicate INSERT, UPDATE, DELETE operations from SQL Server to PostgreSQL with <5 minute lag

**Independent Test**: Insert/update/delete rows in SQL Server, verify they appear in PostgreSQL within 5 minutes

### Tests for User Story 1 (TDD - MUST write and verify failing tests first) ⚠️

- [X] T028 [P] [US1] Contract test for Debezium source connector config validation in tests/contract/test_debezium_schema.py
- [X] T029 [P] [US1] Contract test for JDBC sink connector config validation in tests/contract/test_jdbc_sink_schema.py
- [X] T030 [P] [US1] Integration test for INSERT replication in tests/integration/test_replication_flow.py
- [X] T031 [P] [US1] Integration test for UPDATE replication in tests/integration/test_replication_flow.py
- [X] T032 [P] [US1] Integration test for DELETE replication in tests/integration/test_replication_flow.py
- [X] T033 [P] [US1] Integration test for transactional consistency (multi-row transaction) in tests/integration/test_replication_flow.py
- [X] T034 [P] [US1] Integration test for bulk insert (10K rows) in tests/integration/test_replication_flow.py
- [X] T035 [P] [US1] Integration test for NULL value handling (preserving NULL vs empty string) in tests/integration/test_replication_flow.py
- [X] T036 [P] [US1] Performance benchmark test to validate 10K rows/sec throughput in tests/integration/test_performance.py

**Verify all tests FAIL before proceeding to implementation**

### Implementation for User Story 1

- [X] T037 [US1] Create scripts/bash/deploy-connector.sh to deploy connectors via Kafka Connect REST API
- [X] T038 [US1] Deploy Debezium SQL Server source connector using deploy-connector.sh and validate connector status is RUNNING
- [X] T039 [US1] Create sample SQL Server tables (customers, orders, line_items) with CDC enabled for testing
- [X] T040 [US1] Create corresponding PostgreSQL target tables matching SQL Server schema
- [X] T041 [US1] Deploy JDBC PostgreSQL sink connector using deploy-connector.sh and validate status
- [X] T042 [US1] Verify Kafka topics are created automatically with correct partitioning (3 partitions per table)
- [X] T043 [US1] Execute integration tests and verify all replication tests pass
- [X] T044 [US1] Document connector deployment process in docs/operations.md

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Pipeline Health Monitoring and Alerting (Priority: P2)

**Goal**: Provide comprehensive visibility into pipeline health with metrics, logs, dashboards, and alerts

**Independent Test**: Generate pipeline scenarios (normal, errors, high load), verify metrics update, logs searchable, alerts fire

### Tests for User Story 2 (TDD) ⚠️

- [X] T045 [P] [US2] Integration test for Prometheus metrics collection in tests/integration/test_monitoring.py
- [X] T046 [P] [US2] Integration test for Grafana dashboard accessibility in tests/integration/test_monitoring.py
- [X] T047 [P] [US2] Integration test for alert firing when lag exceeds threshold in tests/integration/test_monitoring.py
- [X] T048 [P] [US2] Integration test for alert firing when error rate exceeds threshold in tests/integration/test_monitoring.py
- [X] T049 [P] [US2] Integration test for Jaeger trace collection in tests/integration/test_monitoring.py
- [X] T050 [P] [US2] Integration test for resource usage validation (4GB memory, 2 CPU limits) in tests/integration/test_monitoring.py

**Verify all tests FAIL before proceeding**

### Implementation for User Story 2

- [X] T051 [P] [US2] Configure Kafka Connect JMX exporter for Prometheus scraping in docker/docker-compose.yml
- [X] T052 [P] [US2] Configure SQL Server exporter for Prometheus (CDC table metrics) in docker/docker-compose.yml
- [X] T053 [P] [US2] Configure PostgreSQL exporter for Prometheus in docker/docker-compose.yml
- [X] T054 [US2] Verify Prometheus scrapes metrics from all exporters and Kafka Connect
- [X] T055 [US2] Import Grafana dashboards (cdc-pipeline.json, kafka-connect.json) and verify data displays
- [X] T056 [US2] Configure Prometheus Alertmanager with webhook for alert notifications
- [X] T057 [US2] Test alert rules by simulating high lag and error scenarios
- [X] T058 [US2] Configure Jaeger agent to capture Kafka Connect traces
- [X] T059 [US2] Create scripts/bash/monitor.sh to query connector status and metrics via REST API
- [X] T060 [US2] Execute monitoring integration tests and verify all pass
- [X] T061 [US2] Document monitoring setup and troubleshooting in docs/operations.md

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - Schema Evolution Handling (Priority: P3)

**Goal**: Detect and handle schema changes (add column, drop column, type change) automatically or with clear alerts

**Independent Test**: Alter SQL Server table schema, verify pipeline detects change and handles appropriately

### Tests for User Story 3 (TDD) ⚠️

- [X] T062 [P] [US3] Integration test for ADD COLUMN detection in tests/integration/test_schema_evolution.py
- [X] T063 [P] [US3] Integration test for DROP COLUMN detection in tests/integration/test_schema_evolution.py
- [X] T064 [P] [US3] Integration test for ALTER COLUMN type change detection in tests/integration/test_schema_evolution.py
- [X] T065 [P] [US3] Integration test for schema mismatch routing to DLQ in tests/integration/test_schema_evolution.py

**Verify all tests FAIL before proceeding**

### Implementation for User Story 3

- [X] T066 [US3] Configure Debezium connector to emit schema change events with include.schema.changes=true
- [X] T067 [US3] Configure JDBC sink connector with auto.evolve=true for automatic column additions
- [X] T068 [US3] Configure errors.deadletterqueue.topic.name for routing incompatible schema changes
- [X] T069 [US3] Create Kafka topic for dead letter queue (dlq-postgresql-sink) with 30-day retention
- [X] T070 [US3] Add schema evolution monitoring to Grafana dashboard showing detected changes
- [X] T071 [US3] Add Prometheus alert rule for schema change detection
- [X] T072 [US3] Execute schema evolution integration tests and verify all pass
- [X] T073 [US3] Document schema evolution handling procedures in docs/operations.md

**Checkpoint**: All three user stories (1, 2, 3) should now be independently functional

---

## Phase 6: User Story 4 - Error Recovery and Data Reconciliation (Priority: P4)

**Goal**: Implement retry logic with exponential backoff, dead letter queue, and reconciliation tool for data validation

**Independent Test**: Simulate failures (network, DB down), verify retries and recovery. Run reconciliation and verify accuracy.

### Tests for User Story 4 (TDD) ⚠️

- [X] T074 [P] [US4] Unit test for row count comparison in tests/unit/test_reconcile.py
- [X] T075 [P] [US4] Unit test for checksum validation in tests/unit/test_reconcile.py
- [X] T076 [P] [US4] Unit test for discrepancy reporting in tests/unit/test_reconcile.py
- [X] T077 [P] [US4] Integration test for PostgreSQL downtime recovery in tests/integration/test_error_recovery.py
- [X] T078 [P] [US4] Integration test for network failure retry logic in tests/integration/test_error_recovery.py
- [X] T079 [P] [US4] Integration test for DLQ routing of validation errors in tests/integration/test_error_recovery.py
- [X] T080 [P] [US4] E2E test for reconciliation tool execution in tests/e2e/test_reconciliation.py

**Verify all tests FAIL before proceeding**

### Implementation for User Story 4

#### Retry Logic Configuration

- [X] T081 [US4] Configure JDBC sink connector with errors.tolerance=all and errors.deadletterqueue
- [X] T082 [US4] Configure connection retry parameters (connection.attempts=10, connection.backoff.ms=5000)
- [X] T083 [US4] Configure task retry parameters with exponential backoff in Kafka Connect worker config
- [ ] T084 [US4] Test retry behavior by stopping PostgreSQL and verifying automatic recovery

#### Reconciliation Tool Development

- [X] T085 [P] [US4] Create src/reconciliation/__init__.py module structure
- [X] T086 [P] [US4] Create src/reconciliation/compare.py for row count and checksum comparison logic
- [X] T087 [P] [US4] Create src/reconciliation/report.py for generating JSON reconciliation reports
- [X] T088 [P] [US4] Create src/reconciliation/scheduler.py for cron-like scheduling using APScheduler
- [X] T089 [US4] Create src/utils/vault_client.py for fetching database credentials from Vault
- [X] T090 [US4] Create scripts/python/reconcile.py CLI tool with argparse for on-demand execution
- [X] T091 [US4] Create scripts/python/setup.py for Python package installation
- [X] T092 [US4] Execute all reconciliation unit tests and verify they pass
- [ ] T093 [US4] Execute error recovery integration tests and verify retries work correctly
- [ ] T094 [US4] Execute reconciliation E2E test and verify report accuracy
- [X] T095 [US4] Document reconciliation usage (on-demand and scheduled) in docs/operations.md

**Checkpoint**: All four user stories (1, 2, 3, 4) should now be independently functional

---

## Phase 7: User Story 5 - Local Development and Testing Environment (Priority: P5)

**Goal**: Complete Docker Compose environment for local testing with health checks and test execution

**Independent Test**: Run `docker-compose up`, execute test suite, verify all services healthy and tests pass

### Tests for User Story 5 (TDD) ⚠️

- [ ] T096 [P] [US5] E2E test for Docker Compose stack startup in tests/e2e/test_docker_environment.py
- [ ] T097 [P] [US5] E2E test for service health checks in tests/e2e/test_docker_environment.py
- [ ] T098 [P] [US5] E2E test for test suite execution in local environment in tests/e2e/test_docker_environment.py
- [ ] T099 [P] [US5] E2E test for failure scenario simulation in tests/e2e/test_docker_environment.py

**Verify all tests FAIL before proceeding**

### Implementation for User Story 5

- [ ] T100 [US5] Add health check configurations to all services in docker/docker-compose.yml
- [ ] T101 [US5] Create docker/wait-for-services.sh script to wait for all services to be healthy
- [ ] T102 [US5] Configure pytest with Testcontainers integration in tests/conftest.py
- [ ] T103 [US5] Create test fixtures for SQL Server connection in tests/conftest.py
- [ ] T104 [US5] Create test fixtures for PostgreSQL connection in tests/conftest.py
- [ ] T105 [US5] Create test fixtures for Kafka Connect API client in tests/conftest.py
- [ ] T106 [US5] Document local test execution workflow in docs/quickstart.md
- [ ] T107 [US5] Execute all E2E Docker environment tests and verify they pass
- [ ] T108 [US5] Verify complete test suite runs successfully in local Docker environment

**Checkpoint**: All five user stories should now be independently functional and locally testable

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T109 [P] Create scripts/bash/scale-connector.sh for adjusting connector task parallelism
- [ ] T110 [P] Create scripts/bash/pause-resume.sh for pausing/resuming connectors during maintenance
- [ ] T111 [P] Create docs/architecture.md with system architecture diagrams (Mermaid format)
- [ ] T112 [P] Create docs/troubleshooting.md with common issues and resolutions
- [ ] T113 [P] Add src/utils/metrics.py for custom metrics publishing to Prometheus
- [ ] T114 Add comprehensive logging configuration for all Python scripts (JSON format, structured)
- [ ] T115 Add security hardening to docker/docker-compose.yml (non-root users, read-only file systems where applicable)
- [ ] T116 Run full test suite and verify 80% code coverage threshold is met (per NFR-008)
- [ ] T117 Validate all connector configurations against JSON schemas in contracts/
- [ ] T118 Perform end-to-end validation following quickstart.md instructions

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-7)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3 → P4 → P5)
- **Polish (Phase 8)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - Requires US1 connectors to monitor
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) - Requires US1 replication to evolve
- **User Story 4 (P4)**: Can start after Foundational (Phase 2) - Requires US1 for reconciliation targets
- **User Story 5 (P5)**: Can start after Foundational (Phase 2) - Provides environment for all stories

**Recommended Order**: US1 → US5 (enables testing) → US2 (enables monitoring) → US4 (robustness) → US3 (flexibility)

### Within Each User Story

- Tests (TDD) MUST be written and FAIL before implementation
- Configuration files before deployment scripts
- Infrastructure components before operational tooling
- Core functionality before edge case handling
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational configuration tasks marked [P] can run in parallel (after Docker Compose base is created)
- Once Foundational phase completes, all user stories can start in parallel (if team capacity allows)
- All test tasks within a user story marked [P] can be written in parallel
- Configuration files within a story marked [P] can be created in parallel
- Different user stories can be worked on in parallel by different team members

---

## Parallel Example: User Story 1

```bash
# Phase 3: Write all tests in parallel (TDD - tests first)
Task T028: Contract test for Debezium source connector
Task T029: Contract test for JDBC sink connector
Task T030: Integration test for INSERT replication
Task T031: Integration test for UPDATE replication
Task T032: Integration test for DELETE replication
Task T033: Integration test for transactional consistency
Task T034: Integration test for bulk insert

# After tests verified failing, implementation can proceed sequentially
Task T035: Create deploy-connector.sh script
Task T036: Deploy Debezium source connector
Task T037: Create SQL Server test tables
Task T038: Create PostgreSQL target tables
Task T039: Deploy JDBC sink connector
Task T040: Verify Kafka topics created
Task T041: Execute integration tests (verify pass)
Task T042: Document deployment process
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1 (Real-Time Data Replication)
4. **STOP and VALIDATE**: Test User Story 1 independently
5. Deploy MVP to staging environment

**MVP Deliverable**: Basic CDC replication from SQL Server to PostgreSQL with <5 minute lag

### Incremental Delivery (Recommended)

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Deploy MVP
3. Add User Story 5 (in parallel with US1) → Enable local testing → Run all tests
4. Add User Story 2 → Test independently → Deploy with monitoring
5. Add User Story 4 → Test independently → Deploy with reconciliation
6. Add User Story 3 → Test independently → Deploy with schema evolution
7. Complete Polish phase → Production-ready

Each story adds value without breaking previous stories.

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (Real-Time Replication) - PRIORITY
   - Developer B: User Story 5 (Local Testing Environment) - ENABLES US1 TESTING
   - Developer C: User Story 2 (Monitoring) - REQUIRES US1 RUNNING
3. After US1 + US5 complete:
   - Developer A: User Story 4 (Error Recovery & Reconciliation)
   - Developer B: User Story 3 (Schema Evolution)
4. Final: Polish phase together

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- **TDD is mandatory**: Verify tests fail before implementing (per NFR-008)
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
- **Constitution compliance**: All tests must be written first per principle I (Test-Driven Development NON-NEGOTIABLE)
