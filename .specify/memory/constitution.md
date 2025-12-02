<!--
Sync Impact Report:
Version: 1.0.0 (Initial creation)
Modified Principles: N/A (new constitution)
Added Sections:
  - Core Principles (5 principles focused on TDD, clean code, CDC architecture)
  - Data Integrity & Consistency
  - Development Workflow
  - Governance
Templates Status:
  - plan-template.md: ✅ Constitution Check section exists, ready for alignment
  - spec-template.md: ✅ User scenarios and requirements structure aligns with principles
  - tasks-template.md: ✅ Test-first approach and task organization aligns with principles
  - All command files: ✅ No agent-specific naming issues found
Follow-up TODOs: None - all placeholders resolved
-->

# SQL Server to PostgreSQL CDC Pipeline Constitution

## Core Principles

### I. Test-Driven Development (NON-NEGOTIABLE)

**Tests MUST be written before implementation.** All features follow the Red-Green-Refactor cycle:

1. Write failing tests that define the expected behavior
2. Obtain user approval for test coverage
3. Verify tests fail (red)
4. Implement minimal code to pass tests (green)
5. Refactor for clarity and maintainability

**Rationale**: TDD ensures code correctness from the start, provides living documentation, and enables confident refactoring. For CDC pipelines where data integrity is paramount, tests prevent silent data corruption and transformation errors.

**Required test coverage**:
- Contract tests for all external interfaces (SQL Server, PostgreSQL, APIs)
- Integration tests for data transformation pipelines
- Unit tests for transformation logic, filtering rules, and error handlers
- End-to-end tests for complete replication scenarios

### II. Data Integrity First

**CDC pipelines MUST guarantee data consistency and correctness.** All data operations must:

- Validate data at system boundaries (source extraction, target loading)
- Implement idempotent operations (safe retries without duplication)
- Preserve transactional semantics where required
- Handle schema evolution gracefully
- Log all data transformations for audit trails
- Implement checkpointing and resumability

**Rationale**: Change Data Capture systems are mission-critical data infrastructure. Silent data loss, corruption, or inconsistency can have severe downstream impacts. Every byte must be accounted for.

**Non-negotiable requirements**:
- ACID guarantees preserved where applicable
- Explicit error handling for data type mismatches
- Validation of row counts, checksums, and data quality metrics
- Dead letter queues for problematic records
- Monitoring and alerting on pipeline health

### III. Clean, Maintainable Code

**Code MUST be self-documenting and easy to understand.** Prioritize clarity over cleverness:

- Functions do one thing and do it well (Single Responsibility)
- Names reveal intent (variables, functions, classes)
- Complex logic includes explanatory comments on "why", not "what"
- Avoid premature optimization—optimize only when profiling proves necessity
- Maximum function length: ~50 lines (guideline, not absolute rule)
- Cyclomatic complexity kept low

**Rationale**: CDC pipelines run continuously in production. Future maintainers (including yourself) must quickly diagnose issues at 3 AM. Clear code reduces MTTR (Mean Time To Recovery).

**Forbidden patterns**:
- Magic numbers (use named constants)
- Deeply nested conditionals (extract functions)
- God classes/modules (split responsibilities)
- Cryptic abbreviations (use full words)
- Comments that repeat the code

### IV. Robust Error Handling & Observability

**All failure modes MUST be anticipated and handled explicitly.** CDC pipelines encounter:

- Network failures (transient and permanent)
- Schema mismatches (column added/removed/renamed)
- Data type incompatibilities
- Resource exhaustion (memory, disk, connections)
- Source system downtime
- Target system slowness/unavailability

**Required observability**:
- Structured logging (JSON format) with context (transaction ID, table, row key)
- Metrics: throughput, lag, error rates, resource utilization
- Distributed tracing for data lineage
- Health check endpoints
- Clear error messages with actionable remediation steps

**Rationale**: Production CDC pipelines must be observable and debuggable. When failures occur, logs and metrics must enable rapid root cause analysis.

### V. Modular Architecture with Clear Boundaries

**System MUST be decomposed into loosely coupled, independently testable modules:**

- **Extractors**: Read changes from source (SQL Server CDC tables)
- **Transformers**: Convert data types, apply business rules, handle schema mapping
- **Loaders**: Write to target (PostgreSQL) with conflict resolution
- **Coordinators**: Orchestrate pipeline flow, manage state, handle retries
- **Monitors**: Track pipeline health, emit metrics, trigger alerts

**Interface contracts**:
- Each module exposes clear, documented interfaces
- Modules communicate via well-defined data structures (not implementation details)
- State management is explicit (no hidden global state)
- Dependencies are injected (facilitates testing and swapping implementations)

**Rationale**: Modular design enables independent development, testing, and deployment of components. It also allows replacing individual modules (e.g., swapping SQL Server source for Oracle) without rewriting the entire pipeline.

## Data Integrity & Consistency

### Schema Management

- All schema changes MUST be versioned (migrations)
- Source and target schema mappings MUST be explicit and versioned
- Breaking changes require data migration plan and rollback strategy
- Schema evolution testing MUST be automated

### Transaction Handling

- Transactional boundaries MUST match source system semantics
- Large transactions MUST be batched appropriately
- Deadlock detection and retry logic required
- Consistency checks between source and target

### Data Quality

- Data validation rules MUST be explicit and testable
- Quality metrics tracked per table/pipeline
- Automated data quality regression tests
- Reconciliation processes to verify source/target consistency

## Development Workflow

### Code Review Requirements

All code changes MUST:
- Include tests that fail before implementation
- Pass all existing tests
- Meet code quality gates (linting, complexity checks)
- Include updated documentation if behavior changes
- Have at least one reviewer approval

### Performance Standards

- Pipeline lag MUST be monitored (target: <5 minutes for real-time CDC)
- Throughput targets defined per table (e.g., 10K rows/sec)
- Resource utilization thresholds (CPU <70%, memory <80%)
- Performance regression tests prevent degradation

### Documentation Requirements

- README with quick start instructions
- Architecture decision records (ADRs) for major design choices
- API/interface documentation (auto-generated where possible)
- Runbooks for common operational scenarios
- Data flow diagrams for complex pipelines

## Governance

**This constitution supersedes all other development practices.** When conflicts arise, constitution principles take precedence.

### Amendment Process

1. Propose change with detailed rationale and impact analysis
2. Review by team/stakeholders
3. Approve with consensus
4. Increment version according to semantic versioning
5. Update all dependent templates and documentation
6. Announce change with migration guidance

### Compliance & Enforcement

- All code reviews MUST verify constitutional compliance
- Automated checks enforce where possible (linting, test coverage)
- Complexity violations require explicit justification
- Deviations documented as technical debt with remediation plan

### Version & Semantic Versioning Policy

- **MAJOR**: Backward-incompatible principle changes (e.g., removing a required test type)
- **MINOR**: New principles added or existing principles significantly expanded
- **PATCH**: Clarifications, wording improvements, typo fixes

**Version**: 1.0.0 | **Ratified**: 2025-12-02 | **Last Amended**: 2025-12-02
