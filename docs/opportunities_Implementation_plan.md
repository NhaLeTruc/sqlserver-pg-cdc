# Implementation Plan: SWOT Opportunities Enhancement

**Project:** SQL Server to PostgreSQL CDC Pipeline
**Date:** 2025-12-21
**Status:** Planning Phase
**Estimated Complexity:** High (Multi-phase implementation)

---

## Executive Summary

This plan addresses 15 high-value opportunities identified in the SWOT analysis to enhance the CDC pipeline's capabilities across reconciliation, performance, testing, security, and observability domains. The implementation is organized into 5 phases with clear dependencies and prioritization.

**Total Estimated Effort:** 280-320 hours (7-8 weeks with 1 developer)

---

## Table of Contents

1. [Phase 1: Foundation & Infrastructure](#phase-1-foundation--infrastructure)
2. [Phase 2: Core Reconciliation Enhancements](#phase-2-core-reconciliation-enhancements)
3. [Phase 3: Performance Optimization](#phase-3-performance-optimization)
4. [Phase 4: Testing Infrastructure](#phase-4-testing-infrastructure)
5. [Phase 5: Observability & Security](#phase-5-observability--security)
6. [Dependencies & Sequencing](#dependencies--sequencing)
7. [Risk Assessment](#risk-assessment)
8. [Success Metrics](#success-metrics)

---

## Phase 1: Foundation & Infrastructure
**Duration:** 2 weeks | **Effort:** 60-70 hours

### 1.1 Pre-Commit Hooks (Opportunity #27)
**Priority:** P1 | **Effort:** 8 hours

#### Objective
Establish automated code quality gates before commits to maintain consistency and catch issues early.

#### Implementation Details

**File:** `.pre-commit-config.yaml`
```yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-added-large-files
      - id: check-merge-conflict
      - id: detect-private-key

  - repo: https://github.com/psf/black
    rev: 23.12.1
    hooks:
      - id: black
        language_version: python3.11
        args: [--line-length=100]

  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.1.8
    hooks:
      - id: ruff
        args: [--fix]

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.7.1
    hooks:
      - id: mypy
        additional_dependencies: [types-all]
        args: [--strict]

  - repo: local
    hooks:
      - id: pytest-quick
        name: Run quick unit tests
        entry: pytest tests/unit -v --tb=short
        language: system
        pass_filenames: false
        always_run: true
```

**Installation Script:** `scripts/setup-pre-commit.sh`
```bash
#!/bin/bash
set -euo pipefail

echo "Installing pre-commit hooks..."
pip install pre-commit
pre-commit install
pre-commit install --hook-type commit-msg
echo "Pre-commit hooks installed successfully"
```

**Makefile Integration:**
```makefile
setup-hooks: ## Install pre-commit hooks
	./scripts/setup-pre-commit.sh

run-hooks: ## Run pre-commit on all files
	pre-commit run --all-files
```

#### Acceptance Criteria
- [ ] All hooks pass on existing codebase
- [ ] Hooks run automatically on commit
- [ ] Documentation updated in README
- [ ] Team onboarding guide created

---

### 1.2 Database Connection Pooling (Opportunity #10)
**Priority:** P1 | **Effort:** 16 hours

#### Objective
Implement efficient connection pooling to reduce overhead and improve performance for reconciliation operations.

#### Implementation Details

**File:** `src/utils/db_pool.py`
```python
"""
Database connection pooling for SQL Server and PostgreSQL

Provides thread-safe connection pools with:
- Automatic connection recycling
- Health checks
- Configurable pool sizes
- Metrics integration
"""

from typing import Any, Dict, Optional, Protocol
import logging
from contextlib import contextmanager
import psycopg2.pool
import pyodbc
import threading

logger = logging.getLogger(__name__)


class ConnectionPool(Protocol):
    """Protocol for database connection pools"""
    def getconn(self) -> Any: ...
    def putconn(self, conn: Any) -> None: ...
    def closeall(self) -> None: ...


class PostgresConnectionPool:
    """PostgreSQL connection pool using psycopg2.pool.ThreadedConnectionPool"""

    def __init__(
        self,
        minconn: int = 2,
        maxconn: int = 10,
        host: str = "localhost",
        port: int = 5432,
        database: str = "",
        user: str = "",
        password: str = "",
        **kwargs
    ):
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            **kwargs
        )
        logger.info(f"PostgreSQL pool created: {minconn}-{maxconn} connections to {host}:{port}/{database}")

    @contextmanager
    def get_connection(self):
        """Get connection from pool with automatic return"""
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)

    def close(self):
        """Close all connections in pool"""
        self.pool.closeall()


class SQLServerConnectionPool:
    """SQL Server connection pool using custom implementation"""

    def __init__(
        self,
        minconn: int = 2,
        maxconn: int = 10,
        server: str = "localhost",
        database: str = "",
        username: str = "",
        password: str = "",
        driver: str = "ODBC Driver 18 for SQL Server",
        **kwargs
    ):
        self.minconn = minconn
        self.maxconn = maxconn
        self.connection_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
        )
        self.pool: list[Any] = []
        self.in_use: set[Any] = set()
        self.lock = threading.Lock()

        # Pre-create minimum connections
        for _ in range(minconn):
            self.pool.append(self._create_connection())

        logger.info(f"SQL Server pool created: {minconn}-{maxconn} connections to {server}/{database}")

    def _create_connection(self) -> Any:
        """Create new database connection"""
        return pyodbc.connect(self.connection_string)

    @contextmanager
    def get_connection(self):
        """Get connection from pool with automatic return"""
        conn = None
        with self.lock:
            if self.pool:
                conn = self.pool.pop()
            elif len(self.in_use) < self.maxconn:
                conn = self._create_connection()
            else:
                # Wait for available connection (simplified)
                raise RuntimeError("Connection pool exhausted")

            self.in_use.add(conn)

        try:
            yield conn
        finally:
            with self.lock:
                self.in_use.remove(conn)
                # Check connection health before returning to pool
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    self.pool.append(conn)
                except Exception:
                    # Connection broken, create new one
                    try:
                        conn.close()
                    except Exception:
                        pass
                    if len(self.pool) + len(self.in_use) < self.minconn:
                        self.pool.append(self._create_connection())

    def close(self):
        """Close all connections in pool"""
        with self.lock:
            for conn in self.pool:
                try:
                    conn.close()
                except Exception:
                    pass
            for conn in self.in_use:
                try:
                    conn.close()
                except Exception:
                    pass
            self.pool.clear()
            self.in_use.clear()


# Global pool instances (singleton pattern)
_postgres_pool: Optional[PostgresConnectionPool] = None
_sqlserver_pool: Optional[SQLServerConnectionPool] = None
_pool_lock = threading.Lock()


def get_postgres_pool(**config) -> PostgresConnectionPool:
    """Get or create PostgreSQL connection pool (singleton)"""
    global _postgres_pool
    with _pool_lock:
        if _postgres_pool is None:
            _postgres_pool = PostgresConnectionPool(**config)
        return _postgres_pool


def get_sqlserver_pool(**config) -> SQLServerConnectionPool:
    """Get or create SQL Server connection pool (singleton)"""
    global _sqlserver_pool
    with _pool_lock:
        if _sqlserver_pool is None:
            _sqlserver_pool = SQLServerConnectionPool(**config)
        return _sqlserver_pool


def close_all_pools():
    """Close all connection pools"""
    global _postgres_pool, _sqlserver_pool
    with _pool_lock:
        if _postgres_pool:
            _postgres_pool.close()
            _postgres_pool = None
        if _sqlserver_pool:
            _sqlserver_pool.close()
            _sqlserver_pool = None
```

**Integration in scheduler.py:**
```python
# Update reconcile_job_wrapper to use connection pools
from src.utils.db_pool import get_postgres_pool, get_sqlserver_pool

def reconcile_job_wrapper(...):
    # Get pools
    pg_pool = get_postgres_pool(**target_config)
    sql_pool = get_sqlserver_pool(**source_config)

    # Use pooled connections
    with sql_pool.get_connection() as source_conn:
        with pg_pool.get_connection() as target_conn:
            source_cursor = source_conn.cursor()
            target_cursor = target_conn.cursor()
            # ... reconciliation logic
```

#### Acceptance Criteria
- [ ] Connection pools created for both databases
- [ ] Pool metrics tracked (active, idle connections)
- [ ] Pool exhaustion handled gracefully
- [ ] Performance benchmarks show improvement
- [ ] Unit tests for pool operations

---

### 1.3 CI/CD Pipeline Foundation
**Priority:** P1 | **Effort:** 20 hours

#### Objective
Establish automated testing and deployment pipeline using GitHub Actions.

#### Implementation Details

**File:** `.github/workflows/ci.yml`
```yaml
name: CI/CD Pipeline

on:
  push:
    branches: [main, develop, '0*']
  pull_request:
    branches: [main, develop]

env:
  PYTHON_VERSION: '3.11'

jobs:
  lint:
    name: Lint and Type Check
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: Install dependencies
        run: |
          pip install black ruff mypy
          pip install -r requirements.txt

      - name: Black format check
        run: black --check .

      - name: Ruff lint
        run: ruff check .

      - name: MyPy type check
        run: mypy src/

  test-unit:
    name: Unit Tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: Install dependencies
        run: |
          pip install -e ".[dev]"

      - name: Run unit tests
        run: |
          pytest tests/unit -v --cov=src --cov-report=xml

      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          files: ./coverage.xml

  test-integration:
    name: Integration Tests
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15-alpine
        env:
          POSTGRES_PASSWORD: test_password
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: Install dependencies
        run: |
          pip install -e ".[dev]"

      - name: Run integration tests
        run: |
          pytest tests/integration -v
        env:
          POSTGRES_HOST: localhost
          POSTGRES_PASSWORD: test_password

  security-scan:
    name: Security Scanning
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Run Trivy vulnerability scanner
        uses: aquasecurity/trivy-action@master
        with:
          scan-type: 'fs'
          scan-ref: '.'
          format: 'sarif'
          output: 'trivy-results.sarif'

      - name: Upload Trivy results to GitHub Security
        uses: github/codeql-action/upload-sarif@v3
        with:
          sarif_file: 'trivy-results.sarif'
```

**File:** `.github/workflows/docker-build.yml`
```yaml
name: Docker Build & Push

on:
  push:
    branches: [main]
    tags: ['v*']

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Build Docker images
        run: |
          docker compose -f docker/docker-compose.yml build

      - name: Run smoke tests
        run: |
          docker compose -f docker/docker-compose.yml up -d
          sleep 30
          make verify
```

#### Acceptance Criteria
- [ ] All CI jobs pass on main branch
- [ ] Pull requests automatically tested
- [ ] Coverage reports uploaded
- [ ] Docker builds validated
- [ ] Security scans integrated

---

### 1.4 Query Optimization (Opportunity #11)
**Priority:** P2 | **Effort:** 16 hours

#### Objective
Optimize database queries for reconciliation with proper indexing and query plan analysis.

#### Implementation Details

**File:** `scripts/sql/create_reconciliation_indexes.sql`
```sql
-- PostgreSQL indexes for reconciliation queries

-- Index on primary keys for faster checksums
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_<table>_pk_checksum
ON <schema>.<table> (<primary_key_column>);

-- Composite index for frequently queried columns
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_<table>_reconcile_cols
ON <schema>.<table> (<col1>, <col2>, <col3>);

-- Partial index for active records (if soft delete pattern)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_<table>_active
ON <schema>.<table> (<primary_key>)
WHERE deleted_at IS NULL;
```

**File:** `src/utils/query_optimizer.py`
```python
"""Query optimization utilities for reconciliation"""

def analyze_query_plan(cursor: Any, query: str) -> Dict[str, Any]:
    """Get query execution plan for analysis"""
    db_type = _get_db_type(cursor)

    if db_type == 'postgresql':
        cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}")
        return cursor.fetchone()[0]
    else:  # SQL Server
        cursor.execute("SET SHOWPLAN_ALL ON")
        cursor.execute(query)
        plan = cursor.fetchall()
        cursor.execute("SET SHOWPLAN_ALL OFF")
        return plan


def suggest_indexes(cursor: Any, table_name: str) -> List[str]:
    """Suggest missing indexes based on query patterns"""
    # Analyze query logs and suggest indexes
    # Implementation uses pg_stat_statements for PostgreSQL
    pass
```

#### Acceptance Criteria
- [ ] Indexes created on all reconciliation tables
- [ ] Query plans analyzed and optimized
- [ ] Performance improvement >50% on large tables
- [ ] Automated index recommendation script

---

## Phase 2: Core Reconciliation Enhancements
**Duration:** 3 weeks | **Effort:** 90-100 hours

### 2.1 Row-Level Reconciliation (Opportunity #1)
**Priority:** P1 | **Effort:** 40 hours

#### Objective
Implement detailed row-by-row comparison to identify specific discrepancies beyond count/checksum validation.

#### Implementation Details

**File:** `src/reconciliation/row_level.py`
```python
"""
Row-level reconciliation for detailed discrepancy detection

Identifies:
- Missing rows (in source but not target)
- Extra rows (in target but not source)
- Modified rows (different values)
- Generates repair SQL scripts
"""

from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class RowDiscrepancy:
    """Represents a single row discrepancy"""
    table: str
    primary_key: Dict[str, Any]
    discrepancy_type: str  # MISSING, EXTRA, MODIFIED
    source_data: Optional[Dict[str, Any]]
    target_data: Optional[Dict[str, Any]]
    modified_columns: Optional[List[str]] = None


class RowLevelReconciler:
    """Performs row-level reconciliation between source and target"""

    def __init__(
        self,
        source_cursor: Any,
        target_cursor: Any,
        pk_columns: List[str],
        compare_columns: Optional[List[str]] = None,
        chunk_size: int = 1000
    ):
        self.source_cursor = source_cursor
        self.target_cursor = target_cursor
        self.pk_columns = pk_columns
        self.compare_columns = compare_columns
        self.chunk_size = chunk_size

    def reconcile_table(
        self,
        source_table: str,
        target_table: str
    ) -> List[RowDiscrepancy]:
        """
        Perform row-level reconciliation

        Returns list of all discrepancies found
        """
        discrepancies = []

        # Get all PKs from both sides
        source_pks = self._get_all_primary_keys(self.source_cursor, source_table)
        target_pks = self._get_all_primary_keys(self.target_cursor, target_table)

        # Find missing and extra rows
        missing_pks = source_pks - target_pks  # In source but not target
        extra_pks = target_pks - source_pks    # In target but not source
        common_pks = source_pks & target_pks   # In both

        logger.info(
            f"Row-level reconciliation: {len(source_pks)} source rows, "
            f"{len(target_pks)} target rows, {len(missing_pks)} missing, "
            f"{len(extra_pks)} extra, {len(common_pks)} common"
        )

        # Record missing rows
        for pk in missing_pks:
            source_data = self._get_row_data(self.source_cursor, source_table, pk)
            discrepancies.append(RowDiscrepancy(
                table=target_table,
                primary_key=pk,
                discrepancy_type="MISSING",
                source_data=source_data,
                target_data=None
            ))

        # Record extra rows
        for pk in extra_pks:
            target_data = self._get_row_data(self.target_cursor, target_table, pk)
            discrepancies.append(RowDiscrepancy(
                table=target_table,
                primary_key=pk,
                discrepancy_type="EXTRA",
                source_data=None,
                target_data=target_data
            ))

        # Compare common rows for modifications
        for pk in common_pks:
            source_data = self._get_row_data(self.source_cursor, source_table, pk)
            target_data = self._get_row_data(self.target_cursor, target_table, pk)

            modified_cols = self._compare_rows(source_data, target_data)
            if modified_cols:
                discrepancies.append(RowDiscrepancy(
                    table=target_table,
                    primary_key=pk,
                    discrepancy_type="MODIFIED",
                    source_data=source_data,
                    target_data=target_data,
                    modified_columns=modified_cols
                ))

        return discrepancies

    def _get_all_primary_keys(
        self,
        cursor: Any,
        table: str
    ) -> Set[Tuple]:
        """Get all primary keys from table"""
        pk_cols = ", ".join([_quote_identifier(cursor, col) for col in self.pk_columns])
        quoted_table = _quote_identifier(cursor, table)

        query = f"SELECT {pk_cols} FROM {quoted_table}"
        cursor.execute(query)

        return set(cursor.fetchall())

    def _get_row_data(
        self,
        cursor: Any,
        table: str,
        pk: Tuple
    ) -> Dict[str, Any]:
        """Get full row data for given primary key"""
        quoted_table = _quote_identifier(cursor, table)

        # Build WHERE clause
        where_conditions = []
        for i, col in enumerate(self.pk_columns):
            quoted_col = _quote_identifier(cursor, col)
            where_conditions.append(f"{quoted_col} = ?")  # Parameterized

        where_clause = " AND ".join(where_conditions)

        if self.compare_columns:
            cols = ", ".join([_quote_identifier(cursor, c) for c in self.compare_columns])
        else:
            cols = "*"

        query = f"SELECT {cols} FROM {quoted_table} WHERE {where_clause}"
        cursor.execute(query, pk)

        row = cursor.fetchone()
        if not row:
            return {}

        # Convert to dictionary
        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, row))

    def _compare_rows(
        self,
        source_data: Dict[str, Any],
        target_data: Dict[str, Any]
    ) -> List[str]:
        """
        Compare two rows and return list of modified columns

        Returns empty list if rows are identical
        """
        modified = []

        for col in source_data.keys():
            if col in self.pk_columns:
                continue  # Skip PK columns

            source_val = source_data.get(col)
            target_val = target_data.get(col)

            # Handle NULL comparisons
            if source_val != target_val:
                # Additional check for numeric precision differences
                if isinstance(source_val, (int, float)) and isinstance(target_val, (int, float)):
                    if abs(source_val - target_val) < 1e-9:
                        continue

                modified.append(col)

        return modified


def generate_repair_script(
    discrepancies: List[RowDiscrepancy],
    target_table: str
) -> str:
    """
    Generate SQL repair script from discrepancies

    Returns SQL script to fix all discrepancies
    """
    script_lines = [
        f"-- Repair script for {target_table}",
        f"-- Generated: {datetime.now(timezone.utc).isoformat()}",
        f"-- Total discrepancies: {len(discrepancies)}",
        "",
        "BEGIN TRANSACTION;",
        ""
    ]

    for disc in discrepancies:
        if disc.discrepancy_type == "MISSING":
            # Generate INSERT
            script_lines.append(f"-- Insert missing row: {disc.primary_key}")
            script_lines.append(_generate_insert_sql(target_table, disc.source_data))

        elif disc.discrepancy_type == "EXTRA":
            # Generate DELETE
            script_lines.append(f"-- Delete extra row: {disc.primary_key}")
            script_lines.append(_generate_delete_sql(target_table, disc.primary_key))

        elif disc.discrepancy_type == "MODIFIED":
            # Generate UPDATE
            script_lines.append(f"-- Update modified row: {disc.primary_key}")
            script_lines.append(f"-- Modified columns: {', '.join(disc.modified_columns)}")
            script_lines.append(_generate_update_sql(
                target_table,
                disc.primary_key,
                disc.source_data,
                disc.modified_columns
            ))

        script_lines.append("")

    script_lines.append("COMMIT;")

    return "\n".join(script_lines)


def _generate_insert_sql(table: str, data: Dict[str, Any]) -> str:
    """Generate INSERT statement"""
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["?" for _ in data])
    return f"INSERT INTO {table} ({columns}) VALUES ({placeholders});"


def _generate_delete_sql(table: str, pk: Dict[str, Any]) -> str:
    """Generate DELETE statement"""
    where_clause = " AND ".join([f"{k} = ?" for k in pk.keys()])
    return f"DELETE FROM {table} WHERE {where_clause};"


def _generate_update_sql(
    table: str,
    pk: Dict[str, Any],
    data: Dict[str, Any],
    modified_cols: List[str]
) -> str:
    """Generate UPDATE statement"""
    set_clause = ", ".join([f"{col} = ?" for col in modified_cols])
    where_clause = " AND ".join([f"{k} = ?" for k in pk.keys()])
    return f"UPDATE {table} SET {set_clause} WHERE {where_clause};"
```

**CLI Integration:**
```python
# Add to src/reconciliation/cli.py

@click.command()
@click.option('--row-level', is_flag=True, help='Perform row-level reconciliation')
@click.option('--generate-repair', is_flag=True, help='Generate repair SQL script')
def reconcile(..., row_level, generate_repair):
    if row_level:
        reconciler = RowLevelReconciler(...)
        discrepancies = reconciler.reconcile_table(...)

        if generate_repair:
            script = generate_repair_script(discrepancies, table)
            with open(f"repair_{table}.sql", "w") as f:
                f.write(script)
```

#### Acceptance Criteria
- [ ] Identifies missing, extra, and modified rows
- [ ] Generates actionable repair scripts
- [ ] Performance acceptable for tables up to 1M rows
- [ ] CLI integration with `--row-level` flag
- [ ] Comprehensive unit tests

---

### 2.2 Incremental Checksums (Opportunity #8)
**Priority:** P1 | **Effort:** 24 hours

#### Objective
Optimize checksum calculation by only processing changed data since last reconciliation.

#### Implementation Details

**File:** `src/reconciliation/incremental.py`
```python
"""
Incremental checksum calculation using CDC metadata

Only checksums rows modified since last reconciliation run.
Stores checksum state for incremental updates.
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any
import hashlib
import json
from pathlib import Path

class IncrementalChecksumTracker:
    """Tracks checksum state for incremental updates"""

    def __init__(self, state_dir: str = "./reconciliation_state"):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def get_last_checksum_timestamp(self, table: str) -> Optional[datetime]:
        """Get timestamp of last checksum calculation"""
        state_file = self.state_dir / f"{table}_checksum_state.json"
        if not state_file.exists():
            return None

        with open(state_file) as f:
            state = json.load(f)
            return datetime.fromisoformat(state['last_run'])

    def save_checksum_state(
        self,
        table: str,
        checksum: str,
        row_count: int,
        timestamp: datetime
    ):
        """Save checksum state for table"""
        state_file = self.state_dir / f"{table}_checksum_state.json"
        state = {
            'table': table,
            'checksum': checksum,
            'row_count': row_count,
            'last_run': timestamp.isoformat()
        }
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)


def calculate_incremental_checksum(
    cursor: Any,
    table_name: str,
    last_checksum_time: Optional[datetime],
    pk_column: str,
    change_tracking_column: str = "modified_at"
) -> str:
    """
    Calculate checksum only for rows changed since last run

    Args:
        cursor: Database cursor
        table_name: Table to checksum
        last_checksum_time: Timestamp of last checksum (None for full checksum)
        pk_column: Primary key column for ordering
        change_tracking_column: Column tracking modification time

    Returns:
        Incremental checksum hash
    """
    quoted_table = _quote_identifier(cursor, table_name)

    if last_checksum_time is None:
        # Full checksum on first run
        return calculate_checksum_chunked(cursor, table_name)

    # Build incremental query
    db_type = _get_db_type(cursor)

    if db_type == 'postgresql':
        query = f"""
            SELECT * FROM {quoted_table}
            WHERE {change_tracking_column} > %s
            ORDER BY {pk_column}
        """
        cursor.execute(query, (last_checksum_time,))
    else:  # SQL Server
        query = f"""
            SELECT * FROM {quoted_table}
            WHERE {change_tracking_column} > ?
            ORDER BY {pk_column}
        """
        cursor.execute(query, (last_checksum_time,))

    # Calculate checksum for changed rows only
    hasher = hashlib.sha256()
    row_count = 0

    for row in cursor:
        row_str = "|".join(str(val) if val is not None else "NULL" for val in row)
        hasher.update(row_str.encode('utf-8'))
        row_count += 1

    logger.info(f"Incremental checksum: {row_count} changed rows since {last_checksum_time}")

    return hasher.hexdigest()
```

#### Acceptance Criteria
- [ ] Incremental checksums 10-100x faster for large tables
- [ ] State persisted between runs
- [ ] Falls back to full checksum when needed
- [ ] Integration with existing reconciliation

---

### 2.3 Data Transformation Layer (Opportunity #2)
**Priority:** P2 | **Effort:** 26 hours

#### Objective
Add data transformation capabilities for PII masking, encryption, and business rule application.

#### Implementation Details

**File:** `src/transformation/transform.py`
```python
"""
Data transformation framework for CDC pipeline

Supports:
- PII masking
- Field-level encryption
- Data type conversions
- Business rule application
"""

from typing import Any, Callable, Dict, List, Optional
from abc import ABC, abstractmethod
import hashlib
import re


class Transformer(ABC):
    """Base class for data transformers"""

    @abstractmethod
    def transform(self, value: Any, context: Dict[str, Any]) -> Any:
        """Transform a single value"""
        pass


class PIIMaskingTransformer(Transformer):
    """Mask PII data (email, phone, SSN, etc.)"""

    def __init__(self, mask_char: str = "*", preserve_format: bool = True):
        self.mask_char = mask_char
        self.preserve_format = preserve_format

    def transform(self, value: Any, context: Dict[str, Any]) -> Any:
        if not isinstance(value, str):
            return value

        field_name = context.get('field_name', '').lower()

        if 'email' in field_name:
            return self._mask_email(value)
        elif 'phone' in field_name or 'mobile' in field_name:
            return self._mask_phone(value)
        elif 'ssn' in field_name or 'social' in field_name:
            return self._mask_ssn(value)
        else:
            return value

    def _mask_email(self, email: str) -> str:
        """Mask email: user@example.com -> u***@example.com"""
        if '@' not in email:
            return email
        local, domain = email.split('@', 1)
        if len(local) <= 1:
            return email
        masked_local = local[0] + self.mask_char * (len(local) - 1)
        return f"{masked_local}@{domain}"

    def _mask_phone(self, phone: str) -> str:
        """Mask phone: (123) 456-7890 -> (***) ***-7890"""
        # Keep last 4 digits
        digits = re.sub(r'\D', '', phone)
        if len(digits) < 4:
            return phone
        masked_digits = self.mask_char * (len(digits) - 4) + digits[-4:]
        # Preserve original format
        result = phone
        for orig, masked in zip(digits, masked_digits):
            result = result.replace(orig, masked, 1)
        return result

    def _mask_ssn(self, ssn: str) -> str:
        """Mask SSN: 123-45-6789 -> ***-**-6789"""
        digits = re.sub(r'\D', '', ssn)
        if len(digits) != 9:
            return ssn
        return f"***-**-{digits[-4:]}"


class HashingTransformer(Transformer):
    """One-way hash transformation for PII"""

    def __init__(self, algorithm: str = 'sha256', salt: str = ''):
        self.algorithm = algorithm
        self.salt = salt

    def transform(self, value: Any, context: Dict[str, Any]) -> Any:
        if value is None:
            return None

        data = f"{self.salt}{str(value)}".encode('utf-8')
        hasher = hashlib.new(self.algorithm)
        hasher.update(data)
        return hasher.hexdigest()


class TransformationPipeline:
    """Chain multiple transformers for a field"""

    def __init__(self):
        self.field_transformers: Dict[str, List[Transformer]] = {}

    def add_transformer(self, field_pattern: str, transformer: Transformer):
        """Add transformer for fields matching pattern"""
        if field_pattern not in self.field_transformers:
            self.field_transformers[field_pattern] = []
        self.field_transformers[field_pattern].append(transformer)

    def transform_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Transform all fields in row"""
        transformed = row.copy()

        for field_name, value in row.items():
            for pattern, transformers in self.field_transformers.items():
                if re.match(pattern, field_name, re.IGNORECASE):
                    for transformer in transformers:
                        value = transformer.transform(
                            value,
                            {'field_name': field_name, 'row': row}
                        )
            transformed[field_name] = value

        return transformed


# Example configuration
def create_pii_pipeline() -> TransformationPipeline:
    """Create standard PII transformation pipeline"""
    pipeline = TransformationPipeline()

    # Mask PII fields
    masker = PIIMaskingTransformer()
    pipeline.add_transformer(r'.*email.*', masker)
    pipeline.add_transformer(r'.*phone.*', masker)
    pipeline.add_transformer(r'.*ssn.*', masker)

    # Hash sensitive IDs
    hasher = HashingTransformer(salt='production_salt_here')
    pipeline.add_transformer(r'.*customer_id.*', hasher)

    return pipeline
```

**Kafka Connect SMT Integration:**
```json
{
  "name": "sqlserver-source-with-transforms",
  "config": {
    "transforms": "maskPII,hashIDs",
    "transforms.maskPII.type": "org.example.MaskPIITransform$Value",
    "transforms.maskPII.field.patterns": "email,phone,ssn",
    "transforms.hashIDs.type": "org.example.HashTransform$Value",
    "transforms.hashIDs.field.patterns": "customer_id,user_id"
  }
}
```

#### Acceptance Criteria
- [ ] PII masking for common field types
- [ ] Configurable transformation pipelines
- [ ] Kafka Connect SMT integration
- [ ] Performance impact <10%
- [ ] Unit tests with PII test data

---

## Phase 3: Performance Optimization
**Duration:** 1.5 weeks | **Effort:** 50-60 hours

### 3.1 Parallel Reconciliation (Opportunity #7)
**Priority:** P1 | **Effort:** 24 hours

#### Objective
Process multiple tables concurrently to reduce total reconciliation time.

#### Implementation Details

**File:** `src/reconciliation/parallel.py`
```python
"""
Parallel reconciliation using concurrent.futures

Processes multiple tables concurrently with:
- Configurable parallelism
- Resource limits
- Result aggregation
- Error handling per table
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Callable
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class ParallelReconciler:
    """Parallel table reconciliation orchestrator"""

    def __init__(
        self,
        max_workers: int = 4,
        timeout_per_table: int = 3600
    ):
        self.max_workers = max_workers
        self.timeout_per_table = timeout_per_table

    def reconcile_tables(
        self,
        tables: List[str],
        reconcile_func: Callable,
        **reconcile_kwargs
    ) -> Dict[str, Any]:
        """
        Reconcile multiple tables in parallel

        Args:
            tables: List of table names to reconcile
            reconcile_func: Function to reconcile single table
            reconcile_kwargs: Additional arguments for reconcile_func

        Returns:
            Aggregated results with per-table status
        """
        start_time = datetime.now(timezone.utc)
        results = {
            'total_tables': len(tables),
            'successful': 0,
            'failed': 0,
            'results': [],
            'errors': []
        }

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_table = {
                executor.submit(
                    self._reconcile_table_wrapper,
                    table,
                    reconcile_func,
                    **reconcile_kwargs
                ): table
                for table in tables
            }

            # Collect results as they complete
            for future in as_completed(future_to_table, timeout=self.timeout_per_table * len(tables)):
                table = future_to_table[future]

                try:
                    result = future.result(timeout=self.timeout_per_table)
                    results['results'].append(result)
                    results['successful'] += 1
                    logger.info(f"✓ Table {table} reconciled successfully")

                except Exception as e:
                    results['failed'] += 1
                    results['errors'].append({
                        'table': table,
                        'error': str(e),
                        'type': type(e).__name__
                    })
                    logger.error(f"✗ Table {table} reconciliation failed: {e}")

        end_time = datetime.now(timezone.utc)
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        results['timestamp'] = end_time.isoformat()

        logger.info(
            f"Parallel reconciliation complete: {results['successful']}/{results['total_tables']} "
            f"successful in {results['duration_seconds']:.2f}s"
        )

        return results

    def _reconcile_table_wrapper(
        self,
        table: str,
        reconcile_func: Callable,
        **kwargs
    ) -> Dict[str, Any]:
        """Wrapper to add table context and timing"""
        start_time = datetime.now(timezone.utc)

        try:
            result = reconcile_func(table=table, **kwargs)
            result['duration_seconds'] = (datetime.now(timezone.utc) - start_time).total_seconds()
            return result
        except Exception as e:
            logger.error(f"Error reconciling table {table}: {e}", exc_info=True)
            raise
```

**Integration in scheduler.py:**
```python
from src.reconciliation.parallel import ParallelReconciler

def reconcile_job_wrapper(...):
    # Use parallel reconciliation
    reconciler = ParallelReconciler(max_workers=4)

    results = reconciler.reconcile_tables(
        tables=tables,
        reconcile_func=reconcile_table,
        source_cursor=source_cursor,
        target_cursor=target_cursor,
        validate_checksum=validate_checksums
    )
```

#### Acceptance Criteria
- [ ] 3-5x speedup for multi-table reconciliation
- [ ] Configurable parallelism
- [ ] Proper error isolation
- [ ] Resource usage stays within limits

---

## Phase 4: Testing Infrastructure
**Duration:** 2 weeks | **Effort:** 60-70 hours

### 4.1 Property-Based Testing (Opportunity #12)
**Priority:** P2 | **Effort:** 20 hours

#### Objective
Use Hypothesis for property-based testing to find edge cases automatically.

#### Implementation Details

**File:** `tests/property/test_reconciliation_properties.py`
```python
"""
Property-based tests for reconciliation logic using Hypothesis

Tests invariants and properties that should hold for all inputs.
"""

from hypothesis import given, strategies as st, settings
from hypothesis.stateful import RuleBasedStateMachine, rule, invariant
from src.reconciliation.compare import compare_row_counts, compare_checksums
import hashlib


# Property: Row count comparison should be symmetric
@given(
    table=st.text(min_size=1, max_size=50),
    source_count=st.integers(min_value=0, max_value=1000000),
    target_count=st.integers(min_value=0, max_value=1000000)
)
def test_row_count_comparison_symmetric(table, source_count, target_count):
    """Row count difference should be symmetric"""
    result = compare_row_counts(table, source_count, target_count)

    assert result['difference'] == target_count - source_count
    assert result['match'] == (source_count == target_count)


# Property: Checksum comparison should be deterministic
@given(
    table=st.text(min_size=1, max_size=50),
    data=st.binary(min_size=0, max_size=1000)
)
def test_checksum_deterministic(table, data):
    """Same data should produce same checksum"""
    checksum1 = hashlib.sha256(data).hexdigest()
    checksum2 = hashlib.sha256(data).hexdigest()

    result = compare_checksums(table, checksum1, checksum2)
    assert result['match'] is True


# Stateful testing for incremental checksums
class IncrementalChecksumMachine(RuleBasedStateMachine):
    """State machine for testing incremental checksum logic"""

    def __init__(self):
        super().__init__()
        self.rows = []
        self.full_checksum = None

    @rule(data=st.binary(min_size=1, max_size=100))
    def add_row(self, data):
        """Add row to dataset"""
        self.rows.append(data)

    @rule()
    def calculate_full_checksum(self):
        """Calculate full checksum"""
        hasher = hashlib.sha256()
        for row in self.rows:
            hasher.update(row)
        self.full_checksum = hasher.hexdigest()

    @invariant()
    def checksum_matches_full_data(self):
        """Incremental checksum should match full checksum"""
        if self.full_checksum is not None:
            hasher = hashlib.sha256()
            for row in self.rows:
                hasher.update(row)
            assert hasher.hexdigest() == self.full_checksum


TestIncrementalChecksum = IncrementalChecksumMachine.TestCase
```

**Add to pyproject.toml:**
```toml
[project.optional-dependencies]
dev = [
    ...
    "hypothesis>=6.92.0",
]
```

#### Acceptance Criteria
- [ ] Property tests for all reconciliation functions
- [ ] Stateful testing for incremental checksums
- [ ] 100+ test cases generated automatically
- [ ] Integration with CI/CD

---

### 4.2 Mutation Testing (Opportunity #13)
**Priority:** P2 | **Effort:** 16 hours

#### Objective
Validate test effectiveness using mutation testing with `mutmut`.

#### Implementation Details

**Installation:**
```bash
pip install mutmut
```

**Configuration:** `.mutmut.yaml`
```yaml
paths_to_mutate:
  - src/reconciliation/
  - src/utils/

tests_dir: tests/

runner: pytest

dict_synonyms:
  - Dict
  - dict

exclude:
  - __init__.py
  - conftest.py
```

**Makefile target:**
```makefile
mutation-test: ## Run mutation tests
	mutmut run --paths-to-mutate=src/reconciliation
	mutmut results
	mutmut html

mutation-report: ## Generate mutation test HTML report
	mutmut html
	@echo "Report available at html/index.html"
```

#### Acceptance Criteria
- [ ] Mutation score >80%
- [ ] Missing test cases identified and added
- [ ] Automated mutation testing in CI

---

### 4.3 Load Testing Framework (Opportunity #15)
**Priority:** P2 | **Effort:** 24 hours

#### Objective
Implement systematic load testing using Locust to understand performance limits.

#### Implementation Details

**File:** `tests/load/locustfile.py`
```python
"""
Load testing for CDC pipeline using Locust

Tests:
- Kafka Connect API load
- Reconciliation throughput
- Database connection limits
"""

from locust import HttpUser, task, between, events
import random


class KafkaConnectUser(HttpUser):
    """Simulate Kafka Connect API load"""

    wait_time = between(1, 5)
    host = "http://localhost:8083"

    @task(3)
    def get_connectors(self):
        """GET /connectors"""
        self.client.get("/connectors")

    @task(2)
    def get_connector_status(self):
        """GET /connectors/{name}/status"""
        connectors = ["sqlserver-source", "postgres-sink"]
        connector = random.choice(connectors)
        self.client.get(f"/connectors/{connector}/status")

    @task(1)
    def get_connector_config(self):
        """GET /connectors/{name}/config"""
        connectors = ["sqlserver-source", "postgres-sink"]
        connector = random.choice(connectors)
        self.client.get(f"/connectors/{connector}/config")


class ReconciliationUser(HttpUser):
    """Simulate reconciliation load"""

    wait_time = between(5, 15)

    def on_start(self):
        """Setup before tasks"""
        # Connect to databases
        pass

    @task
    def reconcile_small_table(self):
        """Reconcile table with <10k rows"""
        # Run reconciliation
        pass

    @task
    def reconcile_large_table(self):
        """Reconcile table with 100k+ rows"""
        # Run reconciliation with chunking
        pass
```

**Run load tests:**
```bash
# Start Locust web UI
locust -f tests/load/locustfile.py --host=http://localhost:8083

# Headless mode
locust -f tests/load/locustfile.py \
    --host=http://localhost:8083 \
    --users 100 \
    --spawn-rate 10 \
    --run-time 5m \
    --headless
```

#### Acceptance Criteria
- [ ] Load test scenarios for all components
- [ ] Performance baselines established
- [ ] Bottlenecks identified and documented
- [ ] Automated load tests in CI

---

## Phase 5: Observability & Security
**Duration:** 2 weeks | **Effort:** 60-70 hours

### 5.1 Distributed Tracing Integration (Opportunity #22)
**Priority:** P1 | **Effort:** 20 hours

#### Objective
Connect Jaeger to application code for end-to-end request tracking.

#### Implementation Details

**File:** `src/utils/tracing.py`
```python
"""
Distributed tracing using OpenTelemetry and Jaeger

Instruments:
- Database queries
- Reconciliation operations
- Kafka operations
"""

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.instrumentation.pyodbc import PyODBCInstrumentor
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


def initialize_tracing(
    service_name: str = "sqlserver-pg-cdc",
    jaeger_host: str = "localhost",
    jaeger_port: int = 6831
):
    """Initialize distributed tracing"""

    # Create Jaeger exporter
    jaeger_exporter = JaegerExporter(
        agent_host_name=jaeger_host,
        agent_port=jaeger_port,
    )

    # Set up tracer provider
    provider = TracerProvider()
    processor = BatchSpanProcessor(jaeger_exporter)
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)

    # Auto-instrument database libraries
    Psycopg2Instrumentor().instrument()
    PyODBCInstrumentor().instrument()

    logger.info(f"Tracing initialized: {service_name} -> {jaeger_host}:{jaeger_port}")

    return trace.get_tracer(service_name)


# Global tracer instance
tracer = None


def get_tracer():
    """Get global tracer instance"""
    global tracer
    if tracer is None:
        tracer = initialize_tracing()
    return tracer


@contextmanager
def trace_operation(operation_name: str, **attributes):
    """Context manager for tracing operations"""
    tracer = get_tracer()

    with tracer.start_as_current_span(operation_name) as span:
        # Add custom attributes
        for key, value in attributes.items():
            span.set_attribute(key, str(value))

        try:
            yield span
        except Exception as e:
            span.set_attribute("error", True)
            span.set_attribute("error.message", str(e))
            span.set_attribute("error.type", type(e).__name__)
            raise
```

**Integration in reconciliation:**
```python
from src.utils.tracing import trace_operation

def reconcile_table(...):
    with trace_operation(
        "reconcile_table",
        table=source_table,
        validate_checksum=validate_checksum
    ):
        # Existing reconciliation logic
        ...
```

**Add dependencies:**
```txt
opentelemetry-api>=1.21.0
opentelemetry-sdk>=1.21.0
opentelemetry-exporter-jaeger>=1.21.0
opentelemetry-instrumentation-psycopg2>=0.42b0
opentelemetry-instrumentation-pyodbc>=0.42b0
```

#### Acceptance Criteria
- [ ] All reconciliation operations traced
- [ ] Database queries visible in Jaeger
- [ ] Trace context propagated across components
- [ ] Performance overhead <5%

---

### 5.2 Custom Grafana Dashboards (Opportunity #24)
**Priority:** P1 | **Effort:** 16 hours

#### Objective
Create pre-configured dashboards for monitoring CDC pipeline health.

#### Implementation Details

**File:** `configs/grafana/dashboards/cdc-pipeline-overview.json`
```json
{
  "dashboard": {
    "title": "CDC Pipeline Overview",
    "panels": [
      {
        "title": "Replication Lag",
        "targets": [
          {
            "expr": "kafka_consumer_lag{topic=~\"sqlserver.*\"}",
            "legendFormat": "{{topic}}"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Reconciliation Status",
        "targets": [
          {
            "expr": "sum(reconciliation_runs_total{status=\"success\"}) by (table_name)",
            "legendFormat": "{{table_name}}"
          }
        ],
        "type": "stat"
      },
      {
        "title": "Row Count Discrepancies",
        "targets": [
          {
            "expr": "reconciliation_row_count_difference",
            "legendFormat": "{{table_name}}"
          }
        ],
        "type": "heatmap",
        "alert": {
          "conditions": [
            {
              "evaluator": {
                "params": [100],
                "type": "gt"
              },
              "operator": {"type": "and"},
              "query": {"params": ["A", "5m", "now"]},
              "type": "query"
            }
          ]
        }
      },
      {
        "title": "Database Connection Pool",
        "targets": [
          {
            "expr": "db_pool_active_connections",
            "legendFormat": "Active - {{database}}"
          },
          {
            "expr": "db_pool_idle_connections",
            "legendFormat": "Idle - {{database}}"
          }
        ],
        "type": "graph"
      }
    ]
  }
}
```

**Dashboard provisioning:** `configs/grafana/provisioning/dashboards.yml`
```yaml
apiVersion: 1

providers:
  - name: 'CDC Dashboards'
    orgId: 1
    folder: 'CDC Pipeline'
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /etc/grafana/provisioning/dashboards
```

**Create additional dashboards:**
1. `cdc-reconciliation-details.json` - Detailed reconciliation metrics
2. `cdc-performance.json` - Performance and throughput
3. `cdc-errors.json` - Error tracking and alerts

#### Acceptance Criteria
- [ ] 3+ pre-configured dashboards
- [ ] Auto-provisioned on Grafana startup
- [ ] Alerts configured for critical metrics
- [ ] Documentation for dashboard usage

---

### 5.3 Vulnerability Scanning (Opportunity #20)
**Priority:** P1 | **Effort:** 12 hours

#### Objective
Integrate Trivy for automated container and dependency scanning.

#### Implementation Details

**File:** `.github/workflows/security-scan.yml`
```yaml
name: Security Scanning

on:
  schedule:
    - cron: '0 0 * * *'  # Daily
  push:
    branches: [main]
  pull_request:

jobs:
  trivy-scan:
    name: Trivy Security Scan
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Run Trivy vulnerability scanner (filesystem)
        uses: aquasecurity/trivy-action@master
        with:
          scan-type: 'fs'
          scan-ref: '.'
          format: 'sarif'
          output: 'trivy-fs-results.sarif'
          severity: 'CRITICAL,HIGH'

      - name: Run Trivy vulnerability scanner (Python dependencies)
        uses: aquasecurity/trivy-action@master
        with:
          scan-type: 'fs'
          scan-ref: 'requirements.txt'
          format: 'table'
          severity: 'CRITICAL,HIGH,MEDIUM'

      - name: Upload Trivy results to GitHub Security
        uses: github/codeql-action/upload-sarif@v3
        if: always()
        with:
          sarif_file: 'trivy-fs-results.sarif'

      - name: Run Snyk Python scan
        uses: snyk/actions/python@master
        env:
          SNYK_TOKEN: ${{ secrets.SNYK_TOKEN }}
        with:
          args: --severity-threshold=high
          command: test

  docker-scan:
    name: Docker Image Scanning
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Build Docker images
        run: docker compose -f docker/docker-compose.yml build

      - name: Scan sqlserver-pg-cdc image
        uses: aquasecurity/trivy-action@master
        with:
          image-ref: 'sqlserver-pg-cdc:latest'
          format: 'sarif'
          output: 'trivy-image-results.sarif'

      - name: Upload image scan results
        uses: github/codeql-action/upload-sarif@v3
        with:
          sarif_file: 'trivy-image-results.sarif'
```

**Makefile integration:**
```makefile
security-scan: ## Run security scans locally
	@echo "$(BLUE)Running Trivy security scan...$(NC)"
	trivy fs --severity HIGH,CRITICAL .
	trivy fs --severity HIGH,CRITICAL requirements.txt
	@echo "$(GREEN)✓ Security scan complete$(NC)"

security-report: ## Generate security report
	trivy fs --format json --output security-report.json .
	@echo "$(GREEN)✓ Security report saved to security-report.json$(NC)"
```

#### Acceptance Criteria
- [ ] Daily automated scans
- [ ] Critical/High vulnerabilities block PRs
- [ ] Security reports in GitHub Security tab
- [ ] Documentation for remediation process

---

### 5.4 Log Aggregation (Opportunity #23)
**Priority:** P2 | **Effort:** 12 hours

#### Objective
Integrate centralized logging with ELK stack or Loki.

#### Implementation Details

**Option 1: Grafana Loki** (Recommended for existing Grafana setup)

**File:** `docker/docker-compose.logging.yml`
```yaml
services:
  loki:
    image: grafana/loki:2.9.3
    container_name: cdc-loki
    ports:
      - "3100:3100"
    volumes:
      - ./configs/loki/loki-config.yml:/etc/loki/local-config.yaml
      - loki-data:/loki
    networks:
      - cdc-network
    command: -config.file=/etc/loki/local-config.yaml

  promtail:
    image: grafana/promtail:2.9.3
    container_name: cdc-promtail
    volumes:
      - /var/log:/var/log
      - ./configs/promtail/promtail-config.yml:/etc/promtail/config.yml
      - /var/lib/docker/containers:/var/lib/docker/containers:ro
    networks:
      - cdc-network
    command: -config.file=/etc/promtail/config.yml

volumes:
  loki-data:
    name: cdc-loki-data
```

**File:** `configs/loki/loki-config.yml`
```yaml
auth_enabled: false

server:
  http_listen_port: 3100

ingester:
  lifecycler:
    address: 127.0.0.1
    ring:
      kvstore:
        store: inmemory
      replication_factor: 1
  chunk_idle_period: 5m
  chunk_retain_period: 30s

schema_config:
  configs:
    - from: 2023-01-01
      store: boltdb-shipper
      object_store: filesystem
      schema: v11
      index:
        prefix: index_
        period: 24h

storage_config:
  boltdb_shipper:
    active_index_directory: /loki/index
    cache_location: /loki/cache
    shared_store: filesystem
  filesystem:
    directory: /loki/chunks

limits_config:
  enforce_metric_name: false
  reject_old_samples: true
  reject_old_samples_max_age: 168h

chunk_store_config:
  max_look_back_period: 0s

table_manager:
  retention_deletes_enabled: true
  retention_period: 168h
```

**File:** `configs/promtail/promtail-config.yml`
```yaml
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://loki:3100/loki/api/v1/push

scrape_configs:
  - job_name: docker
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
        refresh_interval: 5s
    relabel_configs:
      - source_labels: ['__meta_docker_container_name']
        regex: '/(.*)'
        target_label: 'container'
      - source_labels: ['__meta_docker_container_log_stream']
        target_label: 'stream'
```

**Grafana data source configuration:**
```json
{
  "name": "Loki",
  "type": "loki",
  "access": "proxy",
  "url": "http://loki:3100",
  "jsonData": {}
}
```

#### Acceptance Criteria
- [ ] Centralized logging from all containers
- [ ] Searchable logs in Grafana
- [ ] Log retention configured (7 days)
- [ ] Structured logging with JSON format

---

### 5.5 Contract Testing Enhancement (Opportunity #14)
**Priority:** P2 | **Effort:** 12 hours

#### Objective
Enhance existing contract tests with Pact for consumer-driven contracts.

#### Implementation Details

**File:** `tests/contract/test_kafka_connect_api.py`
```python
"""
Contract tests for Kafka Connect REST API using Pact

Ensures API compatibility between versions.
"""

import pytest
from pact import Consumer, Provider, Like, EachLike
import requests


@pytest.fixture(scope='module')
def pact():
    """Setup Pact consumer/provider"""
    pact = Consumer('reconciliation-service').has_pact_with(
        Provider('kafka-connect-api'),
        host_name='localhost',
        port=8083
    )
    pact.start_service()
    yield pact
    pact.stop_service()


def test_get_connectors_contract(pact):
    """Test GET /connectors contract"""
    expected = EachLike('sqlserver-source')

    (pact
     .given('connectors exist')
     .upon_receiving('a request for all connectors')
     .with_request('GET', '/connectors')
     .will_respond_with(200, body=expected))

    with pact:
        response = requests.get(pact.uri + '/connectors')
        assert response.status_code == 200
        assert isinstance(response.json(), list)


def test_get_connector_status_contract(pact):
    """Test GET /connectors/{name}/status contract"""
    expected = {
        'name': Like('sqlserver-source'),
        'connector': {
            'state': Like('RUNNING'),
            'worker_id': Like('kafka-connect:8083')
        },
        'tasks': EachLike({
            'id': Like(0),
            'state': Like('RUNNING'),
            'worker_id': Like('kafka-connect:8083')
        })
    }

    (pact
     .given('connector sqlserver-source exists')
     .upon_receiving('a request for connector status')
     .with_request('GET', '/connectors/sqlserver-source/status')
     .will_respond_with(200, body=expected))

    with pact:
        response = requests.get(pact.uri + '/connectors/sqlserver-source/status')
        assert response.status_code == 200
        assert response.json()['name'] == 'sqlserver-source'
```

**Add to requirements.txt:**
```txt
pact-python>=2.0.0
```

#### Acceptance Criteria
- [ ] Pact contracts for Kafka Connect API
- [ ] Contract verification in CI/CD
- [ ] Pact broker integration (optional)
- [ ] Documentation for contract testing

---

## Dependencies & Sequencing

### Critical Path
```
Phase 1.1 (Pre-commit Hooks)
    └─> Phase 1.3 (CI/CD)
        └─> Phase 4.1 (Property Testing)
        └─> Phase 4.2 (Mutation Testing)

Phase 1.2 (Connection Pooling)
    └─> Phase 2.1 (Row-Level Reconciliation)
    └─> Phase 3.1 (Parallel Reconciliation)

Phase 2.2 (Incremental Checksums)
    └─> Phase 3.1 (Parallel Reconciliation)

Phase 5.1 (Distributed Tracing)
    └─> Phase 5.2 (Grafana Dashboards)
```

### Parallel Tracks
- **Performance Track:** 1.2 → 1.4 → 3.1 → 2.2
- **Quality Track:** 1.1 → 1.3 → 4.1 → 4.2 → 4.3
- **Observability Track:** 5.1 → 5.2 → 5.4
- **Security Track:** 1.1 → 5.3

---

## Risk Assessment

### High Risk Items
1. **Connection Pooling (1.2)** - Could introduce resource leaks if not implemented carefully
   - **Mitigation:** Extensive testing with connection exhaustion scenarios

2. **Parallel Reconciliation (3.1)** - Risk of database overload
   - **Mitigation:** Configurable limits, gradual rollout

3. **Row-Level Reconciliation (2.1)** - Performance impact on large tables
   - **Mitigation:** Chunking, incremental processing, timeouts

### Medium Risk Items
1. **Distributed Tracing (5.1)** - Performance overhead
   - **Mitigation:** Sampling, async exporting

2. **Data Transformation (2.3)** - Data integrity concerns
   - **Mitigation:** Comprehensive testing, validation framework

### Low Risk Items
1. **Pre-commit Hooks (1.1)** - Developer friction
   - **Mitigation:** Clear documentation, easy bypass for emergencies

2. **Dashboards (5.2)** - Minimal risk, UI/configuration only

---

## Success Metrics

### Performance Metrics
- [ ] Reconciliation time reduced by >50% (parallel + incremental)
- [ ] Database query time reduced by >30% (pooling + optimization)
- [ ] Memory usage stable under load (<10% increase)

### Quality Metrics
- [ ] Test coverage increased to >85%
- [ ] Mutation score >80%
- [ ] Property test coverage for all core functions
- [ ] Zero critical/high vulnerabilities

### Observability Metrics
- [ ] End-to-end trace visibility for all operations
- [ ] <1 minute to identify issues using dashboards
- [ ] Centralized logs searchable within 5 seconds

### Developer Experience
- [ ] Pre-commit hooks running in <30 seconds
- [ ] CI/CD pipeline completing in <15 minutes
- [ ] Onboarding time for new developers reduced by 40%

---

## Implementation Sequence Summary

### Week 1-2: Foundation (Phase 1)
1. Pre-commit hooks
2. Connection pooling
3. CI/CD pipeline
4. Query optimization

### Week 3-5: Core Features (Phase 2)
1. Row-level reconciliation
2. Incremental checksums
3. Data transformation layer

### Week 6-7: Performance (Phase 3)
1. Parallel reconciliation
2. Load testing
3. Performance benchmarking

### Week 7-8: Quality (Phase 4)
1. Property-based testing
2. Mutation testing
3. Contract testing

### Week 8-9: Observability & Security (Phase 5)
1. Distributed tracing
2. Grafana dashboards
3. Vulnerability scanning
4. Log aggregation

---

## Next Steps

1. **Review & Approval** - Team review of this plan
2. **Environment Setup** - Prepare development/staging environments
3. **Kickoff Phase 1** - Begin with pre-commit hooks and connection pooling
4. **Weekly Checkpoints** - Review progress, adjust timeline
5. **Documentation** - Update docs as features are implemented

---

**Plan Version:** 1.0
**Last Updated:** 2025-12-21
**Next Review:** After Phase 1 completion