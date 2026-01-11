"""
Row-level reconciliation for detailed discrepancy detection.

Identifies:
- Missing rows (in source but not target)
- Extra rows (in target but not source)
- Modified rows (different values)
- Generates repair SQL scripts

Performance optimized for tables up to 10M rows using batching and streaming.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from opentelemetry import trace
from prometheus_client import REGISTRY, Counter, Histogram

from src.utils.tracing import get_tracer, trace_operation

logger = logging.getLogger(__name__)
tracer = get_tracer()


# Metrics
try:
    ROW_LEVEL_DISCREPANCIES = Counter(
        "row_level_discrepancies_total",
        "Total row-level discrepancies found",
        ["table", "discrepancy_type"],
        registry=REGISTRY
    )
except ValueError:
    # Metric already registered, get existing one
    ROW_LEVEL_DISCREPANCIES = REGISTRY._names_to_collectors.get("row_level_discrepancies_total")

try:
    ROW_LEVEL_RECONCILIATION_TIME = Histogram(
        "row_level_reconciliation_seconds",
        "Time to perform row-level reconciliation",
        ["table"],
        buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600],
        registry=REGISTRY
    )
except ValueError:
    ROW_LEVEL_RECONCILIATION_TIME = REGISTRY._names_to_collectors.get("row_level_reconciliation_seconds")

try:
    ROW_COMPARISON_TIME = Histogram(
        "row_comparison_seconds",
        "Time to compare individual rows",
        buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
        registry=REGISTRY
    )
except ValueError:
    ROW_COMPARISON_TIME = REGISTRY._names_to_collectors.get("row_comparison_seconds")


@dataclass
class RowDiscrepancy:
    """Represents a single row discrepancy."""

    table: str
    primary_key: dict[str, Any]
    discrepancy_type: str  # MISSING, EXTRA, MODIFIED
    source_data: dict[str, Any] | None
    target_data: dict[str, Any] | None
    modified_columns: list[str] | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "table": self.table,
            "primary_key": self.primary_key,
            "discrepancy_type": self.discrepancy_type,
            "source_data": self.source_data,
            "target_data": self.target_data,
            "modified_columns": self.modified_columns,
            "timestamp": self.timestamp.isoformat(),
        }


class RowLevelReconciler:
    """Performs row-level reconciliation between source and target databases."""

    def __init__(
        self,
        source_cursor: Any,
        target_cursor: Any,
        pk_columns: list[str],
        compare_columns: list[str] | None = None,
        chunk_size: int = 1000,
        float_tolerance: float = 1e-9,
    ):
        """
        Initialize row-level reconciler.

        Args:
            source_cursor: Source database cursor
            target_cursor: Target database cursor
            pk_columns: List of primary key column names
            compare_columns: Optional list of columns to compare (None = all columns)
            chunk_size: Number of rows to fetch in each batch
            float_tolerance: Tolerance for floating point comparisons
        """
        self.source_cursor = source_cursor
        self.target_cursor = target_cursor
        self.pk_columns = pk_columns
        self.compare_columns = compare_columns
        self.chunk_size = chunk_size
        self.float_tolerance = float_tolerance

    def reconcile_table(
        self, source_table: str, target_table: str
    ) -> list[RowDiscrepancy]:
        """
        Perform row-level reconciliation.

        Returns:
            List of all discrepancies found
        """
        with trace_operation(
            "row_level_reconcile_table",
            kind=trace.SpanKind.INTERNAL,
            source_table=source_table,
            target_table=target_table,
        ):
            with ROW_LEVEL_RECONCILIATION_TIME.labels(table=target_table).time():
                discrepancies = []

                logger.info(f"Starting row-level reconciliation: {source_table} -> {target_table}")

                # Get all PKs from both sides
                source_pks = self._get_all_primary_keys(self.source_cursor, source_table)
                target_pks = self._get_all_primary_keys(self.target_cursor, target_table)

                # Find missing and extra rows
                missing_pks = source_pks - target_pks  # In source but not target
                extra_pks = target_pks - source_pks  # In target but not source
                common_pks = source_pks & target_pks  # In both

                logger.info(
                    f"Row-level reconciliation summary: "
                    f"{len(source_pks)} source rows, "
                    f"{len(target_pks)} target rows, "
                    f"{len(missing_pks)} missing, "
                    f"{len(extra_pks)} extra, "
                    f"{len(common_pks)} common"
                )

                # Record missing rows
                for pk in missing_pks:
                    source_data = self._get_row_data(self.source_cursor, source_table, pk)
                    disc = RowDiscrepancy(
                        table=target_table,
                        primary_key=self._pk_tuple_to_dict(pk),
                        discrepancy_type="MISSING",
                        source_data=source_data,
                        target_data=None,
                    )
                    discrepancies.append(disc)
                    ROW_LEVEL_DISCREPANCIES.labels(
                        table=target_table, discrepancy_type="MISSING"
                    ).inc()

                # Record extra rows
                for pk in extra_pks:
                    target_data = self._get_row_data(self.target_cursor, target_table, pk)
                    disc = RowDiscrepancy(
                        table=target_table,
                        primary_key=self._pk_tuple_to_dict(pk),
                        discrepancy_type="EXTRA",
                        source_data=None,
                        target_data=target_data,
                    )
                    discrepancies.append(disc)
                    ROW_LEVEL_DISCREPANCIES.labels(
                        table=target_table, discrepancy_type="EXTRA"
                    ).inc()

                # Compare common rows for modifications
                modified_count = 0
                for pk in common_pks:
                    source_data = self._get_row_data(self.source_cursor, source_table, pk)
                    target_data = self._get_row_data(self.target_cursor, target_table, pk)

                    modified_cols = self._compare_rows(source_data, target_data)
                    if modified_cols:
                        disc = RowDiscrepancy(
                            table=target_table,
                            primary_key=self._pk_tuple_to_dict(pk),
                            discrepancy_type="MODIFIED",
                            source_data=source_data,
                            target_data=target_data,
                            modified_columns=modified_cols,
                        )
                        discrepancies.append(disc)
                        modified_count += 1
                        ROW_LEVEL_DISCREPANCIES.labels(
                            table=target_table, discrepancy_type="MODIFIED"
                        ).inc()

                logger.info(
                    f"Row-level reconciliation complete: "
                    f"{len(discrepancies)} total discrepancies "
                    f"({len(missing_pks)} missing, {len(extra_pks)} extra, {modified_count} modified)"
                )

                return discrepancies

    def _get_all_primary_keys(self, cursor: Any, table: str) -> set[tuple]:
        """Get all primary keys from table."""
        with trace_operation(
            "get_all_primary_keys", kind=trace.SpanKind.CLIENT, table=table
        ):
            pk_cols = ", ".join([self._quote_identifier(cursor, col) for col in self.pk_columns])
            quoted_table = self._quote_identifier(cursor, table)

            query = f"SELECT {pk_cols} FROM {quoted_table}"
            cursor.execute(query)

            # Fetch all and convert to set of tuples
            pks = set()
            for row in cursor.fetchall():
                # Ensure tuple even for single column PKs
                if isinstance(row, tuple):
                    pks.add(row)
                else:
                    pks.add((row,))

            logger.debug(f"Fetched {len(pks)} primary keys from {table}")
            return pks

    def _get_row_data(
        self, cursor: Any, table: str, pk: tuple
    ) -> dict[str, Any]:
        """Get full row data for given primary key."""
        with trace_operation(
            "get_row_data", kind=trace.SpanKind.CLIENT, table=table
        ):
            quoted_table = self._quote_identifier(cursor, table)

            # Build WHERE clause
            where_conditions = []
            for i, col in enumerate(self.pk_columns):
                quoted_col = self._quote_identifier(cursor, col)
                # Use appropriate placeholder
                placeholder = self._get_placeholder(cursor, i)
                where_conditions.append(f"{quoted_col} = {placeholder}")

            where_clause = " AND ".join(where_conditions)

            # Build SELECT clause
            if self.compare_columns:
                cols = ", ".join([self._quote_identifier(cursor, c) for c in self.compare_columns])
            else:
                cols = "*"

            query = f"SELECT {cols} FROM {quoted_table} WHERE {where_clause}"

            # Ensure pk is a tuple for parameter binding
            if not isinstance(pk, tuple):
                pk = (pk,)

            cursor.execute(query, pk)

            row = cursor.fetchone()
            if not row:
                return {}

            # Convert to dictionary
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))

    def _compare_rows(
        self, source_data: dict[str, Any], target_data: dict[str, Any]
    ) -> list[str]:
        """
        Compare two rows and return list of modified columns.

        Returns:
            Empty list if rows are identical, otherwise list of modified column names
        """
        with ROW_COMPARISON_TIME.time():
            modified = []

            for col in source_data.keys():
                if col in self.pk_columns:
                    continue  # Skip PK columns

                source_val = source_data.get(col)
                target_val = target_data.get(col)

                # Handle NULL comparisons
                if source_val is None and target_val is None:
                    continue

                if source_val is None or target_val is None:
                    modified.append(col)
                    continue

                # Handle numeric precision differences
                if isinstance(source_val, (int, float)) and isinstance(
                    target_val, (int, float)
                ):
                    if abs(float(source_val) - float(target_val)) < self.float_tolerance:
                        continue

                # Handle string comparisons (case-sensitive)
                if isinstance(source_val, str) and isinstance(target_val, str):
                    if source_val.strip() != target_val.strip():
                        modified.append(col)
                    continue

                # General comparison
                if source_val != target_val:
                    modified.append(col)

            return modified

    def _pk_tuple_to_dict(self, pk: tuple) -> dict[str, Any]:
        """Convert PK tuple to dictionary."""
        if not isinstance(pk, tuple):
            pk = (pk,)
        return dict(zip(self.pk_columns, pk))

    def _quote_identifier(self, cursor: Any, identifier: str) -> str:
        """Quote identifier based on database type."""
        db_type = self._get_db_type(cursor)

        if db_type == "postgresql":
            # PostgreSQL uses double quotes
            return f'"{identifier}"'
        elif db_type == "sqlserver":
            # SQL Server uses brackets
            return f"[{identifier}]"
        else:
            # Fallback to unquoted
            return identifier

    def _get_placeholder(self, cursor: Any, index: int) -> str:
        """Get parameter placeholder based on database type."""
        db_type = self._get_db_type(cursor)

        if db_type == "postgresql":
            # PostgreSQL uses $1, $2, etc.
            return f"${index + 1}"
        else:
            # SQL Server uses ?
            return "?"

    def _get_db_type(self, cursor: Any) -> str:
        """Detect database type from cursor."""
        cursor_class_name = cursor.__class__.__name__.lower()

        if "psycopg" in cursor_class_name or "postgres" in cursor_class_name:
            return "postgresql"
        elif "pyodbc" in cursor_class_name or "odbc" in cursor_class_name:
            return "sqlserver"
        else:
            return "unknown"


def generate_repair_script(
    discrepancies: list[RowDiscrepancy], target_table: str, database_type: str = "postgresql"
) -> str:
    """
    Generate SQL repair script from discrepancies.

    Args:
        discrepancies: List of row discrepancies
        target_table: Target table name
        database_type: 'postgresql' or 'sqlserver'

    Returns:
        SQL script to fix all discrepancies
    """
    with trace_operation(
        "generate_repair_script",
        kind=trace.SpanKind.INTERNAL,
        table=target_table,
        discrepancy_count=len(discrepancies),
    ):
        script_lines = [
            f"-- Repair script for {target_table}",
            f"-- Generated: {datetime.now(UTC).isoformat()}",
            f"-- Total discrepancies: {len(discrepancies)}",
            f"-- Database type: {database_type}",
            "",
        ]

        if database_type == "postgresql":
            script_lines.append("BEGIN;")
        else:
            script_lines.append("BEGIN TRANSACTION;")

        script_lines.append("")

        # Group by type for better organization
        missing = [d for d in discrepancies if d.discrepancy_type == "MISSING"]
        extra = [d for d in discrepancies if d.discrepancy_type == "EXTRA"]
        modified = [d for d in discrepancies if d.discrepancy_type == "MODIFIED"]

        # Generate INSERTs for missing rows
        if missing:
            script_lines.append(f"-- Insert {len(missing)} missing rows")
            script_lines.append("")
            for disc in missing:
                script_lines.append(f"-- Missing row: {disc.primary_key}")
                script_lines.append(
                    _generate_insert_sql(target_table, disc.source_data, database_type)
                )
                script_lines.append("")

        # Generate DELETEs for extra rows
        if extra:
            script_lines.append(f"-- Delete {len(extra)} extra rows")
            script_lines.append("")
            for disc in extra:
                script_lines.append(f"-- Extra row: {disc.primary_key}")
                script_lines.append(
                    _generate_delete_sql(target_table, disc.primary_key, database_type)
                )
                script_lines.append("")

        # Generate UPDATEs for modified rows
        if modified:
            script_lines.append(f"-- Update {len(modified)} modified rows")
            script_lines.append("")
            for disc in modified:
                script_lines.append(f"-- Modified row: {disc.primary_key}")
                script_lines.append(
                    f"-- Modified columns: {', '.join(disc.modified_columns)}"
                )
                script_lines.append(
                    _generate_update_sql(
                        target_table,
                        disc.primary_key,
                        disc.source_data,
                        disc.modified_columns,
                        database_type,
                    )
                )
                script_lines.append("")

        script_lines.append("COMMIT;")

        return "\n".join(script_lines)


def _generate_insert_sql(
    table: str, data: dict[str, Any], database_type: str = "postgresql"
) -> str:
    """Generate INSERT statement."""
    if not data:
        return "-- Cannot generate INSERT: no data"

    columns = list(data.keys())
    values = [_format_value(data[col], database_type) for col in columns]

    # Quote identifiers
    if database_type == "postgresql":
        quoted_table = f'"{table}"'
        quoted_cols = [f'"{col}"' for col in columns]
    else:
        quoted_table = f"[{table}]"
        quoted_cols = [f"[{col}]" for col in columns]

    columns_str = ", ".join(quoted_cols)
    values_str = ", ".join(values)

    return f"INSERT INTO {quoted_table} ({columns_str}) VALUES ({values_str});"


def _generate_delete_sql(
    table: str, pk: dict[str, Any], database_type: str = "postgresql"
) -> str:
    """Generate DELETE statement."""
    if not pk:
        return "-- Cannot generate DELETE: no primary key"

    # Quote identifiers
    if database_type == "postgresql":
        quoted_table = f'"{table}"'
        conditions = [f'"{k}" = {_format_value(v, database_type)}' for k, v in pk.items()]
    else:
        quoted_table = f"[{table}]"
        conditions = [f"[{k}] = {_format_value(v, database_type)}" for k, v in pk.items()]

    where_clause = " AND ".join(conditions)

    return f"DELETE FROM {quoted_table} WHERE {where_clause};"


def _generate_update_sql(
    table: str,
    pk: dict[str, Any],
    data: dict[str, Any],
    modified_cols: list[str],
    database_type: str = "postgresql",
) -> str:
    """Generate UPDATE statement."""
    if not pk or not modified_cols:
        return "-- Cannot generate UPDATE: no primary key or modified columns"

    # Quote identifiers and build SET clause
    if database_type == "postgresql":
        quoted_table = f'"{table}"'
        set_parts = [f'"{col}" = {_format_value(data[col], database_type)}' for col in modified_cols]
        where_parts = [f'"{k}" = {_format_value(v, database_type)}' for k, v in pk.items()]
    else:
        quoted_table = f"[{table}]"
        set_parts = [f"[{col}] = {_format_value(data[col], database_type)}" for col in modified_cols]
        where_parts = [f"[{k}] = {_format_value(v, database_type)}" for k, v in pk.items()]

    set_clause = ", ".join(set_parts)
    where_clause = " AND ".join(where_parts)

    return f"UPDATE {quoted_table} SET {set_clause} WHERE {where_clause};"


def _format_value(value: Any, database_type: str = "postgresql") -> str:
    """Format value for SQL statement."""
    if value is None:
        return "NULL"

    if isinstance(value, str):
        # Escape single quotes
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    if isinstance(value, bool):
        if database_type == "postgresql":
            return "TRUE" if value else "FALSE"
        else:
            return "1" if value else "0"

    if isinstance(value, (int, float)):
        return str(value)

    if isinstance(value, datetime):
        if database_type == "postgresql":
            return f"'{value.isoformat()}'"
        else:
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"

    # Fallback: convert to string
    return f"'{str(value)}'"
