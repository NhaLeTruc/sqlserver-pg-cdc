"""
Row-level reconciliation engine.

This module performs detailed row-by-row comparison between source and target databases,
identifying missing, extra, and modified rows using primary key matching.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from opentelemetry import trace
from prometheus_client import Counter, Histogram, REGISTRY

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
    primary_key: Dict[str, Any]
    discrepancy_type: str  # MISSING, EXTRA, MODIFIED
    source_data: Optional[Dict[str, Any]]
    target_data: Optional[Dict[str, Any]]
    modified_columns: Optional[List[str]] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
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
        pk_columns: List[str],
        compare_columns: Optional[List[str]] = None,
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
    ) -> List[RowDiscrepancy]:
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

    def _get_all_primary_keys(self, cursor: Any, table: str) -> Set[Tuple]:
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
        self, cursor: Any, table: str, pk: Tuple
    ) -> Dict[str, Any]:
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
        self, source_data: Dict[str, Any], target_data: Dict[str, Any]
    ) -> List[str]:
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

    def _pk_tuple_to_dict(self, pk: Tuple) -> Dict[str, Any]:
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
