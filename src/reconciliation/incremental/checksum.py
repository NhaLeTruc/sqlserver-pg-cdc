"""
Incremental checksum calculation using CDC metadata.

Only checksums rows modified since last reconciliation run.
Provides 10-100x speedup on large tables with few changes.
"""

import hashlib
import logging
from datetime import datetime
from typing import Any, Optional, Tuple

from opentelemetry import trace
from prometheus_client import Counter, Histogram

from src.utils.tracing import trace_operation
from .state import IncrementalChecksumTracker

logger = logging.getLogger(__name__)


# Metrics
INCREMENTAL_CHECKSUM_ROWS = Counter(
    "incremental_checksum_rows_total",
    "Total rows processed in incremental checksum",
    ["table", "mode"],  # mode: full or incremental
)

INCREMENTAL_CHECKSUM_TIME = Histogram(
    "incremental_checksum_seconds",
    "Time to calculate incremental checksum",
    ["table", "mode"],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0],
)


def calculate_incremental_checksum(
    cursor: Any,
    table_name: str,
    pk_column: str,
    last_checksum_time: Optional[datetime] = None,
    change_tracking_column: str = "updated_at",
    tracker: Optional[IncrementalChecksumTracker] = None,
) -> Tuple[str, int]:
    """
    Calculate checksum only for rows changed since last run.

    This provides significant performance improvements (10-100x) for large
    tables with relatively few changes between reconciliation runs.

    Args:
        cursor: Database cursor
        table_name: Table to checksum
        pk_column: Primary key column for ordering
        last_checksum_time: Timestamp of last checksum (None for full checksum)
        change_tracking_column: Column tracking modification time
        tracker: Optional checksum tracker for state management

    Returns:
        Tuple of (checksum_hash, row_count)
    """
    with trace_operation(
        "calculate_incremental_checksum",
        kind=trace.SpanKind.CLIENT,
        table=table_name,
        mode="incremental" if last_checksum_time else "full",
    ):
        db_type = _get_db_type(cursor)
        quoted_table = _quote_identifier(cursor, table_name, db_type)
        quoted_pk = _quote_identifier(cursor, pk_column, db_type)
        quoted_change_col = _quote_identifier(cursor, change_tracking_column, db_type)

        mode = "incremental" if last_checksum_time else "full"

        with INCREMENTAL_CHECKSUM_TIME.labels(table=table_name, mode=mode).time():
            if last_checksum_time is None:
                # Full checksum on first run
                logger.info(f"Calculating full checksum for {table_name}")
                checksum, row_count = _calculate_full_checksum(
                    cursor, quoted_table, quoted_pk, db_type
                )
            else:
                # Incremental checksum
                logger.info(
                    f"Calculating incremental checksum for {table_name} "
                    f"since {last_checksum_time.isoformat()}"
                )
                checksum, row_count = _calculate_delta_checksum(
                    cursor,
                    quoted_table,
                    quoted_pk,
                    quoted_change_col,
                    last_checksum_time,
                    db_type,
                )

            INCREMENTAL_CHECKSUM_ROWS.labels(table=table_name, mode=mode).inc(row_count)

            # Save state if tracker provided
            if tracker:
                tracker.save_checksum_state(
                    table=table_name,
                    checksum=checksum,
                    row_count=row_count,
                    mode=mode,
                )

            logger.info(
                f"Checksum calculated for {table_name}: "
                f"{checksum[:16]}... ({row_count} rows, {mode} mode)"
            )

            return checksum, row_count


def _calculate_full_checksum(
    cursor: Any, quoted_table: str, quoted_pk: str, db_type: str
) -> Tuple[str, int]:
    """Calculate checksum for all rows in table."""
    query = f"SELECT * FROM {quoted_table} ORDER BY {quoted_pk}"

    cursor.execute(query)

    hasher = hashlib.sha256()
    row_count = 0

    for row in cursor:
        # Convert row to string representation
        row_str = "|".join(str(val) if val is not None else "NULL" for val in row)
        hasher.update(row_str.encode("utf-8"))
        row_count += 1

        # Log progress for large tables
        if row_count % 100000 == 0:
            logger.debug(f"Processed {row_count} rows...")

    return hasher.hexdigest(), row_count


def _calculate_delta_checksum(
    cursor: Any,
    quoted_table: str,
    quoted_pk: str,
    quoted_change_col: str,
    since_timestamp: datetime,
    db_type: str,
) -> Tuple[str, int]:
    """Calculate checksum only for changed rows."""
    # Build incremental query
    if db_type == "postgresql":
        placeholder = "%s"
    else:  # SQL Server
        placeholder = "?"

    query = f"""
        SELECT * FROM {quoted_table}
        WHERE {quoted_change_col} > {placeholder}
        ORDER BY {quoted_pk}
    """

    cursor.execute(query, (since_timestamp,))

    hasher = hashlib.sha256()
    row_count = 0

    for row in cursor:
        row_str = "|".join(str(val) if val is not None else "NULL" for val in row)
        hasher.update(row_str.encode("utf-8"))
        row_count += 1

    return hasher.hexdigest(), row_count


def calculate_checksum_chunked(
    cursor: Any,
    table_name: str,
    pk_column: str,
    chunk_size: int = 10000,
) -> str:
    """
    Calculate checksum in chunks for memory efficiency.

    Useful for very large tables where fetching all rows at once
    would consume too much memory.

    Args:
        cursor: Database cursor
        table_name: Table to checksum
        pk_column: Primary key column for ordering
        chunk_size: Number of rows per chunk

    Returns:
        Checksum hash
    """
    with trace_operation(
        "calculate_checksum_chunked",
        kind=trace.SpanKind.CLIENT,
        table=table_name,
        chunk_size=chunk_size,
    ):
        db_type = _get_db_type(cursor)
        quoted_table = _quote_identifier(cursor, table_name, db_type)
        quoted_pk = _quote_identifier(cursor, pk_column, db_type)

        hasher = hashlib.sha256()
        total_rows = 0
        offset = 0

        while True:
            # Fetch chunk
            if db_type == "postgresql":
                query = f"""
                    SELECT * FROM {quoted_table}
                    ORDER BY {quoted_pk}
                    LIMIT {chunk_size} OFFSET {offset}
                """
            else:  # SQL Server
                query = f"""
                    SELECT * FROM {quoted_table}
                    ORDER BY {quoted_pk}
                    OFFSET {offset} ROWS
                    FETCH NEXT {chunk_size} ROWS ONLY
                """

            cursor.execute(query)
            rows = cursor.fetchall()

            if not rows:
                break

            # Hash chunk
            for row in rows:
                row_str = "|".join(str(val) if val is not None else "NULL" for val in row)
                hasher.update(row_str.encode("utf-8"))

            rows_in_chunk = len(rows)
            total_rows += rows_in_chunk
            offset += rows_in_chunk

            logger.debug(f"Processed chunk: {total_rows} rows so far...")

            # Stop if we got less than chunk_size (last chunk)
            if rows_in_chunk < chunk_size:
                break

        logger.info(f"Chunked checksum complete: {total_rows} total rows")
        return hasher.hexdigest()


def _get_db_type(cursor: Any) -> str:
    """Detect database type from cursor."""
    cursor_class_name = cursor.__class__.__name__.lower()

    if "psycopg" in cursor_class_name or "postgres" in cursor_class_name:
        return "postgresql"
    elif "pyodbc" in cursor_class_name or "odbc" in cursor_class_name:
        return "sqlserver"
    else:
        return "unknown"


def _quote_identifier(cursor: Any, identifier: str, db_type: str) -> str:
    """Quote identifier based on database type."""
    if db_type == "postgresql":
        return f'"{identifier}"'
    elif db_type == "sqlserver":
        return f"[{identifier}]"
    else:
        return identifier
