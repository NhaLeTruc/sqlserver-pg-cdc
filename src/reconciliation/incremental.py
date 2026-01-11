"""
Incremental checksum calculation using CDC metadata.

Only checksums rows modified since last reconciliation run.
Stores checksum state for incremental updates to achieve 10-100x speedup
on large tables with few changes.
"""

import hashlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from opentelemetry import trace
from prometheus_client import Counter, Histogram

from src.utils.tracing import get_tracer, trace_operation

logger = logging.getLogger(__name__)
tracer = get_tracer()


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

CHECKSUM_STATE_OPERATIONS = Counter(
    "checksum_state_operations_total",
    "Checksum state file operations",
    ["operation"],  # load, save
)


class IncrementalChecksumTracker:
    """
    Tracks checksum state for incremental updates.

    Stores last checksum calculation timestamp, checksum value,
    and row count for each table to enable delta processing.
    """

    def __init__(self, state_dir: str = "./reconciliation_state"):
        """
        Initialize checksum tracker.

        Args:
            state_dir: Directory to store state files
        """
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized checksum tracker with state dir: {self.state_dir}")

    def get_last_checksum_timestamp(self, table: str) -> datetime | None:
        """
        Get timestamp of last checksum calculation.

        Args:
            table: Table name

        Returns:
            Timestamp of last checksum, or None if never calculated
        """
        with trace_operation(
            "get_last_checksum_timestamp",
            kind=trace.SpanKind.INTERNAL,
            table=table,
        ):
            state_file = self._get_state_file(table)

            if not state_file.exists():
                logger.debug(f"No previous checksum state for table {table}")
                return None

            try:
                with open(state_file) as f:
                    state = json.load(f)

                last_run = datetime.fromisoformat(state["last_run"])
                logger.debug(f"Last checksum for {table}: {last_run.isoformat()}")

                CHECKSUM_STATE_OPERATIONS.labels(operation="load").inc()
                return last_run

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning(f"Failed to load checksum state for {table}: {e}")
                return None

    def get_last_checksum(self, table: str) -> str | None:
        """
        Get last calculated checksum value.

        Args:
            table: Table name

        Returns:
            Last checksum value, or None if never calculated
        """
        state_file = self._get_state_file(table)

        if not state_file.exists():
            return None

        try:
            with open(state_file) as f:
                state = json.load(f)
            return state.get("checksum")
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to load checksum for {table}: {e}")
            return None

    def save_checksum_state(
        self,
        table: str,
        checksum: str,
        row_count: int,
        timestamp: datetime | None = None,
        mode: str = "full",
    ) -> None:
        """
        Save checksum state for table.

        Args:
            table: Table name
            checksum: Calculated checksum
            row_count: Number of rows processed
            timestamp: Timestamp of calculation (defaults to now)
            mode: Calculation mode ('full' or 'incremental')
        """
        with trace_operation(
            "save_checksum_state",
            kind=trace.SpanKind.INTERNAL,
            table=table,
            mode=mode,
        ):
            if timestamp is None:
                timestamp = datetime.now(UTC)

            state_file = self._get_state_file(table)

            state = {
                "table": table,
                "checksum": checksum,
                "row_count": row_count,
                "last_run": timestamp.isoformat(),
                "mode": mode,
            }

            try:
                with open(state_file, "w") as f:
                    json.dump(state, f, indent=2)

                logger.info(
                    f"Saved checksum state for {table}: "
                    f"{row_count} rows, mode={mode}"
                )

                CHECKSUM_STATE_OPERATIONS.labels(operation="save").inc()

            except Exception as e:
                logger.error(f"Failed to save checksum state for {table}: {e}")
                raise

    def clear_state(self, table: str) -> None:
        """
        Clear saved state for a table.

        Args:
            table: Table name
        """
        state_file = self._get_state_file(table)

        if state_file.exists():
            state_file.unlink()
            logger.info(f"Cleared checksum state for table {table}")

    def list_tracked_tables(self) -> list[str]:
        """
        List all tables with saved checksum state.

        Returns:
            List of table names
        """
        tables = []

        for state_file in self.state_dir.glob("*_checksum_state.json"):
            table_name = state_file.stem.replace("_checksum_state", "")
            tables.append(table_name)

        return sorted(tables)

    def _get_state_file(self, table: str) -> Path:
        """Get state file path for table."""
        # Sanitize table name for filesystem
        safe_table_name = table.replace("/", "_").replace("\\", "_")
        return self.state_dir / f"{safe_table_name}_checksum_state.json"


def calculate_incremental_checksum(
    cursor: Any,
    table_name: str,
    pk_column: str,
    last_checksum_time: datetime | None = None,
    change_tracking_column: str = "updated_at",
    tracker: IncrementalChecksumTracker | None = None,
) -> tuple[str, int]:
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
) -> tuple[str, int]:
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
) -> tuple[str, int]:
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
