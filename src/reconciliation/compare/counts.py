"""
Row count comparison and table reconciliation logic.

This module provides functions to compare row counts and perform
full table reconciliation including optional checksum validation.
"""

from datetime import UTC, datetime
from typing import Any

from utils.retry import retry_database_operation

from .checksum import calculate_checksum
from .quoting import _quote_identifier


def compare_row_counts(
    table_name: str,
    source_count: int,
    target_count: int
) -> dict[str, Any]:
    """
    Compare row counts between source and target tables

    Args:
        table_name: Name of the table being compared
        source_count: Row count from source database
        target_count: Row count from target database

    Returns:
        Dictionary containing comparison results:
        - table: Table name
        - source_count: Source row count
        - target_count: Target row count
        - match: Boolean indicating if counts match
        - difference: Difference (target - source)
        - status: MATCH or MISMATCH
        - timestamp: ISO format timestamp

    Raises:
        ValueError: If row counts are negative
    """
    if source_count < 0 or target_count < 0:
        raise ValueError(
            f"Row counts cannot be negative: source={source_count}, target={target_count}"
        )

    difference = target_count - source_count
    match = source_count == target_count

    result = {
        "table": table_name,
        "source_count": source_count,
        "target_count": target_count,
        "match": match,
        "difference": difference,
        "status": "MATCH" if match else "MISMATCH",
        "timestamp": datetime.now(UTC).isoformat()
    }

    return result


def compare_checksums(
    table_name: str,
    source_checksum: str | None,
    target_checksum: str | None
) -> dict[str, Any]:
    """
    Compare checksums between source and target tables

    Args:
        table_name: Name of the table being compared
        source_checksum: Checksum from source database
        target_checksum: Checksum from target database

    Returns:
        Dictionary containing comparison results:
        - table: Table name
        - source_checksum: Source checksum
        - target_checksum: Target checksum
        - match: Boolean indicating if checksums match
        - status: MATCH or MISMATCH
        - timestamp: ISO format timestamp

    Raises:
        ValueError: If checksums are None
    """
    if source_checksum is None or target_checksum is None:
        raise ValueError("Checksums cannot be None")

    match = source_checksum == target_checksum

    result = {
        "table": table_name,
        "source_checksum": source_checksum,
        "target_checksum": target_checksum,
        "match": match,
        "status": "MATCH" if match else "MISMATCH",
        "timestamp": datetime.now(UTC).isoformat()
    }

    return result


@retry_database_operation(max_retries=3, base_delay=1.0)
def _execute_row_count_query(cursor: Any, quoted_table: str) -> int:
    """
    Execute row count query with retry logic

    Internal function with database retry logic applied.
    Retries on transient connection/timeout errors.
    """
    query = f"SELECT COUNT(*) FROM {quoted_table}"
    cursor.execute(query)
    result = cursor.fetchone()
    return int(result[0])


def get_row_count(cursor: Any, table_name: str) -> int:
    """
    Get row count for a table using database-native identifier quoting

    Args:
        cursor: Database cursor (pyodbc or psycopg2)
        table_name: Name of the table

    Returns:
        Row count as integer

    Raises:
        ValueError: If table_name contains invalid characters
        Exception: If query fails (after retries)
    """
    # Validate and quote table name using database-native quoting
    quoted_table = _quote_identifier(cursor, table_name)

    # Execute with retry logic
    return _execute_row_count_query(cursor, quoted_table)


def reconcile_table(
    source_cursor: Any,
    target_cursor: Any,
    source_table: str,
    target_table: str,
    validate_checksum: bool = False,
    columns: list | None = None
) -> dict[str, Any]:
    """
    Perform full reconciliation for a single table

    Args:
        source_cursor: Source database cursor
        target_cursor: Target database cursor
        source_table: Source table name
        target_table: Target table name
        validate_checksum: Whether to validate checksums (slower but more thorough)
        columns: Optional list of columns to validate

    Returns:
        Dictionary containing:
        - table: Table name
        - source_count: Source row count
        - target_count: Target row count
        - match: Boolean indicating if row counts match
        - difference: Row count difference
        - source_checksum: Source checksum (if validate_checksum=True)
        - target_checksum: Target checksum (if validate_checksum=True)
        - checksum_match: Boolean (if validate_checksum=True)
        - timestamp: ISO format timestamp
    """
    # Get row counts
    source_count = get_row_count(source_cursor, source_table)
    target_count = get_row_count(target_cursor, target_table)

    result = {
        "table": target_table,
        "source_count": source_count,
        "target_count": target_count,
        "match": source_count == target_count,
        "difference": target_count - source_count,
        "timestamp": datetime.now(UTC).isoformat()
    }

    # Validate checksums if requested
    if validate_checksum:
        source_checksum = calculate_checksum(source_cursor, source_table, columns)
        target_checksum = calculate_checksum(target_cursor, target_table, columns)

        result["source_checksum"] = source_checksum
        result["target_checksum"] = target_checksum
        result["checksum_match"] = source_checksum == target_checksum

        # Overall match only if both counts and checksums match
        result["match"] = result["match"] and result["checksum_match"]

    return result
