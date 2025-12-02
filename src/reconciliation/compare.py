"""
Row count and checksum comparison logic for data reconciliation

This module provides functions to compare data between source and target databases:
- Row count comparison
- Checksum validation
- Table-level data integrity checks
"""

from datetime import datetime
from typing import Dict, Any, Optional
import hashlib


def compare_row_counts(
    table_name: str,
    source_count: int,
    target_count: int
) -> Dict[str, Any]:
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
        raise ValueError("Row counts cannot be negative")

    difference = target_count - source_count
    match = source_count == target_count

    result = {
        "table": table_name,
        "source_count": source_count,
        "target_count": target_count,
        "match": match,
        "difference": difference,
        "status": "MATCH" if match else "MISMATCH",
        "timestamp": datetime.utcnow().isoformat()
    }

    return result


def compare_checksums(
    table_name: str,
    source_checksum: Optional[str],
    target_checksum: Optional[str]
) -> Dict[str, Any]:
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
        "timestamp": datetime.utcnow().isoformat()
    }

    return result


def get_row_count(cursor: Any, table_name: str) -> int:
    """
    Get row count for a table

    Args:
        cursor: Database cursor (pyodbc or psycopg2)
        table_name: Name of the table

    Returns:
        Row count as integer

    Raises:
        Exception: If query fails
    """
    # Handle different table name formats (e.g., schema.table)
    query = f"SELECT COUNT(*) FROM {table_name}"
    cursor.execute(query)
    result = cursor.fetchone()
    return int(result[0])


def calculate_checksum(cursor: Any, table_name: str, columns: Optional[list] = None) -> str:
    """
    Calculate checksum for a table

    This function generates a checksum by:
    1. Ordering all rows by primary key
    2. Concatenating all column values
    3. Computing MD5 hash of the concatenated string

    Args:
        cursor: Database cursor (pyodbc or psycopg2)
        table_name: Name of the table
        columns: Optional list of columns to include (default: all columns)

    Returns:
        MD5 checksum as hexadecimal string

    Raises:
        Exception: If query fails
    """
    # For simplicity, this is a basic implementation
    # In production, you might want to use database-native checksum functions

    if columns is None:
        # Get all columns
        if hasattr(cursor, 'description'):
            # Use introspection to get columns
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
            columns = [desc[0] for desc in cursor.description]
        else:
            # Fallback: use * for all columns
            columns = ["*"]

    # Build query
    column_list = ", ".join(columns) if columns != ["*"] else "*"
    query = f"SELECT {column_list} FROM {table_name} ORDER BY 1"

    cursor.execute(query)

    # Calculate checksum
    hasher = hashlib.md5()

    for row in cursor:
        # Convert row to string and update hash
        row_str = "|".join(str(val) if val is not None else "NULL" for val in row)
        hasher.update(row_str.encode('utf-8'))

    return hasher.hexdigest()


def reconcile_table(
    source_cursor: Any,
    target_cursor: Any,
    source_table: str,
    target_table: str,
    validate_checksum: bool = False,
    columns: Optional[list] = None
) -> Dict[str, Any]:
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
        "timestamp": datetime.utcnow().isoformat()
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
