"""
Checksum calculation for table data validation.

This module provides functions to calculate checksums for database tables
using SHA256 hashing, with support for both full and chunked processing.
"""

import hashlib
from typing import Any

from src.utils.retry import retry_database_operation

from .quoting import _get_db_type, _quote_identifier


def calculate_checksum(cursor: Any, table_name: str, columns: list | None = None) -> str:
    """
    Calculate checksum for a table using database-native identifier quoting

    This function generates a checksum by:
    1. Ordering all rows by primary key
    2. Concatenating all column values
    3. Computing SHA256 hash of the concatenated string

    Args:
        cursor: Database cursor (pyodbc or psycopg2)
        table_name: Name of the table
        columns: Optional list of columns to include (default: all columns)

    Returns:
        SHA256 checksum as hexadecimal string

    Raises:
        ValueError: If table_name or column names contain invalid characters
        Exception: If query fails
    """
    # Validate and quote table name using database-native quoting
    quoted_table = _quote_identifier(cursor, table_name)

    if columns is None:
        # Get all columns
        if hasattr(cursor, 'description') and cursor.description is not None:
            # Use introspection to get columns
            query = f"SELECT * FROM {quoted_table} LIMIT 0"
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
        else:
            # Fallback: use * for all columns
            columns = ["*"]

    # Build query with safely quoted identifiers
    if columns == ["*"]:
        column_list = "*"
    else:
        # Quote each column name
        quoted_columns = [_quote_identifier(cursor, col) for col in columns]
        column_list = ", ".join(quoted_columns)

    query = f"SELECT {column_list} FROM {quoted_table} ORDER BY 1"
    cursor.execute(query)

    # Calculate checksum using SHA256 (more secure than MD5)
    hasher = hashlib.sha256()

    for row in cursor:
        # Convert row to string and update hash
        row_str = "|".join(str(val) if val is not None else "NULL" for val in row)
        hasher.update(row_str.encode('utf-8'))

    return hasher.hexdigest()


def _get_primary_key_column(cursor: Any, table_name: str) -> str | None:
    """
    Get the primary key column for a table

    Args:
        cursor: Database cursor
        table_name: Name of the table

    Returns:
        Primary key column name, or None if not found
    """
    db_type = _get_db_type(cursor)

    try:
        if db_type == 'postgresql':
            # PostgreSQL primary key query
            schema_table = table_name.split('.')
            if len(schema_table) == 2:
                schema, table = schema_table
            else:
                schema, table = 'public', table_name

            query = """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = $1::regclass AND i.indisprimary
                LIMIT 1
            """
            cursor.execute(query, (f"{schema}.{table}",))
            result = cursor.fetchone()
            return result[0] if result else None

        else:  # SQL Server
            # SQL Server primary key query
            parts = table_name.replace('[', '').replace(']', '').split('.')
            if len(parts) == 2:
                schema, table = parts
            else:
                schema, table = 'dbo', parts[0]

            query = """
                SELECT c.name
                FROM sys.indexes i
                JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE i.is_primary_key = 1
                AND OBJECT_NAME(i.object_id) = ?
                AND SCHEMA_NAME(OBJECTPROPERTY(i.object_id, 'SchemaId')) = ?
                ORDER BY ic.key_ordinal
            """
            cursor.execute(query, (table, schema))
            result = cursor.fetchone()
            return result[0] if result else None

    except Exception:
        # If we can't determine primary key, return None
        return None


@retry_database_operation(max_retries=3, base_delay=1.0)
def _execute_chunked_checksum_query(
    cursor: Any,
    quoted_table: str,
    column_list: str,
    pk_column: str,
    offset: int,
    chunk_size: int
) -> list:
    """
    Execute chunked checksum query with retry logic

    Internal function with database retry logic applied.
    """
    db_type = _get_db_type(cursor)

    if db_type == 'postgresql':
        query = f"""
            SELECT {column_list}
            FROM {quoted_table}
            ORDER BY {pk_column}
            LIMIT {chunk_size} OFFSET {offset}
        """
    else:  # SQL Server
        query = f"""
            SELECT {column_list}
            FROM {quoted_table}
            ORDER BY {pk_column}
            OFFSET {offset} ROWS
            FETCH NEXT {chunk_size} ROWS ONLY
        """

    cursor.execute(query)
    return cursor.fetchall()


def calculate_checksum_chunked(
    cursor: Any,
    table_name: str,
    columns: list | None = None,
    chunk_size: int = 10000
) -> str:
    """
    Calculate checksum for a large table using chunked processing

    Processes table in chunks to avoid memory exhaustion on large tables.
    Memory usage bounded to chunk_size * row_size.

    Args:
        cursor: Database cursor (pyodbc or psycopg2)
        table_name: Name of the table
        columns: Optional list of columns to include (default: all columns)
        chunk_size: Number of rows to process per chunk (default: 10000)

    Returns:
        SHA256 checksum as hexadecimal string

    Raises:
        ValueError: If table_name or column names contain invalid characters
        Exception: If query fails or primary key not found
    """
    # Validate and quote table name
    quoted_table = _quote_identifier(cursor, table_name)

    # Get primary key for ordering
    pk_column = _get_primary_key_column(cursor, table_name)
    if not pk_column:
        # Fallback to column 1 if no primary key
        pk_column = "1"

    # Get columns if not specified
    if columns is None:
        if hasattr(cursor, 'description') and cursor.description is not None:
            query = f"SELECT * FROM {quoted_table} LIMIT 0"
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
        else:
            columns = ["*"]

    # Build column list
    if columns == ["*"]:
        column_list = "*"
    else:
        quoted_columns = [_quote_identifier(cursor, col) for col in columns]
        column_list = ", ".join(quoted_columns)

    # Calculate checksum in chunks
    hasher = hashlib.sha256()
    offset = 0
    total_rows = 0

    while True:
        # Fetch chunk with retry logic
        rows = _execute_chunked_checksum_query(
            cursor, quoted_table, column_list, pk_column, offset, chunk_size
        )

        if not rows:
            break

        # Process chunk
        for row in rows:
            row_str = "|".join(str(val) if val is not None else "NULL" for val in row)
            hasher.update(row_str.encode('utf-8'))
            total_rows += 1

        offset += chunk_size

        # Break if we got fewer rows than chunk_size (last chunk)
        if len(rows) < chunk_size:
            break

    return hasher.hexdigest()
