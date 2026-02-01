"""
SQL identifier quoting for SQL injection protection.

This module provides database-specific identifier quoting to prevent SQL injection
while maintaining support for both PostgreSQL and SQL Server.
"""

import re
from typing import Any

try:
    from psycopg2 import sql
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False


# SEC-6: Strict ASCII-only pattern for SQL identifiers (no Unicode via \w)
VALID_IDENTIFIER_PATTERN = re.compile(
    r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$'
)


def _quote_postgres_identifier(identifier: str, cursor: Any = None) -> str:
    """
    Quote PostgreSQL identifier using psycopg2.sql.Identifier for SQL injection protection

    Args:
        identifier: Table or column name (may include schema, e.g., 'schema.table')
        cursor: Optional cursor for psycopg2 quoting (if None, uses manual quoting)

    Returns:
        Safely quoted identifier string

    Raises:
        ValueError: If identifier format is invalid
    """
    # Validate identifier format first (strip brackets for SQL Server compatibility)
    clean_identifier = identifier.replace('[', '').replace(']', '')
    if not VALID_IDENTIFIER_PATTERN.match(clean_identifier):
        raise ValueError(f"Invalid identifier format: {identifier}")

    if not PSYCOPG2_AVAILABLE or cursor is None:
        # Fallback: manual double-quote escaping
        # This provides basic quoting when psycopg2 is unavailable or no cursor provided
        if '.' in identifier:
            parts = identifier.split('.')
            if len(parts) == 2:
                return f'"{parts[0]}"."{parts[1]}"'
            else:
                raise ValueError(f"Invalid schema.table format: {identifier}")
        else:
            return f'"{identifier}"'

    # Use psycopg2's safe identifier quoting with cursor
    if '.' in identifier:
        parts = identifier.split('.')
        if len(parts) == 2:
            # Quote schema and table separately
            return sql.Identifier(parts[0], parts[1]).as_string(cursor)
        else:
            raise ValueError(f"Invalid schema.table format: {identifier}")
    else:
        # Single identifier (table or column name)
        return sql.Identifier(identifier).as_string(cursor)


def _quote_sqlserver_identifier(identifier: str) -> str:
    """
    Quote SQL Server identifier using bracket quoting for SQL injection protection

    Args:
        identifier: Table or column name (may include schema, e.g., 'schema.table' or '[schema].[table]')

    Returns:
        Safely quoted identifier with brackets

    Raises:
        ValueError: If identifier format is invalid
    """
    # Validate identifier format (strip brackets before validation)
    clean_identifier = identifier.replace('[', '').replace(']', '')
    if not VALID_IDENTIFIER_PATTERN.match(clean_identifier):
        raise ValueError(f"Invalid identifier format: {identifier}")

    # Remove existing brackets if present
    identifier = identifier.replace('[', '').replace(']', '')

    # Handle schema.table format
    if '.' in identifier:
        parts = identifier.split('.')
        if len(parts) == 2:
            return f"[{parts[0]}].[{parts[1]}]"
        else:
            raise ValueError(f"Invalid schema.table format: {identifier}")
    else:
        return f"[{identifier}]"


def _get_db_type(cursor: Any) -> str:
    """
    Detect database type from cursor

    Args:
        cursor: Database cursor (pyodbc or psycopg2)

    Returns:
        'postgresql' or 'sqlserver'
    """
    cursor_type = type(cursor).__module__
    if 'psycopg2' in cursor_type or 'psycopg' in cursor_type:
        return 'postgresql'
    elif 'pyodbc' in cursor_type:
        return 'sqlserver'
    else:
        # Default to SQL Server for backward compatibility
        return 'sqlserver'


def _quote_identifier(cursor: Any, identifier: str) -> str:
    """
    Quote identifier based on database type

    Args:
        cursor: Database cursor to detect database type
        identifier: Table or column name to quote

    Returns:
        Safely quoted identifier
    """
    db_type = _get_db_type(cursor)

    if db_type == 'postgresql':
        return _quote_postgres_identifier(identifier)
    else:  # sqlserver
        return _quote_sqlserver_identifier(identifier)
