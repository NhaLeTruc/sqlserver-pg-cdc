"""
SQL safety utilities for preventing SQL injection.

Provides identifier validation and quoting functions for safe SQL query construction.
"""

import re
from typing import Literal


# Strict ASCII-only patterns for SQL identifiers
VALID_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
VALID_SCHEMA_TABLE = re.compile(
    r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$"
)


def validate_identifier(identifier: str) -> None:
    """
    Validate a SQL identifier (table name, column name, etc.).

    Args:
        identifier: The identifier to validate

    Raises:
        ValueError: If the identifier contains invalid characters
    """
    if not identifier:
        raise ValueError("SQL identifier cannot be empty")

    if not VALID_IDENTIFIER.match(identifier):
        raise ValueError(
            f"Invalid SQL identifier: {identifier!r}. "
            "Only ASCII letters, digits, and underscores are allowed, "
            "and must start with a letter or underscore."
        )


def validate_schema_table(schema_table: str) -> None:
    """
    Validate a schema.table identifier.

    Args:
        schema_table: The schema.table identifier to validate

    Raises:
        ValueError: If the identifier format is invalid
    """
    if not schema_table:
        raise ValueError("Schema.table identifier cannot be empty")

    if not VALID_SCHEMA_TABLE.match(schema_table):
        raise ValueError(
            f"Invalid schema.table identifier: {schema_table!r}. "
            "Only ASCII letters, digits, and underscores are allowed."
        )


def quote_identifier(
    identifier: str, db_type: Literal["postgresql", "sqlserver"]
) -> str:
    """
    Safely quote a SQL identifier after validation.

    Args:
        identifier: The identifier to quote (table name, column name, etc.)
        db_type: Database type for proper quoting style

    Returns:
        Quoted identifier safe for use in SQL

    Raises:
        ValueError: If the identifier is invalid
    """
    validate_identifier(identifier)

    if db_type == "postgresql":
        return f'"{identifier}"'
    else:  # sqlserver
        return f"[{identifier}]"


def quote_schema_table(
    schema_table: str, db_type: Literal["postgresql", "sqlserver"]
) -> str:
    """
    Safely quote a schema.table identifier after validation.

    Args:
        schema_table: The schema.table identifier (e.g., "public.users" or just "users")
        db_type: Database type for proper quoting style

    Returns:
        Quoted identifier safe for use in SQL

    Raises:
        ValueError: If the identifier is invalid
    """
    validate_schema_table(schema_table)

    if "." in schema_table:
        schema, table = schema_table.split(".", 1)
        if db_type == "postgresql":
            return f'"{schema}"."{table}"'
        else:  # sqlserver
            return f"[{schema}].[{table}]"
    else:
        return quote_identifier(schema_table, db_type)


def validate_integer_param(value: int, param_name: str, min_value: int = 0) -> None:
    """
    Validate an integer parameter for SQL queries.

    Args:
        value: The value to validate
        param_name: Name of the parameter (for error messages)
        min_value: Minimum allowed value (default 0)

    Raises:
        ValueError: If the value is not a valid integer or below minimum
    """
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(
            f"Invalid {param_name}: {value!r}. Must be an integer."
        )

    if value < min_value:
        raise ValueError(
            f"Invalid {param_name}: {value}. Must be >= {min_value}."
        )
