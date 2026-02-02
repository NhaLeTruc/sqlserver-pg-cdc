"""
Database type enumeration for type-safe database identification.

CQ-2: Replaces hardcoded 'postgresql' and 'sqlserver' strings throughout the codebase.
"""

from enum import Enum


class DatabaseType(str, Enum):
    """
    Enumeration of supported database types.

    Inherits from str for JSON serialization compatibility and
    easy comparison with string values.
    """

    POSTGRESQL = "postgresql"
    SQLSERVER = "sqlserver"
    UNKNOWN = "unknown"

    @classmethod
    def from_cursor(cls, cursor) -> "DatabaseType":
        """
        Detect database type from cursor class name.

        Args:
            cursor: Database cursor object

        Returns:
            DatabaseType enum value
        """
        cursor_class_name = cursor.__class__.__name__.lower()

        if "psycopg" in cursor_class_name or "postgres" in cursor_class_name:
            return cls.POSTGRESQL
        elif "pyodbc" in cursor_class_name or "odbc" in cursor_class_name:
            return cls.SQLSERVER
        else:
            return cls.UNKNOWN

    def get_placeholder(self, index: int = 0) -> str:
        """
        Get parameter placeholder for this database type.

        Args:
            index: Parameter index (0-based)

        Returns:
            Placeholder string
        """
        if self == DatabaseType.POSTGRESQL:
            return f"${index + 1}"
        else:
            return "?"

    def quote_identifier(self, identifier: str) -> str:
        """
        Quote identifier based on database type.

        Args:
            identifier: Column or table name

        Returns:
            Quoted identifier string
        """
        if self == DatabaseType.POSTGRESQL:
            return f'"{identifier}"'
        elif self == DatabaseType.SQLSERVER:
            return f"[{identifier}]"
        else:
            return identifier
