"""SQL Server connection pool implementation."""

from typing import Any

import pyodbc
from opentelemetry import trace

from src.utils.tracing import trace_operation

from .base import BaseConnectionPool


class SQLServerConnectionPool(BaseConnectionPool):
    """Connection pool for SQL Server databases."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        user: str | None = None,
        password: str | None = None,
        driver: str = "ODBC Driver 18 for SQL Server",
        connection_string: str | None = None,
        **kwargs: Any,
    ):
        """
        Initialize SQL Server connection pool.

        Args:
            host: SQL Server host (required if connection_string not provided)
            port: SQL Server port (required if connection_string not provided)
            database: Database name (required if connection_string not provided)
            user: Username (required if connection_string not provided)
            password: Password (required if connection_string not provided)
            driver: ODBC driver name
            connection_string: Complete ODBC connection string (alternative to individual params)
            **kwargs: Additional arguments for BaseConnectionPool
        """
        if connection_string:
            # Use provided connection string directly
            self.connection_string = connection_string
            # Extract host and database for metrics (optional, best effort)
            self.host = self._extract_from_conn_str(connection_string, "SERVER")
            self.database = self._extract_from_conn_str(connection_string, "DATABASE")
        else:
            # Build connection string from individual parameters
            if not all([host, port, database, user, password]):
                raise ValueError(
                    "Either connection_string or all of (host, port, database, user, password) must be provided"
                )
            self.host = host
            self.port = port
            self.database = database
            self.user = user
            self.password = password
            self.driver = driver
            self.connection_string = None

        super().__init__(**kwargs)

    def _extract_from_conn_str(self, conn_str: str, key: str) -> str:
        """Extract a value from connection string for metrics."""
        try:
            for part in conn_str.split(";"):
                if part.strip().upper().startswith(key.upper() + "="):
                    return part.split("=", 1)[1].strip()
        except Exception:
            pass
        return "unknown"

    def _create_connection(self) -> pyodbc.Connection:
        """Create a new SQL Server connection."""
        with trace_operation(
            "sqlserver_connect",
            kind=trace.SpanKind.CLIENT,
            db_host=self.host,
            db_name=self.database,
        ):
            if self.connection_string:
                # Use provided connection string
                conn_str = self.connection_string
            else:
                # Build connection string from parameters
                conn_str = (
                    f"DRIVER={{{self.driver}}};"
                    f"SERVER={self.host},{self.port};"
                    f"DATABASE={self.database};"
                    f"UID={self.user};"
                    f"PWD={self.password};"
                    f"TrustServerCertificate=yes;"
                    f"Encrypt=yes;"
                )
            conn = pyodbc.connect(conn_str, timeout=10)
            # Set autocommit
            conn.autocommit = True
            return conn

    def _is_connection_healthy(self, conn: pyodbc.Connection) -> bool:
        """Check if SQL Server connection is healthy."""
        if conn is None:
            return False

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except pyodbc.Error:
            # BUG-8: Only catch specific database exceptions
            return False
        except Exception as e:
            # BUG-8: Re-raise critical exceptions
            if isinstance(e, (SystemExit, KeyboardInterrupt, GeneratorExit)):
                raise
            return False

    def _close_connection(self, conn: pyodbc.Connection) -> None:
        """Close SQL Server connection."""
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _get_db_type(self) -> str:
        """Get database type for metrics."""
        return "sqlserver"
