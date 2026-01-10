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
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        driver: str = "ODBC Driver 18 for SQL Server",
        **kwargs: Any,
    ):
        """
        Initialize SQL Server connection pool.

        Args:
            host: SQL Server host
            port: SQL Server port
            database: Database name
            user: Username
            password: Password
            driver: ODBC driver name
            **kwargs: Additional arguments for BaseConnectionPool
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.driver = driver

        super().__init__(**kwargs)

    def _create_connection(self) -> pyodbc.Connection:
        """Create a new SQL Server connection."""
        with trace_operation(
            "sqlserver_connect",
            kind=trace.SpanKind.CLIENT,
            db_host=self.host,
            db_name=self.database,
        ):
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
        except Exception:
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
