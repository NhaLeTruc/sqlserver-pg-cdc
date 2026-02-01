"""PostgreSQL connection pool implementation."""

from typing import Any

import psycopg2
import psycopg2.extensions
from opentelemetry import trace

from src.utils.tracing import trace_operation

from .base import BaseConnectionPool


class PostgresConnectionPool(BaseConnectionPool):
    """Connection pool for PostgreSQL databases."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        **kwargs: Any,
    ):
        """
        Initialize PostgreSQL connection pool.

        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            database: Database name
            user: Username
            password: Password
            **kwargs: Additional arguments for BaseConnectionPool
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        super().__init__(**kwargs)

    def _create_connection(self) -> psycopg2.extensions.connection:
        """Create a new PostgreSQL connection."""
        with trace_operation(
            "postgres_connect",
            kind=trace.SpanKind.CLIENT,
            db_host=self.host,
            db_name=self.database,
        ):
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=10,
            )
            # Set autocommit to avoid transaction issues in pool
            conn.set_session(autocommit=True)
            return conn

    def _is_connection_healthy(self, conn: psycopg2.extensions.connection) -> bool:
        """Check if PostgreSQL connection is healthy."""
        if conn is None or conn.closed:
            return False

        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except (psycopg2.Error, psycopg2.Warning):
            # BUG-8: Only catch specific database exceptions
            return False
        except Exception as e:
            # BUG-8: Re-raise critical exceptions
            if isinstance(e, (SystemExit, KeyboardInterrupt, GeneratorExit)):
                raise
            return False

    def _close_connection(self, conn: psycopg2.extensions.connection) -> None:
        """Close PostgreSQL connection."""
        if conn is not None and not conn.closed:
            conn.close()

    def _get_db_type(self) -> str:
        """Get database type for metrics."""
        return "postgresql"
