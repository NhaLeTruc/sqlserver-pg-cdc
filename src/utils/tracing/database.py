"""
Database and HTTP operation tracing utilities.

Provides context managers for tracing database queries and HTTP requests
with appropriate semantic attributes.
"""

from typing import Any

from opentelemetry import trace

from .context import trace_operation


def trace_database_query(
    query_type: str,
    table: str,
    database: str = "unknown"
) -> Any:
    """
    Context manager for tracing database queries.

    Args:
        query_type: Type of query (SELECT, INSERT, UPDATE, etc.)
        table: Table name
        database: Database name

    Example:
        >>> with trace_database_query("SELECT", "customers", "warehouse_target"):
        ...     cursor.execute("SELECT COUNT(*) FROM customers")
        ...     count = cursor.fetchone()[0]
    """
    return trace_operation(
        f"db.{query_type.lower()}",
        kind=trace.SpanKind.CLIENT,
        **{
            "db.operation": query_type,
            "db.table": table,
            "db.system": database,
            "component": "database"
        }
    )


def trace_http_request(method: str, url: str, **extra_attrs):
    """
    Context manager for tracing HTTP requests.

    Args:
        method: HTTP method (GET, POST, etc.)
        url: Request URL
        **extra_attrs: Additional attributes

    Example:
        >>> with trace_http_request("GET", "http://localhost:8083/connectors"):
        ...     response = requests.get(url)
    """
    return trace_operation(
        f"http.{method.lower()}",
        kind=trace.SpanKind.CLIENT,
        **{
            "http.method": method,
            "http.url": url,
            "component": "http",
            **extra_attrs
        }
    )
