"""
Database connection pooling for PostgreSQL and SQL Server.

Provides thread-safe connection pools with health checks, metrics,
and automatic connection recycling to prevent stale connections.
"""

import logging
from typing import Any, Dict, Optional

from .base import (
    BaseConnectionPool,
    ConnectionPoolError,
    PoolClosedError,
    PooledConnection,
    PoolExhaustedError,
)
from .postgres import PostgresConnectionPool
from .sqlserver import SQLServerConnectionPool

logger = logging.getLogger(__name__)


# Global pool instances (initialized by application)
_postgres_pool: Optional[PostgresConnectionPool] = None
_sqlserver_pool: Optional[SQLServerConnectionPool] = None


def initialize_pools(
    postgres_config: Optional[Dict[str, Any]] = None,
    sqlserver_config: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Initialize global connection pools.

    Args:
        postgres_config: PostgreSQL configuration dict
        sqlserver_config: SQL Server configuration dict
    """
    global _postgres_pool, _sqlserver_pool

    if postgres_config:
        logger.info("Initializing PostgreSQL connection pool")
        _postgres_pool = PostgresConnectionPool(**postgres_config)

    if sqlserver_config:
        logger.info("Initializing SQL Server connection pool")
        _sqlserver_pool = SQLServerConnectionPool(**sqlserver_config)


def get_postgres_pool() -> PostgresConnectionPool:
    """Get the global PostgreSQL connection pool."""
    if _postgres_pool is None:
        raise RuntimeError("PostgreSQL pool not initialized. Call initialize_pools() first.")
    return _postgres_pool


def get_sqlserver_pool() -> SQLServerConnectionPool:
    """Get the global SQL Server connection pool."""
    if _sqlserver_pool is None:
        raise RuntimeError("SQL Server pool not initialized. Call initialize_pools() first.")
    return _sqlserver_pool


def close_pools() -> None:
    """Close all global connection pools."""
    global _postgres_pool, _sqlserver_pool

    if _postgres_pool:
        _postgres_pool.close()
        _postgres_pool = None

    if _sqlserver_pool:
        _sqlserver_pool.close()
        _sqlserver_pool = None

    logger.info("All connection pools closed")


__all__ = [
    "BaseConnectionPool",
    "PostgresConnectionPool",
    "SQLServerConnectionPool",
    "PooledConnection",
    "ConnectionPoolError",
    "PoolExhaustedError",
    "PoolClosedError",
    "initialize_pools",
    "get_postgres_pool",
    "get_sqlserver_pool",
    "close_pools",
]
