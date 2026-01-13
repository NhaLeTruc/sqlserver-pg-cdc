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
_postgres_pool: PostgresConnectionPool | None = None
_sqlserver_pool: SQLServerConnectionPool | None = None


def initialize_pools(
    postgres_config: dict[str, Any] | None = None,
    sqlserver_config: dict[str, Any] | None = None,
    postgres_pool_size: int | None = None,
    postgres_max_overflow: int | None = None,
    postgres_min_size: int | None = None,
    postgres_max_size: int | None = None,
    sqlserver_pool_size: int | None = None,
    sqlserver_max_overflow: int | None = None,
    sqlserver_min_size: int | None = None,
    sqlserver_max_size: int | None = None,
    **pool_kwargs: Any,
) -> None:
    """
    Initialize global connection pools.

    Args:
        postgres_config: PostgreSQL configuration dict (host, port, database, user, password)
        sqlserver_config: SQL Server configuration dict (connection_string or individual params)
        postgres_pool_size: PostgreSQL pool size (used as min_size if no min/max specified)
        postgres_max_overflow: PostgreSQL max overflow connections (added to pool_size for max_size)
        postgres_min_size: PostgreSQL minimum pool size (overrides pool_size)
        postgres_max_size: PostgreSQL maximum pool size (overrides pool_size + max_overflow)
        sqlserver_pool_size: SQL Server pool size (used as min_size if no min/max specified)
        sqlserver_max_overflow: SQL Server max overflow connections (added to pool_size for max_size)
        sqlserver_min_size: SQL Server minimum pool size (overrides pool_size)
        sqlserver_max_size: SQL Server maximum pool size (overrides pool_size + max_overflow)
        **pool_kwargs: Additional pool configuration (max_idle_time, max_lifetime, etc.)
    """
    global _postgres_pool, _sqlserver_pool

    if postgres_config:
        logger.info("Initializing PostgreSQL connection pool")

        # Build pool-specific kwargs
        pg_pool_kwargs = dict(pool_kwargs)

        # Handle pool sizing parameters
        if postgres_min_size is not None:
            pg_pool_kwargs['min_size'] = postgres_min_size
        elif postgres_pool_size is not None:
            pg_pool_kwargs['min_size'] = postgres_pool_size

        if postgres_max_size is not None:
            pg_pool_kwargs['max_size'] = postgres_max_size
        elif postgres_pool_size is not None and postgres_max_overflow is not None:
            pg_pool_kwargs['max_size'] = postgres_pool_size + postgres_max_overflow
        elif postgres_pool_size is not None:
            pg_pool_kwargs['max_size'] = postgres_pool_size

        _postgres_pool = PostgresConnectionPool(**postgres_config, **pg_pool_kwargs)

    if sqlserver_config:
        logger.info("Initializing SQL Server connection pool")

        # Build pool-specific kwargs
        ss_pool_kwargs = dict(pool_kwargs)

        # Handle pool sizing parameters
        if sqlserver_min_size is not None:
            ss_pool_kwargs['min_size'] = sqlserver_min_size
        elif sqlserver_pool_size is not None:
            ss_pool_kwargs['min_size'] = sqlserver_pool_size

        if sqlserver_max_size is not None:
            ss_pool_kwargs['max_size'] = sqlserver_max_size
        elif sqlserver_pool_size is not None and sqlserver_max_overflow is not None:
            ss_pool_kwargs['max_size'] = sqlserver_pool_size + sqlserver_max_overflow
        elif sqlserver_pool_size is not None:
            ss_pool_kwargs['max_size'] = sqlserver_pool_size

        _sqlserver_pool = SQLServerConnectionPool(**sqlserver_config, **ss_pool_kwargs)


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
