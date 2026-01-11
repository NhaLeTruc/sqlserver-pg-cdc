"""
Base classes and functionality for database connection pooling.

Provides thread-safe connection pools with health checks, metrics,
and automatic connection recycling to prevent stale connections.
"""

import logging
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from queue import Empty, Queue
from typing import Any

from opentelemetry import trace
from prometheus_client import Counter, Gauge, Histogram

from src.utils.tracing import trace_operation

logger = logging.getLogger(__name__)


# Metrics
CONNECTION_POOL_SIZE = Gauge(
    "db_connection_pool_size",
    "Current size of database connection pool",
    ["database_type", "pool_name"],
)

CONNECTION_POOL_ACTIVE = Gauge(
    "db_connection_pool_active",
    "Number of active connections in pool",
    ["database_type", "pool_name"],
)

CONNECTION_POOL_IDLE = Gauge(
    "db_connection_pool_idle",
    "Number of idle connections in pool",
    ["database_type", "pool_name"],
)

CONNECTION_POOL_WAITS = Counter(
    "db_connection_pool_waits_total",
    "Number of times a connection request had to wait",
    ["database_type", "pool_name"],
)

CONNECTION_POOL_TIMEOUTS = Counter(
    "db_connection_pool_timeouts_total",
    "Number of connection pool timeout errors",
    ["database_type", "pool_name"],
)

CONNECTION_POOL_ERRORS = Counter(
    "db_connection_pool_errors_total",
    "Number of connection pool errors",
    ["database_type", "pool_name", "error_type"],
)

CONNECTION_ACQUIRE_TIME = Histogram(
    "db_connection_acquire_seconds",
    "Time to acquire a connection from pool",
    ["database_type", "pool_name"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

CONNECTION_HEALTH_CHECK_TIME = Histogram(
    "db_connection_health_check_seconds",
    "Time to perform connection health check",
    ["database_type", "pool_name"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
)


@dataclass
class PooledConnection:
    """Wrapper for a pooled database connection with metadata."""

    connection: Any
    created_at: datetime
    last_used: datetime
    use_count: int = 0
    is_healthy: bool = True

    def mark_used(self) -> None:
        """Mark connection as used and update timestamp."""
        self.last_used = datetime.utcnow()
        self.use_count += 1


class ConnectionPoolError(Exception):
    """Base exception for connection pool errors."""

    pass


class PoolExhaustedError(ConnectionPoolError):
    """Raised when the connection pool is exhausted."""

    pass


class PoolClosedError(ConnectionPoolError):
    """Raised when attempting to use a closed pool."""

    pass


class BaseConnectionPool:
    """
    Base class for database connection pools.

    Provides common functionality for managing a pool of database connections
    with health checks, metrics, and automatic recycling.
    """

    def __init__(
        self,
        min_size: int = 2,
        max_size: int = 10,
        max_idle_time: int = 300,
        max_lifetime: int = 3600,
        health_check_interval: int = 60,
        acquire_timeout: float = 30.0,
        pool_name: str = "default",
    ):
        """
        Initialize connection pool.

        Args:
            min_size: Minimum number of connections to maintain
            max_size: Maximum number of connections allowed
            max_idle_time: Maximum idle time in seconds before recycling
            max_lifetime: Maximum connection lifetime in seconds
            health_check_interval: Interval for health checks in seconds
            acquire_timeout: Timeout for acquiring connection in seconds
            pool_name: Name of the pool for metrics
        """
        self.min_size = min_size
        self.max_size = max_size
        self.max_idle_time = timedelta(seconds=max_idle_time)
        self.max_lifetime = timedelta(seconds=max_lifetime)
        self.health_check_interval = health_check_interval
        self.acquire_timeout = acquire_timeout
        self.pool_name = pool_name

        self._pool: Queue[PooledConnection] = Queue(maxsize=max_size)
        self._all_connections: list[PooledConnection] = []
        self._lock = threading.RLock()
        self._closed = False
        self._last_health_check = datetime.utcnow()

        # Initialize pool with minimum connections
        self._initialize_pool()

        # Start background health check thread
        self._health_check_thread = threading.Thread(
            target=self._health_check_worker, daemon=True
        )
        self._health_check_thread.start()

        logger.info(
            f"Initialized {self.__class__.__name__} '{pool_name}' "
            f"(min={min_size}, max={max_size})"
        )

    def _initialize_pool(self) -> None:
        """Initialize pool with minimum number of connections."""
        with self._lock:
            for _ in range(self.min_size):
                try:
                    conn = self._create_connection()
                    pooled_conn = PooledConnection(
                        connection=conn,
                        created_at=datetime.utcnow(),
                        last_used=datetime.utcnow(),
                    )
                    self._all_connections.append(pooled_conn)
                    self._pool.put(pooled_conn)
                except Exception as e:
                    logger.error(f"Failed to create initial connection: {e}")
                    CONNECTION_POOL_ERRORS.labels(
                        database_type=self._get_db_type(),
                        pool_name=self.pool_name,
                        error_type="initialization",
                    ).inc()

            self._update_metrics()

    def _create_connection(self) -> Any:
        """Create a new database connection. Must be implemented by subclasses."""
        raise NotImplementedError

    def _is_connection_healthy(self, conn: Any) -> bool:
        """Check if connection is healthy. Must be implemented by subclasses."""
        raise NotImplementedError

    def _close_connection(self, conn: Any) -> None:
        """Close a database connection. Must be implemented by subclasses."""
        raise NotImplementedError

    def _get_db_type(self) -> str:
        """Get database type for metrics. Must be implemented by subclasses."""
        raise NotImplementedError

    def _check_connection_health(self, pooled_conn: PooledConnection) -> bool:
        """
        Check if a pooled connection is healthy.

        Checks:
        - Connection is not None
        - Connection passes health check query
        - Connection has not exceeded max lifetime
        - Connection has not been idle too long
        """
        with CONNECTION_HEALTH_CHECK_TIME.labels(
            database_type=self._get_db_type(), pool_name=self.pool_name
        ).time():
            now = datetime.utcnow()

            # Check lifetime
            if now - pooled_conn.created_at > self.max_lifetime:
                logger.debug("Connection exceeded max lifetime, recycling")
                return False

            # Check idle time
            if now - pooled_conn.last_used > self.max_idle_time:
                logger.debug("Connection exceeded max idle time, recycling")
                return False

            # Check actual connection health
            try:
                is_healthy = self._is_connection_healthy(pooled_conn.connection)
                pooled_conn.is_healthy = is_healthy
                return is_healthy
            except Exception as e:
                logger.warning(f"Health check failed: {e}")
                pooled_conn.is_healthy = False
                CONNECTION_POOL_ERRORS.labels(
                    database_type=self._get_db_type(),
                    pool_name=self.pool_name,
                    error_type="health_check",
                ).inc()
                return False

    def _recycle_connection(self, pooled_conn: PooledConnection) -> None:
        """Close and remove a connection from the pool."""
        try:
            self._close_connection(pooled_conn.connection)
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")
        finally:
            with self._lock:
                if pooled_conn in self._all_connections:
                    self._all_connections.remove(pooled_conn)

    def _health_check_worker(self) -> None:
        """Background worker to perform periodic health checks."""
        while not self._closed:
            try:
                time.sleep(self.health_check_interval)
                self._perform_health_checks()
            except Exception as e:
                logger.error(f"Health check worker error: {e}")

    def _perform_health_checks(self) -> None:
        """Perform health checks on idle connections and maintain minimum size."""
        if self._closed:
            return

        with self._lock:
            # Get snapshot of current connections
            connections_to_check = list(self._all_connections)

        unhealthy_connections = []

        for pooled_conn in connections_to_check:
            # Only check connections that are currently in the pool (idle)
            try:
                # Non-blocking check if connection is in pool
                if pooled_conn in self._pool.queue:
                    if not self._check_connection_health(pooled_conn):
                        unhealthy_connections.append(pooled_conn)
            except Exception as e:
                logger.warning(f"Error during health check: {e}")

        # Recycle unhealthy connections
        for pooled_conn in unhealthy_connections:
            try:
                # Remove from pool if present
                temp_queue: Queue[PooledConnection] = Queue(maxsize=self.max_size)
                while True:
                    try:
                        conn = self._pool.get_nowait()
                        if conn != pooled_conn:
                            temp_queue.put_nowait(conn)
                    except Empty:
                        break

                # Put back all connections except the unhealthy one
                while True:
                    try:
                        conn = temp_queue.get_nowait()
                        self._pool.put_nowait(conn)
                    except Empty:
                        break

                # Recycle the unhealthy connection
                self._recycle_connection(pooled_conn)
                logger.info("Recycled unhealthy connection")
            except Exception as e:
                logger.error(f"Error recycling connection: {e}")

        # Ensure minimum pool size
        with self._lock:
            current_size = len(self._all_connections)
            if current_size < self.min_size:
                needed = self.min_size - current_size
                logger.info(f"Replenishing pool with {needed} connections")
                for _ in range(needed):
                    try:
                        conn = self._create_connection()
                        pooled_conn = PooledConnection(
                            connection=conn,
                            created_at=datetime.utcnow(),
                            last_used=datetime.utcnow(),
                        )
                        self._all_connections.append(pooled_conn)
                        self._pool.put(pooled_conn)
                    except Exception as e:
                        logger.error(f"Failed to create replacement connection: {e}")
                        CONNECTION_POOL_ERRORS.labels(
                            database_type=self._get_db_type(),
                            pool_name=self.pool_name,
                            error_type="replenishment",
                        ).inc()

            self._update_metrics()

    def _update_metrics(self) -> None:
        """Update Prometheus metrics."""
        with self._lock:
            total_size = len(self._all_connections)
            idle_size = self._pool.qsize()
            active_size = total_size - idle_size

            CONNECTION_POOL_SIZE.labels(
                database_type=self._get_db_type(), pool_name=self.pool_name
            ).set(total_size)

            CONNECTION_POOL_ACTIVE.labels(
                database_type=self._get_db_type(), pool_name=self.pool_name
            ).set(active_size)

            CONNECTION_POOL_IDLE.labels(
                database_type=self._get_db_type(), pool_name=self.pool_name
            ).set(idle_size)

    @contextmanager
    def acquire(self) -> Iterator[Any]:
        """
        Acquire a connection from the pool.

        Yields:
            Database connection

        Raises:
            PoolClosedError: If pool is closed
            PoolExhaustedError: If no connection available within timeout
        """
        if self._closed:
            raise PoolClosedError("Connection pool is closed")

        pooled_conn: PooledConnection | None = None
        start_time = time.time()

        with trace_operation(
            "db_pool_acquire",
            kind=trace.SpanKind.CLIENT,
            database_type=self._get_db_type(),
            pool_name=self.pool_name,
        ):
            try:
                # Loop to handle unhealthy connections
                while True:
                    # Check timeout
                    elapsed = time.time() - start_time
                    if elapsed >= self.acquire_timeout:
                        raise PoolExhaustedError("Connection pool timeout")

                    remaining_timeout = self.acquire_timeout - elapsed

                    # Try to get existing connection
                    try:
                        pooled_conn = self._pool.get(timeout=0.1)
                    except Empty:
                        # No idle connection available, try to create new one
                        with self._lock:
                            if len(self._all_connections) < self.max_size:
                                try:
                                    conn = self._create_connection()
                                    pooled_conn = PooledConnection(
                                        connection=conn,
                                        created_at=datetime.utcnow(),
                                        last_used=datetime.utcnow(),
                                    )
                                    self._all_connections.append(pooled_conn)
                                    logger.debug("Created new connection for pool")
                                except Exception as e:
                                    logger.error(f"Failed to create new connection: {e}")
                                    CONNECTION_POOL_ERRORS.labels(
                                        database_type=self._get_db_type(),
                                        pool_name=self.pool_name,
                                        error_type="creation",
                                    ).inc()

                        # If we didn't create a new connection, wait for one
                        if pooled_conn is None:
                            CONNECTION_POOL_WAITS.labels(
                                database_type=self._get_db_type(),
                                pool_name=self.pool_name,
                            ).inc()

                            if remaining_timeout <= 0:
                                raise PoolExhaustedError("Connection pool timeout")

                            try:
                                pooled_conn = self._pool.get(timeout=remaining_timeout)
                            except Empty:
                                CONNECTION_POOL_TIMEOUTS.labels(
                                    database_type=self._get_db_type(),
                                    pool_name=self.pool_name,
                                ).inc()
                                raise PoolExhaustedError(
                                    f"No connection available within {self.acquire_timeout}s"
                                )

                    # Validate connection health
                    if not self._check_connection_health(pooled_conn):
                        logger.info("Connection unhealthy, recycling and retrying")
                        self._recycle_connection(pooled_conn)
                        pooled_conn = None
                        # Continue loop to try another connection
                        continue

                    # Connection is healthy, break out of loop
                    break

                # Mark connection as used
                pooled_conn.mark_used()
                self._update_metrics()

                # Record acquisition time
                acquire_time = time.time() - start_time
                CONNECTION_ACQUIRE_TIME.labels(
                    database_type=self._get_db_type(), pool_name=self.pool_name
                ).observe(acquire_time)

                # Yield connection to user
                yield pooled_conn.connection

            finally:
                # Return connection to pool
                if pooled_conn is not None and not self._closed:
                    try:
                        self._pool.put(pooled_conn, timeout=1.0)
                        self._update_metrics()
                    except Exception as e:
                        logger.error(f"Failed to return connection to pool: {e}")
                        self._recycle_connection(pooled_conn)

    def close(self) -> None:
        """Close all connections and shutdown the pool."""
        if self._closed:
            return

        logger.info(f"Closing connection pool '{self.pool_name}'")
        self._closed = True

        with self._lock:
            # Close all connections
            for pooled_conn in self._all_connections:
                try:
                    self._close_connection(pooled_conn.connection)
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")

            self._all_connections.clear()

            # Clear the pool
            while True:
                try:
                    self._pool.get_nowait()
                except Empty:
                    break

        logger.info(f"Connection pool '{self.pool_name}' closed")

    def get_stats(self) -> dict[str, Any]:
        """Get pool statistics."""
        with self._lock:
            total_size = len(self._all_connections)
            idle_size = self._pool.qsize()
            active_size = total_size - idle_size

            return {
                "pool_name": self.pool_name,
                "total_connections": total_size,
                "idle_connections": idle_size,
                "active_connections": active_size,
                "min_size": self.min_size,
                "max_size": self.max_size,
                "closed": self._closed,
            }
