"""
Unit tests for database connection pooling.

Tests connection pool behavior, health checks, metrics, and error handling.
"""

import time
from datetime import datetime, timedelta
from threading import Thread
from unittest.mock import MagicMock, Mock, patch

import psycopg2
import pytest
import pyodbc

from src.utils.db_pool import (
    BaseConnectionPool,
    ConnectionPoolError,
    PoolClosedError,
    PooledConnection,
    PoolExhaustedError,
    PostgresConnectionPool,
    SQLServerConnectionPool,
    close_pools,
    get_postgres_pool,
    get_sqlserver_pool,
    initialize_pools,
)


class TestPooledConnection:
    """Test PooledConnection dataclass."""

    def test_mark_used_updates_timestamp(self):
        """Test that mark_used updates last_used timestamp and use_count."""
        mock_conn = Mock()
        pooled = PooledConnection(
            connection=mock_conn,
            created_at=datetime.utcnow(),
            last_used=datetime.utcnow() - timedelta(minutes=5),
        )

        old_last_used = pooled.last_used
        old_use_count = pooled.use_count

        time.sleep(0.01)
        pooled.mark_used()

        assert pooled.last_used > old_last_used
        assert pooled.use_count == old_use_count + 1

    def test_initial_state(self):
        """Test initial state of PooledConnection."""
        mock_conn = Mock()
        now = datetime.utcnow()
        pooled = PooledConnection(
            connection=mock_conn, created_at=now, last_used=now
        )

        assert pooled.connection is mock_conn
        assert pooled.created_at == now
        assert pooled.last_used == now
        assert pooled.use_count == 0
        assert pooled.is_healthy is True


class MockConnectionPool(BaseConnectionPool):
    """Mock implementation of BaseConnectionPool for testing."""

    def __init__(self, **kwargs):
        self.connections_created = 0
        self.connections_closed = 0
        self.health_check_calls = 0
        self.create_should_fail = False
        self.health_check_should_fail = False
        super().__init__(**kwargs)

    def _create_connection(self):
        if self.create_should_fail:
            raise Exception("Connection creation failed")
        self.connections_created += 1
        return Mock(spec=["execute", "close"])

    def _is_connection_healthy(self, conn):
        self.health_check_calls += 1
        if self.health_check_should_fail:
            return False
        return True

    def _close_connection(self, conn):
        self.connections_closed += 1
        if hasattr(conn, "close"):
            conn.close()

    def _get_db_type(self):
        return "mock"


class TestBaseConnectionPool:
    """Test BaseConnectionPool functionality."""

    def test_initialization_creates_minimum_connections(self):
        """Test that pool initializes with min_size connections."""
        pool = MockConnectionPool(min_size=3, max_size=10)
        time.sleep(0.1)  # Allow initialization to complete

        stats = pool.get_stats()
        assert stats["total_connections"] >= 3
        assert pool.connections_created >= 3

        pool.close()

    def test_acquire_returns_connection(self):
        """Test acquiring a connection from pool."""
        pool = MockConnectionPool(min_size=2, max_size=5)
        time.sleep(0.1)

        with pool.acquire() as conn:
            assert conn is not None

        pool.close()

    def test_acquire_reuses_connections(self):
        """Test that connections are reused from pool."""
        pool = MockConnectionPool(min_size=2, max_size=5)
        time.sleep(0.1)
        initial_created = pool.connections_created

        # Acquire and release multiple times
        for _ in range(5):
            with pool.acquire() as conn:
                pass

        # Should not create new connections if pool has idle ones
        assert pool.connections_created <= initial_created + 3

        pool.close()

    def test_acquire_creates_new_connection_when_needed(self):
        """Test that new connections are created when pool is depleted."""
        pool = MockConnectionPool(min_size=1, max_size=3)
        time.sleep(0.1)

        context_managers = []
        connections = []
        # Acquire all connections without releasing
        for _ in range(3):
            cm = pool.acquire()
            context_managers.append(cm)
            conn = cm.__enter__()  # Enter context manager
            connections.append(conn)

        # All connections should be created
        assert pool.connections_created == 3

        # Release connections
        for cm in context_managers:
            cm.__exit__(None, None, None)

        pool.close()

    def test_acquire_timeout_when_pool_exhausted(self):
        """Test that acquire times out when pool is exhausted."""
        pool = MockConnectionPool(min_size=1, max_size=1, acquire_timeout=0.5)
        time.sleep(0.1)

        with pool.acquire():
            # Pool is now exhausted (1/1 connection in use)
            # Try to acquire another - should timeout
            with pytest.raises(PoolExhaustedError, match="No connection available"):
                with pool.acquire():
                    pass

        pool.close()

    def test_acquire_raises_error_when_pool_closed(self):
        """Test that acquire raises error when pool is closed."""
        pool = MockConnectionPool(min_size=1, max_size=5)
        time.sleep(0.1)
        pool.close()

        with pytest.raises(PoolClosedError, match="Connection pool is closed"):
            with pool.acquire():
                pass

    def test_connection_returned_after_exception(self):
        """Test that connection is returned to pool even if exception occurs."""
        pool = MockConnectionPool(min_size=2, max_size=5)
        time.sleep(0.1)

        stats_before = pool.get_stats()

        try:
            with pool.acquire() as conn:
                raise ValueError("Test exception")
        except ValueError:
            pass

        time.sleep(0.1)
        stats_after = pool.get_stats()

        # Connection should be returned to pool
        assert stats_after["idle_connections"] == stats_before["idle_connections"]

        pool.close()

    def test_unhealthy_connection_recycled(self):
        """Test that unhealthy connections are recycled."""
        pool = MockConnectionPool(min_size=2, max_size=5)
        time.sleep(0.1)

        # Make health checks fail
        pool.health_check_should_fail = True

        # Acquire should detect unhealthy connection and recycle it
        with pytest.raises(PoolExhaustedError):
            # Will keep trying to recycle until timeout
            with pool.acquire():
                pass

        assert pool.connections_closed > 0

        pool.close()

    def test_max_lifetime_exceeded(self):
        """Test that connections exceeding max lifetime are recycled."""
        pool = MockConnectionPool(
            min_size=1, max_size=3, max_lifetime=1  # 1 second lifetime
        )
        time.sleep(0.1)

        with pool.acquire() as conn:
            first_conn = conn

        # Wait for lifetime to exceed
        time.sleep(1.1)

        # Get a pooled connection and manually check health
        pooled_conn = pool._pool.get()
        is_healthy = pool._check_connection_health(pooled_conn)
        pool._pool.put(pooled_conn)

        # Should be marked as unhealthy due to lifetime
        assert not is_healthy

        pool.close()

    def test_max_idle_time_exceeded(self):
        """Test that connections idle too long are recycled."""
        pool = MockConnectionPool(
            min_size=1, max_size=3, max_idle_time=1  # 1 second idle time
        )
        time.sleep(0.1)

        with pool.acquire() as conn:
            pass

        # Wait for idle time to exceed
        time.sleep(1.1)

        # Get pooled connection and check health
        pooled_conn = pool._pool.get()
        is_healthy = pool._check_connection_health(pooled_conn)
        pool._pool.put(pooled_conn)

        # Should be marked unhealthy due to idle time
        assert not is_healthy

        pool.close()

    def test_health_check_worker_maintains_minimum_size(self):
        """Test that health check worker maintains minimum pool size."""
        pool = MockConnectionPool(
            min_size=3, max_size=10, health_check_interval=1
        )
        time.sleep(0.2)

        initial_stats = pool.get_stats()
        assert initial_stats["total_connections"] >= 3

        # Manually recycle a connection
        pooled_conn = pool._pool.get()
        pool._recycle_connection(pooled_conn)

        # Wait for health check worker to replenish
        time.sleep(1.5)

        stats = pool.get_stats()
        # Should maintain minimum size
        assert stats["total_connections"] >= 3

        pool.close()

    def test_concurrent_access(self):
        """Test that pool handles concurrent access correctly."""
        pool = MockConnectionPool(min_size=2, max_size=10)
        time.sleep(0.1)

        results = []
        errors = []

        def worker():
            try:
                with pool.acquire() as conn:
                    time.sleep(0.01)
                    results.append(conn)
            except Exception as e:
                errors.append(e)

        threads = [Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All threads should have acquired connections
        assert len(results) == 20
        assert len(errors) == 0

        pool.close()

    def test_get_stats(self):
        """Test get_stats returns correct pool statistics."""
        pool = MockConnectionPool(min_size=2, max_size=5, pool_name="test_pool")
        time.sleep(0.1)

        stats = pool.get_stats()

        assert stats["pool_name"] == "test_pool"
        assert stats["min_size"] == 2
        assert stats["max_size"] == 5
        assert stats["total_connections"] >= 2
        assert stats["idle_connections"] >= 0
        assert stats["active_connections"] >= 0
        assert stats["closed"] is False

        pool.close()

        stats_after_close = pool.get_stats()
        assert stats_after_close["closed"] is True

    def test_close_closes_all_connections(self):
        """Test that close() closes all connections."""
        pool = MockConnectionPool(min_size=3, max_size=5)
        time.sleep(0.1)

        pool.close()

        assert pool.connections_closed >= 3
        assert pool.get_stats()["closed"] is True
        assert len(pool._all_connections) == 0


@patch("psycopg2.connect")
class TestPostgresConnectionPool:
    """Test PostgresConnectionPool functionality."""

    def test_create_connection(self, mock_connect):
        """Test PostgreSQL connection creation."""
        mock_conn = MagicMock(spec=psycopg2.extensions.connection)
        mock_connect.return_value = mock_conn

        pool = PostgresConnectionPool(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
            min_size=1,
            max_size=3,
        )
        time.sleep(0.1)

        # Should have called psycopg2.connect
        mock_connect.assert_called()
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs["host"] == "localhost"
        assert call_kwargs["port"] == 5432
        assert call_kwargs["database"] == "testdb"
        assert call_kwargs["user"] == "testuser"
        assert call_kwargs["password"] == "testpass"

        pool.close()

    def test_is_connection_healthy_with_healthy_connection(self, mock_connect):
        """Test health check with healthy PostgreSQL connection."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock(spec=psycopg2.extensions.connection)
        mock_conn.closed = False
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        pool = PostgresConnectionPool(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
            min_size=1,
            max_size=3,
        )
        time.sleep(0.1)

        # Health check should return True
        is_healthy = pool._is_connection_healthy(mock_conn)
        assert is_healthy is True

        mock_cursor.execute.assert_called_with("SELECT 1")
        mock_cursor.fetchone.assert_called_once()

        pool.close()

    def test_is_connection_healthy_with_closed_connection(self, mock_connect):
        """Test health check with closed PostgreSQL connection."""
        mock_conn = MagicMock(spec=psycopg2.extensions.connection)
        mock_conn.closed = True
        mock_connect.return_value = mock_conn

        pool = PostgresConnectionPool(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
            min_size=0,
            max_size=3,
        )
        time.sleep(0.1)

        # Health check should return False for closed connection
        is_healthy = pool._is_connection_healthy(mock_conn)
        assert is_healthy is False

        pool.close()

    def test_is_connection_healthy_with_exception(self, mock_connect):
        """Test health check when query raises exception."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg2.OperationalError("Connection lost")
        mock_conn = MagicMock(spec=psycopg2.extensions.connection)
        mock_conn.closed = False
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        pool = PostgresConnectionPool(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
            min_size=0,
            max_size=3,
        )
        time.sleep(0.1)

        # Health check should return False when exception occurs
        is_healthy = pool._is_connection_healthy(mock_conn)
        assert is_healthy is False

        pool.close()

    def test_close_connection(self, mock_connect):
        """Test closing PostgreSQL connection."""
        mock_conn = MagicMock(spec=psycopg2.extensions.connection)
        mock_conn.closed = False
        mock_connect.return_value = mock_conn

        pool = PostgresConnectionPool(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
            min_size=1,
            max_size=3,
        )
        time.sleep(0.1)

        pool._close_connection(mock_conn)
        mock_conn.close.assert_called_once()

        pool.close()

    def test_get_db_type(self, mock_connect):
        """Test _get_db_type returns 'postgresql'."""
        mock_conn = MagicMock(spec=psycopg2.extensions.connection)
        mock_connect.return_value = mock_conn

        pool = PostgresConnectionPool(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
            min_size=1,
            max_size=3,
        )
        time.sleep(0.1)

        assert pool._get_db_type() == "postgresql"

        pool.close()


@patch("pyodbc.connect")
class TestSQLServerConnectionPool:
    """Test SQLServerConnectionPool functionality."""

    def test_create_connection(self, mock_connect):
        """Test SQL Server connection creation."""
        mock_conn = MagicMock(spec=pyodbc.Connection)
        mock_connect.return_value = mock_conn

        pool = SQLServerConnectionPool(
            host="localhost",
            port=1433,
            database="testdb",
            user="testuser",
            password="testpass",
            min_size=1,
            max_size=3,
        )
        time.sleep(0.1)

        # Should have called pyodbc.connect with connection string
        mock_connect.assert_called()
        conn_str = mock_connect.call_args[0][0]
        assert "DRIVER={ODBC Driver 18 for SQL Server}" in conn_str
        assert "SERVER=localhost,1433" in conn_str
        assert "DATABASE=testdb" in conn_str
        assert "UID=testuser" in conn_str
        assert "PWD=testpass" in conn_str

        pool.close()

    def test_create_connection_with_custom_driver(self, mock_connect):
        """Test SQL Server connection with custom ODBC driver."""
        mock_conn = MagicMock(spec=pyodbc.Connection)
        mock_connect.return_value = mock_conn

        pool = SQLServerConnectionPool(
            host="localhost",
            port=1433,
            database="testdb",
            user="testuser",
            password="testpass",
            driver="ODBC Driver 17 for SQL Server",
            min_size=1,
            max_size=3,
        )
        time.sleep(0.1)

        conn_str = mock_connect.call_args[0][0]
        assert "DRIVER={ODBC Driver 17 for SQL Server}" in conn_str

        pool.close()

    def test_is_connection_healthy_with_healthy_connection(self, mock_connect):
        """Test health check with healthy SQL Server connection."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock(spec=pyodbc.Connection)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        pool = SQLServerConnectionPool(
            host="localhost",
            port=1433,
            database="testdb",
            user="testuser",
            password="testpass",
            min_size=1,
            max_size=3,
        )
        time.sleep(0.1)

        # Health check should return True
        is_healthy = pool._is_connection_healthy(mock_conn)
        assert is_healthy is True

        mock_cursor.execute.assert_called_with("SELECT 1")
        mock_cursor.fetchone.assert_called_once()
        mock_cursor.close.assert_called_once()

        pool.close()

    def test_is_connection_healthy_with_none_connection(self, mock_connect):
        """Test health check with None connection."""
        mock_conn = MagicMock(spec=pyodbc.Connection)
        mock_connect.return_value = mock_conn

        pool = SQLServerConnectionPool(
            host="localhost",
            port=1433,
            database="testdb",
            user="testuser",
            password="testpass",
            min_size=0,
            max_size=3,
        )
        time.sleep(0.1)

        # Health check should return False for None
        is_healthy = pool._is_connection_healthy(None)
        assert is_healthy is False

        pool.close()

    def test_is_connection_healthy_with_exception(self, mock_connect):
        """Test health check when query raises exception."""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = pyodbc.OperationalError("Connection lost")
        mock_conn = MagicMock(spec=pyodbc.Connection)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        pool = SQLServerConnectionPool(
            host="localhost",
            port=1433,
            database="testdb",
            user="testuser",
            password="testpass",
            min_size=0,
            max_size=3,
        )
        time.sleep(0.1)

        # Health check should return False when exception occurs
        is_healthy = pool._is_connection_healthy(mock_conn)
        assert is_healthy is False

        pool.close()

    def test_close_connection(self, mock_connect):
        """Test closing SQL Server connection."""
        mock_conn = MagicMock(spec=pyodbc.Connection)
        mock_connect.return_value = mock_conn

        pool = SQLServerConnectionPool(
            host="localhost",
            port=1433,
            database="testdb",
            user="testuser",
            password="testpass",
            min_size=1,
            max_size=3,
        )
        time.sleep(0.1)

        pool._close_connection(mock_conn)
        mock_conn.close.assert_called_once()

        pool.close()

    def test_get_db_type(self, mock_connect):
        """Test _get_db_type returns 'sqlserver'."""
        mock_conn = MagicMock(spec=pyodbc.Connection)
        mock_connect.return_value = mock_conn

        pool = SQLServerConnectionPool(
            host="localhost",
            port=1433,
            database="testdb",
            user="testuser",
            password="testpass",
            min_size=1,
            max_size=3,
        )
        time.sleep(0.1)

        assert pool._get_db_type() == "sqlserver"

        pool.close()


class TestGlobalPoolManagement:
    """Test global pool initialization and management."""

    def teardown_method(self):
        """Clean up after each test."""
        close_pools()

    @patch("psycopg2.connect")
    def test_initialize_postgres_pool(self, mock_connect):
        """Test initializing global PostgreSQL pool."""
        mock_conn = MagicMock(spec=psycopg2.extensions.connection)
        mock_connect.return_value = mock_conn

        config = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
            "min_size": 2,
            "max_size": 5,
        }

        initialize_pools(postgres_config=config)
        time.sleep(0.1)

        pool = get_postgres_pool()
        assert pool is not None
        assert isinstance(pool, PostgresConnectionPool)

        close_pools()

    @patch("pyodbc.connect")
    def test_initialize_sqlserver_pool(self, mock_connect):
        """Test initializing global SQL Server pool."""
        mock_conn = MagicMock(spec=pyodbc.Connection)
        mock_connect.return_value = mock_conn

        config = {
            "host": "localhost",
            "port": 1433,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
            "min_size": 2,
            "max_size": 5,
        }

        initialize_pools(sqlserver_config=config)
        time.sleep(0.1)

        pool = get_sqlserver_pool()
        assert pool is not None
        assert isinstance(pool, SQLServerConnectionPool)

        close_pools()

    @patch("psycopg2.connect")
    @patch("pyodbc.connect")
    def test_initialize_both_pools(self, mock_pyodbc_connect, mock_psycopg2_connect):
        """Test initializing both pools simultaneously."""
        mock_pg_conn = MagicMock(spec=psycopg2.extensions.connection)
        mock_psycopg2_connect.return_value = mock_pg_conn

        mock_sql_conn = MagicMock(spec=pyodbc.Connection)
        mock_pyodbc_connect.return_value = mock_sql_conn

        pg_config = {
            "host": "pg-host",
            "port": 5432,
            "database": "pgdb",
            "user": "pguser",
            "password": "pgpass",
            "min_size": 2,
            "max_size": 5,
        }

        sql_config = {
            "host": "sql-host",
            "port": 1433,
            "database": "sqldb",
            "user": "sqluser",
            "password": "sqlpass",
            "min_size": 2,
            "max_size": 5,
        }

        initialize_pools(postgres_config=pg_config, sqlserver_config=sql_config)
        time.sleep(0.1)

        pg_pool = get_postgres_pool()
        sql_pool = get_sqlserver_pool()

        assert pg_pool is not None
        assert sql_pool is not None
        assert isinstance(pg_pool, PostgresConnectionPool)
        assert isinstance(sql_pool, SQLServerConnectionPool)

        close_pools()

    def test_get_pool_before_initialization_raises_error(self):
        """Test that getting pool before initialization raises error."""
        close_pools()  # Ensure pools are closed

        with pytest.raises(RuntimeError, match="PostgreSQL pool not initialized"):
            get_postgres_pool()

        with pytest.raises(RuntimeError, match="SQL Server pool not initialized"):
            get_sqlserver_pool()

    @patch("psycopg2.connect")
    @patch("pyodbc.connect")
    def test_close_pools_closes_all(self, mock_pyodbc_connect, mock_psycopg2_connect):
        """Test that close_pools closes all initialized pools."""
        mock_pg_conn = MagicMock(spec=psycopg2.extensions.connection)
        mock_psycopg2_connect.return_value = mock_pg_conn

        mock_sql_conn = MagicMock(spec=pyodbc.Connection)
        mock_pyodbc_connect.return_value = mock_sql_conn

        pg_config = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
            "min_size": 1,
            "max_size": 3,
        }

        sql_config = {
            "host": "localhost",
            "port": 1433,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
            "min_size": 1,
            "max_size": 3,
        }

        initialize_pools(postgres_config=pg_config, sqlserver_config=sql_config)
        time.sleep(0.1)

        close_pools()

        # Getting pools should now raise errors
        with pytest.raises(RuntimeError):
            get_postgres_pool()

        with pytest.raises(RuntimeError):
            get_sqlserver_pool()
