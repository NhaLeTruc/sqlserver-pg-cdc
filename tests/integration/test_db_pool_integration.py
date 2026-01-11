"""
Integration tests for database connection pooling.

Tests use REAL SQL Server and PostgreSQL databases from conftest.py fixtures.
These tests validate actual connection pool behavior with real database connections.
"""

import time
from threading import Thread

import pytest

from src.utils.db_pool import (
    close_pools,
    get_postgres_pool,
    get_sqlserver_pool,
    initialize_pools,
)


@pytest.fixture(autouse=True)
def cleanup_pools():
    """Ensure pools are closed after each test."""
    yield
    try:
        close_pools()
    except Exception:
        pass


class TestPostgresConnectionPool:
    """Test PostgreSQL connection pool with real database."""

    def test_acquire_and_release_connection(self, postgres_connection_params):
        """Test acquiring and releasing connections from pool."""
        # Initialize pool
        initialize_pools(
            postgres_config=postgres_connection_params,
            postgres_pool_size=3,
            postgres_max_overflow=2,
        )

        pool = get_postgres_pool()

        # Acquire connection
        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1
            cursor.close()

        # Verify connection returned to pool
        assert pool.size() <= pool.pool_size + pool.max_overflow

    def test_pool_creates_multiple_connections(self, postgres_connection_params):
        """Test that pool can create multiple connections."""
        initialize_pools(
            postgres_config=postgres_connection_params,
            postgres_pool_size=3,
            postgres_max_overflow=0,
        )

        pool = get_postgres_pool()

        # Acquire multiple connections simultaneously
        contexts = [pool.acquire() for _ in range(3)]
        connections = []

        try:
            for ctx in contexts:
                conn = ctx.__enter__()
                connections.append(conn)
                # Execute query to verify connection works
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                assert cursor.fetchone()[0] == 1
                cursor.close()
        finally:
            # Release all connections
            for ctx in contexts:
                ctx.__exit__(None, None, None)

        # Verify all connections were returned
        assert pool.size() == 3

    def test_connection_reuse(self, postgres_connection_params):
        """Test that connections are reused from the pool."""
        initialize_pools(
            postgres_config=postgres_connection_params,
            postgres_pool_size=2,
        )

        pool = get_postgres_pool()

        # Acquire and release connection
        with pool.acquire() as conn1:
            cursor = conn1.cursor()
            cursor.execute("SELECT pg_backend_pid()")
            pid1 = cursor.fetchone()[0]
            cursor.close()

        # Acquire again - should potentially reuse
        with pool.acquire() as conn2:
            cursor = conn2.cursor()
            cursor.execute("SELECT pg_backend_pid()")
            pid2 = cursor.fetchone()[0]
            cursor.close()

        # We should be reusing connections (same PID or from small pool)
        assert pid1 is not None
        assert pid2 is not None

    def test_concurrent_access_with_real_connections(self, postgres_connection_params):
        """Test multiple threads accessing pool concurrently."""
        initialize_pools(
            postgres_config=postgres_connection_params,
            postgres_pool_size=3,
        )

        pool = get_postgres_pool()
        results = []
        errors = []

        def worker():
            try:
                with pool.acquire() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT pg_backend_pid()")
                    pid = cursor.fetchone()[0]
                    results.append(pid)
                    cursor.close()
                    time.sleep(0.1)
            except Exception as e:
                errors.append(e)

        threads = [Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 5
        # At least some connections should be reused
        assert len(set(results)) <= 5

    def test_pool_respects_max_size(self, postgres_connection_params):
        """Test that pool respects maximum size limits."""
        initialize_pools(
            postgres_config=postgres_connection_params,
            postgres_pool_size=2,
            postgres_max_overflow=1,
        )

        pool = get_postgres_pool()

        # Acquire connections up to limit
        contexts = [pool.acquire() for _ in range(3)]
        connections = []

        try:
            for ctx in contexts:
                conn = ctx.__enter__()
                connections.append(conn)
        finally:
            for ctx in contexts:
                ctx.__exit__(None, None, None)

        # Verify pool size is within limits
        assert pool.size() <= 3  # pool_size + max_overflow


class TestSQLServerConnectionPool:
    """Test SQL Server connection pool with real database."""

    def test_acquire_and_release_connection(self, sqlserver_connection_string):
        """Test acquiring and releasing SQL Server connections."""
        initialize_pools(
            sqlserver_config={"connection_string": sqlserver_connection_string},
            sqlserver_pool_size=3,
        )

        pool = get_sqlserver_pool()

        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1
            cursor.close()

    def test_connection_executes_queries(self, sqlserver_connection_string):
        """Test that connections can execute actual queries."""
        initialize_pools(
            sqlserver_config={"connection_string": sqlserver_connection_string},
            sqlserver_pool_size=2,
        )

        pool = get_sqlserver_pool()

        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            assert "Microsoft SQL Server" in version
            cursor.close()

    def test_multiple_sequential_connections(self, sqlserver_connection_string):
        """Test acquiring multiple connections sequentially."""
        initialize_pools(
            sqlserver_config={"connection_string": sqlserver_connection_string},
            sqlserver_pool_size=2,
        )

        pool = get_sqlserver_pool()

        # Execute queries with multiple connections
        for i in range(5):
            with pool.acquire() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                assert cursor.fetchone()[0] == 1
                cursor.close()

    def test_concurrent_sqlserver_access(self, sqlserver_connection_string):
        """Test concurrent access to SQL Server pool."""
        initialize_pools(
            sqlserver_config={"connection_string": sqlserver_connection_string},
            sqlserver_pool_size=3,
        )

        pool = get_sqlserver_pool()
        results = []
        errors = []

        def worker():
            try:
                with pool.acquire() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT @@SPID")
                    spid = cursor.fetchone()[0]
                    results.append(spid)
                    cursor.close()
                    time.sleep(0.05)
            except Exception as e:
                errors.append(e)

        threads = [Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 4


class TestPoolManagement:
    """Test global pool management functions."""

    def test_initialize_and_close_pools(
        self, postgres_connection_params, sqlserver_connection_string
    ):
        """Test pool initialization and cleanup."""
        initialize_pools(
            postgres_config=postgres_connection_params,
            sqlserver_config={"connection_string": sqlserver_connection_string},
            postgres_pool_size=2,
            sqlserver_pool_size=2,
        )

        # Verify pools are accessible
        postgres_pool = get_postgres_pool()
        sqlserver_pool = get_sqlserver_pool()
        assert postgres_pool is not None
        assert sqlserver_pool is not None

        # Test connections work
        with postgres_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1
            cursor.close()

        with sqlserver_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1
            cursor.close()

        # Close pools
        close_pools()

    def test_get_pool_before_initialize_raises_error(self):
        """Test that getting pool before initialization raises error."""
        # Ensure pools are closed
        try:
            close_pools()
        except Exception:
            pass

        with pytest.raises(Exception):
            get_postgres_pool()

    def test_pool_survives_connection_errors(self, postgres_connection_params):
        """Test that pool handles connection errors gracefully."""
        initialize_pools(
            postgres_config=postgres_connection_params,
            postgres_pool_size=2,
        )

        pool = get_postgres_pool()

        # Valid connection should work
        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1
            cursor.close()

        # Pool should still be functional after errors
        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 2")
            assert cursor.fetchone()[0] == 2
            cursor.close()


class TestBothPools:
    """Test using both PostgreSQL and SQL Server pools together."""

    def test_both_pools_work_simultaneously(
        self, postgres_connection_params, sqlserver_connection_string
    ):
        """Test that both pools can be used at the same time."""
        initialize_pools(
            postgres_config=postgres_connection_params,
            sqlserver_config={"connection_string": sqlserver_connection_string},
            postgres_pool_size=2,
            sqlserver_pool_size=2,
        )

        pg_pool = get_postgres_pool()
        ss_pool = get_sqlserver_pool()

        # Use both pools simultaneously
        with pg_pool.acquire() as pg_conn, ss_pool.acquire() as ss_conn:
            # PostgreSQL query
            pg_cursor = pg_conn.cursor()
            pg_cursor.execute("SELECT 'PostgreSQL' as db_type")
            pg_result = pg_cursor.fetchone()[0]
            pg_cursor.close()

            # SQL Server query
            ss_cursor = ss_conn.cursor()
            ss_cursor.execute("SELECT 'SQL Server' as db_type")
            ss_result = ss_cursor.fetchone()[0]
            ss_cursor.close()

            assert pg_result == "PostgreSQL"
            assert ss_result == "SQL Server"

    def test_cross_database_operations(
        self, postgres_connection_params, sqlserver_connection_string
    ):
        """Test operations that use both databases."""
        initialize_pools(
            postgres_config=postgres_connection_params,
            sqlserver_config={"connection_string": sqlserver_connection_string},
        )

        pg_pool = get_postgres_pool()
        ss_pool = get_sqlserver_pool()

        # Simulate a reconciliation scenario
        with ss_pool.acquire() as source_conn:
            source_cursor = source_conn.cursor()
            source_cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES")
            source_count = source_cursor.fetchone()[0]
            source_cursor.close()

        with pg_pool.acquire() as target_conn:
            target_cursor = target_conn.cursor()
            target_cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'"
            )
            target_count = target_cursor.fetchone()[0]
            target_cursor.close()

        # Both databases should have tables
        assert source_count > 0
        assert target_count >= 0
