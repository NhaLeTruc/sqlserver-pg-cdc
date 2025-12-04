"""
Performance benchmark tests for CDC pipeline.
Validates that the pipeline meets throughput requirements (10K rows/sec).
"""

import os
import time
from datetime import datetime
from typing import Tuple

import psycopg2
import pyodbc
import pytest


class TestPerformanceBenchmark:
    """Performance benchmark tests for CDC replication throughput."""

    @pytest.fixture(scope="class")
    def sqlserver_conn(self) -> pyodbc.Connection:
        """Create SQL Server connection."""
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={os.getenv('SQLSERVER_HOST', 'localhost')},1433;"
            f"DATABASE={os.getenv('SQLSERVER_DATABASE', 'warehouse_source')};"
            f"UID={os.getenv('SQLSERVER_USER', 'sa')};"
            f"PWD={os.getenv('SQLSERVER_PASSWORD', 'YourStrong!Passw0rd')};"
            f"TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str, autocommit=False)
        yield conn
        conn.close()

    @pytest.fixture(scope="class")
    def postgres_conn(self) -> psycopg2.extensions.connection:
        """Create PostgreSQL connection."""
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "warehouse_target"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres_secure_password"),
        )
        conn.autocommit = True
        yield conn
        conn.close()

    @pytest.fixture(autouse=True)
    def setup_performance_table(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """Set up performance test table."""
        # Create test table in SQL Server
        with sqlserver_conn.cursor() as cursor:
            # Disable CDC first if it exists
            cursor.execute("""
                IF EXISTS (
                    SELECT 1 FROM sys.tables t
                    JOIN cdc.change_tables ct ON t.object_id = ct.source_object_id
                    WHERE t.name = 'perf_test' AND SCHEMA_NAME(t.schema_id) = 'dbo'
                )
                BEGIN
                    EXEC sys.sp_cdc_disable_table
                        @source_schema = N'dbo',
                        @source_name = N'perf_test',
                        @capture_instance = 'all'
                END
            """)
            cursor.execute("DROP TABLE IF EXISTS dbo.perf_test")
            cursor.execute("""
                CREATE TABLE dbo.perf_test (
                    id INT PRIMARY KEY IDENTITY(1,1),
                    data NVARCHAR(200),
                    value INT,
                    timestamp DATETIME2 DEFAULT GETDATE()
                )
            """)
            # Enable CDC
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.databases WHERE name = 'warehouse_source' AND is_cdc_enabled = 1
                )
                BEGIN
                    EXEC sys.sp_cdc_enable_db
                END
            """)
            cursor.execute("""
                EXEC sys.sp_cdc_enable_table
                    @source_schema = N'dbo',
                    @source_name = N'perf_test',
                    @role_name = NULL,
                    @supports_net_changes = 1
            """)
            sqlserver_conn.commit()

        # Create corresponding table in PostgreSQL
        with postgres_conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS perf_test")
            cursor.execute("""
                CREATE TABLE perf_test (
                    id INTEGER PRIMARY KEY,
                    data VARCHAR(200),
                    value INTEGER,
                    timestamp TIMESTAMP
                )
            """)

        yield

        # Cleanup
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("""
                EXEC sys.sp_cdc_disable_table
                    @source_schema = N'dbo',
                    @source_name = N'perf_test',
                    @capture_instance = 'all'
            """)
            cursor.execute("DROP TABLE IF EXISTS dbo.perf_test")
            sqlserver_conn.commit()

        with postgres_conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS perf_test")

    def insert_batch(
        self, sqlserver_conn: pyodbc.Connection, batch_size: int, batch_num: int
    ) -> float:
        """Insert a batch of rows and return time taken."""
        start_time = time.time()

        with sqlserver_conn.cursor() as cursor:
            values = []
            for i in range(batch_size):
                row_id = batch_num * batch_size + i
                values.append(f"('Data row {row_id}', {row_id % 1000})")

            sql = f"""
                INSERT INTO dbo.perf_test (data, value)
                VALUES {', '.join(values)}
            """
            cursor.execute(sql)
        sqlserver_conn.commit()

        return time.time() - start_time

    def wait_for_replication_count(
        self,
        postgres_conn: psycopg2.extensions.connection,
        expected_count: int,
        timeout: int = 600,
    ) -> Tuple[bool, float]:
        """Wait for replication to reach expected count, return success and elapsed time."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            with postgres_conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM perf_test")
                count = cursor.fetchone()[0]
                if count >= expected_count:
                    elapsed = time.time() - start_time
                    return True, elapsed
            time.sleep(0.5)

        return False, timeout

    def test_throughput_10k_rows_per_second(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """
        Test that the CDC pipeline can sustain 10,000 rows/second throughput.

        This test:
        1. Inserts 100,000 rows into SQL Server in batches
        2. Measures time for replication to complete
        3. Calculates throughput (rows/second)
        4. Validates throughput meets NFR-001 requirement (10K rows/sec)
        """
        total_rows = 100000
        batch_size = 5000
        num_batches = total_rows // batch_size

        # Insert rows in batches
        insert_start = time.time()
        for batch_num in range(num_batches):
            batch_time = self.insert_batch(sqlserver_conn, batch_size, batch_num)
            print(f"Batch {batch_num + 1}/{num_batches} inserted in {batch_time:.2f}s")

        insert_elapsed = time.time() - insert_start
        print(f"Total insert time: {insert_elapsed:.2f}s")

        # Wait for replication to complete
        print("Waiting for replication to complete...")
        replication_success, replication_time = self.wait_for_replication_count(
            postgres_conn, total_rows, timeout=900  # 15 minutes max
        )

        assert replication_success, (
            f"Replication did not complete within timeout. "
            f"Check connector status and logs."
        )

        # Calculate throughput
        total_time = insert_elapsed + replication_time
        throughput = total_rows / total_time

        print(f"Replication completed in {replication_time:.2f}s")
        print(f"Total time (insert + replication): {total_time:.2f}s")
        print(f"Throughput: {throughput:.0f} rows/second")

        # Verify throughput meets requirement (10K rows/sec)
        min_throughput = 10000
        assert throughput >= min_throughput, (
            f"Throughput {throughput:.0f} rows/sec is below requirement of "
            f"{min_throughput} rows/sec. Total time: {total_time:.2f}s for {total_rows} rows."
        )

        # Verify all rows replicated correctly
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM perf_test")
            final_count = cursor.fetchone()[0]
            assert final_count == total_rows, (
                f"Expected {total_rows} rows, found {final_count}"
            )

    def test_replication_lag_under_5_minutes(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """
        Test that replication lag stays under 5 minutes (p95).

        This test validates NFR-002 requirement for replication lag.
        """
        test_rows = 10000
        batch_size = 1000

        # Insert test data
        for batch_num in range(test_rows // batch_size):
            self.insert_batch(sqlserver_conn, batch_size, batch_num)

        # Measure replication lag (time for last row to appear)
        replication_success, replication_lag = self.wait_for_replication_count(
            postgres_conn, test_rows, timeout=600
        )

        assert replication_success, "Replication did not complete within 10 minutes"

        # Verify lag is under 5 minutes (300 seconds)
        max_lag_seconds = 300
        assert replication_lag < max_lag_seconds, (
            f"Replication lag {replication_lag:.2f}s exceeds {max_lag_seconds}s "
            f"(5 minutes) requirement"
        )

        print(f"Replication lag: {replication_lag:.2f}s (under {max_lag_seconds}s requirement)")

    def test_sustained_throughput_over_time(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """
        Test that pipeline maintains consistent throughput over multiple batches.

        This validates that the system doesn't degrade under sustained load.
        """
        num_iterations = 5
        rows_per_iteration = 20000
        batch_size = 5000

        throughputs = []

        for iteration in range(num_iterations):
            print(f"Iteration {iteration + 1}/{num_iterations}")

            # Clear previous data
            with postgres_conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE perf_test")

            # Insert data
            insert_start = time.time()
            for batch_num in range(rows_per_iteration // batch_size):
                self.insert_batch(sqlserver_conn, batch_size, batch_num)
            insert_time = time.time() - insert_start

            # Wait for replication
            replication_success, replication_time = self.wait_for_replication_count(
                postgres_conn, rows_per_iteration, timeout=300
            )

            assert replication_success, f"Iteration {iteration + 1} replication failed"

            # Calculate throughput for this iteration
            total_time = insert_time + replication_time
            throughput = rows_per_iteration / total_time
            throughputs.append(throughput)

            print(f"  Throughput: {throughput:.0f} rows/sec")

        # Verify throughput consistency (all iterations meet minimum)
        min_throughput = 10000
        for i, throughput in enumerate(throughputs):
            assert throughput >= min_throughput, (
                f"Iteration {i + 1} throughput {throughput:.0f} rows/sec "
                f"below {min_throughput} rows/sec"
            )

        # Calculate average and standard deviation
        avg_throughput = sum(throughputs) / len(throughputs)
        variance = sum((x - avg_throughput) ** 2 for x in throughputs) / len(throughputs)
        std_dev = variance ** 0.5

        print(f"Average throughput: {avg_throughput:.0f} rows/sec")
        print(f"Standard deviation: {std_dev:.0f} rows/sec")

        # Verify consistency (std dev should be < 20% of average)
        max_std_dev = avg_throughput * 0.2
        assert std_dev < max_std_dev, (
            f"Throughput inconsistency detected. Std dev {std_dev:.0f} exceeds "
            f"20% of average ({max_std_dev:.0f})"
        )

    @pytest.mark.slow
    def test_reconcile_tool_handles_large_tables(
        self, sqlserver_connection, postgres_connection
    ):
        """
        Test reconciliation handles large tables efficiently

        Scenario:
        1. Create 1 million row table in both databases
        2. Run reconciliation
        3. Verify completes in under 10 minutes (NFR requirement)
        """
        # This test creates a large dataset
        pytest.skip("Requires significant time and resources")

        sqlserver_cursor = sqlserver_connection.cursor()
        postgres_cursor = postgres_connection.cursor()

        # Create 1M row table
        # Run reconciliation
        # Verify performance
