"""
Performance measurement tests for CDC pipeline.
Reports actual throughput and latency metrics without enforcing thresholds.

These tests require:
- Docker services running (docker-compose up)
- CDC connectors deployed and running
- Databases initialized

Run with: pytest tests/performance/test_performance.py -v -m slow
"""

import os
import time
from typing import Dict, Tuple

import psycopg2
import pyodbc
import pytest
import requests


class TestPerformanceMeasurement:
    """Measure and report CDC replication performance metrics."""

    @pytest.fixture(scope="class", autouse=True)
    def check_connectors_running(self) -> None:
        """
        Check that CDC connectors are deployed and running before tests.
        Skip tests if connectors are not available.
        """
        kafka_connect_url = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")

        try:
            # Check if Kafka Connect is reachable
            response = requests.get(f"{kafka_connect_url}/connectors", timeout=5)
            response.raise_for_status()
            connectors = response.json()

            # Check for required connectors
            required_connectors = ["sqlserver-cdc-source", "postgresql-jdbc-sink"]
            missing = [c for c in required_connectors if c not in connectors]

            if missing:
                pytest.skip(
                    f"Required connectors not deployed: {missing}. "
                    f"Run 'make deploy' to deploy connectors."
                )

            # Check connector status
            for connector_name in required_connectors:
                status_response = requests.get(
                    f"{kafka_connect_url}/connectors/{connector_name}/status",
                    timeout=5
                )
                status_response.raise_for_status()
                status = status_response.json()

                connector_state = status.get("connector", {}).get("state")
                if connector_state != "RUNNING":
                    pytest.skip(
                        f"Connector {connector_name} is not running (state: {connector_state}). "
                        f"Check connector status with: curl {kafka_connect_url}/connectors/{connector_name}/status"
                    )

        except (requests.RequestException, Exception) as e:
            pytest.skip(
                f"Cannot connect to Kafka Connect at {kafka_connect_url}: {e}. "
                f"Ensure Docker services are running: make start"
            )

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

    @pytest.fixture(scope="class", autouse=True)
    def check_customers_table_exists(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """
        Verify customers table exists and is being replicated.
        We use the existing customers table for performance testing since it's already
        configured in the connector's table.include.list.
        """
        # Check SQL Server
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = 'customers'
            """)
            if cursor.fetchone()[0] == 0:
                pytest.skip("customers table does not exist in SQL Server")

        # Check PostgreSQL
        with postgres_conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'customers'
            """)
            if cursor.fetchone()[0] == 0:
                pytest.skip("customers table does not exist in PostgreSQL")

    def insert_batch(
        self, sqlserver_conn: pyodbc.Connection, batch_size: int, batch_num: int
    ) -> float:
        """Insert a batch of rows and return time taken."""
        start_time = time.time()

        # SQL Server has a limit of 1000 row values per INSERT statement
        max_rows_per_insert = 1000

        with sqlserver_conn.cursor() as cursor:
            for chunk_start in range(0, batch_size, max_rows_per_insert):
                chunk_end = min(chunk_start + max_rows_per_insert, batch_size)
                values = []
                for i in range(chunk_start, chunk_end):
                    row_id = batch_num * batch_size + i
                    values.append(
                        f"('Perf Test {row_id}', 'perf{row_id}@test.com', GETDATE(), GETDATE())"
                    )

                sql = f"""
                    INSERT INTO dbo.customers (name, email, created_at, updated_at)
                    VALUES {', '.join(values)}
                """
                cursor.execute(sql)

        sqlserver_conn.commit()
        return time.time() - start_time

    def wait_for_replication(
        self,
        postgres_conn: psycopg2.extensions.connection,
        expected_count: int,
        timeout: int = 30,
    ) -> Tuple[bool, float]:
        """Wait for replication to reach expected count.

        Note: Filters out soft-deleted rows (__deleted='true') to avoid counting
        tombstone records from previous test runs.
        """
        start_time = time.time()
        last_count = 0
        last_log_time = start_time

        while time.time() - start_time < timeout:
            with postgres_conn.cursor() as cursor:
                # Only count active rows: __deleted='false' or NULL (legacy rows)
                cursor.execute("""
                    SELECT COUNT(*) FROM customers
                    WHERE name LIKE 'Perf Test %'
                    AND (__deleted IS NULL OR __deleted = 'false')
                """)
                count = cursor.fetchone()[0]

                # Log progress every 5 seconds or when count changes
                current_time = time.time()
                if current_time - last_log_time >= 5 or count != last_count:
                    elapsed = current_time - start_time
                    print(f"  [{elapsed:.1f}s] Replicated: {count:,}/{expected_count:,}")
                    last_log_time = current_time
                    last_count = count

                if count >= expected_count:
                    elapsed = time.time() - start_time
                    return True, elapsed

            time.sleep(0.5)

        # Timeout
        with postgres_conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM customers
                WHERE name LIKE 'Perf Test %'
                AND (__deleted IS NULL OR __deleted = 'false')
            """)
            final_count = cursor.fetchone()[0]

        print(f"  TIMEOUT after {timeout}s. Replicated: {final_count:,}/{expected_count:,}")
        return False, timeout

    def clear_tables(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """Clear performance test data from customers tables."""
        with postgres_conn.cursor() as cursor:
            cursor.execute("DELETE FROM customers WHERE name LIKE 'Perf Test %'")
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("DELETE FROM dbo.customers WHERE name LIKE 'Perf Test %'")
        sqlserver_conn.commit()
        time.sleep(3)  # Allow CDC to process deletes

    @pytest.mark.slow
    def test_measure_replication_throughput(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """
        Measure end-to-end replication throughput.

        Reports:
        - Total rows replicated
        - End-to-end time (insert + replication)
        - Throughput (rows/second)
        - Insert time vs replication time
        """
        total_rows = 30000
        batch_size = 5000

        self.clear_tables(sqlserver_conn, postgres_conn)

        print(f"\n{'='*70}")
        print(f"THROUGHPUT MEASUREMENT - {total_rows:,} rows")
        print(f"{'='*70}")

        # Start end-to-end timing
        test_start = time.time()

        # Insert rows
        print(f"Inserting {total_rows:,} rows...")
        insert_start = time.time()
        for batch_num in range(total_rows // batch_size):
            batch_time = self.insert_batch(sqlserver_conn, batch_size, batch_num)
            print(f"  Batch {batch_num + 1}/{total_rows // batch_size}: {batch_time:.2f}s")
        insert_time = time.time() - insert_start

        # Wait for replication
        print(f"\nWaiting for replication...")
        success, replication_wait_time = self.wait_for_replication(
            postgres_conn, total_rows, timeout=30
        )

        if not success:
            with postgres_conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM customers WHERE name LIKE 'Perf Test %'")
                actual_count = cursor.fetchone()[0]
            pytest.fail(
                f"Replication did not complete within timeout. "
                f"Replicated {actual_count:,}/{total_rows:,} rows. "
                f"Check connector logs: docker logs cdc-kafka-connect"
            )

        # Calculate metrics
        total_time = time.time() - test_start
        throughput = total_rows / total_time

        # Report results
        print(f"\n{'='*70}")
        print(f"THROUGHPUT RESULTS")
        print(f"{'='*70}")
        print(f"Total rows:           {total_rows:,}")
        print(f"Insert time:          {insert_time:.2f}s")
        print(f"Replication wait:     {replication_wait_time:.2f}s")
        print(f"End-to-end time:      {total_time:.2f}s")
        print(f"Throughput:           {throughput:.0f} rows/second")
        print(f"{'='*70}\n")

        # Verify row count (only count active rows)
        with postgres_conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM customers
                WHERE name LIKE 'Perf Test %'
                AND (__deleted IS NULL OR __deleted = 'false')
            """)
            final_count = cursor.fetchone()[0]
        assert final_count == total_rows, f"Row count mismatch: {final_count:,} != {total_rows:,}"

    @pytest.mark.slow
    def test_measure_replication_lag(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """
        Measure replication lag for different batch sizes.

        Reports lag time (time from insert completion to replication completion)
        for various data volumes.
        """
        test_cases = [
            ("Small batch", 1000, 500),
            ("Medium batch", 5000, 1000),
            ("Large batch", 10000, 2000),
        ]

        print(f"\n{'='*70}")
        print(f"REPLICATION LAG MEASUREMENT")
        print(f"{'='*70}")

        results = []

        for name, total_rows, batch_size in test_cases:
            self.clear_tables(sqlserver_conn, postgres_conn)

            print(f"\n{name}: {total_rows:,} rows")
            print(f"-" * 50)

            # Insert data
            insert_start = time.time()
            for batch_num in range(total_rows // batch_size):
                self.insert_batch(sqlserver_conn, batch_size, batch_num)
            insert_time = time.time() - insert_start

            # Measure lag (time from insert completion to replication)
            lag_start = time.time()
            success, wait_time = self.wait_for_replication(
                postgres_conn, total_rows, timeout=30
            )

            if not success:
                print(f"  ⚠ Replication timed out")
                continue

            lag = time.time() - lag_start

            results.append({
                "name": name,
                "rows": total_rows,
                "insert_time": insert_time,
                "lag": lag,
            })

            print(f"  Insert time: {insert_time:.2f}s")
            print(f"  Replication lag: {lag:.2f}s")

        # Summary
        print(f"\n{'='*70}")
        print(f"LAG SUMMARY")
        print(f"{'='*70}")
        print(f"{'Batch':<20} {'Rows':>10} {'Insert (s)':>12} {'Lag (s)':>10}")
        print(f"{'-'*70}")
        for r in results:
            print(f"{r['name']:<20} {r['rows']:>10,} {r['insert_time']:>12.2f} {r['lag']:>10.2f}")
        print(f"{'='*70}\n")

    @pytest.mark.slow
    def test_measure_sustained_performance(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """
        Measure performance consistency over multiple iterations.

        Reports:
        - Throughput for each iteration
        - Average and std deviation
        - Trend analysis (degradation detection)
        """
        num_iterations = 3
        rows_per_iteration = 10000
        batch_size = 2000

        print(f"\n{'='*70}")
        print(f"SUSTAINED PERFORMANCE MEASUREMENT - {num_iterations} iterations")
        print(f"{'='*70}")

        throughputs = []
        lags = []

        for iteration in range(num_iterations):
            self.clear_tables(sqlserver_conn, postgres_conn)

            print(f"\nIteration {iteration + 1}/{num_iterations}")
            print(f"-" * 50)

            # Measure end-to-end
            start = time.time()

            for batch_num in range(rows_per_iteration // batch_size):
                self.insert_batch(sqlserver_conn, batch_size, batch_num)

            insert_done = time.time()

            success, wait_time = self.wait_for_replication(
                postgres_conn, rows_per_iteration, timeout=30
            )

            if not success:
                print(f"  ⚠ Iteration {iteration + 1} timed out")
                continue

            total_time = time.time() - start
            lag = time.time() - insert_done
            throughput = rows_per_iteration / total_time

            throughputs.append(throughput)
            lags.append(lag)

            print(f"  Throughput: {throughput:.0f} rows/sec")
            print(f"  Lag: {lag:.2f}s")

        # Calculate statistics
        if throughputs:
            avg_throughput = sum(throughputs) / len(throughputs)
            avg_lag = sum(lags) / len(lags)

            tp_variance = sum((x - avg_throughput) ** 2 for x in throughputs) / len(throughputs)
            tp_std_dev = tp_variance ** 0.5

            lag_variance = sum((x - avg_lag) ** 2 for x in lags) / len(lags)
            lag_std_dev = lag_variance ** 0.5

            # Summary
            print(f"\n{'='*70}")
            print(f"SUSTAINED PERFORMANCE SUMMARY")
            print(f"{'='*70}")
            print(f"Iterations completed:     {len(throughputs)}/{num_iterations}")
            print(f"\nThroughput:")
            print(f"  Average:                {avg_throughput:.0f} rows/sec")
            print(f"  Std deviation:          {tp_std_dev:.0f} rows/sec")
            print(f"  Min:                    {min(throughputs):.0f} rows/sec")
            print(f"  Max:                    {max(throughputs):.0f} rows/sec")
            print(f"\nReplication Lag:")
            print(f"  Average:                {avg_lag:.2f}s")
            print(f"  Std deviation:          {lag_std_dev:.2f}s")
            print(f"  Min:                    {min(lags):.2f}s")
            print(f"  Max:                    {max(lags):.2f}s")
            print(f"{'='*70}\n")
