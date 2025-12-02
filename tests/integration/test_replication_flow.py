"""
Integration tests for SQL Server to PostgreSQL CDC replication flow.
Tests INSERT, UPDATE, DELETE operations, transactional consistency, bulk operations, and NULL handling.

These tests require the full Docker Compose stack to be running.
"""

import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
import pyodbc
import pytest


class TestReplicationFlow:
    """Integration tests for CDC replication from SQL Server to PostgreSQL."""

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
    def setup_test_table(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """Set up test table before each test."""
        # Create test table in SQL Server
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS dbo.test_customers")
            cursor.execute("""
                CREATE TABLE dbo.test_customers (
                    id INT PRIMARY KEY IDENTITY(1,1),
                    name NVARCHAR(100),
                    email NVARCHAR(100),
                    age INT,
                    created_at DATETIME2 DEFAULT GETDATE(),
                    updated_at DATETIME2
                )
            """)
            # Enable CDC on the table
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
                    @source_name = N'test_customers',
                    @role_name = NULL,
                    @supports_net_changes = 1
            """)
            sqlserver_conn.commit()

        # Create corresponding table in PostgreSQL
        with postgres_conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS test_customers")
            cursor.execute("""
                CREATE TABLE test_customers (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    age INTEGER,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """)

        yield

        # Cleanup
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("""
                EXEC sys.sp_cdc_disable_table
                    @source_schema = N'dbo',
                    @source_name = N'test_customers',
                    @capture_instance = 'all'
            """)
            cursor.execute("DROP TABLE IF EXISTS dbo.test_customers")
            sqlserver_conn.commit()

        with postgres_conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS test_customers")

    def wait_for_replication(
        self,
        postgres_conn: psycopg2.extensions.connection,
        expected_count: int,
        timeout: int = 300,
    ) -> bool:
        """Wait for replication to complete by polling PostgreSQL."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            with postgres_conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM test_customers")
                count = cursor.fetchone()[0]
                if count >= expected_count:
                    return True
            time.sleep(1)
        return False

    def test_insert_replication(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """Test that INSERT operations are replicated from SQL Server to PostgreSQL."""
        # Insert data into SQL Server
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dbo.test_customers (name, email, age)
                VALUES ('John Doe', 'john@example.com', 30)
            """)
            sqlserver_conn.commit()

        # Wait for replication
        assert self.wait_for_replication(postgres_conn, 1), (
            "Replication did not complete within timeout"
        )

        # Verify data in PostgreSQL
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT name, email, age FROM test_customers WHERE name = 'John Doe'")
            row = cursor.fetchone()
            assert row is not None, "Row not found in PostgreSQL"
            assert row[0] == "John Doe"
            assert row[1] == "john@example.com"
            assert row[2] == 30

    def test_update_replication(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """Test that UPDATE operations are replicated from SQL Server to PostgreSQL."""
        # Insert initial data
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dbo.test_customers (name, email, age)
                VALUES ('Jane Smith', 'jane@example.com', 25)
            """)
            sqlserver_conn.commit()

        # Wait for initial insert
        assert self.wait_for_replication(postgres_conn, 1)

        # Update data in SQL Server
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("""
                UPDATE dbo.test_customers
                SET age = 26, updated_at = GETDATE()
                WHERE name = 'Jane Smith'
            """)
            sqlserver_conn.commit()

        # Wait for update to propagate
        time.sleep(5)

        # Verify updated data in PostgreSQL
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT age FROM test_customers WHERE name = 'Jane Smith'")
            row = cursor.fetchone()
            assert row is not None, "Row not found in PostgreSQL"
            assert row[0] == 26, f"Age not updated, expected 26, got {row[0]}"

    def test_delete_replication(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """Test that DELETE operations are replicated from SQL Server to PostgreSQL."""
        # Insert data
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dbo.test_customers (name, email, age)
                VALUES ('Bob Johnson', 'bob@example.com', 40)
            """)
            sqlserver_conn.commit()

        # Wait for insert
        assert self.wait_for_replication(postgres_conn, 1)

        # Delete data in SQL Server
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("DELETE FROM dbo.test_customers WHERE name = 'Bob Johnson'")
            sqlserver_conn.commit()

        # Wait for delete to propagate
        time.sleep(5)

        # Verify data deleted in PostgreSQL
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM test_customers WHERE name = 'Bob Johnson'")
            count = cursor.fetchone()[0]
            assert count == 0, f"Row should be deleted, found {count} rows"

    def test_transactional_consistency(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """Test that multi-row transactions maintain consistency during replication."""
        # Insert multiple rows in a single transaction
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dbo.test_customers (name, email, age)
                VALUES
                    ('Alice Brown', 'alice@example.com', 28),
                    ('Charlie Davis', 'charlie@example.com', 35),
                    ('Diana Evans', 'diana@example.com', 32)
            """)
            sqlserver_conn.commit()

        # Wait for replication
        assert self.wait_for_replication(postgres_conn, 3), (
            "Transaction replication did not complete"
        )

        # Verify all rows are present
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM test_customers")
            count = cursor.fetchone()[0]
            assert count == 3, f"Expected 3 rows, got {count}"

            # Verify all names are present
            cursor.execute("SELECT name FROM test_customers ORDER BY name")
            names = [row[0] for row in cursor.fetchall()]
            expected_names = ["Alice Brown", "Charlie Davis", "Diana Evans"]
            assert names == expected_names, f"Names mismatch: {names} != {expected_names}"

    def test_bulk_insert_10k_rows(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """Test bulk insert of 10K rows replicates successfully."""
        # Generate 10K rows
        rows_to_insert = 10000
        batch_size = 1000

        with sqlserver_conn.cursor() as cursor:
            for batch_start in range(0, rows_to_insert, batch_size):
                values = []
                for i in range(batch_start, min(batch_start + batch_size, rows_to_insert)):
                    values.append(
                        f"('User{i}', 'user{i}@example.com', {20 + (i % 50)})"
                    )

                sql = f"""
                    INSERT INTO dbo.test_customers (name, email, age)
                    VALUES {', '.join(values)}
                """
                cursor.execute(sql)
            sqlserver_conn.commit()

        # Wait for replication (may take longer for 10K rows)
        assert self.wait_for_replication(postgres_conn, rows_to_insert, timeout=600), (
            f"Bulk replication of {rows_to_insert} rows did not complete within timeout"
        )

        # Verify count in PostgreSQL
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM test_customers")
            count = cursor.fetchone()[0]
            assert count == rows_to_insert, (
                f"Expected {rows_to_insert} rows, got {count}"
            )

    def test_null_value_handling(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """Test that NULL values are preserved (not converted to empty strings)."""
        # Insert row with NULL values
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dbo.test_customers (name, email, age, updated_at)
                VALUES ('Frank Green', NULL, NULL, NULL)
            """)
            sqlserver_conn.commit()

        # Wait for replication
        assert self.wait_for_replication(postgres_conn, 1)

        # Verify NULL values in PostgreSQL
        with postgres_conn.cursor() as cursor:
            cursor.execute("""
                SELECT email, age, updated_at
                FROM test_customers
                WHERE name = 'Frank Green'
            """)
            row = cursor.fetchone()
            assert row is not None, "Row not found in PostgreSQL"

            # Verify NULL values are preserved (not empty strings or 0)
            assert row[0] is None, f"email should be NULL, got {row[0]}"
            assert row[1] is None, f"age should be NULL, got {row[1]}"
            assert row[2] is None, f"updated_at should be NULL, got {row[2]}"
