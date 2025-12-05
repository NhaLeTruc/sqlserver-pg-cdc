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
import requests


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

    @pytest.fixture(scope="class", autouse=True)
    def setup_test_table(
        self, sqlserver_conn: pyodbc.Connection, postgres_conn: psycopg2.extensions.connection
    ) -> None:
        """Set up test table once for all tests in the class."""
        # Create test table in SQL Server
        with sqlserver_conn.cursor() as cursor:
            # Disable CDC first if it exists
            cursor.execute("""
                IF EXISTS (
                    SELECT 1 FROM sys.tables t
                    JOIN cdc.change_tables ct ON t.object_id = ct.source_object_id
                    WHERE t.name = 'test_customers' AND SCHEMA_NAME(t.schema_id) = 'dbo'
                )
                BEGIN
                    EXEC sys.sp_cdc_disable_table
                        @source_schema = N'dbo',
                        @source_name = N'test_customers',
                        @capture_instance = 'all'
                END
            """)
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

        # Truncate PostgreSQL table if it exists (preserve schema for connector)
        with postgres_conn.cursor() as cursor:
            try:
                cursor.execute("TRUNCATE TABLE test_customers")
            except psycopg2.errors.UndefinedTable:
                # Table doesn't exist yet, connector will create it
                pass

        # Wait for connector to detect the new SQL Server CDC table
        time.sleep(10)

        yield

        # Cleanup - truncate PostgreSQL table
        with postgres_conn.cursor() as cursor:
            try:
                cursor.execute("TRUNCATE TABLE test_customers")
            except psycopg2.errors.UndefinedTable:
                pass

        # Cleanup SQL Server
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
        retries: int = 3,
    ) -> bool:
        """Wait for replication to complete by polling PostgreSQL."""
        i = 0
        while i <= retries:
            try:
                with postgres_conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM test_customers")
                    count = cursor.fetchone()[0]
                    if count >= expected_count:
                        return True
            except psycopg2.errors.UndefinedTable:
                # Table doesn't exist yet, connector will create it
                pass
            i += 1
            time.sleep(5)
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

        # Wait for initial insert (cumulative count, previous test inserted 1 row)
        assert self.wait_for_replication(postgres_conn, 2)

        # Update data in SQL Server
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("""
                UPDATE dbo.test_customers
                SET age = 26, updated_at = GETDATE()
                WHERE name = 'Jane Smith'
            """)
            sqlserver_conn.commit()

        # Wait for update to propagate with retries
        max_retries = 6
        for attempt in range(max_retries):
            time.sleep(5)
            with postgres_conn.cursor() as cursor:
                cursor.execute("SELECT age FROM test_customers WHERE name = 'Jane Smith'")
                row = cursor.fetchone()
                if row and row[0] == 26:
                    break

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

        # Wait for insert (cumulative count, previous tests inserted 2 rows)
        assert self.wait_for_replication(postgres_conn, 3)

        # Delete data in SQL Server
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("DELETE FROM dbo.test_customers WHERE name = 'Bob Johnson'")
            sqlserver_conn.commit()

        # Wait for delete to propagate with retries
        max_retries = 6
        deleted_marked = False
        for attempt in range(max_retries):
            time.sleep(5)
            with postgres_conn.cursor() as cursor:
                cursor.execute("SELECT __deleted FROM test_customers WHERE name = 'Bob Johnson'")
                row = cursor.fetchone()
                if row and row[0] == 'true':
                    deleted_marked = True
                    break

        # Verify data marked as deleted in PostgreSQL (__deleted column)
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT __deleted FROM test_customers WHERE name = 'Bob Johnson'")
            row = cursor.fetchone()
            assert row is not None, "Row not found in PostgreSQL"
            assert row[0] == 'true', f"Row should be marked as deleted (__deleted='true'), got __deleted='{row[0]}'"

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

        # Wait for replication (cumulative count: previous tests inserted 3, all still present with soft delete)
        assert self.wait_for_replication(postgres_conn, 6), (
            "Transaction replication did not complete"
        )

        # Verify these 3 specific rows are present
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM test_customers WHERE name IN ('Alice Brown', 'Charlie Davis', 'Diana Evans')")
            count = cursor.fetchone()[0]
            assert count == 3, f"Expected 3 new rows, got {count}"

            # Verify all names are present
            cursor.execute("SELECT name FROM test_customers WHERE name IN ('Alice Brown', 'Charlie Davis', 'Diana Evans') ORDER BY name")
            names = [row[0] for row in cursor.fetchall()]
            expected_names = ["Alice Brown", "Charlie Davis", "Diana Evans"]
            assert names == expected_names, f"Names mismatch: {names} != {expected_names}"

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

        # Wait for replication (cumulative count: 6 previous rows + 1 new = 7 total)
        assert self.wait_for_replication(postgres_conn, 7)

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
