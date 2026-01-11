"""
Independent integration tests for SQL Server to PostgreSQL CDC replication.

Each test is completely independent with its own setup and teardown,
addressing BUG-003 and BUG-008 from SWOT analysis.

These tests can run in any order without dependencies on other tests.
"""

import os
import time

import psycopg2
import pyodbc
import pytest


class TestReplicationIndependent:
    """Independent integration tests for CDC replication."""

    @pytest.fixture(scope="class", autouse=True)
    def ensure_test_table_exists(self) -> None:
        """Ensure the test_customers table exists and is CDC-enabled before any tests run."""
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={os.getenv('SQLSERVER_HOST', 'localhost')},1433;"
            f"DATABASE={os.getenv('SQLSERVER_DATABASE', 'warehouse_source')};"
            f"UID={os.getenv('SQLSERVER_USER', 'sa')};"
            f"PWD={os.getenv('SQLSERVER_PASSWORD', 'YourStrong!Passw0rd')};"
            f"TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str, autocommit=False)

        try:
            with conn.cursor() as cursor:
                # Check if table exists
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'test_customers'
                """)
                table_exists = cursor.fetchone()[0] > 0

                if not table_exists:
                    # Create table
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

                    # Enable CDC on database if not already enabled
                    cursor.execute("""
                        IF NOT EXISTS (
                            SELECT 1 FROM sys.databases
                            WHERE name = 'warehouse_source' AND is_cdc_enabled = 1
                        )
                        BEGIN
                            EXEC sys.sp_cdc_enable_db
                        END
                    """)

                    # Enable CDC on the table
                    cursor.execute("""
                        EXEC sys.sp_cdc_enable_table
                            @source_schema = N'dbo',
                            @source_name = N'test_customers',
                            @role_name = NULL,
                            @supports_net_changes = 1
                    """)
                    conn.commit()

                    # Wait for CDC to initialize
                    time.sleep(10)
        finally:
            conn.close()

    @pytest.fixture
    def unique_table_name(self) -> str:
        """
        Use a fixed table name that's in the connector's table.include.list.
        Tests remain independent through proper cleanup between runs.
        """
        return "test_customers"

    @pytest.fixture
    def sqlserver_conn(self) -> pyodbc.Connection:
        """Create SQL Server connection for each test."""
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

    @pytest.fixture
    def postgres_conn(self) -> psycopg2.extensions.connection:
        """Create PostgreSQL connection for each test."""
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

    def create_test_table(
        self,
        sqlserver_conn: pyodbc.Connection,
        table_name: str
    ) -> None:
        """Create a test table with CDC enabled."""
        with sqlserver_conn.cursor() as cursor:
            # Disable CDC if it exists
            cursor.execute(f"""
                IF EXISTS (
                    SELECT 1 FROM sys.tables t
                    JOIN cdc.change_tables ct ON t.object_id = ct.source_object_id
                    WHERE t.name = '{table_name}' AND SCHEMA_NAME(t.schema_id) = 'dbo'
                )
                BEGIN
                    EXEC sys.sp_cdc_disable_table
                        @source_schema = N'dbo',
                        @source_name = N'{table_name}',
                        @capture_instance = 'all'
                END
            """)

            # Drop table if exists
            cursor.execute(f"DROP TABLE IF EXISTS dbo.{table_name}")

            # Create table
            cursor.execute(f"""
                CREATE TABLE dbo.{table_name} (
                    id INT PRIMARY KEY IDENTITY(1,1),
                    name NVARCHAR(100),
                    email NVARCHAR(100),
                    age INT,
                    created_at DATETIME2 DEFAULT GETDATE(),
                    updated_at DATETIME2
                )
            """)

            # Enable CDC on database if not already enabled
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.databases
                    WHERE name = 'warehouse_source' AND is_cdc_enabled = 1
                )
                BEGIN
                    EXEC sys.sp_cdc_enable_db
                END
            """)

            # Enable CDC on the table
            cursor.execute(f"""
                EXEC sys.sp_cdc_enable_table
                    @source_schema = N'dbo',
                    @source_name = N'{table_name}',
                    @role_name = NULL,
                    @supports_net_changes = 1
            """)
            sqlserver_conn.commit()

    def cleanup_test_table(
        self,
        sqlserver_conn: pyodbc.Connection,
        postgres_conn: psycopg2.extensions.connection,
        table_name: str
    ) -> None:
        """Clean up test table from both databases."""
        # Clean up PostgreSQL - truncate instead of drop to preserve connector-created structure
        with postgres_conn.cursor() as cursor:
            try:
                cursor.execute(f"TRUNCATE TABLE {table_name}")
            except psycopg2.Error:
                # If table doesn't exist or can't be truncated, try dropping
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                except psycopg2.Error:
                    pass

        # Clean up SQL Server - truncate data but keep CDC enabled table
        with sqlserver_conn.cursor() as cursor:
            try:
                cursor.execute(f"TRUNCATE TABLE dbo.{table_name}")
                sqlserver_conn.commit()
            except pyodbc.Error:
                # If truncate fails, try delete
                try:
                    cursor.execute(f"DELETE FROM dbo.{table_name}")
                    sqlserver_conn.commit()
                except pyodbc.Error:
                    pass

    def wait_for_replication(
        self,
        postgres_conn: psycopg2.extensions.connection,
        table_name: str,
        expected_count: int,
        timeout: int = 30
    ) -> bool:
        """Wait for replication to complete."""
        elapsed = 0
        check_interval = 2

        while elapsed < timeout:
            try:
                with postgres_conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    if count >= expected_count:
                        return True
            except psycopg2.errors.UndefinedTable:
                # Table doesn't exist yet
                pass

            time.sleep(check_interval)
            elapsed += check_interval

        return False

    def test_insert_single_row_independent(
        self,
        sqlserver_conn: pyodbc.Connection,
        postgres_conn: psycopg2.extensions.connection,
        unique_table_name: str
    ) -> None:
        """Independent test: INSERT single row reusing existing table."""
        # Cleanup before test to ensure clean state
        self.cleanup_test_table(sqlserver_conn, postgres_conn, unique_table_name)

        # Brief wait for cleanup to complete
        time.sleep(2)

        try:
            # Insert data into SQL Server
            with sqlserver_conn.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO dbo.{unique_table_name} (name, email, age)
                    VALUES ('Test User Independent', 'test@example.com', 25)
                """)
                sqlserver_conn.commit()

            # Wait for replication
            assert self.wait_for_replication(postgres_conn, unique_table_name, 1), (
                f"INSERT replication failed for {unique_table_name}"
            )

            # Verify data in PostgreSQL
            with postgres_conn.cursor() as cursor:
                cursor.execute(f"SELECT name, email, age FROM {unique_table_name} WHERE name = 'Test User Independent'")
                row = cursor.fetchone()
                assert row is not None, "Row not found in PostgreSQL"
                assert row[0] == 'Test User Independent'
                assert row[1] == 'test@example.com'
                assert row[2] == 25

        finally:
            # Cleanup
            self.cleanup_test_table(sqlserver_conn, postgres_conn, unique_table_name)

    def test_update_operation_independent(
        self,
        sqlserver_conn: pyodbc.Connection,
        postgres_conn: psycopg2.extensions.connection,
        unique_table_name: str
    ) -> None:
        """Independent test: UPDATE operation reusing existing table."""
        # Cleanup before test to ensure clean state
        self.cleanup_test_table(sqlserver_conn, postgres_conn, unique_table_name)
        time.sleep(2)

        try:
            # Insert initial data
            with sqlserver_conn.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO dbo.{unique_table_name} (name, email, age)
                    VALUES ('Update Test', 'update@example.com', 30)
                """)
                sqlserver_conn.commit()

            # Wait for initial insert
            assert self.wait_for_replication(postgres_conn, unique_table_name, 1)

            # Perform update
            with sqlserver_conn.cursor() as cursor:
                cursor.execute(f"""
                    UPDATE dbo.{unique_table_name}
                    SET age = 31, updated_at = GETDATE()
                    WHERE name = 'Update Test'
                """)
                sqlserver_conn.commit()

            # Wait for update to propagate
            time.sleep(10)

            # Verify updated data
            with postgres_conn.cursor() as cursor:
                cursor.execute(f"SELECT age FROM {unique_table_name} WHERE name = 'Update Test'")
                row = cursor.fetchone()
                assert row is not None, "Row not found"
                assert row[0] == 31, f"Age not updated, expected 31, got {row[0]}"

        finally:
            # Cleanup
            self.cleanup_test_table(sqlserver_conn, postgres_conn, unique_table_name)

    def test_bulk_insert_independent(
        self,
        sqlserver_conn: pyodbc.Connection,
        postgres_conn: psycopg2.extensions.connection,
        unique_table_name: str
    ) -> None:
        """Independent test: Bulk INSERT reusing existing table."""
        # Cleanup before test to ensure clean state
        self.cleanup_test_table(sqlserver_conn, postgres_conn, unique_table_name)
        time.sleep(2)

        try:
            # Insert multiple rows
            with sqlserver_conn.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO dbo.{unique_table_name} (name, email, age)
                    VALUES
                        ('Bulk User 1', 'bulk1@example.com', 20),
                        ('Bulk User 2', 'bulk2@example.com', 25),
                        ('Bulk User 3', 'bulk3@example.com', 30),
                        ('Bulk User 4', 'bulk4@example.com', 35),
                        ('Bulk User 5', 'bulk5@example.com', 40)
                """)
                sqlserver_conn.commit()

            # Wait for replication
            assert self.wait_for_replication(postgres_conn, unique_table_name, 5, timeout=45), (
                "Bulk INSERT replication failed"
            )

            # Verify all 5 specific rows were inserted
            with postgres_conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM {unique_table_name}
                    WHERE name LIKE 'Bulk User%'
                """)
                count = cursor.fetchone()[0]
                assert count == 5, f"Expected 5 bulk user rows, got {count}"

        finally:
            # Cleanup
            self.cleanup_test_table(sqlserver_conn, postgres_conn, unique_table_name)

    def test_null_handling_independent(
        self,
        sqlserver_conn: pyodbc.Connection,
        postgres_conn: psycopg2.extensions.connection,
        unique_table_name: str
    ) -> None:
        """Independent test: NULL value handling reusing existing table."""
        # Cleanup before test to ensure clean state
        self.cleanup_test_table(sqlserver_conn, postgres_conn, unique_table_name)
        time.sleep(2)

        try:
            # Insert row with NULL values
            with sqlserver_conn.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO dbo.{unique_table_name} (name, email, age, updated_at)
                    VALUES ('Null Test', NULL, NULL, NULL)
                """)
                sqlserver_conn.commit()

            # Wait for the specific row to replicate (not just any row)
            timeout = 30
            elapsed = 0
            check_interval = 2
            row = None

            while elapsed < timeout:
                try:
                    with postgres_conn.cursor() as cursor:
                        cursor.execute(f"""
                            SELECT email, age, updated_at
                            FROM {unique_table_name}
                            WHERE name = 'Null Test'
                        """)
                        row = cursor.fetchone()
                        if row is not None:
                            break
                except psycopg2.errors.UndefinedTable:
                    # Table doesn't exist yet
                    pass

                time.sleep(check_interval)
                elapsed += check_interval

            # Verify the row was found and NULL values are preserved
            assert row is not None, f"Row with name='Null Test' not found after {timeout}s"
            assert row[0] is None, f"email should be NULL, got {row[0]}"
            assert row[1] is None, f"age should be NULL, got {row[1]}"
            assert row[2] is None, f"updated_at should be NULL, got {row[2]}"

        finally:
            # Cleanup
            self.cleanup_test_table(sqlserver_conn, postgres_conn, unique_table_name)

    def test_delete_operation_independent(
        self,
        sqlserver_conn: pyodbc.Connection,
        postgres_conn: psycopg2.extensions.connection,
        unique_table_name: str
    ) -> None:
        """Independent test: DELETE operation reusing existing table."""
        # Cleanup before test to ensure clean state
        self.cleanup_test_table(sqlserver_conn, postgres_conn, unique_table_name)
        time.sleep(2)

        try:
            # Insert data
            with sqlserver_conn.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO dbo.{unique_table_name} (name, email, age)
                    VALUES ('Delete Test', 'delete@example.com', 40)
                """)
                sqlserver_conn.commit()

            # Wait for insert
            assert self.wait_for_replication(postgres_conn, unique_table_name, 1)

            # Delete the row
            with sqlserver_conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM dbo.{unique_table_name} WHERE name = 'Delete Test'")
                sqlserver_conn.commit()

            # Wait for delete to propagate
            time.sleep(10)

            # Verify row is marked as deleted (Debezium soft delete)
            with postgres_conn.cursor() as cursor:
                cursor.execute(f"SELECT __deleted FROM {unique_table_name} WHERE name = 'Delete Test'")
                row = cursor.fetchone()
                if row:  # Some CDC configs may actually remove the row
                    assert row[0] == 'true', f"Row should be marked deleted, got __deleted='{row[0]}'"

        finally:
            # Cleanup
            self.cleanup_test_table(sqlserver_conn, postgres_conn, unique_table_name)
