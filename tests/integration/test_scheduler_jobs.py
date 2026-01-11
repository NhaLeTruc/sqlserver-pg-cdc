"""
Integration tests for scheduler job execution.

Tests use REAL SQL Server and PostgreSQL databases to validate actual
reconciliation job execution through the scheduler.
"""

import json
from datetime import datetime
from pathlib import Path

import pytest

from src.reconciliation.scheduler import reconcile_job_wrapper
from src.utils.db_pool import close_pools, initialize_pools


@pytest.fixture(autouse=True)
def setup_pools(postgres_connection_params, sqlserver_connection_string):
    """Initialize database pools for each test."""
    initialize_pools(
        postgres_config=postgres_connection_params,
        sqlserver_config={"connection_string": sqlserver_connection_string},
        postgres_pool_size=3,
        sqlserver_pool_size=3,
    )
    yield
    close_pools()


@pytest.fixture
def source_config(sqlserver_connection_string):
    """Provide SQL Server source configuration."""
    return {"connection_string": sqlserver_connection_string}


@pytest.fixture
def target_config(postgres_connection_params):
    """Provide PostgreSQL target configuration."""
    return postgres_connection_params


@pytest.fixture
def test_output_dir(tmp_path):
    """Provide temporary output directory for reports."""
    output_dir = tmp_path / "reports"
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(output_dir)


@pytest.fixture
def setup_test_data(sqlserver_connection, postgres_connection):
    """Create test tables with matching data in both databases."""
    # Create test table in SQL Server
    sqlserver_cursor = sqlserver_connection.cursor()
    sqlserver_cursor.execute("""
        IF OBJECT_ID('dbo.test_scheduler_data', 'U') IS NOT NULL
            DROP TABLE dbo.test_scheduler_data
    """)
    sqlserver_cursor.execute("""
        CREATE TABLE dbo.test_scheduler_data (
            id INT PRIMARY KEY,
            name NVARCHAR(100),
            value INT
        )
    """)
    sqlserver_cursor.execute("""
        INSERT INTO dbo.test_scheduler_data (id, name, value)
        VALUES
            (1, 'test1', 100),
            (2, 'test2', 200),
            (3, 'test3', 300)
    """)
    sqlserver_connection.commit()

    # Create matching table in PostgreSQL
    postgres_cursor = postgres_connection.cursor()
    postgres_cursor.execute("""
        DROP TABLE IF EXISTS test_scheduler_data
    """)
    postgres_cursor.execute("""
        CREATE TABLE test_scheduler_data (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            value INT
        )
    """)
    postgres_cursor.execute("""
        INSERT INTO test_scheduler_data (id, name, value)
        VALUES
            (1, 'test1', 100),
            (2, 'test2', 200),
            (3, 'test3', 300)
    """)
    postgres_connection.commit()

    yield

    # Cleanup
    try:
        sqlserver_cursor.execute("DROP TABLE IF EXISTS dbo.test_scheduler_data")
        sqlserver_connection.commit()
        sqlserver_cursor.close()

        postgres_cursor.execute("DROP TABLE IF EXISTS test_scheduler_data")
        postgres_connection.commit()
        postgres_cursor.close()
    except Exception:
        pass


class TestReconcileJobWrapper:
    """Test reconcile_job_wrapper with real database connections."""

    def test_reconcile_job_with_matching_data(
        self, source_config, target_config, test_output_dir, setup_test_data
    ):
        """Test reconciliation job with matching data in both databases."""
        # Execute reconciliation job
        reconcile_job_wrapper(
            source_config=source_config,
            target_config=target_config,
            tables=["test_scheduler_data"],
            output_dir=test_output_dir,
            validate_checksums=False,
        )

        # Verify report was created
        report_files = list(Path(test_output_dir).glob("reconcile_*.json"))
        assert len(report_files) > 0, "No report file created"

        # Read and validate report
        with open(report_files[0]) as f:
            report = json.load(f)

        assert report["status"] == "PASS"
        assert report["total_tables"] == 1
        assert report["tables_matched"] == 1
        assert report["tables_mismatched"] == 0

    def test_reconcile_job_detects_mismatch(
        self,
        source_config,
        target_config,
        test_output_dir,
        sqlserver_connection,
        postgres_connection,
    ):
        """Test that reconciliation job detects data mismatches."""
        # Create test tables with different data
        sqlserver_cursor = sqlserver_connection.cursor()
        sqlserver_cursor.execute("""
            IF OBJECT_ID('dbo.test_mismatch', 'U') IS NOT NULL
                DROP TABLE dbo.test_mismatch
        """)
        sqlserver_cursor.execute("""
            CREATE TABLE dbo.test_mismatch (
                id INT PRIMARY KEY,
                value INT
            )
        """)
        sqlserver_cursor.execute("""
            INSERT INTO dbo.test_mismatch (id, value)
            VALUES (1, 100), (2, 200), (3, 300)
        """)
        sqlserver_connection.commit()

        postgres_cursor = postgres_connection.cursor()
        postgres_cursor.execute("DROP TABLE IF EXISTS test_mismatch")
        postgres_cursor.execute("""
            CREATE TABLE test_mismatch (
                id INT PRIMARY KEY,
                value INT
            )
        """)
        # Different data - only 2 rows
        postgres_cursor.execute("""
            INSERT INTO test_mismatch (id, value)
            VALUES (1, 100), (2, 200)
        """)
        postgres_connection.commit()

        try:
            # Execute reconciliation job
            reconcile_job_wrapper(
                source_config=source_config,
                target_config=target_config,
                tables=["test_mismatch"],
                output_dir=test_output_dir,
                validate_checksums=False,
            )

            # Verify report shows mismatch
            report_files = list(Path(test_output_dir).glob("reconcile_*.json"))
            assert len(report_files) > 0

            with open(report_files[0]) as f:
                report = json.load(f)

            assert report["status"] == "FAIL"
            assert report["tables_mismatched"] == 1
            assert len(report["discrepancies"]) > 0

        finally:
            # Cleanup
            sqlserver_cursor.execute("DROP TABLE IF EXISTS dbo.test_mismatch")
            sqlserver_connection.commit()
            sqlserver_cursor.close()

            postgres_cursor.execute("DROP TABLE IF EXISTS test_mismatch")
            postgres_connection.commit()
            postgres_cursor.close()

    def test_reconcile_job_with_multiple_tables(
        self,
        source_config,
        target_config,
        test_output_dir,
        sqlserver_connection,
        postgres_connection,
    ):
        """Test reconciliation job with multiple tables."""
        # Create multiple test tables
        tables = ["test_multi_1", "test_multi_2"]

        sqlserver_cursor = sqlserver_connection.cursor()
        postgres_cursor = postgres_connection.cursor()

        try:
            for table in tables:
                # SQL Server
                sqlserver_cursor.execute(f"""
                    IF OBJECT_ID('dbo.{table}', 'U') IS NOT NULL
                        DROP TABLE dbo.{table}
                """)
                sqlserver_cursor.execute(f"""
                    CREATE TABLE dbo.{table} (
                        id INT PRIMARY KEY,
                        data NVARCHAR(50)
                    )
                """)
                sqlserver_cursor.execute(f"""
                    INSERT INTO dbo.{table} (id, data)
                    VALUES (1, 'row1'), (2, 'row2')
                """)

                # PostgreSQL
                postgres_cursor.execute(f"DROP TABLE IF EXISTS {table}")
                postgres_cursor.execute(f"""
                    CREATE TABLE {table} (
                        id INT PRIMARY KEY,
                        data VARCHAR(50)
                    )
                """)
                postgres_cursor.execute(f"""
                    INSERT INTO {table} (id, data)
                    VALUES (1, 'row1'), (2, 'row2')
                """)

            sqlserver_connection.commit()
            postgres_connection.commit()

            # Execute reconciliation job
            reconcile_job_wrapper(
                source_config=source_config,
                target_config=target_config,
                tables=tables,
                output_dir=test_output_dir,
                validate_checksums=False,
            )

            # Verify report
            report_files = list(Path(test_output_dir).glob("reconcile_*.json"))
            assert len(report_files) > 0

            with open(report_files[0]) as f:
                report = json.load(f)

            assert report["total_tables"] == 2
            assert report["tables_matched"] == 2
            assert report["status"] == "PASS"

        finally:
            # Cleanup
            for table in tables:
                sqlserver_cursor.execute(f"DROP TABLE IF EXISTS dbo.{table}")
                postgres_cursor.execute(f"DROP TABLE IF EXISTS {table}")
            sqlserver_connection.commit()
            postgres_connection.commit()
            sqlserver_cursor.close()
            postgres_cursor.close()

    def test_reconcile_job_creates_timestamped_output(
        self, source_config, target_config, test_output_dir, setup_test_data
    ):
        """Test that reconciliation job creates timestamped output files."""
        before_time = datetime.utcnow()

        # Execute reconciliation job
        reconcile_job_wrapper(
            source_config=source_config,
            target_config=target_config,
            tables=["test_scheduler_data"],
            output_dir=test_output_dir,
            validate_checksums=False,
        )

        after_time = datetime.utcnow()

        # Verify timestamped file was created
        report_files = list(Path(test_output_dir).glob("reconcile_*.json"))
        assert len(report_files) > 0

        # Verify filename contains timestamp
        filename = report_files[0].name
        assert filename.startswith("reconcile_")
        assert filename.endswith(".json")

    def test_reconcile_job_handles_table_error_gracefully(
        self,
        source_config,
        target_config,
        test_output_dir,
        sqlserver_connection,
        postgres_connection,
    ):
        """Test that job continues when one table fails."""
        # Create one valid table
        sqlserver_cursor = sqlserver_connection.cursor()
        postgres_cursor = postgres_connection.cursor()

        try:
            sqlserver_cursor.execute("""
                IF OBJECT_ID('dbo.test_valid', 'U') IS NOT NULL
                    DROP TABLE dbo.test_valid
            """)
            sqlserver_cursor.execute("""
                CREATE TABLE dbo.test_valid (id INT PRIMARY KEY, val INT)
            """)
            sqlserver_cursor.execute("INSERT INTO dbo.test_valid VALUES (1, 100)")
            sqlserver_connection.commit()

            postgres_cursor.execute("DROP TABLE IF EXISTS test_valid")
            postgres_cursor.execute("""
                CREATE TABLE test_valid (id INT PRIMARY KEY, val INT)
            """)
            postgres_cursor.execute("INSERT INTO test_valid VALUES (1, 100)")
            postgres_connection.commit()

            # Execute job with one valid and one non-existent table
            reconcile_job_wrapper(
                source_config=source_config,
                target_config=target_config,
                tables=["test_valid", "nonexistent_table"],
                output_dir=test_output_dir,
                validate_checksums=False,
            )

            # Verify report was still created
            report_files = list(Path(test_output_dir).glob("reconcile_*.json"))
            assert len(report_files) > 0

        finally:
            # Cleanup
            sqlserver_cursor.execute("DROP TABLE IF EXISTS dbo.test_valid")
            sqlserver_connection.commit()
            sqlserver_cursor.close()

            postgres_cursor.execute("DROP TABLE IF EXISTS test_valid")
            postgres_connection.commit()
            postgres_cursor.close()
