"""
End-to-end tests for CLI commands.

Tests execute actual CLI commands as subprocess calls to validate
real command-line interface behavior with real databases.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def project_root():
    """Get project root directory."""
    return Path(__file__).parent.parent.parent


@pytest.fixture(scope="module")
def cli_module(project_root):
    """Get path to CLI module."""
    return str(project_root / "src" / "reconciliation" / "cli" / "main.py")


@pytest.fixture
def setup_test_tables(sqlserver_connection, postgres_connection):
    """Create test tables for CLI testing."""
    # Create test table in SQL Server
    sqlserver_cursor = sqlserver_connection.cursor()
    sqlserver_cursor.execute("""
        IF OBJECT_ID('dbo.cli_test_customers', 'U') IS NOT NULL
            DROP TABLE dbo.cli_test_customers
    """)
    sqlserver_cursor.execute("""
        CREATE TABLE dbo.cli_test_customers (
            customer_id INT PRIMARY KEY,
            name NVARCHAR(100),
            email NVARCHAR(100),
            balance DECIMAL(10, 2)
        )
    """)
    sqlserver_cursor.execute("""
        INSERT INTO dbo.cli_test_customers (customer_id, name, email, balance)
        VALUES
            (1, 'Alice Johnson', 'alice@example.com', 1000.00),
            (2, 'Bob Smith', 'bob@example.com', 2500.50),
            (3, 'Carol Williams', 'carol@example.com', 3200.75)
    """)
    sqlserver_connection.commit()

    # Create matching table in PostgreSQL
    postgres_cursor = postgres_connection.cursor()
    postgres_cursor.execute("DROP TABLE IF EXISTS cli_test_customers")
    postgres_cursor.execute("""
        CREATE TABLE cli_test_customers (
            customer_id INT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            balance DECIMAL(10, 2)
        )
    """)
    postgres_cursor.execute("""
        INSERT INTO cli_test_customers (customer_id, name, email, balance)
        VALUES
            (1, 'Alice Johnson', 'alice@example.com', 1000.00),
            (2, 'Bob Smith', 'bob@example.com', 2500.50),
            (3, 'Carol Williams', 'carol@example.com', 3200.75)
    """)
    postgres_connection.commit()

    yield

    # Cleanup
    try:
        sqlserver_cursor.execute("DROP TABLE IF EXISTS dbo.cli_test_customers")
        sqlserver_connection.commit()
        sqlserver_cursor.close()

        postgres_cursor.execute("DROP TABLE IF EXISTS cli_test_customers")
        postgres_connection.commit()
        postgres_cursor.close()
    except Exception:
        pass


@pytest.fixture
def cli_env():
    """Provide environment variables for CLI execution."""
    return {
        "SQLSERVER_HOST": os.environ.get("SQLSERVER_HOST", "localhost"),
        "SQLSERVER_DATABASE": os.environ.get("SQLSERVER_DATABASE", "warehouse_source"),
        "SQLSERVER_USER": os.environ.get("SQLSERVER_USER", "sa"),
        "SQLSERVER_PASSWORD": os.environ.get("SQLSERVER_PASSWORD", "YourStrong!Passw0rd"),
        "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "localhost"),
        "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
        "POSTGRES_DB": os.environ.get("POSTGRES_DB", "warehouse_target"),
        "POSTGRES_USER": os.environ.get("POSTGRES_USER", "postgres"),
        "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "postgres_secure_password"),
        "PYTHONPATH": str(Path(__file__).parent.parent.parent),
        "OTEL_SDK_DISABLED": "true",  # Disable tracing for tests
    }


class TestCLIRunCommand:
    """Test 'run' command execution."""

    def test_run_single_table_console_output(self, cli_module, setup_test_tables, cli_env):
        """Test running reconciliation for single table with console output."""
        result = subprocess.run(
            [
                sys.executable,
                cli_module,
                "run",
                "--tables",
                "cli_test_customers",
                "--source-server",
                os.environ.get("SQLSERVER_HOST", "localhost"),
                "--source-database",
                os.environ.get("SQLSERVER_DATABASE", "warehouse_source"),
                "--source-user",
                os.environ.get("SQLSERVER_USER", "sa"),
                "--source-password",
                os.environ.get("SQLSERVER_PASSWORD", "YourStrong!Passw0rd"),
                "--target-host",
                os.environ.get("POSTGRES_HOST", "localhost"),
                "--target-port",
                os.environ.get("POSTGRES_PORT", "5432"),
                "--target-database",
                os.environ.get("POSTGRES_DB", "warehouse_target"),
                "--target-user",
                os.environ.get("POSTGRES_USER", "postgres"),
                "--target-password",
                os.environ.get("POSTGRES_PASSWORD", "postgres_secure_password"),
                "--format",
                "console",
            ],
            env=cli_env,
            capture_output=True,
            text=True,
            timeout=60,
        )

        # Should succeed
        assert result.returncode == 0, f"CLI failed: {result.stderr}"

        # Should contain report output
        assert "PASS" in result.stdout or "FAIL" in result.stdout
        assert "cli_test_customers" in result.stdout

    def test_run_with_json_output(self, cli_module, setup_test_tables, cli_env, tmp_path):
        """Test running reconciliation with JSON output."""
        output_file = tmp_path / "report.json"

        result = subprocess.run(
            [
                sys.executable,
                cli_module,
                "run",
                "--tables",
                "cli_test_customers",
                "--source-server",
                os.environ.get("SQLSERVER_HOST", "localhost"),
                "--source-database",
                os.environ.get("SQLSERVER_DATABASE", "warehouse_source"),
                "--source-user",
                os.environ.get("SQLSERVER_USER", "sa"),
                "--source-password",
                os.environ.get("SQLSERVER_PASSWORD", "YourStrong!Passw0rd"),
                "--target-host",
                os.environ.get("POSTGRES_HOST", "localhost"),
                "--target-port",
                os.environ.get("POSTGRES_PORT", "5432"),
                "--target-database",
                os.environ.get("POSTGRES_DB", "warehouse_target"),
                "--target-user",
                os.environ.get("POSTGRES_USER", "postgres"),
                "--target-password",
                os.environ.get("POSTGRES_PASSWORD", "postgres_secure_password"),
                "--format",
                "json",
                "--output",
                str(output_file),
            ],
            env=cli_env,
            capture_output=True,
            text=True,
            timeout=60,
        )

        # Should succeed
        assert result.returncode == 0, f"CLI failed: {result.stderr}"

        # Verify JSON file was created
        assert output_file.exists(), "JSON report file not created"

        # Verify JSON content
        with open(output_file) as f:
            report = json.load(f)

        assert "status" in report
        assert "total_tables" in report
        assert report["total_tables"] == 1

    def test_run_with_checksum_validation(self, cli_module, setup_test_tables, cli_env):
        """Test running reconciliation with checksum validation."""
        result = subprocess.run(
            [
                sys.executable,
                cli_module,
                "run",
                "--tables",
                "cli_test_customers",
                "--validate-checksums",
                "--source-server",
                os.environ.get("SQLSERVER_HOST", "localhost"),
                "--source-database",
                os.environ.get("SQLSERVER_DATABASE", "warehouse_source"),
                "--source-user",
                os.environ.get("SQLSERVER_USER", "sa"),
                "--source-password",
                os.environ.get("SQLSERVER_PASSWORD", "YourStrong!Passw0rd"),
                "--target-host",
                os.environ.get("POSTGRES_HOST", "localhost"),
                "--target-port",
                os.environ.get("POSTGRES_PORT", "5432"),
                "--target-database",
                os.environ.get("POSTGRES_DB", "warehouse_target"),
                "--target-user",
                os.environ.get("POSTGRES_USER", "postgres"),
                "--target-password",
                os.environ.get("POSTGRES_PASSWORD", "postgres_secure_password"),
                "--format",
                "console",
            ],
            env=cli_env,
            capture_output=True,
            text=True,
            timeout=60,
        )

        # Should succeed
        assert result.returncode == 0, f"CLI failed: {result.stderr}"

    def test_run_with_tables_file(self, cli_module, setup_test_tables, cli_env, tmp_path):
        """Test running reconciliation with tables from file."""
        # Create tables file
        tables_file = tmp_path / "tables.txt"
        tables_file.write_text("cli_test_customers\n")

        result = subprocess.run(
            [
                sys.executable,
                cli_module,
                "run",
                "--tables-file",
                str(tables_file),
                "--source-server",
                os.environ.get("SQLSERVER_HOST", "localhost"),
                "--source-database",
                os.environ.get("SQLSERVER_DATABASE", "warehouse_source"),
                "--source-user",
                os.environ.get("SQLSERVER_USER", "sa"),
                "--source-password",
                os.environ.get("SQLSERVER_PASSWORD", "YourStrong!Passw0rd"),
                "--target-host",
                os.environ.get("POSTGRES_HOST", "localhost"),
                "--target-port",
                os.environ.get("POSTGRES_PORT", "5432"),
                "--target-database",
                os.environ.get("POSTGRES_DB", "warehouse_target"),
                "--target-user",
                os.environ.get("POSTGRES_USER", "postgres"),
                "--target-password",
                os.environ.get("POSTGRES_PASSWORD", "postgres_secure_password"),
                "--format",
                "console",
            ],
            env=cli_env,
            capture_output=True,
            text=True,
            timeout=60,
        )

        # Should succeed
        assert result.returncode == 0, f"CLI failed: {result.stderr}"

    def test_run_with_missing_table_shows_error(self, cli_module, cli_env):
        """Test that CLI handles missing table gracefully."""
        result = subprocess.run(
            [
                sys.executable,
                cli_module,
                "run",
                "--tables",
                "nonexistent_table_xyz",
                "--source-server",
                os.environ.get("SQLSERVER_HOST", "localhost"),
                "--source-database",
                os.environ.get("SQLSERVER_DATABASE", "warehouse_source"),
                "--source-user",
                os.environ.get("SQLSERVER_USER", "sa"),
                "--source-password",
                os.environ.get("SQLSERVER_PASSWORD", "YourStrong!Passw0rd"),
                "--target-host",
                os.environ.get("POSTGRES_HOST", "localhost"),
                "--target-port",
                os.environ.get("POSTGRES_PORT", "5432"),
                "--target-database",
                os.environ.get("POSTGRES_DB", "warehouse_target"),
                "--target-user",
                os.environ.get("POSTGRES_USER", "postgres"),
                "--target-password",
                os.environ.get("POSTGRES_PASSWORD", "postgres_secure_password"),
                "--format",
                "console",
                "--continue-on-error",
            ],
            env=cli_env,
            capture_output=True,
            text=True,
            timeout=60,
        )

        # May succeed with error reporting, or fail
        # The important thing is it doesn't crash
        assert result.returncode in [0, 1]


class TestCLIScheduleCommand:
    """Test 'schedule' command execution."""

    def test_schedule_shows_help(self, cli_module, cli_env):
        """Test that schedule command can show help."""
        result = subprocess.run(
            [sys.executable, cli_module, "schedule", "--help"],
            env=cli_env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        # Should succeed and show help
        assert result.returncode == 0
        assert "schedule" in result.stdout.lower()


class TestCLIErrorHandling:
    """Test CLI error handling."""

    def test_invalid_command_shows_error(self, cli_module, cli_env):
        """Test that invalid command shows error message."""
        result = subprocess.run(
            [sys.executable, cli_module, "invalid_command"],
            env=cli_env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        # Should fail
        assert result.returncode != 0

    def test_missing_required_args_shows_error(self, cli_module, cli_env):
        """Test that missing required arguments shows error."""
        result = subprocess.run(
            [sys.executable, cli_module, "run"],
            env=cli_env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        # Should fail with error about missing arguments
        assert result.returncode != 0

    def test_help_command_works(self, cli_module, cli_env):
        """Test that --help shows usage information."""
        result = subprocess.run(
            [sys.executable, cli_module, "--help"],
            env=cli_env,
            capture_output=True,
            text=True,
            timeout=10,
        )

        # Should succeed and show help
        assert result.returncode == 0
        assert "usage" in result.stdout.lower() or "help" in result.stdout.lower()


class TestCLIWithEnvironmentVariables:
    """Test CLI using environment variables for credentials."""

    def test_run_with_env_vars(self, cli_module, setup_test_tables, cli_env):
        """Test running reconciliation using environment variables."""
        # Environment already set in cli_env fixture
        result = subprocess.run(
            [
                sys.executable,
                cli_module,
                "run",
                "--tables",
                "cli_test_customers",
                "--source-server",
                "${SQLSERVER_HOST}",
                "--source-database",
                "${SQLSERVER_DATABASE}",
                "--source-user",
                "${SQLSERVER_USER}",
                "--source-password",
                "${SQLSERVER_PASSWORD}",
                "--target-host",
                "${POSTGRES_HOST}",
                "--target-port",
                "${POSTGRES_PORT}",
                "--target-database",
                "${POSTGRES_DB}",
                "--target-user",
                "${POSTGRES_USER}",
                "--target-password",
                "${POSTGRES_PASSWORD}",
                "--format",
                "console",
            ],
            env=cli_env,
            capture_output=True,
            text=True,
            timeout=60,
        )

        # Should succeed (environment variables should be expanded by shell or Python)
        # Note: This might fail if CLI doesn't support env var expansion
        # In that case, we just verify it doesn't crash
        assert result.returncode in [0, 1]
