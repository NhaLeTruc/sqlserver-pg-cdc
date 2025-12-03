"""
End-to-end tests for reconciliation tool

Tests verify:
- T080: Reconciliation tool execution with real databases

These tests follow TDD - they should FAIL until implementation is complete.

IMPORTANT: These tests require actual database infrastructure (SQL Server, PostgreSQL, Vault).
They are skipped by default. To run them:
1. Start required infrastructure: docker-compose up -d
2. Run: pytest tests/e2e/ -v --no-cov
"""

import pytest
import subprocess
import json
import time
import sys
import os
from pathlib import Path
from typing import Dict, Any
import psycopg2
import pyodbc

# Get the Python interpreter from the virtual environment
PYTHON_BIN = sys.executable


class TestReconciliationE2E:
    """End-to-end tests for reconciliation tool"""

    @pytest.fixture
    def sqlserver_connection(self):
        """SQL Server connection for testing"""
        try:
            conn = pyodbc.connect(
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=localhost,1433;"
                "DATABASE=warehouse_source;"
                "UID=sa;"
                "PWD=YourStrong!Passw0rd",
                timeout=5
            )
            yield conn
            conn.close()
        except Exception as e:
            pytest.skip(f"SQL Server not available: {e}")

    @pytest.fixture
    def postgres_connection(self):
        """PostgreSQL connection for testing"""
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="warehouse_target",
                user="postgres",
                password="postgres_secure_password",
                connect_timeout=5
            )
            yield conn
            conn.close()
        except Exception as e:
            pytest.skip(f"PostgreSQL not available: {e}")

    @pytest.mark.e2e
    def test_reconcile_tool_basic_execution(
        self, sqlserver_connection, postgres_connection
    ):
        """
        Test basic reconciliation tool execution

        Scenario:
        1. Create matching data in SQL Server and PostgreSQL
        2. Run reconciliation tool
        3. Verify report shows all tables match
        """
        # Setup: Create test tables with matching data
        sqlserver_cursor = sqlserver_connection.cursor()
        postgres_cursor = postgres_connection.cursor()

        # Create and populate SQL Server table with unique test name
        test_table = "test_customers_e2e"

        sqlserver_cursor.execute(f"""
            IF OBJECT_ID('dbo.{test_table}', 'U') IS NOT NULL
                DROP TABLE dbo.{test_table};

            CREATE TABLE dbo.{test_table} (
                customer_id INT PRIMARY KEY,
                name NVARCHAR(100),
                email NVARCHAR(100),
                created_at DATETIME2
            );
        """)

        # Insert 1000 rows
        for i in range(1, 1001):
            sqlserver_cursor.execute(f"""
                INSERT INTO dbo.{test_table} (customer_id, name, email, created_at)
                VALUES (?, ?, ?, GETDATE())
            """, (i, f"Customer {i}", f"customer{i}@example.com"))

        sqlserver_connection.commit()

        # Create and populate PostgreSQL table with same data
        postgres_cursor.execute(f"""
            DROP TABLE IF EXISTS {test_table};

            CREATE TABLE {test_table} (
                customer_id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP
            );
        """)

        for i in range(1, 1001):
            postgres_cursor.execute(f"""
                INSERT INTO {test_table} (customer_id, name, email, created_at)
                VALUES (%s, %s, %s, NOW())
            """, (i, f"Customer {i}", f"customer{i}@example.com"))

        postgres_connection.commit()

        # Execute reconciliation tool
        result = subprocess.run(
            [
                PYTHON_BIN,
                "scripts/python/reconcile.py",
                "--source-server", "localhost",
                "--source-database", "warehouse_source",
                "--source-username", "sa",
                "--source-password", "YourStrong!Passw0rd",
                "--target-host", "localhost",
                "--target-database", "warehouse_target",
                "--target-username", "postgres",
                "--target-password", "postgres_secure_password",
                "--source-table", f"dbo.{test_table}",
                "--target-table", test_table,
                "--output", "/tmp/reconcile_report.json"
            ],
            capture_output=True,
            text=True
        )

        assert result.returncode == 0, f"Reconcile tool failed: {result.stderr}"

        # Load and verify report
        report_path = Path("/tmp/reconcile_report.json")
        assert report_path.exists(), "Report file not created"

        with open(report_path) as f:
            report = json.load(f)

        assert report["status"] == "PASS"
        assert report["total_tables"] == 1
        assert report["tables_matched"] == 1
        assert report["tables_mismatched"] == 0
        assert len(report["discrepancies"]) == 0

        # Verify report contains expected fields
        assert "timestamp" in report
        assert "summary" in report
        assert "source_total_rows" in report
        assert "target_total_rows" in report

    @pytest.mark.e2e
    def test_reconcile_tool_detects_row_count_mismatch(
        self, sqlserver_connection, postgres_connection
    ):
        """
        Test reconciliation detects row count mismatches

        Scenario:
        1. Create SQL Server table with 1000 rows
        2. Create PostgreSQL table with 950 rows (50 missing)
        3. Run reconciliation
        4. Verify report shows mismatch
        """
        sqlserver_cursor = sqlserver_connection.cursor()
        postgres_cursor = postgres_connection.cursor()

        # Use unique test table name
        test_table = "test_orders_e2e"

        # Create SQL Server table with 1000 rows
        sqlserver_cursor.execute(f"""
            IF OBJECT_ID('dbo.{test_table}', 'U') IS NOT NULL
                DROP TABLE dbo.{test_table};

            CREATE TABLE dbo.{test_table} (
                order_id INT PRIMARY KEY,
                customer_id INT,
                amount DECIMAL(10, 2)
            );
        """)

        for i in range(1, 1001):
            sqlserver_cursor.execute(
                f"INSERT INTO dbo.{test_table} VALUES (?, ?, ?)",
                (i, i % 100, float(i * 10.5))
            )

        sqlserver_connection.commit()

        # Create PostgreSQL table with only 950 rows
        postgres_cursor.execute(f"""
            DROP TABLE IF EXISTS {test_table};

            CREATE TABLE {test_table} (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                amount NUMERIC(10, 2)
            );
        """)

        for i in range(1, 951):  # Only 950 rows
            postgres_cursor.execute(
                f"INSERT INTO {test_table} VALUES (%s, %s, %s)",
                (i, i % 100, float(i * 10.5))
            )

        postgres_connection.commit()

        # Execute reconciliation
        result = subprocess.run(
            [
                PYTHON_BIN,
                "scripts/python/reconcile.py",
                "--source-server", "localhost",
                "--source-database", "warehouse_source",
                "--source-username", "sa",
                "--source-password", "YourStrong!Passw0rd",
                "--target-host", "localhost",
                "--target-database", "warehouse_target",
                "--target-username", "postgres",
                "--target-password", "postgres_secure_password",
                "--source-table", f"dbo.{test_table}",
                "--target-table", test_table,
                "--output", "/tmp/reconcile_mismatch_report.json"
            ],
            capture_output=True,
            text=True
        )

        # Tool should exit with non-zero status when mismatches found
        assert result.returncode != 0, "Expected non-zero exit code for mismatch"

        # Load and verify report
        report_path = Path("/tmp/reconcile_mismatch_report.json")
        with open(report_path) as f:
            report = json.load(f)

        assert report["status"] == "FAIL"
        assert report["tables_mismatched"] == 1
        assert len(report["discrepancies"]) > 0

        # Verify discrepancy details
        discrepancy = report["discrepancies"][0]
        assert discrepancy["table"] == "orders"
        assert discrepancy["issue_type"] == "ROW_COUNT_MISMATCH"
        assert discrepancy["severity"] in ["HIGH", "CRITICAL"]
        assert discrepancy["details"]["missing_rows"] == 50

    @pytest.mark.e2e
    def test_reconcile_tool_detects_checksum_mismatch(
        self, sqlserver_connection, postgres_connection
    ):
        """
        Test reconciliation detects data corruption via checksums

        Scenario:
        1. Create matching row counts
        2. Modify data in PostgreSQL (different values)
        3. Run reconciliation with checksum validation
        4. Verify report shows checksum mismatch
        """
        sqlserver_cursor = sqlserver_connection.cursor()
        postgres_cursor = postgres_connection.cursor()

        # Use unique test table name
        test_table = "test_products_e2e"

        # Create SQL Server table
        sqlserver_cursor.execute(f"""
            IF OBJECT_ID('dbo.{test_table}', 'U') IS NOT NULL
                DROP TABLE dbo.{test_table};

            CREATE TABLE dbo.{test_table} (
                product_id INT PRIMARY KEY,
                name NVARCHAR(100),
                price DECIMAL(10, 2)
            );
        """)

        for i in range(1, 101):
            sqlserver_cursor.execute(
                f"INSERT INTO dbo.{test_table} VALUES (?, ?, ?)",
                (i, f"Product {i}", float(i * 9.99))
            )

        sqlserver_connection.commit()

        # Create PostgreSQL table with same count but different values
        postgres_cursor.execute(f"""
            DROP TABLE IF EXISTS {test_table};

            CREATE TABLE {test_table} (
                product_id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                price NUMERIC(10, 2)
            );
        """)

        for i in range(1, 101):
            # Different price values (data corruption simulation)
            postgres_cursor.execute(
                f"INSERT INTO {test_table} VALUES (%s, %s, %s)",
                (i, f"Product {i}", float(i * 8.99))  # Different prices
            )

        postgres_connection.commit()

        # Execute reconciliation with checksum validation
        result = subprocess.run(
            [
                PYTHON_BIN,
                "scripts/python/reconcile.py",
                "--source-server", "localhost",
                "--source-database", "warehouse_source",
                "--source-username", "sa",
                "--source-password", "YourStrong!Passw0rd",
                "--target-host", "localhost",
                "--target-database", "warehouse_target",
                "--target-username", "postgres",
                "--target-password", "postgres_secure_password",
                "--source-table", f"dbo.{test_table}",
                "--target-table", test_table,
                "--validate-checksums",
                "--output", "/tmp/reconcile_checksum_report.json"
            ],
            capture_output=True,
            text=True
        )

        assert result.returncode != 0

        # Load and verify report
        report_path = Path("/tmp/reconcile_checksum_report.json")
        with open(report_path) as f:
            report = json.load(f)

        assert report["status"] == "FAIL"

        # Should detect checksum mismatch
        checksum_discrepancies = [
            d for d in report["discrepancies"]
            if d["issue_type"] == "CHECKSUM_MISMATCH"
        ]

        assert len(checksum_discrepancies) > 0
        assert checksum_discrepancies[0]["severity"] == "CRITICAL"

    @pytest.mark.e2e
    @pytest.mark.vault
    def test_reconcile_tool_with_vault_credentials(self):
        """
        Test reconciliation tool fetches credentials from Vault

        Scenario:
        1. Store database credentials in Vault
        2. Run reconciliation without explicit credentials
        3. Verify tool fetches credentials from Vault
        4. Verify reconciliation completes successfully
        """
        # Get vault binary path or skip
        vault_bin = os.environ.get("VAULT_BIN", "vault")

        # Check if vault is available
        try:
            subprocess.run([vault_bin, "status"], capture_output=True, timeout=2)
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pytest.skip(f"Vault not available at {vault_bin}")

        # Setup: Store credentials in Vault
        subprocess.run(
            [
                vault_bin,
                "kv",
                "put",
                "secret/database/sqlserver",
                "username=sa",
                "password=YourStrong!Passw0rd",
                "server=localhost",
                "database=warehouse_source"
            ],
            check=True
        )

        subprocess.run(
            [
                vault_bin,
                "kv",
                "put",
                "secret/database/postgresql",
                "username=postgres",
                "password=postgres_secure_password",
                "host=localhost",
                "database=warehouse_target"
            ],
            check=True
        )

        # Run reconciliation without explicit credentials
        result = subprocess.run(
            [
                PYTHON_BIN,
                "scripts/python/reconcile.py",
                "--source-table", "dbo.customers",
                "--target-table", "customers",
                "--use-vault",
                "--output", "/tmp/reconcile_vault_report.json"
            ],
            capture_output=True,
            text=True,
            env={
                "VAULT_ADDR": "http://localhost:8200",
                "VAULT_TOKEN": "dev-token"
            }
        )

        assert result.returncode == 0, f"Reconciliation failed: {result.stderr}"

        # Verify report was generated
        report_path = Path("/tmp/reconcile_vault_report.json")
        assert report_path.exists()

    @pytest.mark.e2e
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

    @pytest.mark.e2e
    def test_reconcile_tool_output_formats(self):
        """
        Test reconciliation supports multiple output formats

        Verify JSON, CSV, and console table formats
        """
        # JSON format (default)
        result = subprocess.run(
            [
                PYTHON_BIN,
                "scripts/python/reconcile.py",
                "--source-server", "localhost",
                "--source-database", "warehouse_source",
                "--source-username", "sa",
                "--source-password", "YourStrong!Passw0rd",
                "--target-host", "localhost",
                "--target-database", "warehouse_target",
                "--target-username", "postgres",
                "--target-password", "postgres_secure_password",
                "--source-table", "dbo.customers",
                "--target-table", "customers",
                "--output", "/tmp/report.json"
            ],
            capture_output=True,
            text=True
        )

        assert result.returncode == 0, f"JSON format failed: {result.stderr}"
        assert Path("/tmp/report.json").exists()

        # CSV format
        result = subprocess.run(
            [
                PYTHON_BIN,
                "scripts/python/reconcile.py",
                "--source-server", "localhost",
                "--source-database", "warehouse_source",
                "--source-username", "sa",
                "--source-password", "YourStrong!Passw0rd",
                "--target-host", "localhost",
                "--target-database", "warehouse_target",
                "--target-username", "postgres",
                "--target-password", "postgres_secure_password",
                "--source-table", "dbo.customers",
                "--target-table", "customers",
                "--output", "/tmp/report.csv",
                "--format", "csv"
            ],
            capture_output=True,
            text=True
        )

        assert result.returncode == 0, f"CSV format failed: {result.stderr}"
        assert Path("/tmp/report.csv").exists()

        # Console format (no file output)
        result = subprocess.run(
            [
                PYTHON_BIN,
                "scripts/python/reconcile.py",
                "--source-server", "localhost",
                "--source-database", "warehouse_source",
                "--source-username", "sa",
                "--source-password", "YourStrong!Passw0rd",
                "--target-host", "localhost",
                "--target-database", "warehouse_target",
                "--target-username", "postgres",
                "--target-password", "postgres_secure_password",
                "--source-table", "dbo.customers",
                "--target-table", "customers",
                "--format", "console"
            ],
            capture_output=True,
            text=True
        )

        assert result.returncode == 0
        assert "Table" in result.stdout
        assert "Status" in result.stdout

    @pytest.mark.e2e
    def test_reconcile_tool_scheduled_mode(self):
        """
        Test reconciliation tool in scheduled mode

        Scenario:
        1. Start reconciliation scheduler (every 1 minute)
        2. Wait for 2 minutes
        3. Verify multiple reconciliation reports generated
        4. Stop scheduler
        """
        # Start scheduler in background
        process = subprocess.Popen(
            [
                PYTHON_BIN,
                "scripts/python/reconcile.py",
                "--source-server", "localhost",
                "--source-database", "warehouse_source",
                "--source-username", "sa",
                "--source-password", "YourStrong!Passw0rd",
                "--target-host", "localhost",
                "--target-database", "warehouse_target",
                "--target-username", "postgres",
                "--target-password", "postgres_secure_password",
                "--schedule",
                "--interval", "60",  # Every 60 seconds
                "--output-dir", "/tmp/reconcile_scheduled/"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        try:
            # Wait for 2.5 minutes (should generate 2 reports)
            time.sleep(150)

            # Check for generated reports
            output_dir = Path("/tmp/reconcile_scheduled")
            assert output_dir.exists()

            report_files = list(output_dir.glob("reconcile_*.json"))
            assert len(report_files) >= 2, "Expected at least 2 scheduled reports"

            # Verify reports are valid JSON
            for report_file in report_files:
                with open(report_file) as f:
                    report = json.load(f)
                    assert "status" in report
                    assert "timestamp" in report

        finally:
            # Stop scheduler
            process.terminate()
            process.wait(timeout=10)
