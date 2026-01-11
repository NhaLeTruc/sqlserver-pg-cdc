"""
Unit tests for CLI module

Tests for the command-line interface functionality including
argument parsing, credential handling, and command execution.
"""

import argparse
import logging
from unittest.mock import MagicMock, mock_open, patch

import pytest

from src.reconciliation.cli import (
    cmd_report,
    cmd_run,
    cmd_schedule,
    get_credentials_from_vault_or_env,
    main,
)
from src.reconciliation.cli import setup_logging as cli_setup_logging


class TestSetupLogging:
    """Tests for setup_logging function"""

    def test_setup_logging_with_info_level(self):
        """Test setup_logging configures INFO level correctly"""
        import logging as test_logging
        test_logging.root.handlers = []
        cli_setup_logging("INFO")
        assert test_logging.getLogger().level == logging.INFO

    def test_setup_logging_with_debug_level(self):
        """Test setup_logging configures DEBUG level correctly"""
        import logging as test_logging
        test_logging.root.handlers = []
        cli_setup_logging("DEBUG")
        assert test_logging.getLogger().level == logging.DEBUG

    def test_setup_logging_with_warning_level(self):
        """Test setup_logging configures WARNING level correctly"""
        import logging as test_logging
        test_logging.root.handlers = []
        cli_setup_logging("WARNING")
        assert test_logging.getLogger().level == logging.WARNING

    def test_setup_logging_with_error_level(self):
        """Test setup_logging configures ERROR level correctly"""
        import logging as test_logging
        test_logging.root.handlers = []
        cli_setup_logging("ERROR")
        assert test_logging.getLogger().level == logging.ERROR

    def test_setup_logging_with_lowercase_level(self):
        """Test setup_logging handles lowercase log level"""
        import logging as test_logging
        test_logging.root.handlers = []
        cli_setup_logging("info")
        assert test_logging.getLogger().level == logging.INFO


class TestGetCredentialsFromVaultOrEnv:
    """Tests for get_credentials_from_vault_or_env function"""

    @patch('src.reconciliation.cli.credentials.VaultClient')
    def test_get_credentials_from_vault_success(self, mock_vault_client_class):
        """Test successful credential retrieval from Vault"""
        # Setup mock
        mock_vault_client = MagicMock()
        mock_vault_client_class.return_value = mock_vault_client

        mock_vault_client.get_database_credentials.side_effect = [
            {
                "server": "sqlserver.example.com",
                "database": "testdb",
                "username": "testuser",
                "password": "testpass"
            },
            {
                "host": "postgres.example.com",
                "port": 5432,
                "database": "targetdb",
                "username": "pguser",
                "password": "pgpass"
            }
        ]

        # Create args
        args = argparse.Namespace(use_vault=True)

        # Call function
        source_config, target_config = get_credentials_from_vault_or_env(args)

        # Assertions
        assert source_config["server"] == "sqlserver.example.com"
        assert source_config["database"] == "testdb"
        assert source_config["username"] == "testuser"
        assert source_config["password"] == "testpass"

        assert target_config["host"] == "postgres.example.com"
        assert target_config["port"] == 5432
        assert target_config["database"] == "targetdb"
        assert target_config["username"] == "pguser"
        assert target_config["password"] == "pgpass"

    @patch('src.reconciliation.cli.credentials.VaultClient')
    def test_get_credentials_from_vault_missing_port(self, mock_vault_client_class):
        """Test Vault credentials with missing port defaults to 5432"""
        mock_vault_client = MagicMock()
        mock_vault_client_class.return_value = mock_vault_client

        mock_vault_client.get_database_credentials.side_effect = [
            {
                "server": "sqlserver.example.com",
                "database": "testdb",
                "username": "testuser",
                "password": "testpass"
            },
            {
                "host": "postgres.example.com",
                "database": "targetdb",
                "username": "pguser",
                "password": "pgpass"
            }
        ]

        args = argparse.Namespace(use_vault=True)
        source_config, target_config = get_credentials_from_vault_or_env(args)

        assert target_config["port"] == 5432

    @patch('src.reconciliation.cli.credentials.VaultClient')
    def test_get_credentials_from_vault_failure(self, mock_vault_client_class):
        """Test Vault credential retrieval failure exits with error"""
        mock_vault_client = MagicMock()
        mock_vault_client_class.return_value = mock_vault_client
        mock_vault_client.get_database_credentials.side_effect = Exception("Vault error")

        args = argparse.Namespace(use_vault=True)

        with pytest.raises(SystemExit) as exc_info:
            get_credentials_from_vault_or_env(args)

        assert exc_info.value.code == 1

    @patch.dict('os.environ', {
        'SQLSERVER_HOST': 'env_sqlserver',
        'SQLSERVER_DATABASE': 'env_sqldb',
        'SQLSERVER_USER': 'env_sqluser',
        'SQLSERVER_PASSWORD': 'env_sqlpass',
        'POSTGRES_HOST': 'env_pghost',
        'POSTGRES_PORT': '5433',
        'POSTGRES_DB': 'env_pgdb',
        'POSTGRES_USER': 'env_pguser',
        'POSTGRES_PASSWORD': 'env_pgpass'
    })
    def test_get_credentials_from_env_variables(self):
        """Test credential retrieval from environment variables"""
        args = argparse.Namespace(
            use_vault=False,
            source_server=None,
            source_database=None,
            source_user=None,
            source_password=None,
            target_host=None,
            target_port=None,
            target_database=None,
            target_user=None,
            target_password=None
        )

        source_config, target_config = get_credentials_from_vault_or_env(args)

        assert source_config["server"] == "env_sqlserver"
        assert source_config["database"] == "env_sqldb"
        assert source_config["username"] == "env_sqluser"
        assert source_config["password"] == "env_sqlpass"

        assert target_config["host"] == "env_pghost"
        assert target_config["port"] == 5433
        assert target_config["database"] == "env_pgdb"
        assert target_config["username"] == "env_pguser"
        assert target_config["password"] == "env_pgpass"

    @patch.dict('os.environ', {
        'SQLSERVER_PASSWORD': 'env_sqlpass',
        'POSTGRES_PASSWORD': 'env_pgpass'
    })
    def test_get_credentials_from_args_override_env(self):
        """Test command-line args override environment variables"""
        args = argparse.Namespace(
            use_vault=False,
            source_server='arg_sqlserver',
            source_database='arg_sqldb',
            source_user='arg_sqluser',
            source_password='arg_sqlpass',
            target_host='arg_pghost',
            target_port='5434',
            target_database='arg_pgdb',
            target_user='arg_pguser',
            target_password='arg_pgpass'
        )

        source_config, target_config = get_credentials_from_vault_or_env(args)

        assert source_config["server"] == "arg_sqlserver"
        assert source_config["password"] == "arg_sqlpass"
        assert target_config["host"] == "arg_pghost"
        assert target_config["port"] == 5434

    @patch.dict('os.environ', {}, clear=True)
    @patch('src.reconciliation.cli.commands.sys.exit')
    def test_get_credentials_missing_source_password(self, mock_exit):
        """Test missing source password exits with error"""
        args = argparse.Namespace(
            use_vault=False,
            source_server='server',
            source_database='db',
            source_user='user',
            source_password=None,
            target_host='host',
            target_port='5432',
            target_database='db',
            target_user='user',
            target_password='pass'
        )

        get_credentials_from_vault_or_env(args)

        mock_exit.assert_called_once_with(1)

    @patch.dict('os.environ', {
        'SQLSERVER_PASSWORD': 'sqlpass'
    }, clear=True)
    @patch('src.reconciliation.cli.commands.sys.exit')
    def test_get_credentials_missing_target_password(self, mock_exit):
        """Test missing target password exits with error"""
        args = argparse.Namespace(
            use_vault=False,
            source_server='server',
            source_database='db',
            source_user='user',
            source_password='sqlpass',
            target_host='host',
            target_port='5432',
            target_database='db',
            target_user='user',
            target_password=None
        )

        get_credentials_from_vault_or_env(args)

        mock_exit.assert_called_once_with(1)


class TestCmdRun:
    """Tests for cmd_run command"""

    @patch('src.reconciliation.cli.get_credentials_from_vault_or_env')
    @patch('pyodbc.connect')
    @patch('psycopg2.connect')
    @patch('src.reconciliation.cli.commands.reconcile_table')
    @patch('src.reconciliation.cli.commands.generate_report')
    @patch('src.reconciliation.cli.commands.format_report_console')
    @patch('src.reconciliation.cli.commands.sys.exit')
    @patch('builtins.print')
    def test_cmd_run_basic_success(
        self, mock_print, mock_exit, mock_format_console,
        mock_generate_report, mock_reconcile_table,
        mock_psycopg2_connect, mock_pyodbc_connect, mock_get_credentials
    ):
        """Test basic successful reconciliation run"""
        # Setup mocks
        mock_get_credentials.return_value = (
            {"server": "s", "database": "d", "username": "u", "password": "p"},
            {"host": "h", "port": 5432, "database": "d", "username": "u", "password": "p"}
        )

        mock_source_conn = MagicMock()
        mock_source_cursor = MagicMock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = MagicMock()
        mock_target_cursor = MagicMock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        mock_reconcile_table.return_value = {
            "source_table": "customers",
            "target_table": "customers",
            "match": True
        }

        mock_generate_report.return_value = {
            "status": "PASS",
            "total_tables": 1,
            "matched": 1
        }

        mock_format_console.return_value = "Report output"

        # Create args
        args = argparse.Namespace(
            tables="customers",
            tables_file=None,
            validate_checksums=False,
            output=None,
            format="console",
            continue_on_error=False,
            parallel=False,
            parallel_workers=4,
            parallel_timeout=3600,
            row_level=False,
            row_level_chunk_size=1000,
            use_vault=False,
            source_server="s",
            source_database="d",
            source_user="u",
            source_password="p",
            target_host="h",
            target_port="5432",
            target_database="d",
            target_user="u",
            target_password="p"
        )

        # Call function
        cmd_run(args)

        # Assertions
        mock_reconcile_table.assert_called_once()
        mock_print.assert_called_once_with("Report output")
        mock_exit.assert_called_once_with(0)

    @patch('src.reconciliation.cli.get_credentials_from_vault_or_env')
    @patch('builtins.open', new_callable=mock_open, read_data="table1\ntable2\ntable3\n")
    @patch('pyodbc.connect')
    @patch('psycopg2.connect')
    @patch('src.reconciliation.cli.commands.reconcile_table')
    @patch('src.reconciliation.cli.commands.generate_report')
    @patch('src.reconciliation.cli.commands.format_report_console')
    @patch('src.reconciliation.cli.commands.sys.exit')
    @patch('builtins.print')
    def test_cmd_run_with_tables_file(
        self, mock_print, mock_exit, mock_format_console,
        mock_generate_report, mock_reconcile_table,
        mock_psycopg2_connect, mock_pyodbc_connect, mock_file, mock_get_credentials
    ):
        """Test reconciliation with tables from file"""
        mock_get_credentials.return_value = (
            {"server": "s", "database": "d", "username": "u", "password": "p"},
            {"host": "h", "port": 5432, "database": "d", "username": "u", "password": "p"}
        )

        mock_source_conn = MagicMock()
        mock_source_cursor = MagicMock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = MagicMock()
        mock_target_cursor = MagicMock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        mock_reconcile_table.return_value = {"match": True}
        mock_generate_report.return_value = {"status": "PASS"}
        mock_format_console.return_value = "Report"

        args = argparse.Namespace(
            tables=None,
            tables_file="tables.txt",
            validate_checksums=False,
            output=None,
            format="console",
            continue_on_error=False,
            parallel=False,
            parallel_workers=4,
            parallel_timeout=3600,
            row_level=False,
            row_level_chunk_size=1000,
            use_vault=False,
            source_server="s",
            source_database="d",
            source_user="u",
            source_password="p",
            target_host="h",
            target_port="5432",
            target_database="d",
            target_user="u",
            target_password="p"
        )

        cmd_run(args)

        # Should be called 3 times for 3 tables
        assert mock_reconcile_table.call_count == 3

    @patch('src.reconciliation.cli.get_credentials_from_vault_or_env')
    @patch('pyodbc.connect')
    @patch('psycopg2.connect')
    @patch('src.reconciliation.cli.commands.reconcile_table')
    @patch('src.reconciliation.cli.commands.generate_report')
    @patch('src.reconciliation.cli.commands.export_report_json')
    @patch('src.reconciliation.cli.commands.sys.exit')
    @patch('pathlib.Path.mkdir')
    def test_cmd_run_with_json_output(
        self, mock_mkdir, mock_exit, mock_export_json,
        mock_generate_report, mock_reconcile_table,
        mock_psycopg2_connect, mock_pyodbc_connect, mock_get_credentials
    ):
        """Test reconciliation with JSON output"""
        mock_get_credentials.return_value = (
            {"server": "s", "database": "d", "username": "u", "password": "p"},
            {"host": "h", "port": 5432, "database": "d", "username": "u", "password": "p"}
        )

        mock_source_conn = MagicMock()
        mock_source_cursor = MagicMock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = MagicMock()
        mock_target_cursor = MagicMock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        mock_reconcile_table.return_value = {"match": True}
        mock_generate_report.return_value = {"status": "PASS"}

        args = argparse.Namespace(
            tables="customers",
            tables_file=None,
            validate_checksums=True,
            output="output/report.json",
            format="json",
            continue_on_error=False,
            parallel=False,
            parallel_workers=4,
            parallel_timeout=3600,
            row_level=False,
            row_level_chunk_size=1000,
            use_vault=False,
            source_server="s",
            source_database="d",
            source_user="u",
            source_password="p",
            target_host="h",
            target_port="5432",
            target_database="d",
            target_user="u",
            target_password="p"
        )

        cmd_run(args)

        mock_export_json.assert_called_once()
        mock_mkdir.assert_called_once()

    @patch('src.reconciliation.cli.get_credentials_from_vault_or_env')
    @patch('pyodbc.connect')
    @patch('psycopg2.connect')
    @patch('src.reconciliation.cli.commands.reconcile_table')
    @patch('src.reconciliation.cli.commands.generate_report')
    @patch('src.reconciliation.cli.commands.export_report_csv')
    @patch('src.reconciliation.cli.commands.sys.exit')
    @patch('pathlib.Path.mkdir')
    def test_cmd_run_with_csv_output(
        self, mock_mkdir, mock_exit, mock_export_csv,
        mock_generate_report, mock_reconcile_table,
        mock_psycopg2_connect, mock_pyodbc_connect, mock_get_credentials
    ):
        """Test reconciliation with CSV output"""
        mock_get_credentials.return_value = (
            {"server": "s", "database": "d", "username": "u", "password": "p"},
            {"host": "h", "port": 5432, "database": "d", "username": "u", "password": "p"}
        )

        mock_source_conn = MagicMock()
        mock_source_cursor = MagicMock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = MagicMock()
        mock_target_cursor = MagicMock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        mock_reconcile_table.return_value = {"match": True}
        mock_generate_report.return_value = {"status": "PASS"}

        args = argparse.Namespace(
            tables="customers",
            tables_file=None,
            validate_checksums=False,
            output="report.csv",
            format="csv",
            continue_on_error=False,
            parallel=False,
            parallel_workers=4,
            parallel_timeout=3600,
            row_level=False,
            row_level_chunk_size=1000,
            use_vault=False,
            source_server="s",
            source_database="d",
            source_user="u",
            source_password="p",
            target_host="h",
            target_port="5432",
            target_database="d",
            target_user="u",
            target_password="p"
        )

        cmd_run(args)

        mock_export_csv.assert_called_once()

    @patch('src.reconciliation.cli.get_credentials_from_vault_or_env')
    @patch('pyodbc.connect')
    @patch('psycopg2.connect')
    @patch('src.reconciliation.cli.commands.reconcile_table')
    @patch('src.reconciliation.cli.commands.generate_report')
    @patch('src.reconciliation.cli.commands.format_report_console')
    @patch('src.reconciliation.cli.commands.sys.exit')
    @patch('builtins.print')
    def test_cmd_run_with_mismatch_exits_with_error(
        self, mock_print, mock_exit, mock_format_console,
        mock_generate_report, mock_reconcile_table,
        mock_psycopg2_connect, mock_pyodbc_connect, mock_get_credentials
    ):
        """Test reconciliation with mismatched data exits with code 1"""
        mock_get_credentials.return_value = (
            {"server": "s", "database": "d", "username": "u", "password": "p"},
            {"host": "h", "port": 5432, "database": "d", "username": "u", "password": "p"}
        )

        mock_source_conn = MagicMock()
        mock_source_cursor = MagicMock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = MagicMock()
        mock_target_cursor = MagicMock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        mock_reconcile_table.return_value = {"match": False}
        mock_generate_report.return_value = {"status": "FAIL"}
        mock_format_console.return_value = "Report"

        args = argparse.Namespace(
            tables="customers",
            tables_file=None,
            validate_checksums=False,
            output=None,
            format="console",
            continue_on_error=False,
            use_vault=False,
            source_server="s",
            source_database="d",
            source_user="u",
            source_password="p",
            target_host="h",
            target_port="5432",
            target_database="d",
            target_user="u",
            target_password="p"
        )

        cmd_run(args)

        mock_exit.assert_called_with(1)

    @patch('src.reconciliation.cli.get_credentials_from_vault_or_env')
    @patch('pyodbc.connect')
    @patch('psycopg2.connect')
    @patch('src.reconciliation.cli.commands.sys.exit')
    def test_cmd_run_connection_error_exits(
        self, mock_exit, mock_psycopg2_connect, mock_pyodbc_connect, mock_get_credentials
    ):
        """Test connection error exits with code 1"""
        mock_get_credentials.return_value = (
            {"server": "s", "database": "d", "username": "u", "password": "p"},
            {"host": "h", "port": 5432, "database": "d", "username": "u", "password": "p"}
        )

        mock_pyodbc_connect.side_effect = Exception("Connection failed")

        args = argparse.Namespace(
            tables="customers",
            tables_file=None,
            validate_checksums=False,
            output=None,
            format="console",
            continue_on_error=False,
            use_vault=False,
            source_server="s",
            source_database="d",
            source_user="u",
            source_password="p",
            target_host="h",
            target_port="5432",
            target_database="d",
            target_user="u",
            target_password="p"
        )

        cmd_run(args)

        mock_exit.assert_called_with(1)

    @patch('src.reconciliation.cli.get_credentials_from_vault_or_env')
    @patch('pyodbc.connect')
    @patch('psycopg2.connect')
    @patch('src.reconciliation.cli.commands.reconcile_table')
    @patch('src.reconciliation.cli.commands.generate_report')
    @patch('src.reconciliation.cli.commands.format_report_console')
    @patch('src.reconciliation.cli.commands.sys.exit')
    @patch('builtins.print')
    def test_cmd_run_continue_on_error(
        self, mock_print, mock_exit, mock_format_console,
        mock_generate_report, mock_reconcile_table,
        mock_psycopg2_connect, mock_pyodbc_connect, mock_get_credentials
    ):
        """Test reconciliation continues on error when flag is set"""
        mock_get_credentials.return_value = (
            {"server": "s", "database": "d", "username": "u", "password": "p"},
            {"host": "h", "port": 5432, "database": "d", "username": "u", "password": "p"}
        )

        mock_source_conn = MagicMock()
        mock_source_cursor = MagicMock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = MagicMock()
        mock_target_cursor = MagicMock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        # First table fails, second succeeds
        mock_reconcile_table.side_effect = [
            Exception("Table error"),
            {"match": True}
        ]

        mock_generate_report.return_value = {"status": "PASS"}
        mock_format_console.return_value = "Report"

        args = argparse.Namespace(
            tables="table1,table2",
            tables_file=None,
            validate_checksums=False,
            output=None,
            format="console",
            continue_on_error=True,
            parallel=False,
            parallel_workers=4,
            parallel_timeout=3600,
            row_level=False,
            row_level_chunk_size=1000,
            use_vault=False,
            source_server="s",
            source_database="d",
            source_user="u",
            source_password="p",
            target_host="h",
            target_port="5432",
            target_database="d",
            target_user="u",
            target_password="p"
        )

        cmd_run(args)

        # Should call reconcile_table twice despite first error
        assert mock_reconcile_table.call_count == 2


class TestCmdSchedule:
    """Tests for cmd_schedule command"""

    @patch('src.reconciliation.cli.get_credentials_from_vault_or_env')
    @patch('src.reconciliation.cli.commands.ReconciliationScheduler')
    @patch('pathlib.Path.mkdir')
    def test_cmd_schedule_with_cron(
        self, mock_mkdir, mock_scheduler_class, mock_get_credentials
    ):
        """Test scheduling with cron expression"""
        mock_get_credentials.return_value = (
            {"server": "s", "database": "d", "username": "u", "password": "p"},
            {"host": "h", "port": 5432, "database": "d", "username": "u", "password": "p"}
        )

        mock_scheduler = MagicMock()
        mock_scheduler_class.return_value = mock_scheduler

        args = argparse.Namespace(
            tables="customers",
            tables_file=None,
            validate_checksums=False,
            cron="0 */6 * * *",
            interval=3600,
            output_dir=None,
            use_vault=False,
            source_server="s",
            source_database="d",
            source_user="u",
            source_password="p",
            target_host="h",
            target_port="5432",
            target_database="d",
            target_user="u",
            target_password="p"
        )

        cmd_schedule(args)

        mock_scheduler.add_cron_job.assert_called_once()
        mock_scheduler.start.assert_called_once()

    @patch('src.reconciliation.cli.get_credentials_from_vault_or_env')
    @patch('src.reconciliation.cli.commands.ReconciliationScheduler')
    @patch('pathlib.Path.mkdir')
    def test_cmd_schedule_with_interval(
        self, mock_mkdir, mock_scheduler_class, mock_get_credentials
    ):
        """Test scheduling with interval"""
        mock_get_credentials.return_value = (
            {"server": "s", "database": "d", "username": "u", "password": "p"},
            {"host": "h", "port": 5432, "database": "d", "username": "u", "password": "p"}
        )

        mock_scheduler = MagicMock()
        mock_scheduler_class.return_value = mock_scheduler

        args = argparse.Namespace(
            tables="customers",
            tables_file=None,
            validate_checksums=True,
            cron=None,
            interval=7200,
            output_dir="/custom/dir",
            use_vault=False,
            source_server="s",
            source_database="d",
            source_user="u",
            source_password="p",
            target_host="h",
            target_port="5432",
            target_database="d",
            target_user="u",
            target_password="p"
        )

        cmd_schedule(args)

        mock_scheduler.add_interval_job.assert_called_once()
        assert mock_scheduler.add_interval_job.call_args[0][1] == 7200

    @patch('src.reconciliation.cli.get_credentials_from_vault_or_env')
    @patch('builtins.open', new_callable=mock_open, read_data="table1\ntable2\n")
    @patch('src.reconciliation.cli.commands.ReconciliationScheduler')
    @patch('pathlib.Path.mkdir')
    def test_cmd_schedule_with_tables_file(
        self, mock_mkdir, mock_scheduler_class, mock_file, mock_get_credentials
    ):
        """Test scheduling with tables from file"""
        mock_get_credentials.return_value = (
            {"server": "s", "database": "d", "username": "u", "password": "p"},
            {"host": "h", "port": 5432, "database": "d", "username": "u", "password": "p"}
        )

        mock_scheduler = MagicMock()
        mock_scheduler_class.return_value = mock_scheduler

        args = argparse.Namespace(
            tables=None,
            tables_file="tables.txt",
            validate_checksums=False,
            cron=None,
            interval=3600,
            output_dir=None,
            use_vault=False,
            source_server="s",
            source_database="d",
            source_user="u",
            source_password="p",
            target_host="h",
            target_port="5432",
            target_database="d",
            target_user="u",
            target_password="p"
        )

        cmd_schedule(args)

        # Verify tables were read from file
        call_args = mock_scheduler.add_interval_job.call_args
        assert call_args[1]['tables'] == ['table1', 'table2']


class TestCmdReport:
    """Tests for cmd_report command"""

    @patch('builtins.open', new_callable=mock_open, read_data='{"status": "PASS"}')
    @patch('src.reconciliation.cli.commands.format_report_console')
    @patch('builtins.print')
    def test_cmd_report_console_format(
        self, mock_print, mock_format_console, mock_file
    ):
        """Test report command with console format"""
        mock_format_console.return_value = "Formatted report"

        args = argparse.Namespace(
            input="report.json",
            format="console",
            output=None
        )

        cmd_report(args)

        mock_print.assert_called_once_with("Formatted report")

    @patch('builtins.open', new_callable=mock_open, read_data='{"status": "PASS"}')
    @patch('src.reconciliation.cli.commands.export_report_csv')
    def test_cmd_report_csv_format(
        self, mock_export_csv, mock_file
    ):
        """Test report command with CSV format"""
        args = argparse.Namespace(
            input="report.json",
            format="csv",
            output="output.csv"
        )

        cmd_report(args)

        mock_export_csv.assert_called_once()

    @patch('builtins.open', new_callable=mock_open, read_data='{"status": "PASS"}')
    @patch('src.reconciliation.cli.commands.export_report_json')
    def test_cmd_report_json_format(
        self, mock_export_json, mock_file
    ):
        """Test report command with JSON format"""
        args = argparse.Namespace(
            input="report.json",
            format="json",
            output="output.json"
        )

        cmd_report(args)

        mock_export_json.assert_called_once()

    @patch('builtins.open', new_callable=mock_open, read_data='{"status": "PASS"}')
    @patch('src.reconciliation.cli.commands.sys.exit')
    def test_cmd_report_csv_without_output_fails(
        self, mock_exit, mock_file
    ):
        """Test report command with CSV format but no output file"""
        args = argparse.Namespace(
            input="report.json",
            format="csv",
            output=None
        )

        cmd_report(args)

        mock_exit.assert_called_once_with(1)

    @patch('builtins.open', new_callable=mock_open, read_data='{"status": "PASS"}')
    @patch('src.reconciliation.cli.commands.sys.exit')
    def test_cmd_report_json_without_output_fails(
        self, mock_exit, mock_file
    ):
        """Test report command with JSON format but no output file"""
        args = argparse.Namespace(
            input="report.json",
            format="json",
            output=None
        )

        cmd_report(args)

        mock_exit.assert_called_once_with(1)

    @patch('builtins.open', side_effect=FileNotFoundError("File not found"))
    @patch('src.reconciliation.cli.commands.sys.exit')
    def test_cmd_report_file_not_found(
        self, mock_exit, mock_file
    ):
        """Test report command with non-existent input file"""
        args = argparse.Namespace(
            input="nonexistent.json",
            format="console",
            output=None
        )

        cmd_report(args)

        mock_exit.assert_called_once_with(1)


class TestMain:
    """Tests for main function"""

    @patch('sys.argv', ['reconcile', '--log-level', 'DEBUG', 'run',
                        '--tables', 'customers', '--source-password', 'pass',
                        '--target-password', 'pass'])
    @patch('src.reconciliation.cli.cmd_run')
    def test_main_run_command(self, mock_cmd_run):
        """Test main function executes run command"""
        main()

        mock_cmd_run.assert_called_once()

    @patch('sys.argv', ['reconcile', 'schedule', '--tables', 'customers',
                        '--source-password', 'pass', '--target-password', 'pass'])
    @patch('src.reconciliation.cli.cmd_schedule')
    def test_main_schedule_command(self, mock_cmd_schedule):
        """Test main function executes schedule command"""
        main()

        mock_cmd_schedule.assert_called_once()

    @patch('sys.argv', ['reconcile', 'report', '--input', 'report.json'])
    @patch('src.reconciliation.cli.cmd_report')
    def test_main_report_command(self, mock_cmd_report):
        """Test main function executes report command"""
        main()

        mock_cmd_report.assert_called_once()

    @patch('sys.argv', ['reconcile', 'run', '--source-password', 'pass', '--target-password', 'pass'])
    def test_main_run_without_tables_error(self):
        """Test main function errors when run command has no tables"""
        with pytest.raises(SystemExit) as exc_info:
            main()
        # argparse exits with code 2 for invalid arguments
        assert exc_info.value.code == 2

    @patch('sys.argv', ['reconcile', 'schedule', '--source-password', 'pass', '--target-password', 'pass'])
    def test_main_schedule_without_tables_error(self):
        """Test main function errors when schedule command has no tables"""
        with pytest.raises(SystemExit) as exc_info:
            main()
        # argparse exits with code 2 for invalid arguments
        assert exc_info.value.code == 2

    @patch('sys.argv', ['reconcile'])
    @patch('sys.exit')
    def test_main_no_command_prints_help(self, mock_exit):
        """Test main function prints help when no command given"""
        main()

        mock_exit.assert_called_once_with(1)

    @patch('sys.argv', ['reconcile', '--log-level', 'WARNING', 'report', '--input', 'report.json'])
    @patch('src.reconciliation.cli.cmd_report')
    def test_main_custom_log_level(self, mock_cmd_report):
        """Test main function sets custom log level"""
        # Reset logging to ensure clean state
        import logging as test_logging
        test_logging.root.handlers = []

        main()

        # The logging level should be WARNING after main() runs
        assert test_logging.getLogger().level == logging.WARNING
