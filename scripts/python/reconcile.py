#!/usr/bin/env python3
"""
Reconciliation CLI tool for SQL Server to PostgreSQL CDC pipeline

This tool compares data between source (SQL Server) and target (PostgreSQL)
databases to validate replication accuracy.

Usage:
    # On-demand reconciliation
    python reconcile.py --source-table dbo.customers --target-table customers --output report.json

    # With checksum validation
    python reconcile.py --source-table dbo.customers --target-table customers --validate-checksums

    # Using Vault for credentials
    python reconcile.py --source-table dbo.customers --target-table customers --use-vault

    # Scheduled mode (every 6 hours)
    python reconcile.py --schedule --interval 21600 --output-dir /var/reconcile/reports

    # Multiple tables
    python reconcile.py --source-tables dbo.customers,dbo.orders --target-tables customers,orders
"""

import argparse
import sys
import logging
from pathlib import Path
from typing import List, Optional
import pyodbc
import psycopg2

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.reconciliation.compare import reconcile_table
from src.reconciliation.report import (
    generate_report,
    export_report_json,
    export_report_csv,
    format_report_console
)
from src.reconciliation.scheduler import (
    ReconciliationScheduler,
    reconcile_job_wrapper
)
from src.utils.vault_client import get_credentials_from_vault
from src.utils.logging_config import setup_logging, get_logger
from src.utils.metrics import initialize_metrics


logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Reconciliation tool for CDC pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Mode selection
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Run in scheduled mode"
    )

    # Table selection (for both on-demand and scheduled modes)
    parser.add_argument(
        "--source-table",
        help="Source table name (e.g., dbo.customers)"
    )
    parser.add_argument(
        "--source-tables",
        help="Comma-separated list of source table names"
    )
    parser.add_argument(
        "--target-table",
        help="Target table name (e.g., customers)"
    )
    parser.add_argument(
        "--target-tables",
        help="Comma-separated list of target table names"
    )

    # Database connection (if not using Vault)
    parser.add_argument(
        "--source-server",
        help="SQL Server hostname or IP"
    )
    parser.add_argument(
        "--source-database",
        help="SQL Server database name"
    )
    parser.add_argument(
        "--source-username",
        help="SQL Server username"
    )
    parser.add_argument(
        "--source-password",
        help="SQL Server password"
    )

    parser.add_argument(
        "--target-host",
        help="PostgreSQL hostname or IP"
    )
    parser.add_argument(
        "--target-port",
        type=int,
        default=5432,
        help="PostgreSQL port (default: 5432)"
    )
    parser.add_argument(
        "--target-database",
        help="PostgreSQL database name"
    )
    parser.add_argument(
        "--target-username",
        help="PostgreSQL username"
    )
    parser.add_argument(
        "--target-password",
        help="PostgreSQL password"
    )

    # Vault integration
    parser.add_argument(
        "--use-vault",
        action="store_true",
        help="Fetch credentials from HashiCorp Vault"
    )

    # Validation options
    parser.add_argument(
        "--validate-checksums",
        action="store_true",
        help="Perform checksum validation (slower but more thorough)"
    )

    # Output options
    parser.add_argument(
        "--output",
        help="Output file path for report (JSON format)"
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv", "console"],
        default="json",
        help="Output format (default: json)"
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory for scheduled reports"
    )

    # Scheduler options
    parser.add_argument(
        "--interval",
        type=int,
        help="Interval in seconds for scheduled mode"
    )
    parser.add_argument(
        "--cron",
        help="Cron expression for scheduled mode (e.g., '0 */6 * * *')"
    )

    # Logging
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    parser.add_argument(
        "--log-file",
        help="Log file path (default: console only)"
    )
    parser.add_argument(
        "--json-logs",
        action="store_true",
        help="Use JSON format for logs"
    )

    # Metrics
    parser.add_argument(
        "--enable-metrics",
        action="store_true",
        help="Enable Prometheus metrics endpoint"
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=9091,
        help="Prometheus metrics port (default: 9091)"
    )

    return parser.parse_args()


def get_database_connections(args):
    """
    Get database connections based on arguments

    Returns:
        Tuple of (source_conn, target_conn)
    """
    if args.use_vault:
        # Fetch credentials from Vault
        logger.info("Fetching credentials from Vault...")

        source_creds = get_credentials_from_vault("sqlserver")
        target_creds = get_credentials_from_vault("postgresql")

        # Connect to SQL Server
        source_conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={source_creds['server']};"
            f"DATABASE={source_creds['database']};"
            f"UID={source_creds['username']};"
            f"PWD={source_creds['password']}"
        )

        # Connect to PostgreSQL
        target_conn = psycopg2.connect(
            host=target_creds['host'],
            port=target_creds.get('port', 5432),
            database=target_creds['database'],
            user=target_creds['username'],
            password=target_creds['password']
        )

    else:
        # Use provided credentials
        if not all([
            args.source_server,
            args.source_database,
            args.source_username,
            args.source_password
        ]):
            raise ValueError(
                "Source database credentials required. Use --use-vault or provide "
                "--source-server, --source-database, --source-username, --source-password"
            )

        if not all([
            args.target_host,
            args.target_database,
            args.target_username,
            args.target_password
        ]):
            raise ValueError(
                "Target database credentials required. Use --use-vault or provide "
                "--target-host, --target-database, --target-username, --target-password"
            )

        # Connect to SQL Server
        source_conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={args.source_server};"
            f"DATABASE={args.source_database};"
            f"UID={args.source_username};"
            f"PWD={args.source_password}"
        )

        # Connect to PostgreSQL
        target_conn = psycopg2.connect(
            host=args.target_host,
            port=args.target_port,
            database=args.target_database,
            user=args.target_username,
            password=args.target_password
        )

    logger.info("Database connections established")
    return source_conn, target_conn


def reconcile_on_demand(args):
    """
    Perform on-demand reconciliation

    Returns:
        Exit code (0 for success/match, 1 for failure/mismatch)
    """
    # Parse tables
    if args.source_tables and args.target_tables:
        source_tables = [t.strip() for t in args.source_tables.split(",")]
        target_tables = [t.strip() for t in args.target_tables.split(",")]
    elif args.source_table and args.target_table:
        source_tables = [args.source_table]
        target_tables = [args.target_table]
    else:
        logger.error("Must provide either --source-table/--target-table or --source-tables/--target-tables")
        return 1

    if len(source_tables) != len(target_tables):
        logger.error("Number of source and target tables must match")
        return 1

    # Get database connections
    try:
        source_conn, target_conn = get_database_connections(args)
    except Exception as e:
        logger.error(f"Failed to connect to databases: {e}")
        return 1

    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()

    # Reconcile each table
    comparison_results = []

    for source_table, target_table in zip(source_tables, target_tables):
        logger.info(f"Reconciling {source_table} -> {target_table}")

        try:
            result = reconcile_table(
                source_cursor,
                target_cursor,
                source_table,
                target_table,
                validate_checksum=args.validate_checksums
            )
            comparison_results.append(result)

            status = "✓ MATCH" if result["match"] else "✗ MISMATCH"
            logger.info(f"  {status} - Source: {result['source_count']}, Target: {result['target_count']}")

        except Exception as e:
            logger.error(f"Failed to reconcile {source_table}: {e}")
            # Continue with other tables

    # Generate report
    report = generate_report(comparison_results)

    # Output report
    if args.format == "json":
        if args.output:
            export_report_json(report, args.output)
            logger.info(f"Report saved to {args.output}")
        else:
            import json
            print(json.dumps(report, indent=2))

    elif args.format == "csv":
        if args.output:
            export_report_csv(report, args.output)
            logger.info(f"Report saved to {args.output}")
        else:
            logger.error("CSV format requires --output parameter")
            return 1

    elif args.format == "console":
        print(format_report_console(report))

    # Close connections
    source_cursor.close()
    source_conn.close()
    target_cursor.close()
    target_conn.close()

    # Return exit code based on status
    return 0 if report["status"] == "PASS" else 1


def reconcile_scheduled(args):
    """
    Run reconciliation in scheduled mode

    Returns:
        Exit code
    """
    if not args.output_dir:
        logger.error("Scheduled mode requires --output-dir parameter")
        return 1

    if not (args.interval or args.cron):
        logger.error("Scheduled mode requires --interval or --cron parameter")
        return 1

    # Parse tables - support both singular and plural forms
    if args.source_tables and args.target_tables:
        source_tables = [t.strip() for t in args.source_tables.split(",")]
        target_tables = [t.strip() for t in args.target_tables.split(",")]
    elif args.source_table and args.target_table:
        source_tables = [args.source_table]
        target_tables = [args.target_table]
    else:
        logger.error("Scheduled mode requires either --source-table/--target-table or --source-tables/--target-tables")
        return 1

    # Get credentials
    if args.use_vault:
        source_config = get_credentials_from_vault("sqlserver")
        target_config = get_credentials_from_vault("postgresql")
    else:
        source_config = {
            "server": args.source_server,
            "database": args.source_database,
            "username": args.source_username,
            "password": args.source_password
        }

        target_config = {
            "host": args.target_host,
            "port": args.target_port,
            "database": args.target_database,
            "username": args.target_username,
            "password": args.target_password
        }

    # Create scheduler
    scheduler = ReconciliationScheduler()

    # Add job
    job_kwargs = {
        "source_config": source_config,
        "target_config": target_config,
        "tables": source_tables,
        "output_dir": args.output_dir,
        "validate_checksums": args.validate_checksums
    }

    if args.interval:
        scheduler.add_interval_job(
            reconcile_job_wrapper,
            interval_seconds=args.interval,
            job_id="reconcile_interval",
            **job_kwargs
        )
    elif args.cron:
        scheduler.add_cron_job(
            reconcile_job_wrapper,
            cron_expression=args.cron,
            job_id="reconcile_cron",
            **job_kwargs
        )

    # Start scheduler
    logger.info("Starting scheduler...")
    scheduler.start()

    return 0


def main():
    """Main entry point"""
    args = parse_args()

    # Setup logging with JSON format if specified
    setup_logging(
        level=args.log_level,
        console_output=True,
        json_format=args.json_logs if hasattr(args, 'json_logs') else False,
        log_file=args.log_file if hasattr(args, 'log_file') else None,
    )

    logger = get_logger(__name__)

    # Initialize metrics if enabled
    metrics = None
    if hasattr(args, 'enable_metrics') and args.enable_metrics:
        metrics = initialize_metrics(port=args.metrics_port if hasattr(args, 'metrics_port') else 9091)
        logger.info(f"Metrics enabled on port {args.metrics_port if hasattr(args, 'metrics_port') else 9091}")

    try:
        if args.schedule:
            return reconcile_scheduled(args)
        else:
            return reconcile_on_demand(args)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
