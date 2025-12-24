"""
Command-line interface for data reconciliation

This module provides a CLI for running reconciliation jobs between
SQL Server source and PostgreSQL target databases.
"""

import argparse
import sys
import logging
from pathlib import Path
from typing import Dict, Any, List
import json

from src.reconciliation.compare import reconcile_table
from src.reconciliation.report import (
    generate_report,
    export_report_json,
    export_report_csv,
    format_report_console
)
from src.reconciliation.row_level import RowLevelReconciler, generate_repair_script
from src.reconciliation.scheduler import ReconciliationScheduler, reconcile_job_wrapper
from src.reconciliation.parallel import ParallelReconciler
from src.utils.vault_client import VaultClient


logger = logging.getLogger(__name__)


def setup_logging(log_level: str = "INFO") -> None:
    """
    Setup logging configuration

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def get_credentials_from_vault_or_env(args: argparse.Namespace) -> tuple:
    """
    Get database credentials from Vault or environment/args

    Args:
        args: Parsed command-line arguments

    Returns:
        Tuple of (source_config, target_config)
    """
    import os

    if args.use_vault:
        try:
            vault_client = VaultClient()
            source_creds = vault_client.get_database_credentials("sqlserver")
            target_creds = vault_client.get_database_credentials("postgresql")

            source_config = {
                "server": source_creds["server"],
                "database": source_creds["database"],
                "username": source_creds["username"],
                "password": source_creds["password"]
            }

            target_config = {
                "host": target_creds["host"],
                "port": target_creds.get("port", 5432),
                "database": target_creds["database"],
                "username": target_creds["username"],
                "password": target_creds["password"]
            }

            logger.info("Successfully fetched credentials from Vault")
        except Exception as e:
            logger.error(f"Failed to fetch credentials from Vault: {e}")
            sys.exit(1)
    else:
        # Get from command-line args or environment variables
        source_config = {
            "server": args.source_server or os.getenv("SQLSERVER_HOST", "localhost"),
            "database": args.source_database or os.getenv("SQLSERVER_DATABASE", "warehouse_source"),
            "username": args.source_user or os.getenv("SQLSERVER_USER", "sa"),
            "password": args.source_password or os.getenv("SQLSERVER_PASSWORD")
        }

        target_config = {
            "host": args.target_host or os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(args.target_port or os.getenv("POSTGRES_PORT", "5432")),
            "database": args.target_database or os.getenv("POSTGRES_DB", "warehouse_target"),
            "username": args.target_user or os.getenv("POSTGRES_USER", "postgres"),
            "password": args.target_password or os.getenv("POSTGRES_PASSWORD")
        }

        # Validate passwords
        if not source_config["password"]:
            logger.error("Source database password not provided")
            sys.exit(1)
        if not target_config["password"]:
            logger.error("Target database password not provided")
            sys.exit(1)

    return source_config, target_config


def cmd_run(args: argparse.Namespace) -> None:
    """
    Run a one-time reconciliation

    Args:
        args: Parsed command-line arguments
    """
    import pyodbc
    import psycopg2

    logger.info("Starting reconciliation run")

    # Get credentials
    source_config, target_config = get_credentials_from_vault_or_env(args)

    # Parse tables
    if args.tables_file:
        with open(args.tables_file, 'r') as f:
            tables = [line.strip() for line in f if line.strip()]
    else:
        tables = args.tables.split(',')

    logger.info(f"Reconciling {len(tables)} table(s): {', '.join(tables)}")

    try:
        # Connect to source database (SQL Server)
        source_conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={source_config['server']};"
            f"DATABASE={source_config['database']};"
            f"UID={source_config['username']};"
            f"PWD={source_config['password']};"
            f"TrustServerCertificate=yes;"
        )
        source_cursor = source_conn.cursor()
        logger.info("Connected to SQL Server source")

        # Connect to target database (PostgreSQL)
        target_conn = psycopg2.connect(
            host=target_config['host'],
            port=target_config['port'],
            database=target_config['database'],
            user=target_config['username'],
            password=target_config['password']
        )
        target_cursor = target_conn.cursor()
        logger.info("Connected to PostgreSQL target")

        # Reconcile tables (parallel or sequential)
        comparison_results = []
        all_row_discrepancies = {}

        if args.parallel and len(tables) > 1:
            # Use parallel reconciliation
            logger.info(f"Using parallel reconciliation with {args.parallel_workers} workers")

            def reconcile_single_table(table, source_cursor, target_cursor,
                                      validate_checksum, row_level_enabled=False,
                                      pk_columns_str='id', row_level_chunk_size=1000,
                                      generate_repair_enabled=False, output_dir='.'):
                """Wrapper function for parallel reconciliation."""
                result = reconcile_table(
                    source_cursor,
                    target_cursor,
                    source_table=table,
                    target_table=table,
                    validate_checksum=validate_checksum
                )
                result['table'] = table

                # Handle row-level reconciliation if needed
                if row_level_enabled and not result["match"]:
                    pk_columns = pk_columns_str.split(',')
                    reconciler = RowLevelReconciler(
                        source_cursor=source_cursor,
                        target_cursor=target_cursor,
                        pk_columns=pk_columns,
                        compare_columns=None,
                        chunk_size=row_level_chunk_size,
                    )
                    discrepancies = reconciler.reconcile_table(table, table)
                    result['row_discrepancies'] = discrepancies

                    if generate_repair_enabled and discrepancies:
                        repair_output = Path(output_dir) / f"repair_{table}.sql"
                        repair_output.parent.mkdir(parents=True, exist_ok=True)
                        script = generate_repair_script(discrepancies, table, "postgresql")
                        with open(repair_output, 'w') as f:
                            f.write(script)
                        result['repair_script'] = str(repair_output)

                return result

            parallel_reconciler = ParallelReconciler(
                max_workers=args.parallel_workers,
                timeout_per_table=args.parallel_timeout,
                fail_fast=not args.continue_on_error
            )

            parallel_results = parallel_reconciler.reconcile_tables(
                tables=tables,
                reconcile_func=reconcile_single_table,
                source_cursor=source_cursor,
                target_cursor=target_cursor,
                validate_checksum=args.validate_checksums,
                row_level_enabled=args.row_level,
                pk_columns_str=args.pk_columns or 'id',
                row_level_chunk_size=args.row_level_chunk_size,
                generate_repair_enabled=args.generate_repair,
                output_dir=args.output_dir or '.'
            )

            # Extract comparison results
            comparison_results = parallel_results['results']

            # Log summary
            logger.info(
                f"Parallel reconciliation complete: "
                f"{parallel_results['successful']}/{parallel_results['total_tables']} successful, "
                f"{parallel_results['failed']} failed, "
                f"{parallel_results['timeout']} timeout "
                f"in {parallel_results['duration_seconds']:.2f}s"
            )

            # Handle errors
            if parallel_results['errors']:
                logger.error(f"Errors encountered: {len(parallel_results['errors'])}")
                for error in parallel_results['errors']:
                    logger.error(f"  {error['table']}: {error['error']}")

        else:
            # Sequential reconciliation
            for table in tables:
                logger.info(f"Reconciling table: {table}")

                try:
                    result = reconcile_table(
                        source_cursor,
                        target_cursor,
                        source_table=table,
                        target_table=table,
                        validate_checksum=args.validate_checksums
                    )
                    comparison_results.append(result)
                    status = "MATCH" if result["match"] else "MISMATCH"
                    logger.info(f"  {table}: {status}")

                    # Perform row-level reconciliation if requested and there's a mismatch
                    if args.row_level and not result["match"]:
                        logger.info(f"  Performing row-level reconciliation for {table}")

                        # Parse primary key columns
                        pk_columns = args.pk_columns.split(',') if args.pk_columns else ['id']

                        reconciler = RowLevelReconciler(
                            source_cursor=source_cursor,
                            target_cursor=target_cursor,
                            pk_columns=pk_columns,
                            compare_columns=None,  # Compare all columns
                            chunk_size=args.row_level_chunk_size,
                        )

                        discrepancies = reconciler.reconcile_table(table, table)
                        all_row_discrepancies[table] = discrepancies

                        logger.info(
                            f"  Found {len(discrepancies)} row-level discrepancies in {table}"
                        )

                        # Generate repair script if requested
                        if args.generate_repair and discrepancies:
                            repair_output = Path(args.output_dir or ".") / f"repair_{table}.sql"
                            repair_output.parent.mkdir(parents=True, exist_ok=True)

                            # Detect database type from cursor
                            db_type = "postgresql"  # Default to target database type

                            script = generate_repair_script(discrepancies, table, db_type)

                            with open(repair_output, 'w') as f:
                                f.write(script)

                            logger.info(f"  Generated repair script: {repair_output}")

                except Exception as e:
                    logger.error(f"Error reconciling table {table}: {e}")
                    if not args.continue_on_error:
                        raise

        # Generate report
        report = generate_report(comparison_results)

        # Export report
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if args.format == "json":
                export_report_json(report, str(output_path))
                logger.info(f"Report saved to {output_path}")
            elif args.format == "csv":
                export_report_csv(report, str(output_path))
                logger.info(f"Report saved to {output_path}")
            else:  # console
                print(format_report_console(report))
        else:
            # Print to console
            print(format_report_console(report))

        # Close connections
        source_cursor.close()
        source_conn.close()
        target_cursor.close()
        target_conn.close()

        # Exit with appropriate code
        if report["status"] == "FAIL":
            logger.warning("Reconciliation found discrepancies")
            sys.exit(1)
        else:
            logger.info("Reconciliation completed successfully")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Reconciliation failed: {e}")
        sys.exit(1)


def cmd_schedule(args: argparse.Namespace) -> None:
    """
    Schedule periodic reconciliation jobs

    Args:
        args: Parsed command-line arguments
    """
    logger.info("Setting up reconciliation scheduler")

    # Get credentials
    source_config, target_config = get_credentials_from_vault_or_env(args)

    # Parse tables
    if args.tables_file:
        with open(args.tables_file, 'r') as f:
            tables = [line.strip() for line in f if line.strip()]
    else:
        tables = args.tables.split(',')

    # Create output directory
    output_dir = args.output_dir or "./reconciliation_reports"
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Create scheduler
    scheduler = ReconciliationScheduler()

    # Add job
    if args.cron:
        scheduler.add_cron_job(
            reconcile_job_wrapper,
            args.cron,
            "reconciliation_job",
            source_config=source_config,
            target_config=target_config,
            tables=tables,
            output_dir=output_dir,
            validate_checksums=args.validate_checksums
        )
        logger.info(f"Scheduled reconciliation with cron: {args.cron}")
    else:
        scheduler.add_interval_job(
            reconcile_job_wrapper,
            args.interval,
            "reconciliation_job",
            source_config=source_config,
            target_config=target_config,
            tables=tables,
            output_dir=output_dir,
            validate_checksums=args.validate_checksums
        )
        logger.info(f"Scheduled reconciliation every {args.interval} seconds")

    # Start scheduler (blocking)
    logger.info("Starting scheduler (press Ctrl+C to stop)")
    scheduler.start()


def cmd_report(args: argparse.Namespace) -> None:
    """
    Generate a report from a previous reconciliation JSON file

    Args:
        args: Parsed command-line arguments
    """
    logger.info(f"Loading reconciliation report from {args.input}")

    try:
        with open(args.input, 'r') as f:
            report = json.load(f)

        if args.format == "console":
            print(format_report_console(report))
        elif args.format == "csv":
            if not args.output:
                logger.error("Output file required for CSV format")
                sys.exit(1)
            export_report_csv(report, args.output)
            logger.info(f"Report exported to {args.output}")
        elif args.format == "json":
            if not args.output:
                logger.error("Output file required for JSON format")
                sys.exit(1)
            export_report_json(report, args.output)
            logger.info(f"Report exported to {args.output}")

    except Exception as e:
        logger.error(f"Failed to process report: {e}")
        sys.exit(1)


def main() -> None:
    """Main entry point for the reconcile CLI"""
    parser = argparse.ArgumentParser(
        description="Data reconciliation tool for SQL Server to PostgreSQL CDC pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run one-time reconciliation for specific tables
  reconcile run --tables customers,orders,products --output report.json

  # Use Vault for credentials
  reconcile run --use-vault --tables customers --validate-checksums

  # Perform row-level reconciliation with repair script generation
  reconcile run --tables customers --row-level --generate-repair --pk-columns customer_id

  # Row-level reconciliation with composite primary key
  reconcile run --tables user_orgs --row-level --pk-columns user_id,org_id --generate-repair

  # Parallel reconciliation (3-5x faster for multiple tables)
  reconcile run --tables customers,orders,products,users --parallel --parallel-workers 4

  # Parallel reconciliation with row-level analysis
  reconcile run --tables-file tables.txt --parallel --parallel-workers 8 --row-level

  # Schedule periodic reconciliation every 6 hours
  reconcile schedule --cron "0 */6 * * *" --tables-file tables.txt

  # Schedule with interval (in seconds)
  reconcile schedule --interval 3600 --tables customers

  # Generate console report from previous run
  reconcile report --input report.json --format console
        """
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Run command
    run_parser = subparsers.add_parser('run', help='Run one-time reconciliation')
    run_parser.add_argument(
        '--tables',
        help='Comma-separated list of tables to reconcile'
    )
    run_parser.add_argument(
        '--tables-file',
        help='File containing list of tables (one per line)'
    )
    run_parser.add_argument(
        '--validate-checksums',
        action='store_true',
        help='Validate data checksums (slower but more thorough)'
    )
    run_parser.add_argument(
        '--output',
        help='Output file path for report'
    )
    run_parser.add_argument(
        '--format',
        choices=['console', 'json', 'csv'],
        default='console',
        help='Output format (default: console)'
    )
    run_parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue with remaining tables if one fails'
    )
    run_parser.add_argument(
        '--row-level',
        action='store_true',
        help='Perform row-level reconciliation (detailed comparison)'
    )
    run_parser.add_argument(
        '--pk-columns',
        default='id',
        help='Comma-separated list of primary key columns (default: id)'
    )
    run_parser.add_argument(
        '--row-level-chunk-size',
        type=int,
        default=1000,
        help='Chunk size for row-level reconciliation (default: 1000)'
    )
    run_parser.add_argument(
        '--generate-repair',
        action='store_true',
        help='Generate SQL repair scripts for row-level discrepancies'
    )
    run_parser.add_argument(
        '--output-dir',
        help='Output directory for repair scripts (default: current directory)'
    )
    run_parser.add_argument(
        '--parallel',
        action='store_true',
        help='Enable parallel table reconciliation (3-5x faster for multiple tables)'
    )
    run_parser.add_argument(
        '--parallel-workers',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )
    run_parser.add_argument(
        '--parallel-timeout',
        type=int,
        default=3600,
        help='Timeout per table in seconds for parallel mode (default: 3600)'
    )
    run_parser.add_argument(
        '--use-vault',
        action='store_true',
        help='Fetch credentials from HashiCorp Vault'
    )
    # Source database options
    run_parser.add_argument('--source-server', help='SQL Server host')
    run_parser.add_argument('--source-database', help='SQL Server database name')
    run_parser.add_argument('--source-user', help='SQL Server username')
    run_parser.add_argument('--source-password', help='SQL Server password')
    # Target database options
    run_parser.add_argument('--target-host', help='PostgreSQL host')
    run_parser.add_argument('--target-port', help='PostgreSQL port')
    run_parser.add_argument('--target-database', help='PostgreSQL database name')
    run_parser.add_argument('--target-user', help='PostgreSQL username')
    run_parser.add_argument('--target-password', help='PostgreSQL password')

    # Schedule command
    schedule_parser = subparsers.add_parser('schedule', help='Schedule periodic reconciliation')
    schedule_parser.add_argument(
        '--tables',
        help='Comma-separated list of tables to reconcile'
    )
    schedule_parser.add_argument(
        '--tables-file',
        help='File containing list of tables (one per line)'
    )
    schedule_parser.add_argument(
        '--validate-checksums',
        action='store_true',
        help='Validate data checksums (slower but more thorough)'
    )
    schedule_parser.add_argument(
        '--cron',
        help='Cron expression (e.g., "0 */6 * * *" for every 6 hours)'
    )
    schedule_parser.add_argument(
        '--interval',
        type=int,
        default=3600,
        help='Interval in seconds (default: 3600 = 1 hour)'
    )
    schedule_parser.add_argument(
        '--output-dir',
        help='Directory to save reconciliation reports'
    )
    schedule_parser.add_argument(
        '--use-vault',
        action='store_true',
        help='Fetch credentials from HashiCorp Vault'
    )
    # Source database options
    schedule_parser.add_argument('--source-server', help='SQL Server host')
    schedule_parser.add_argument('--source-database', help='SQL Server database name')
    schedule_parser.add_argument('--source-user', help='SQL Server username')
    schedule_parser.add_argument('--source-password', help='SQL Server password')
    # Target database options
    schedule_parser.add_argument('--target-host', help='PostgreSQL host')
    schedule_parser.add_argument('--target-port', help='PostgreSQL port')
    schedule_parser.add_argument('--target-database', help='PostgreSQL database name')
    schedule_parser.add_argument('--target-user', help='PostgreSQL username')
    schedule_parser.add_argument('--target-password', help='PostgreSQL password')

    # Report command
    report_parser = subparsers.add_parser('report', help='Generate report from previous run')
    report_parser.add_argument(
        '--input',
        required=True,
        help='Input JSON report file'
    )
    report_parser.add_argument(
        '--format',
        choices=['console', 'json', 'csv'],
        default='console',
        help='Output format (default: console)'
    )
    report_parser.add_argument(
        '--output',
        help='Output file path (required for json and csv formats)'
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_level)

    # Execute command
    if args.command == 'run':
        if not args.tables and not args.tables_file:
            parser.error("Either --tables or --tables-file is required")
        cmd_run(args)
    elif args.command == 'schedule':
        if not args.tables and not args.tables_file:
            parser.error("Either --tables or --tables-file is required")
        cmd_schedule(args)
    elif args.command == 'report':
        cmd_report(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()