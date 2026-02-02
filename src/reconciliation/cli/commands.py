"""
CLI command implementations.

This module contains the implementation of the three main CLI commands:
- run: One-time reconciliation
- schedule: Periodic scheduled reconciliation
- report: Report generation from previous runs
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import psycopg2
import pyodbc

from reconciliation.compare import reconcile_table
from reconciliation.parallel import ParallelReconciler
from reconciliation.report import (
    export_report_csv,
    export_report_json,
    format_report_console,
    generate_report,
)
from reconciliation.row_level import RowLevelReconciler, generate_repair_script
from reconciliation.scheduler import ReconciliationScheduler, reconcile_job_wrapper

from .credentials import get_credentials_from_vault_or_env

logger = logging.getLogger(__name__)


def cmd_run(args: argparse.Namespace) -> None:
    """
    Run a one-time reconciliation

    Args:
        args: Parsed command-line arguments
    """
    logger.info("Starting reconciliation run")

    # Get credentials
    source_config, target_config = get_credentials_from_vault_or_env(args)

    # Parse tables
    if args.tables_file:
        with open(args.tables_file) as f:
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
        with open(args.tables_file) as f:
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
        with open(args.input) as f:
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
