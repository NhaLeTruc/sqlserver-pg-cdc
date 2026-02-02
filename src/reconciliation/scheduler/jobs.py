"""
Job wrapper functions for scheduled reconciliation tasks.

This module provides wrapper functions that can be called by the scheduler
to perform reconciliation jobs and save reports.
"""

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def reconcile_job_wrapper(
    source_config: dict[str, Any],
    target_config: dict[str, Any],
    tables: list[str],
    output_dir: str,
    validate_checksums: bool = False,
    use_connection_pool: bool = True
) -> None:
    """
    Wrapper function for scheduled reconciliation jobs

    This function is designed to be called by the scheduler.
    It performs reconciliation and saves reports to the output directory.

    Args:
        source_config: Source database connection configuration
        target_config: Target database connection configuration
        tables: List of table names to reconcile
        output_dir: Directory to save reconciliation reports
        validate_checksums: Whether to validate checksums
        use_connection_pool: Whether to use connection pooling (default: True)
    """
    from reconciliation.report import export_report_json, generate_report

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    output_path = Path(output_dir) / f"reconcile_{timestamp}.json"

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting scheduled reconciliation at {timestamp}")

    try:
        if use_connection_pool:
            # Use connection pooling for better performance
            from utils.db_pool import get_postgres_pool, get_sqlserver_pool

            postgres_pool = get_postgres_pool()
            sqlserver_pool = get_sqlserver_pool()

            with sqlserver_pool.acquire() as source_conn, postgres_pool.acquire() as target_conn:
                source_cursor = source_conn.cursor()
                target_cursor = target_conn.cursor()

                # Reconcile tables using pooled connections
                comparison_results, failed_tables = _reconcile_tables(
                    source_cursor,
                    target_cursor,
                    tables,
                    validate_checksums
                )

                source_cursor.close()
                target_cursor.close()
        else:
            # Legacy mode: create new connections for each job
            import psycopg2
            import pyodbc

            # Connect to source database (SQL Server)
            source_conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={source_config['server']};"
                f"DATABASE={source_config['database']};"
                f"UID={source_config['username']};"
                f"PWD={source_config['password']}"
            )
            source_cursor = source_conn.cursor()

            # Connect to target database (PostgreSQL)
            target_conn = psycopg2.connect(
                host=target_config['host'],
                port=target_config.get('port', 5432),
                database=target_config['database'],
                user=target_config['username'],
                password=target_config['password']
            )
            target_cursor = target_conn.cursor()

            try:
                # Reconcile tables
                comparison_results, failed_tables = _reconcile_tables(
                    source_cursor,
                    target_cursor,
                    tables,
                    validate_checksums
                )
            finally:
                # Close connections
                source_cursor.close()
                source_conn.close()
                target_cursor.close()
                target_conn.close()

        # Generate and save report
        report = generate_report(comparison_results)

        # Add failed tables to report if any
        if failed_tables:
            report["failed_tables"] = failed_tables
            logger.warning(f"Failed to reconcile {len(failed_tables)} table(s): {[ft['table'] for ft in failed_tables]}")

        export_report_json(report, str(output_path))

        logger.info(f"Reconciliation complete. Report saved to {output_path}")
        logger.info(f"Status: {report['status']}")
        logger.info(f"Tables reconciled: {len(comparison_results)}, Failed: {len(failed_tables)}")

    except Exception as e:
        logger.error(f"Reconciliation job failed: {e}")
        raise


def _reconcile_tables(
    source_cursor: Any,
    target_cursor: Any,
    tables: list[str],
    validate_checksums: bool
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """
    Reconcile a list of tables.

    Args:
        source_cursor: Source database cursor
        target_cursor: Target database cursor
        tables: List of table names to reconcile
        validate_checksums: Whether to validate checksums

    Returns:
        Tuple of (comparison_results, failed_tables)
    """
    from reconciliation.compare import reconcile_table

    comparison_results = []
    failed_tables = []

    for table in tables:
        logger.info(f"Reconciling table: {table}")

        try:
            result = reconcile_table(
                source_cursor,
                target_cursor,
                source_table=table,
                target_table=table,
                validate_checksum=validate_checksums
            )
            comparison_results.append(result)

        except Exception as e:
            logger.error(f"Error reconciling table {table}: {e}", exc_info=True)
            # Track failed tables
            failed_tables.append({"table": table, "error": str(e)})
            # Continue with other tables

    return comparison_results, failed_tables


def setup_logging(log_level: str = "INFO") -> None:
    """
    Setup logging configuration for scheduler

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
