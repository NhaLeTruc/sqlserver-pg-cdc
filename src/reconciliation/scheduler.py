"""
Reconciliation scheduler module

Provides cron-like scheduling functionality for automated reconciliation
using APScheduler.
"""

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)


class ReconciliationScheduler:
    """
    Scheduler for automated reconciliation tasks

    This class provides cron-like scheduling for reconciliation jobs,
    allowing periodic execution with configurable intervals.
    """

    def __init__(self):
        """Initialize the scheduler"""
        self.scheduler = BlockingScheduler()
        self.jobs = []

    def add_interval_job(
        self,
        job_func: Callable,
        interval_seconds: int,
        job_id: str,
        **kwargs
    ) -> None:
        """
        Add a job that runs at fixed intervals

        Args:
            job_func: Function to execute
            interval_seconds: Interval in seconds
            job_id: Unique identifier for the job
            **kwargs: Additional arguments to pass to job_func
        """
        trigger = IntervalTrigger(seconds=interval_seconds)

        job = self.scheduler.add_job(
            job_func,
            trigger=trigger,
            id=job_id,
            kwargs=kwargs,
            replace_existing=True
        )

        self.jobs.append(job)
        logger.info(
            f"Added interval job '{job_id}' with {interval_seconds}s interval"
        )

    def add_cron_job(
        self,
        job_func: Callable,
        cron_expression: str,
        job_id: str,
        **kwargs
    ) -> None:
        """
        Add a job that runs on a cron schedule

        Args:
            job_func: Function to execute
            cron_expression: Cron expression (e.g., "0 */6 * * *" for every 6 hours)
            job_id: Unique identifier for the job
            **kwargs: Additional arguments to pass to job_func

        Example cron expressions:
            "0 */6 * * *"  - Every 6 hours
            "0 0 * * *"    - Daily at midnight
            "0 0 * * 0"    - Weekly on Sunday at midnight
            "*/30 * * * *" - Every 30 minutes
        """
        # Parse cron expression (minute hour day month day_of_week)
        parts = cron_expression.split()

        if len(parts) != 5:
            raise ValueError(
                "Cron expression must have 5 parts: minute hour day month day_of_week"
            )

        minute, hour, day, month, day_of_week = parts

        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week
        )

        job = self.scheduler.add_job(
            job_func,
            trigger=trigger,
            id=job_id,
            kwargs=kwargs,
            replace_existing=True
        )

        self.jobs.append(job)
        logger.info(f"Added cron job '{job_id}' with schedule '{cron_expression}'")

    def remove_job(self, job_id: str) -> None:
        """
        Remove a scheduled job

        Args:
            job_id: Unique identifier of the job to remove
        """
        self.scheduler.remove_job(job_id)
        self.jobs = [job for job in self.jobs if job.id != job_id]
        logger.info(f"Removed job '{job_id}'")

    def start(self) -> None:
        """
        Start the scheduler

        This will block the current thread and run scheduled jobs.
        Use Ctrl+C to stop.
        """
        logger.info("Starting reconciliation scheduler...")
        logger.info(f"Scheduled {len(self.jobs)} job(s)")

        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped by user")
            self.stop()

    def stop(self) -> None:
        """Stop the scheduler"""
        self.scheduler.shutdown()
        logger.info("Scheduler stopped")

    def list_jobs(self) -> list[dict[str, Any]]:
        """
        List all scheduled jobs

        Returns:
            List of job information dictionaries
        """
        job_list = []

        for job in self.scheduler.get_jobs():
            job_list.append({
                "id": job.id,
                "name": job.name,
                "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger)
            })

        return job_list


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
    from src.reconciliation.report import export_report_json, generate_report

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    output_path = Path(output_dir) / f"reconcile_{timestamp}.json"

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting scheduled reconciliation at {timestamp}")

    try:
        if use_connection_pool:
            # Use connection pooling for better performance
            from src.utils.db_pool import get_postgres_pool, get_sqlserver_pool

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
    from src.reconciliation.compare import reconcile_table

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
