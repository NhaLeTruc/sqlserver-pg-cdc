"""
APScheduler-based reconciliation scheduler.

This module provides the ReconciliationScheduler class for scheduling
periodic reconciliation jobs using interval or cron triggers.
"""

import logging
from typing import List, Dict, Any, Callable

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

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

    def list_jobs(self) -> List[Dict[str, Any]]:
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
