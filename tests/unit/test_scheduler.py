"""
Unit tests for reconciliation scheduler module

Tests verify:
- ReconciliationScheduler initialization
- Interval job scheduling
- Cron job scheduling
- Job management (list, remove)
- Scheduler lifecycle (start, stop)
- reconcile_job_wrapper functionality

All tests use mocking to avoid actual scheduling and database connections.
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest

class TestIntervalJobs:
    """Test interval-based job scheduling"""

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_add_interval_job_with_valid_params(self, mock_scheduler_class):
        """Test add_interval_job with valid parameters"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_job = Mock()
        mock_job.id = "test_job"
        mock_scheduler.add_job.return_value = mock_job

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_interval_job(job_func, 300, "test_job", param1="value1")

        mock_scheduler.add_job.assert_called_once()
        assert len(scheduler.jobs) == 1
        assert scheduler.jobs[0] == mock_job


    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_add_interval_job_with_kwargs(self, mock_scheduler_class):
        """Test add_interval_job passes kwargs to job function"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.add_job.return_value = Mock(id="test_job")

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_interval_job(
            job_func,
            300,
            "test_job",
            source_config={"host": "localhost"},
            target_config={"host": "localhost"}
        )

        call_args = mock_scheduler.add_job.call_args
        assert 'kwargs' in call_args[1]
        assert call_args[1]['kwargs']['source_config'] == {"host": "localhost"}

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_add_interval_job_with_small_interval(self, mock_scheduler_class):
        """Test add_interval_job with 1 second interval"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.add_job.return_value = Mock(id="fast_job")

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_interval_job(job_func, 1, "fast_job")

        assert len(scheduler.jobs) == 1

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_add_interval_job_with_large_interval(self, mock_scheduler_class):
        """Test add_interval_job with large interval (1 day)"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.add_job.return_value = Mock(id="daily_job")

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_interval_job(job_func, 86400, "daily_job")

        assert len(scheduler.jobs) == 1

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_add_interval_job_replace_existing(self, mock_scheduler_class):
        """Test add_interval_job replaces existing job with same ID"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_job = Mock(id="replaceable_job")
        mock_scheduler.add_job.return_value = mock_job

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        # Add job twice with same ID
        scheduler.add_interval_job(job_func, 300, "replaceable_job")
        scheduler.add_interval_job(job_func, 600, "replaceable_job")

        # Should have called add_job twice with replace_existing=True
        assert mock_scheduler.add_job.call_count == 2
        for call_args in mock_scheduler.add_job.call_args_list:
            assert call_args[1]['replace_existing'] is True



# ============================================================================
# Test Cron Jobs
# ============================================================================

class TestCronJobs:
    """Test cron-based job scheduling"""

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_add_cron_job_with_valid_expression(self, mock_scheduler_class):
        """Test add_cron_job with valid cron expression"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.add_job.return_value = Mock(id="cron_job")

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_cron_job(job_func, "0 */6 * * *", "cron_job")

        assert len(scheduler.jobs) == 1
        mock_scheduler.add_job.assert_called_once()

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_add_cron_job_every_six_hours(self, mock_scheduler_class):
        """Test add_cron_job with 'every 6 hours' expression"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.add_job.return_value = Mock(id="six_hour_job")

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_cron_job(job_func, "0 */6 * * *", "six_hour_job")

        call_args = mock_scheduler.add_job.call_args
        assert call_args[0][0] == job_func

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_add_cron_job_daily_midnight(self, mock_scheduler_class):
        """Test add_cron_job with daily at midnight expression"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.add_job.return_value = Mock(id="daily_job")

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_cron_job(job_func, "0 0 * * *", "daily_job")

        assert len(scheduler.jobs) == 1

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_add_cron_job_every_thirty_minutes(self, mock_scheduler_class):
        """Test add_cron_job with every 30 minutes expression"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.add_job.return_value = Mock(id="half_hour_job")

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_cron_job(job_func, "*/30 * * * *", "half_hour_job")

        assert len(scheduler.jobs) == 1

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_add_cron_job_weekly_sunday(self, mock_scheduler_class):
        """Test add_cron_job with weekly on Sunday expression"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.add_job.return_value = Mock(id="weekly_job")

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_cron_job(job_func, "0 0 * * 0", "weekly_job")

        assert len(scheduler.jobs) == 1

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_add_cron_job_invalid_too_few_parts(self, mock_scheduler_class):
        """Test add_cron_job raises ValueError for too few parts"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        with pytest.raises(ValueError, match="must have 5 parts"):
            scheduler.add_cron_job(job_func, "0 0 * *", "invalid_job")

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_add_cron_job_invalid_too_many_parts(self, mock_scheduler_class):
        """Test add_cron_job raises ValueError for too many parts"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        with pytest.raises(ValueError, match="must have 5 parts"):
            scheduler.add_cron_job(job_func, "0 0 * * * *", "invalid_job")

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_add_cron_job_with_kwargs(self, mock_scheduler_class):
        """Test add_cron_job passes kwargs to job function"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.add_job.return_value = Mock(id="cron_with_kwargs")

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_cron_job(
            job_func,
            "0 0 * * *",
            "cron_with_kwargs",
            tables=["customers", "orders"]
        )

        call_args = mock_scheduler.add_job.call_args
        assert 'kwargs' in call_args[1]
        assert call_args[1]['kwargs']['tables'] == ["customers", "orders"]



# ============================================================================
# Test Job Management
# ============================================================================

class TestJobManagement:
    """Test job management operations"""

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_remove_job_removes_from_scheduler(self, mock_scheduler_class):
        """Test remove_job removes job from scheduler"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_job = Mock(id="removable_job")
        mock_scheduler.add_job.return_value = mock_job

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_interval_job(job_func, 300, "removable_job")
        scheduler.remove_job("removable_job")

        mock_scheduler.remove_job.assert_called_once_with("removable_job")

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_remove_job_removes_from_jobs_list(self, mock_scheduler_class):
        """Test remove_job removes job from internal jobs list"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_job = Mock(id="removable_job")
        mock_scheduler.add_job.return_value = mock_job

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_interval_job(job_func, 300, "removable_job")
        assert len(scheduler.jobs) == 1

        scheduler.remove_job("removable_job")
        assert len(scheduler.jobs) == 0


    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_list_jobs_returns_empty_list(self, mock_scheduler_class):
        """Test list_jobs returns empty list when no jobs"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.get_jobs.return_value = []

        scheduler = ReconciliationScheduler()

        jobs = scheduler.list_jobs()

        assert jobs == []
        assert isinstance(jobs, list)

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_list_jobs_returns_job_information(self, mock_scheduler_class):
        """Test list_jobs returns formatted job information"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler

        mock_job = Mock()
        mock_job.id = "test_job"
        mock_job.name = "test_job_function"
        mock_job.next_run_time = datetime(2025, 12, 4, 10, 0, 0)
        mock_job.trigger = "interval[0:05:00]"

        mock_scheduler.get_jobs.return_value = [mock_job]

        scheduler = ReconciliationScheduler()

        jobs = scheduler.list_jobs()

        assert len(jobs) == 1
        assert jobs[0]['id'] == "test_job"
        assert jobs[0]['name'] == "test_job_function"
        assert jobs[0]['next_run_time'] == "2025-12-04T10:00:00"
        assert jobs[0]['trigger'] == "interval[0:05:00]"

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_list_jobs_with_multiple_jobs(self, mock_scheduler_class):
        """Test list_jobs returns multiple jobs"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler

        mock_job1 = Mock()
        mock_job1.id = "job1"
        mock_job1.name = "job1_func"
        mock_job1.next_run_time = datetime(2025, 12, 4, 10, 0, 0)
        mock_job1.trigger = "interval[0:05:00]"

        mock_job2 = Mock()
        mock_job2.id = "job2"
        mock_job2.name = "job2_func"
        mock_job2.next_run_time = datetime(2025, 12, 4, 11, 0, 0)
        mock_job2.trigger = "cron[hour='*/6']"

        mock_scheduler.get_jobs.return_value = [mock_job1, mock_job2]

        scheduler = ReconciliationScheduler()

        jobs = scheduler.list_jobs()

        assert len(jobs) == 2
        assert jobs[0]['id'] == "job1"
        assert jobs[1]['id'] == "job2"

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    def test_list_jobs_with_none_next_run_time(self, mock_scheduler_class):
        """Test list_jobs handles None next_run_time"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler

        mock_job = Mock()
        mock_job.id = "paused_job"
        mock_job.name = "paused_job_func"
        mock_job.next_run_time = None
        mock_job.trigger = "interval[0:05:00]"

        mock_scheduler.get_jobs.return_value = [mock_job]

        scheduler = ReconciliationScheduler()

        jobs = scheduler.list_jobs()

        assert len(jobs) == 1
        assert jobs[0]['next_run_time'] is None


# ============================================================================
# Test Scheduler Lifecycle
# ============================================================================

class TestSchedulerLifecycle:
    """Test scheduler start, stop, and lifecycle management"""



    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    @patch('src.reconciliation.scheduler.scheduler.logger')
    def test_start_handles_keyboard_interrupt(self, mock_logger, mock_scheduler_class):
        """Test start() handles KeyboardInterrupt gracefully"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.start.side_effect = KeyboardInterrupt()
        mock_scheduler.shutdown.return_value = None

        scheduler = ReconciliationScheduler()
        scheduler.start()

        # Should call stop() which calls shutdown
        mock_scheduler.shutdown.assert_called_once()

    @patch('src.reconciliation.scheduler.scheduler.BlockingScheduler')
    @patch('src.reconciliation.scheduler.scheduler.logger')
    def test_start_handles_system_exit(self, mock_logger, mock_scheduler_class):
        """Test start() handles SystemExit gracefully"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.start.side_effect = SystemExit()
        mock_scheduler.shutdown.return_value = None

        scheduler = ReconciliationScheduler()
        scheduler.start()

        mock_scheduler.shutdown.assert_called_once()






# ============================================================================
# Test setup_logging Function
# ============================================================================

class TestSetupLogging:
    """Test setup_logging utility function"""

    @patch('src.reconciliation.scheduler.jobs.logging.basicConfig')
    def test_setup_logging_default_level(self, mock_basic_config):
        """Test setup_logging with default INFO level"""
        from src.reconciliation.scheduler import setup_logging

        setup_logging()

        mock_basic_config.assert_called_once()
        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == 20  # logging.INFO

    @patch('src.reconciliation.scheduler.jobs.logging.basicConfig')
    def test_setup_logging_debug_level(self, mock_basic_config):
        """Test setup_logging with DEBUG level"""
        from src.reconciliation.scheduler import setup_logging

        setup_logging("DEBUG")

        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == 10  # logging.DEBUG

    @patch('src.reconciliation.scheduler.jobs.logging.basicConfig')
    def test_setup_logging_warning_level(self, mock_basic_config):
        """Test setup_logging with WARNING level"""
        from src.reconciliation.scheduler import setup_logging

        setup_logging("WARNING")

        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == 30  # logging.WARNING

    @patch('src.reconciliation.scheduler.jobs.logging.basicConfig')
    def test_setup_logging_error_level(self, mock_basic_config):
        """Test setup_logging with ERROR level"""
        from src.reconciliation.scheduler import setup_logging

        setup_logging("ERROR")

        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == 40  # logging.ERROR

    @patch('src.reconciliation.scheduler.jobs.logging.basicConfig')
    def test_setup_logging_format_and_dateformat(self, mock_basic_config):
        """Test setup_logging sets format and datefmt"""
        from src.reconciliation.scheduler import setup_logging

        setup_logging()

        call_kwargs = mock_basic_config.call_args[1]
        assert 'format' in call_kwargs
        assert 'datefmt' in call_kwargs
        assert '%(asctime)s' in call_kwargs['format']
        assert '%(levelname)s' in call_kwargs['format']
