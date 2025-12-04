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

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime
from pathlib import Path


# ============================================================================
# Test ReconciliationScheduler Initialization
# ============================================================================

class TestReconciliationSchedulerInit:
    """Test scheduler initialization"""

    def test_scheduler_initializes_with_blocking_scheduler(self):
        """Test ReconciliationScheduler creates BlockingScheduler"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        scheduler = ReconciliationScheduler()

        assert scheduler.scheduler is not None
        assert scheduler.jobs == []

    def test_scheduler_has_empty_jobs_list(self):
        """Test scheduler initializes with empty jobs list"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        scheduler = ReconciliationScheduler()

        assert isinstance(scheduler.jobs, list)
        assert len(scheduler.jobs) == 0

    @patch('src.reconciliation.scheduler.BlockingScheduler')
    def test_scheduler_creates_blocking_scheduler_instance(self, mock_blocking_scheduler):
        """Test scheduler instantiates BlockingScheduler"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        ReconciliationScheduler()

        mock_blocking_scheduler.assert_called_once()


# ============================================================================
# Test Interval Jobs
# ============================================================================

class TestIntervalJobs:
    """Test interval-based job scheduling"""

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
    def test_add_interval_job_creates_interval_trigger(self, mock_scheduler_class):
        """Test add_interval_job creates IntervalTrigger"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.add_job.return_value = Mock(id="test_job")

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_interval_job(job_func, 600, "test_job")

        call_args = mock_scheduler.add_job.call_args
        assert call_args[0][0] == job_func
        assert call_args[1]['id'] == "test_job"
        assert call_args[1]['replace_existing'] is True

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
    @patch('src.reconciliation.scheduler.logger')
    def test_add_interval_job_logs_addition(self, mock_logger, mock_scheduler_class):
        """Test add_interval_job logs job addition"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.add_job.return_value = Mock(id="logged_job")

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_interval_job(job_func, 300, "logged_job")

        mock_logger.info.assert_called_once()
        log_message = mock_logger.info.call_args[0][0]
        assert "logged_job" in log_message
        assert "300" in log_message


# ============================================================================
# Test Cron Jobs
# ============================================================================

class TestCronJobs:
    """Test cron-based job scheduling"""

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
    def test_add_cron_job_invalid_too_few_parts(self, mock_scheduler_class):
        """Test add_cron_job raises ValueError for too few parts"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        with pytest.raises(ValueError, match="must have 5 parts"):
            scheduler.add_cron_job(job_func, "0 0 * *", "invalid_job")

    @patch('src.reconciliation.scheduler.BlockingScheduler')
    def test_add_cron_job_invalid_too_many_parts(self, mock_scheduler_class):
        """Test add_cron_job raises ValueError for too many parts"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        with pytest.raises(ValueError, match="must have 5 parts"):
            scheduler.add_cron_job(job_func, "0 0 * * * *", "invalid_job")

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
    @patch('src.reconciliation.scheduler.logger')
    def test_add_cron_job_logs_addition(self, mock_logger, mock_scheduler_class):
        """Test add_cron_job logs job addition"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.add_job.return_value = Mock(id="logged_cron")

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_cron_job(job_func, "0 0 * * *", "logged_cron")

        mock_logger.info.assert_called_once()
        log_message = mock_logger.info.call_args[0][0]
        assert "logged_cron" in log_message
        assert "0 0 * * *" in log_message


# ============================================================================
# Test Job Management
# ============================================================================

class TestJobManagement:
    """Test job management operations"""

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
    @patch('src.reconciliation.scheduler.logger')
    def test_remove_job_logs_removal(self, mock_logger, mock_scheduler_class):
        """Test remove_job logs job removal"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_job = Mock(id="logged_removal")
        mock_scheduler.add_job.return_value = mock_job

        scheduler = ReconciliationScheduler()
        job_func = Mock()

        scheduler.add_interval_job(job_func, 300, "logged_removal")
        scheduler.remove_job("logged_removal")

        # Check that info was called for removal (after the add_job info call)
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Removed job" in msg and "logged_removal" in msg for msg in info_calls)

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
    @patch('src.reconciliation.scheduler.logger')
    def test_start_logs_starting_message(self, mock_logger, mock_scheduler_class):
        """Test start() logs starting message"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.start.side_effect = KeyboardInterrupt()

        scheduler = ReconciliationScheduler()

        try:
            scheduler.start()
        except KeyboardInterrupt:
            pass

        # Check that info was called with starting message
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Starting reconciliation scheduler" in msg for msg in info_calls)

    @patch('src.reconciliation.scheduler.BlockingScheduler')
    def test_start_calls_scheduler_start(self, mock_scheduler_class):
        """Test start() calls scheduler.start()"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler
        mock_scheduler.start.side_effect = KeyboardInterrupt()

        scheduler = ReconciliationScheduler()

        try:
            scheduler.start()
        except KeyboardInterrupt:
            pass

        mock_scheduler.start.assert_called_once()

    @patch('src.reconciliation.scheduler.BlockingScheduler')
    @patch('src.reconciliation.scheduler.logger')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
    @patch('src.reconciliation.scheduler.logger')
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

    @patch('src.reconciliation.scheduler.BlockingScheduler')
    @patch('src.reconciliation.scheduler.logger')
    def test_stop_calls_scheduler_shutdown(self, mock_logger, mock_scheduler_class):
        """Test stop() calls scheduler.shutdown()"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler

        scheduler = ReconciliationScheduler()
        scheduler.stop()

        mock_scheduler.shutdown.assert_called_once()

    @patch('src.reconciliation.scheduler.BlockingScheduler')
    @patch('src.reconciliation.scheduler.logger')
    def test_stop_logs_stopped_message(self, mock_logger, mock_scheduler_class):
        """Test stop() logs stopped message"""
        from src.reconciliation.scheduler import ReconciliationScheduler

        mock_scheduler = Mock()
        mock_scheduler_class.return_value = mock_scheduler

        scheduler = ReconciliationScheduler()
        scheduler.stop()

        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Scheduler stopped" in msg for msg in info_calls)


# ============================================================================
# Test reconcile_job_wrapper Function
# ============================================================================

class TestReconcileJobWrapper:
    """Test reconcile_job_wrapper function"""

    @patch('src.reconciliation.scheduler.Path')
    @patch('psycopg2.connect')
    @patch('pyodbc.connect')
    @patch('src.reconciliation.compare.reconcile_table')
    @patch('src.reconciliation.report.generate_report')
    @patch('src.reconciliation.report.export_report_json')
    @patch('src.reconciliation.scheduler.datetime')
    def test_reconcile_job_wrapper_successful_execution(
        self, mock_datetime, mock_export, mock_generate, mock_reconcile,
        mock_pyodbc_connect, mock_psycopg2_connect, mock_path
    ):
        """Test reconcile_job_wrapper executes successfully"""
        from src.reconciliation.scheduler import reconcile_job_wrapper

        # Setup mocks
        mock_datetime.utcnow.return_value.strftime.return_value = "20251204_100000"
        mock_source_conn = Mock()
        mock_source_cursor = Mock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = Mock()
        mock_target_cursor = Mock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        mock_reconcile.return_value = {
            "table": "customers",
            "match": True
        }
        mock_generate.return_value = {
            "status": "PASS"
        }

        source_config = {
            "server": "localhost",
            "database": "sourcedb",
            "username": "user",
            "password": "pass"
        }
        target_config = {
            "host": "localhost",
            "database": "targetdb",
            "username": "user",
            "password": "pass"
        }

        reconcile_job_wrapper(
            source_config,
            target_config,
            ["customers"],
            "/tmp/reports"
        )

        mock_pyodbc_connect.assert_called_once()
        mock_psycopg2_connect.assert_called_once()
        mock_reconcile.assert_called_once()
        mock_generate.assert_called_once()
        mock_export.assert_called_once()

    @patch('src.reconciliation.scheduler.Path')
    @patch('psycopg2.connect')
    @patch('pyodbc.connect')
    def test_reconcile_job_wrapper_creates_sqlserver_connection(
        self, mock_pyodbc_connect, mock_psycopg2_connect, mock_path
    ):
        """Test reconcile_job_wrapper creates SQL Server connection"""
        from src.reconciliation.scheduler import reconcile_job_wrapper

        mock_source_conn = Mock()
        mock_source_cursor = Mock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = Mock()
        mock_target_cursor = Mock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        source_config = {
            "server": "sqlserver.local",
            "database": "testdb",
            "username": "testuser",
            "password": "testpass"
        }
        target_config = {
            "host": "postgres.local",
            "database": "testdb",
            "username": "testuser",
            "password": "testpass"
        }

        with patch('src.reconciliation.compare.reconcile_table'), \
             patch('src.reconciliation.report.generate_report'), \
             patch('src.reconciliation.report.export_report_json'), \
             patch('src.reconciliation.scheduler.datetime'):

            reconcile_job_wrapper(source_config, target_config, ["test_table"], "/tmp")

        # Verify SQL Server connection string format
        connection_string = mock_pyodbc_connect.call_args[0][0]
        assert "DRIVER={ODBC Driver 17 for SQL Server}" in connection_string
        assert "SERVER=sqlserver.local" in connection_string
        assert "DATABASE=testdb" in connection_string
        assert "UID=testuser" in connection_string
        assert "PWD=testpass" in connection_string

    @patch('src.reconciliation.scheduler.Path')
    @patch('psycopg2.connect')
    @patch('pyodbc.connect')
    def test_reconcile_job_wrapper_creates_postgres_connection(
        self, mock_pyodbc_connect, mock_psycopg2_connect, mock_path
    ):
        """Test reconcile_job_wrapper creates PostgreSQL connection"""
        from src.reconciliation.scheduler import reconcile_job_wrapper

        mock_source_conn = Mock()
        mock_source_cursor = Mock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = Mock()
        mock_target_cursor = Mock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        source_config = {"server": "sql", "database": "db", "username": "u", "password": "p"}
        target_config = {
            "host": "pg.local",
            "port": 5433,
            "database": "testdb",
            "username": "testuser",
            "password": "testpass"
        }

        with patch('src.reconciliation.compare.reconcile_table'), \
             patch('src.reconciliation.report.generate_report'), \
             patch('src.reconciliation.report.export_report_json'), \
             patch('src.reconciliation.scheduler.datetime'):

            reconcile_job_wrapper(source_config, target_config, ["test_table"], "/tmp")

        mock_psycopg2_connect.assert_called_once_with(
            host="pg.local",
            port=5433,
            database="testdb",
            user="testuser",
            password="testpass"
        )

    @patch('src.reconciliation.scheduler.Path')
    @patch('psycopg2.connect')
    @patch('pyodbc.connect')
    @patch('src.reconciliation.compare.reconcile_table')
    def test_reconcile_job_wrapper_iterates_tables(
        self, mock_reconcile, mock_pyodbc_connect, mock_psycopg2_connect, mock_path
    ):
        """Test reconcile_job_wrapper reconciles each table"""
        from src.reconciliation.scheduler import reconcile_job_wrapper

        mock_source_conn = Mock()
        mock_source_cursor = Mock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = Mock()
        mock_target_cursor = Mock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        mock_reconcile.return_value = {"table": "test", "match": True}

        source_config = {"server": "s", "database": "d", "username": "u", "password": "p"}
        target_config = {"host": "h", "database": "d", "username": "u", "password": "p"}

        with patch('src.reconciliation.report.generate_report'), \
             patch('src.reconciliation.report.export_report_json'), \
             patch('src.reconciliation.scheduler.datetime'):

            reconcile_job_wrapper(
                source_config,
                target_config,
                ["customers", "orders", "products"],
                "/tmp"
            )

        assert mock_reconcile.call_count == 3

    @patch('src.reconciliation.scheduler.Path')
    @patch('psycopg2.connect')
    @patch('pyodbc.connect')
    @patch('src.reconciliation.compare.reconcile_table')
    @patch('src.reconciliation.scheduler.logger')
    def test_reconcile_job_wrapper_continues_on_table_error(
        self, mock_logger, mock_reconcile, mock_pyodbc_connect, mock_psycopg2_connect, mock_path
    ):
        """Test reconcile_job_wrapper continues on per-table errors"""
        from src.reconciliation.scheduler import reconcile_job_wrapper

        mock_source_conn = Mock()
        mock_source_cursor = Mock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = Mock()
        mock_target_cursor = Mock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        # First table fails, second succeeds
        mock_reconcile.side_effect = [
            Exception("Table error"),
            {"table": "orders", "match": True}
        ]

        source_config = {"server": "s", "database": "d", "username": "u", "password": "p"}
        target_config = {"host": "h", "database": "d", "username": "u", "password": "p"}

        with patch('src.reconciliation.report.generate_report'), \
             patch('src.reconciliation.report.export_report_json'), \
             patch('src.reconciliation.scheduler.datetime'):

            reconcile_job_wrapper(
                source_config,
                target_config,
                ["customers", "orders"],
                "/tmp"
            )

        # Should have attempted both tables
        assert mock_reconcile.call_count == 2
        # Should have logged error
        mock_logger.error.assert_called()

    @patch('src.reconciliation.scheduler.Path')
    @patch('psycopg2.connect')
    @patch('pyodbc.connect')
    @patch('src.reconciliation.compare.reconcile_table')
    @patch('src.reconciliation.report.generate_report')
    @patch('src.reconciliation.report.export_report_json')
    @patch('src.reconciliation.scheduler.datetime')
    def test_reconcile_job_wrapper_creates_output_directory(
        self, mock_datetime, mock_export, mock_generate, mock_reconcile,
        mock_pyodbc_connect, mock_psycopg2_connect, mock_path_class
    ):
        """Test reconcile_job_wrapper creates output directory"""
        from src.reconciliation.scheduler import reconcile_job_wrapper

        mock_datetime.utcnow.return_value.strftime.return_value = "20251204_100000"

        mock_source_conn = Mock()
        mock_source_cursor = Mock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = Mock()
        mock_target_cursor = Mock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        mock_reconcile.return_value = {"table": "test", "match": True}
        mock_generate.return_value = {"status": "PASS"}

        mock_path_instance = Mock()
        mock_path_class.return_value = mock_path_instance

        source_config = {"server": "s", "database": "d", "username": "u", "password": "p"}
        target_config = {"host": "h", "database": "d", "username": "u", "password": "p"}

        reconcile_job_wrapper(source_config, target_config, ["test"], "/output/dir")

        # Verify mkdir was called
        mock_path_instance.mkdir.assert_called_with(parents=True, exist_ok=True)

    @patch('src.reconciliation.scheduler.Path')
    @patch('psycopg2.connect')
    @patch('pyodbc.connect')
    @patch('src.reconciliation.compare.reconcile_table')
    @patch('src.reconciliation.scheduler.generate_report')
    @patch('src.reconciliation.scheduler.export_report_json')
    @patch('src.reconciliation.scheduler.datetime')
    def test_reconcile_job_wrapper_uses_timestamp_in_filename(
        self, mock_datetime, mock_export, mock_generate, mock_reconcile,
        mock_pyodbc, mock_psycopg2, mock_path
    ):
        """Test reconcile_job_wrapper includes timestamp in report filename"""
        from src.reconciliation.scheduler import reconcile_job_wrapper

        mock_datetime.utcnow.return_value.strftime.return_value = "20251204_103045"

        mock_source_conn = Mock()
        mock_source_cursor = Mock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = Mock()
        mock_target_cursor = Mock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        mock_reconcile.return_value = {"table": "test", "match": True}
        mock_generate.return_value = {"status": "PASS"}

        source_config = {"server": "s", "database": "d", "username": "u", "password": "p"}
        target_config = {"host": "h", "database": "d", "username": "u", "password": "p"}

        reconcile_job_wrapper(source_config, target_config, ["test"], "/reports")

        # Verify export was called with timestamped filename
        export_call_args = mock_export.call_args[0]
        assert "reconcile_20251204_103045.json" in export_call_args[1]

    @patch('src.reconciliation.scheduler.Path')
    @patch('psycopg2.connect')
    @patch('pyodbc.connect')
    @patch('src.reconciliation.compare.reconcile_table')
    def test_reconcile_job_wrapper_closes_connections(
        self, mock_reconcile, mock_pyodbc_connect, mock_psycopg2_connect, mock_path
    ):
        """Test reconcile_job_wrapper closes database connections"""
        from src.reconciliation.scheduler import reconcile_job_wrapper

        mock_source_conn = Mock()
        mock_source_cursor = Mock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = Mock()
        mock_target_cursor = Mock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        mock_reconcile.return_value = {"table": "test", "match": True}

        source_config = {"server": "s", "database": "d", "username": "u", "password": "p"}
        target_config = {"host": "h", "database": "d", "username": "u", "password": "p"}

        with patch('src.reconciliation.report.generate_report'), \
             patch('src.reconciliation.report.export_report_json'), \
             patch('src.reconciliation.scheduler.datetime'):

            reconcile_job_wrapper(source_config, target_config, ["test"], "/tmp")

        mock_source_cursor.close.assert_called_once()
        mock_source_conn.close.assert_called_once()
        mock_target_cursor.close.assert_called_once()
        mock_target_conn.close.assert_called_once()

    @patch('src.reconciliation.scheduler.Path')
    @patch('psycopg2.connect')
    @patch('pyodbc.connect')
    @patch('src.reconciliation.scheduler.logger')
    def test_reconcile_job_wrapper_propagates_fatal_exception(
        self, mock_logger, mock_pyodbc_connect, mock_psycopg2_connect, mock_path
    ):
        """Test reconcile_job_wrapper propagates fatal exceptions"""
        from src.reconciliation.scheduler import reconcile_job_wrapper

        mock_pyodbc_connect.side_effect = Exception("Connection failed")

        source_config = {"server": "s", "database": "d", "username": "u", "password": "p"}
        target_config = {"host": "h", "database": "d", "username": "u", "password": "p"}

        with pytest.raises(Exception, match="Connection failed"):
            reconcile_job_wrapper(source_config, target_config, ["test"], "/tmp")

        mock_logger.error.assert_called()

    @patch('src.reconciliation.scheduler.Path')
    @patch('psycopg2.connect')
    @patch('pyodbc.connect')
    @patch('src.reconciliation.compare.reconcile_table')
    def test_reconcile_job_wrapper_with_checksum_validation(
        self, mock_reconcile, mock_pyodbc_connect, mock_psycopg2_connect, mock_path
    ):
        """Test reconcile_job_wrapper with checksum validation enabled"""
        from src.reconciliation.scheduler import reconcile_job_wrapper

        mock_source_conn = Mock()
        mock_source_cursor = Mock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = Mock()
        mock_target_cursor = Mock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        mock_reconcile.return_value = {"table": "test", "match": True}

        source_config = {"server": "s", "database": "d", "username": "u", "password": "p"}
        target_config = {"host": "h", "database": "d", "username": "u", "password": "p"}

        with patch('src.reconciliation.report.generate_report'), \
             patch('src.reconciliation.report.export_report_json'), \
             patch('src.reconciliation.scheduler.datetime'):

            reconcile_job_wrapper(
                source_config,
                target_config,
                ["test"],
                "/tmp",
                validate_checksums=True
            )

        # Verify reconcile_table was called with validate_checksum=True
        call_args = mock_reconcile.call_args
        assert call_args[1]['validate_checksum'] is True

    @patch('src.reconciliation.scheduler.Path')
    @patch('psycopg2.connect')
    @patch('pyodbc.connect')
    @patch('src.reconciliation.compare.reconcile_table')
    def test_reconcile_job_wrapper_without_checksum_validation(
        self, mock_reconcile, mock_pyodbc_connect, mock_psycopg2_connect, mock_path
    ):
        """Test reconcile_job_wrapper with checksum validation disabled (default)"""
        from src.reconciliation.scheduler import reconcile_job_wrapper

        mock_source_conn = Mock()
        mock_source_cursor = Mock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_pyodbc_connect.return_value = mock_source_conn

        mock_target_conn = Mock()
        mock_target_cursor = Mock()
        mock_target_conn.cursor.return_value = mock_target_cursor
        mock_psycopg2_connect.return_value = mock_target_conn

        mock_reconcile.return_value = {"table": "test", "match": True}

        source_config = {"server": "s", "database": "d", "username": "u", "password": "p"}
        target_config = {"host": "h", "database": "d", "username": "u", "password": "p"}

        with patch('src.reconciliation.report.generate_report'), \
             patch('src.reconciliation.report.export_report_json'), \
             patch('src.reconciliation.scheduler.datetime'):

            reconcile_job_wrapper(
                source_config,
                target_config,
                ["test"],
                "/tmp",
                validate_checksums=False
            )

        # Verify reconcile_table was called with validate_checksum=False
        call_args = mock_reconcile.call_args
        assert call_args[1]['validate_checksum'] is False


# ============================================================================
# Test setup_logging Function
# ============================================================================

class TestSetupLogging:
    """Test setup_logging utility function"""

    @patch('src.reconciliation.scheduler.logging.basicConfig')
    def test_setup_logging_default_level(self, mock_basic_config):
        """Test setup_logging with default INFO level"""
        from src.reconciliation.scheduler import setup_logging

        setup_logging()

        mock_basic_config.assert_called_once()
        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == 20  # logging.INFO

    @patch('src.reconciliation.scheduler.logging.basicConfig')
    def test_setup_logging_debug_level(self, mock_basic_config):
        """Test setup_logging with DEBUG level"""
        from src.reconciliation.scheduler import setup_logging

        setup_logging("DEBUG")

        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == 10  # logging.DEBUG

    @patch('src.reconciliation.scheduler.logging.basicConfig')
    def test_setup_logging_warning_level(self, mock_basic_config):
        """Test setup_logging with WARNING level"""
        from src.reconciliation.scheduler import setup_logging

        setup_logging("WARNING")

        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == 30  # logging.WARNING

    @patch('src.reconciliation.scheduler.logging.basicConfig')
    def test_setup_logging_error_level(self, mock_basic_config):
        """Test setup_logging with ERROR level"""
        from src.reconciliation.scheduler import setup_logging

        setup_logging("ERROR")

        call_kwargs = mock_basic_config.call_args[1]
        assert call_kwargs['level'] == 40  # logging.ERROR

    @patch('src.reconciliation.scheduler.logging.basicConfig')
    def test_setup_logging_format_and_dateformat(self, mock_basic_config):
        """Test setup_logging sets format and datefmt"""
        from src.reconciliation.scheduler import setup_logging

        setup_logging()

        call_kwargs = mock_basic_config.call_args[1]
        assert 'format' in call_kwargs
        assert 'datefmt' in call_kwargs
        assert '%(asctime)s' in call_kwargs['format']
        assert '%(levelname)s' in call_kwargs['format']
