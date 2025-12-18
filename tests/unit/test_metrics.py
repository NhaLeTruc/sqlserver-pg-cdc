"""
Unit tests for src/utils/metrics.py

This module provides comprehensive tests for Prometheus metrics publishing functionality,
including MetricsPublisher, ReconciliationMetrics, ConnectorMetrics, VaultMetrics, and ApplicationInfo.
"""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock, call
from prometheus_client import CollectorRegistry
from src.utils.metrics import (
    MetricsPublisher,
    ReconciliationMetrics,
    ConnectorMetrics,
    VaultMetrics,
    ApplicationInfo,
    initialize_metrics,
)


class TestMetricsPublisher:
    """Test MetricsPublisher class"""

    def test_init_with_default_port(self):
        """Test initialization with default port"""
        # Arrange & Act
        publisher = MetricsPublisher()

        # Assert
        assert publisher.port == 9091
        assert publisher.registry is not None
        assert publisher._server_started is False

    def test_init_with_custom_port(self):
        """Test initialization with custom port"""
        # Arrange & Act
        publisher = MetricsPublisher(port=8080)

        # Assert
        assert publisher.port == 8080

    def test_init_with_custom_registry(self):
        """Test initialization with custom registry"""
        # Arrange
        custom_registry = CollectorRegistry()

        # Act
        publisher = MetricsPublisher(registry=custom_registry)

        # Assert
        assert publisher.registry is custom_registry

    @patch('src.utils.metrics.start_http_server')
    @patch('src.utils.metrics.logger')
    def test_start_successful(self, mock_logger, mock_start_http_server):
        """Test successful server start"""
        # Arrange
        publisher = MetricsPublisher(port=9091)

        # Act
        publisher.start()

        # Assert
        mock_start_http_server.assert_called_once_with(9091, registry=publisher.registry)
        assert publisher._server_started is True
        mock_logger.info.assert_called_once()
        assert "Metrics server started on port 9091" in mock_logger.info.call_args[0][0]

    @patch('src.utils.metrics.start_http_server')
    @patch('src.utils.metrics.logger')
    def test_start_already_started_warning(self, mock_logger, mock_start_http_server):
        """Test that starting already-started server logs warning"""
        # Arrange
        publisher = MetricsPublisher(port=9091)
        publisher._server_started = True

        # Act
        publisher.start()

        # Assert
        mock_start_http_server.assert_not_called()
        mock_logger.warning.assert_called_once()
        assert "already running" in mock_logger.warning.call_args[0][0]

    @patch('src.utils.metrics.start_http_server')
    @patch('src.utils.metrics.logger')
    def test_start_port_already_in_use(self, mock_logger, mock_start_http_server):
        """Test handling of port already in use"""
        # Arrange
        publisher = MetricsPublisher(port=9091)
        mock_start_http_server.side_effect = OSError("Address already in use")

        # Act & Assert
        with pytest.raises(RuntimeError) as exc_info:
            publisher.start()

        assert publisher._server_started is False
        assert "Metrics server port 9091 is already in use" in str(exc_info.value)
        assert "Cannot start metrics collection" in str(exc_info.value)

    @patch('src.utils.metrics.start_http_server')
    def test_start_other_os_error_raises(self, mock_start_http_server):
        """Test that non-port OSErrors are raised"""
        # Arrange
        publisher = MetricsPublisher(port=9091)
        mock_start_http_server.side_effect = OSError("Permission denied")

        # Act & Assert
        with pytest.raises(OSError) as exc_info:
            publisher.start()

        assert "Permission denied" in str(exc_info.value)

    def test_is_started_returns_false_initially(self):
        """Test that is_started returns False initially"""
        # Arrange
        publisher = MetricsPublisher()

        # Act
        result = publisher.is_started()

        # Assert
        assert result is False

    @patch('src.utils.metrics.start_http_server')
    def test_is_started_returns_true_after_start(self, mock_start_http_server):
        """Test that is_started returns True after starting"""
        # Arrange
        publisher = MetricsPublisher()

        # Act
        publisher.start()
        result = publisher.is_started()

        # Assert
        assert result is True


class TestReconciliationMetrics:
    """Test ReconciliationMetrics class"""

    def test_init_creates_all_metrics(self):
        """Test that initialization creates all expected metrics"""
        # Arrange
        registry = CollectorRegistry()

        # Act
        metrics = ReconciliationMetrics(registry=registry)

        # Assert
        assert metrics.reconciliation_runs_total is not None
        assert metrics.reconciliation_duration_seconds is not None
        assert metrics.reconciliation_last_run_timestamp is not None
        assert metrics.row_count_mismatch_total is not None
        assert metrics.row_count_difference is not None
        assert metrics.checksum_mismatch_total is not None
        assert metrics.rows_compared_total is not None
        assert metrics.comparison_rate is not None

    @patch('src.utils.metrics.time.time')
    @patch('src.utils.metrics.logger')
    def test_record_reconciliation_run_success(self, mock_logger, mock_time):
        """Test recording a successful reconciliation run"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ReconciliationMetrics(registry=registry)
        mock_time.return_value = 1234567890.0

        # Act
        metrics.record_reconciliation_run(
            table_name="customers",
            success=True,
            duration=45.2,
            rows_compared=1000
        )

        # Assert
        # Verify counter incremented
        counter_value = metrics.reconciliation_runs_total.labels(
            table_name="customers", status="success"
        )._value.get()
        assert counter_value == 1.0

        # Verify logging
        mock_logger.info.assert_called_once()
        log_message = mock_logger.info.call_args[0][0]
        assert "customers" in log_message
        assert "success" in log_message
        assert "45.2" in log_message

    @patch('src.utils.metrics.time.time')
    @patch('src.utils.metrics.logger')
    def test_record_reconciliation_run_failure(self, mock_logger, mock_time):
        """Test recording a failed reconciliation run"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ReconciliationMetrics(registry=registry)
        mock_time.return_value = 1234567890.0

        # Act
        metrics.record_reconciliation_run(
            table_name="orders",
            success=False,
            duration=12.5
        )

        # Assert
        counter_value = metrics.reconciliation_runs_total.labels(
            table_name="orders", status="failed"
        )._value.get()
        assert counter_value == 1.0

    @patch('src.utils.metrics.time.time')
    def test_record_reconciliation_run_calculates_rate(self, mock_time):
        """Test that comparison rate is calculated correctly"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ReconciliationMetrics(registry=registry)
        mock_time.return_value = 1234567890.0

        # Act
        metrics.record_reconciliation_run(
            table_name="products",
            success=True,
            duration=10.0,
            rows_compared=1000
        )

        # Assert
        rate_value = metrics.comparison_rate.labels(table_name="products")._value.get()
        assert rate_value == 100.0  # 1000 rows / 10 seconds

    @patch('src.utils.metrics.time.time')
    def test_record_reconciliation_run_zero_duration_no_rate(self, mock_time):
        """Test that rate is not calculated for zero duration"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ReconciliationMetrics(registry=registry)
        mock_time.return_value = 1234567890.0

        # Act
        metrics.record_reconciliation_run(
            table_name="products",
            success=True,
            duration=0.0,
            rows_compared=1000
        )

        # Assert - rate should not be set (remains 0)
        rate_value = metrics.comparison_rate.labels(table_name="products")._value.get()
        assert rate_value == 0.0

    @patch('src.utils.metrics.time.time')
    def test_record_reconciliation_run_without_rows_compared(self, mock_time):
        """Test recording run without rows_compared parameter"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ReconciliationMetrics(registry=registry)
        mock_time.return_value = 1234567890.0

        # Act
        metrics.record_reconciliation_run(
            table_name="customers",
            success=True,
            duration=45.2
        )

        # Assert
        counter_value = metrics.reconciliation_runs_total.labels(
            table_name="customers", status="success"
        )._value.get()
        assert counter_value == 1.0

        # rows_compared_total should not be incremented
        rows_value = metrics.rows_compared_total.labels(table_name="customers")._value.get()
        assert rows_value == 0.0

    @patch('src.utils.metrics.logger')
    def test_record_row_count_mismatch(self, mock_logger):
        """Test recording row count mismatch"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ReconciliationMetrics(registry=registry)

        # Act
        metrics.record_row_count_mismatch(
            table_name="orders",
            source_count=1000,
            target_count=998
        )

        # Assert
        mismatch_count = metrics.row_count_mismatch_total.labels(
            table_name="orders"
        )._value.get()
        assert mismatch_count == 1.0

        difference_value = metrics.row_count_difference.labels(
            table_name="orders"
        )._value.get()
        assert difference_value == 2.0  # 1000 - 998

        mock_logger.warning.assert_called_once()
        log_message = mock_logger.warning.call_args[0][0]
        assert "Row count mismatch detected" in log_message
        assert "orders" in log_message

    @patch('src.utils.metrics.logger')
    def test_record_row_count_mismatch_negative_difference(self, mock_logger):
        """Test recording row count mismatch with negative difference"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ReconciliationMetrics(registry=registry)

        # Act
        metrics.record_row_count_mismatch(
            table_name="products",
            source_count=500,
            target_count=550
        )

        # Assert
        difference_value = metrics.row_count_difference.labels(
            table_name="products"
        )._value.get()
        assert difference_value == -50.0  # 500 - 550

    @patch('src.utils.metrics.logger')
    def test_record_checksum_mismatch(self, mock_logger):
        """Test recording checksum mismatch"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ReconciliationMetrics(registry=registry)

        # Act
        metrics.record_checksum_mismatch(table_name="customers")

        # Assert
        mismatch_count = metrics.checksum_mismatch_total.labels(
            table_name="customers"
        )._value.get()
        assert mismatch_count == 1.0

        mock_logger.warning.assert_called_once()
        assert "Checksum mismatch detected" in mock_logger.warning.call_args[0][0]

    def test_reset_difference(self):
        """Test resetting row count difference"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ReconciliationMetrics(registry=registry)

        # Set initial difference
        metrics.row_count_difference.labels(table_name="orders").set(100)

        # Act
        metrics.reset_difference(table_name="orders")

        # Assert
        difference_value = metrics.row_count_difference.labels(
            table_name="orders"
        )._value.get()
        assert difference_value == 0.0


class TestConnectorMetrics:
    """Test ConnectorMetrics class"""

    def test_init_creates_all_metrics(self):
        """Test that initialization creates all expected metrics"""
        # Arrange
        registry = CollectorRegistry()

        # Act
        metrics = ConnectorMetrics(registry=registry)

        # Assert
        assert metrics.connector_deployments_total is not None
        assert metrics.connector_deployment_duration_seconds is not None
        assert metrics.connector_operations_total is not None
        assert metrics.connector_state is not None
        assert metrics.connector_tasks_total is not None
        assert metrics.connector_tasks_running is not None
        assert metrics.connector_tasks_failed is not None

    @patch('src.utils.metrics.logger')
    def test_record_deployment_success(self, mock_logger):
        """Test recording successful deployment"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ConnectorMetrics(registry=registry)

        # Act
        metrics.record_deployment(
            connector_name="sqlserver-source",
            success=True,
            duration=5.3
        )

        # Assert
        counter_value = metrics.connector_deployments_total.labels(
            connector_name="sqlserver-source", status="success"
        )._value.get()
        assert counter_value == 1.0

        mock_logger.info.assert_called_once()
        log_message = mock_logger.info.call_args[0][0]
        assert "sqlserver-source" in log_message
        assert "success" in log_message

    @patch('src.utils.metrics.logger')
    def test_record_deployment_failure(self, mock_logger):
        """Test recording failed deployment"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ConnectorMetrics(registry=registry)

        # Act
        metrics.record_deployment(
            connector_name="postgres-sink",
            success=False,
            duration=12.1
        )

        # Assert
        counter_value = metrics.connector_deployments_total.labels(
            connector_name="postgres-sink", status="failed"
        )._value.get()
        assert counter_value == 1.0

    @patch('src.utils.metrics.logger')
    def test_record_operation_pause_success(self, mock_logger):
        """Test recording successful pause operation"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ConnectorMetrics(registry=registry)

        # Act
        metrics.record_operation(
            connector_name="test-connector",
            operation="pause",
            success=True
        )

        # Assert
        counter_value = metrics.connector_operations_total.labels(
            connector_name="test-connector",
            operation="pause",
            status="success"
        )._value.get()
        assert counter_value == 1.0

        mock_logger.info.assert_called_once()
        assert "pause" in mock_logger.info.call_args[0][0]

    @patch('src.utils.metrics.logger')
    def test_record_operation_restart_failure(self, mock_logger):
        """Test recording failed restart operation"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ConnectorMetrics(registry=registry)

        # Act
        metrics.record_operation(
            connector_name="test-connector",
            operation="restart",
            success=False
        )

        # Assert
        counter_value = metrics.connector_operations_total.labels(
            connector_name="test-connector",
            operation="restart",
            status="failed"
        )._value.get()
        assert counter_value == 1.0

    @patch('src.utils.metrics.logger')
    def test_update_connector_state_running(self, mock_logger):
        """Test updating connector state to RUNNING"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ConnectorMetrics(registry=registry)

        # Act
        metrics.update_connector_state(
            connector_name="test-connector",
            state="RUNNING",
            tasks_total=4,
            tasks_running=4,
            tasks_failed=0
        )

        # Assert
        state_value = metrics.connector_state.labels(
            connector_name="test-connector"
        )._value.get()
        assert state_value == 1  # RUNNING = 1

        tasks_total_value = metrics.connector_tasks_total.labels(
            connector_name="test-connector"
        )._value.get()
        assert tasks_total_value == 4

        tasks_running_value = metrics.connector_tasks_running.labels(
            connector_name="test-connector"
        )._value.get()
        assert tasks_running_value == 4

    @patch('src.utils.metrics.logger')
    def test_update_connector_state_paused(self, mock_logger):
        """Test updating connector state to PAUSED"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ConnectorMetrics(registry=registry)

        # Act
        metrics.update_connector_state(
            connector_name="test-connector",
            state="PAUSED",
            tasks_total=2,
            tasks_running=0,
            tasks_failed=0
        )

        # Assert
        state_value = metrics.connector_state.labels(
            connector_name="test-connector"
        )._value.get()
        assert state_value == 2  # PAUSED = 2

    @patch('src.utils.metrics.logger')
    def test_update_connector_state_failed(self, mock_logger):
        """Test updating connector state to FAILED"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ConnectorMetrics(registry=registry)

        # Act
        metrics.update_connector_state(
            connector_name="test-connector",
            state="FAILED",
            tasks_total=4,
            tasks_running=0,
            tasks_failed=4
        )

        # Assert
        state_value = metrics.connector_state.labels(
            connector_name="test-connector"
        )._value.get()
        assert state_value == 3  # FAILED = 3

        tasks_failed_value = metrics.connector_tasks_failed.labels(
            connector_name="test-connector"
        )._value.get()
        assert tasks_failed_value == 4

    @patch('src.utils.metrics.logger')
    def test_update_connector_state_stopped(self, mock_logger):
        """Test updating connector state to STOPPED"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ConnectorMetrics(registry=registry)

        # Act
        metrics.update_connector_state(
            connector_name="test-connector",
            state="STOPPED",
            tasks_total=0,
            tasks_running=0,
            tasks_failed=0
        )

        # Assert
        state_value = metrics.connector_state.labels(
            connector_name="test-connector"
        )._value.get()
        assert state_value == 0  # STOPPED = 0

    @patch('src.utils.metrics.logger')
    def test_update_connector_state_unknown_state(self, mock_logger):
        """Test updating connector state with unknown state"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ConnectorMetrics(registry=registry)

        # Act
        metrics.update_connector_state(
            connector_name="test-connector",
            state="UNKNOWN",
            tasks_total=2,
            tasks_running=1,
            tasks_failed=0
        )

        # Assert
        state_value = metrics.connector_state.labels(
            connector_name="test-connector"
        )._value.get()
        assert state_value == -1  # Unknown state = -1

    @patch('src.utils.metrics.logger')
    def test_update_connector_state_lowercase(self, mock_logger):
        """Test updating connector state with lowercase state name"""
        # Arrange
        registry = CollectorRegistry()
        metrics = ConnectorMetrics(registry=registry)

        # Act
        metrics.update_connector_state(
            connector_name="test-connector",
            state="running",
            tasks_total=2,
            tasks_running=2,
            tasks_failed=0
        )

        # Assert
        state_value = metrics.connector_state.labels(
            connector_name="test-connector"
        )._value.get()
        assert state_value == 1  # Should handle lowercase


class TestVaultMetrics:
    """Test VaultMetrics class"""

    def test_init_creates_all_metrics(self):
        """Test that initialization creates all expected metrics"""
        # Arrange
        registry = CollectorRegistry()

        # Act
        metrics = VaultMetrics(registry=registry)

        # Assert
        assert metrics.credential_retrievals_total is not None
        assert metrics.credential_retrieval_duration_seconds is not None
        assert metrics.vault_health_check_total is not None
        assert metrics.vault_is_sealed is not None

    @patch('src.utils.metrics.logger')
    def test_record_credential_retrieval_success(self, mock_logger):
        """Test recording successful credential retrieval"""
        # Arrange
        registry = CollectorRegistry()
        metrics = VaultMetrics(registry=registry)

        # Act
        metrics.record_credential_retrieval(
            secret_path="secret/database/postgres",
            success=True,
            duration=0.123
        )

        # Assert
        counter_value = metrics.credential_retrievals_total.labels(
            secret_path="secret/database/postgres", status="success"
        )._value.get()
        assert counter_value == 1.0

        mock_logger.debug.assert_called_once()
        log_message = mock_logger.debug.call_args[0][0]
        assert "secret/database/postgres" in log_message
        assert "success" in log_message

    @patch('src.utils.metrics.logger')
    def test_record_credential_retrieval_failure(self, mock_logger):
        """Test recording failed credential retrieval"""
        # Arrange
        registry = CollectorRegistry()
        metrics = VaultMetrics(registry=registry)

        # Act
        metrics.record_credential_retrieval(
            secret_path="secret/database/mysql",
            success=False,
            duration=2.5
        )

        # Assert
        counter_value = metrics.credential_retrievals_total.labels(
            secret_path="secret/database/mysql", status="failed"
        )._value.get()
        assert counter_value == 1.0

    @patch('src.utils.metrics.logger')
    def test_record_health_check_healthy_unsealed(self, mock_logger):
        """Test recording healthy and unsealed Vault"""
        # Arrange
        registry = CollectorRegistry()
        metrics = VaultMetrics(registry=registry)

        # Act
        metrics.record_health_check(
            is_healthy=True,
            is_sealed=False
        )

        # Assert
        health_counter = metrics.vault_health_check_total.labels(
            status="healthy"
        )._value.get()
        assert health_counter == 1.0

        sealed_value = metrics.vault_is_sealed._value.get()
        assert sealed_value == 0  # 0 = unsealed

    @patch('src.utils.metrics.logger')
    def test_record_health_check_unhealthy_sealed(self, mock_logger):
        """Test recording unhealthy and sealed Vault"""
        # Arrange
        registry = CollectorRegistry()
        metrics = VaultMetrics(registry=registry)

        # Act
        metrics.record_health_check(
            is_healthy=False,
            is_sealed=True
        )

        # Assert
        health_counter = metrics.vault_health_check_total.labels(
            status="unhealthy"
        )._value.get()
        assert health_counter == 1.0

        sealed_value = metrics.vault_is_sealed._value.get()
        assert sealed_value == 1  # 1 = sealed

    @patch('src.utils.metrics.logger')
    def test_record_health_check_healthy_sealed(self, mock_logger):
        """Test recording healthy but sealed Vault (standby)"""
        # Arrange
        registry = CollectorRegistry()
        metrics = VaultMetrics(registry=registry)

        # Act
        metrics.record_health_check(
            is_healthy=True,
            is_sealed=True
        )

        # Assert
        health_counter = metrics.vault_health_check_total.labels(
            status="healthy"
        )._value.get()
        assert health_counter == 1.0

        sealed_value = metrics.vault_is_sealed._value.get()
        assert sealed_value == 1  # 1 = sealed


class TestApplicationInfo:
    """Test ApplicationInfo class"""

    def test_init_with_default_values(self):
        """Test initialization with default values"""
        # Arrange
        registry = CollectorRegistry()

        # Act
        app_info = ApplicationInfo(registry=registry)

        # Assert
        assert app_info.info is not None
        assert app_info.uptime_seconds is not None
        assert app_info._start_time is not None

    def test_init_with_custom_values(self):
        """Test initialization with custom app name and version"""
        # Arrange
        registry = CollectorRegistry()

        # Act
        app_info = ApplicationInfo(
            app_name="my-custom-app",
            version="2.0.0",
            registry=registry
        )

        # Assert
        assert app_info is not None

    @patch('src.utils.metrics.time.time')
    def test_update_uptime(self, mock_time):
        """Test updating uptime metric"""
        # Arrange
        registry = CollectorRegistry()
        mock_time.return_value = 1000.0
        app_info = ApplicationInfo(registry=registry)

        # Simulate 100 seconds passing
        mock_time.return_value = 1100.0

        # Act
        app_info.update_uptime()

        # Assert
        uptime_value = app_info.uptime_seconds._value.get()
        assert uptime_value == 100.0

    @patch('src.utils.metrics.time.time')
    def test_get_uptime(self, mock_time):
        """Test getting current uptime"""
        # Arrange
        registry = CollectorRegistry()
        mock_time.return_value = 1000.0
        app_info = ApplicationInfo(registry=registry)

        # Simulate 250 seconds passing
        mock_time.return_value = 1250.0

        # Act
        uptime = app_info.get_uptime()

        # Assert
        assert uptime == 250.0

    @patch('src.utils.metrics.time.time')
    def test_uptime_starts_at_zero(self, mock_time):
        """Test that uptime calculation starts from initialization"""
        # Arrange
        mock_time.return_value = 5000.0
        registry = CollectorRegistry()

        # Act
        app_info = ApplicationInfo(registry=registry)
        uptime = app_info.get_uptime()

        # Assert
        assert uptime == 0.0


class TestInitializeMetrics:
    """Test initialize_metrics convenience function"""

    @patch('src.utils.metrics.MetricsPublisher')
    @patch('src.utils.metrics.logger')
    def test_initialize_metrics_with_defaults(self, mock_logger, mock_publisher_class):
        """Test initialize_metrics with default parameters"""
        # Arrange
        mock_publisher = Mock()
        mock_publisher_class.return_value = mock_publisher

        # Act
        result = initialize_metrics()

        # Assert
        mock_publisher_class.assert_called_once_with(port=9091, registry=None)
        mock_publisher.start.assert_called_once()

        assert "publisher" in result
        assert "reconciliation" in result
        assert "connector" in result
        assert "vault" in result
        assert "app_info" in result

    @patch('src.utils.metrics.MetricsPublisher')
    @patch('src.utils.metrics.logger')
    def test_initialize_metrics_with_custom_port(self, mock_logger, mock_publisher_class):
        """Test initialize_metrics with custom port"""
        # Arrange
        mock_publisher = Mock()
        mock_publisher_class.return_value = mock_publisher
        custom_registry = CollectorRegistry()

        # Act
        result = initialize_metrics(port=8080, registry=custom_registry)

        # Assert
        mock_publisher_class.assert_called_once_with(port=8080, registry=custom_registry)
        mock_logger.info.assert_called()

    @patch('src.utils.metrics.MetricsPublisher')
    @patch('src.utils.metrics.logger')
    def test_initialize_metrics_with_custom_registry(self, mock_logger, mock_publisher_class):
        """Test initialize_metrics with custom registry"""
        # Arrange
        mock_publisher = Mock()
        mock_publisher_class.return_value = mock_publisher
        custom_registry = CollectorRegistry()

        # Act
        result = initialize_metrics(registry=custom_registry)

        # Assert
        mock_publisher_class.assert_called_once_with(port=9091, registry=custom_registry)

    @patch('src.utils.metrics.MetricsPublisher')
    @patch('src.utils.metrics.logger')
    def test_initialize_metrics_returns_all_components(self, mock_logger, mock_publisher_class):
        """Test that initialize_metrics returns all metric components"""
        # Arrange
        mock_publisher = Mock()
        mock_publisher_class.return_value = mock_publisher
        custom_registry = CollectorRegistry()

        # Act
        result = initialize_metrics(registry=custom_registry)

        # Assert
        assert isinstance(result, dict)
        assert len(result) == 5
        assert result["publisher"] == mock_publisher
        assert isinstance(result["reconciliation"], ReconciliationMetrics)
        assert isinstance(result["connector"], ConnectorMetrics)
        assert isinstance(result["vault"], VaultMetrics)
        assert isinstance(result["app_info"], ApplicationInfo)

    @patch('src.utils.metrics.MetricsPublisher')
    @patch('src.utils.metrics.logger')
    def test_initialize_metrics_logs_initialization(self, mock_logger, mock_publisher_class):
        """Test that initialization is logged"""
        # Arrange
        mock_publisher = Mock()
        mock_publisher_class.return_value = mock_publisher
        custom_registry = CollectorRegistry()

        # Act
        initialize_metrics(port=9999, registry=custom_registry)

        # Assert
        mock_logger.info.assert_called()
        log_message = mock_logger.info.call_args[0][0]
        assert "Initializing metrics" in log_message
        assert "9999" in log_message
