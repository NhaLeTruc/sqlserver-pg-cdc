"""
Custom metrics publishing to Prometheus

This module provides utilities for publishing custom application metrics
to Prometheus for monitoring and alerting.

Usage:
    from src.utils.metrics import MetricsPublisher, ReconciliationMetrics

    # Initialize publisher
    metrics = MetricsPublisher(port=9091)
    metrics.start()

    # Record reconciliation metrics
    recon_metrics = ReconciliationMetrics()
    recon_metrics.record_reconciliation_run("customers", success=True, duration=45.2)
    recon_metrics.record_row_count_mismatch("orders", source_count=1000, target_count=998)
"""

import logging
import time
from typing import Optional, Dict, Any
from prometheus_client import (
    start_http_server,
    Counter,
    Gauge,
    Histogram,
    Summary,
    Info,
    CollectorRegistry,
    REGISTRY,
)

logger = logging.getLogger(__name__)


class MetricsPublisher:
    """
    Base class for publishing metrics to Prometheus

    Starts an HTTP server that exposes metrics on /metrics endpoint.
    """

    def __init__(
        self,
        port: int = 9091,
        registry: Optional[CollectorRegistry] = None,
    ):
        """
        Initialize metrics publisher

        Args:
            port: Port to expose metrics on (default: 9091)
            registry: Custom Prometheus registry (default: global REGISTRY)
        """
        self.port = port
        self.registry = registry or REGISTRY
        self._server_started = False

    def start(self) -> None:
        """Start the metrics HTTP server"""
        if self._server_started:
            logger.warning(f"Metrics server already running on port {self.port}")
            return

        try:
            start_http_server(self.port, registry=self.registry)
            self._server_started = True
            logger.info(f"Metrics server started on port {self.port}")
        except OSError as e:
            if "Address already in use" in str(e):
                logger.warning(f"Port {self.port} already in use, metrics server not started")
            else:
                raise

    def is_started(self) -> bool:
        """Check if metrics server is running"""
        return self._server_started


class ReconciliationMetrics:
    """
    Metrics for data reconciliation operations

    Tracks reconciliation runs, discrepancies, and performance.
    """

    def __init__(self, registry: Optional[CollectorRegistry] = None):
        """
        Initialize reconciliation metrics

        Args:
            registry: Custom Prometheus registry (default: global REGISTRY)
        """
        self.registry = registry or REGISTRY

        # Reconciliation run metrics
        self.reconciliation_runs_total = Counter(
            "reconciliation_runs_total",
            "Total number of reconciliation runs",
            ["table_name", "status"],
            registry=self.registry,
        )

        self.reconciliation_duration_seconds = Histogram(
            "reconciliation_duration_seconds",
            "Duration of reconciliation runs in seconds",
            ["table_name"],
            buckets=(1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600),
            registry=self.registry,
        )

        self.reconciliation_last_run_timestamp = Gauge(
            "reconciliation_last_run_timestamp",
            "Timestamp of last reconciliation run",
            ["table_name"],
            registry=self.registry,
        )

        # Discrepancy metrics
        self.row_count_mismatch_total = Counter(
            "reconciliation_row_count_mismatch_total",
            "Total number of row count mismatches detected",
            ["table_name"],
            registry=self.registry,
        )

        self.row_count_difference = Gauge(
            "reconciliation_row_count_difference",
            "Difference in row counts (source - target)",
            ["table_name"],
            registry=self.registry,
        )

        self.checksum_mismatch_total = Counter(
            "reconciliation_checksum_mismatch_total",
            "Total number of checksum mismatches detected",
            ["table_name"],
            registry=self.registry,
        )

        # Performance metrics
        self.rows_compared_total = Counter(
            "reconciliation_rows_compared_total",
            "Total number of rows compared",
            ["table_name"],
            registry=self.registry,
        )

        self.comparison_rate = Gauge(
            "reconciliation_comparison_rate_rows_per_second",
            "Rate of row comparison (rows/second)",
            ["table_name"],
            registry=self.registry,
        )

    def record_reconciliation_run(
        self,
        table_name: str,
        success: bool,
        duration: float,
        rows_compared: Optional[int] = None,
    ) -> None:
        """
        Record a reconciliation run

        Args:
            table_name: Name of the table reconciled
            success: Whether the run completed successfully
            duration: Duration in seconds
            rows_compared: Number of rows compared (optional)
        """
        status = "success" if success else "failed"

        self.reconciliation_runs_total.labels(
            table_name=table_name,
            status=status,
        ).inc()

        self.reconciliation_duration_seconds.labels(
            table_name=table_name,
        ).observe(duration)

        self.reconciliation_last_run_timestamp.labels(
            table_name=table_name,
        ).set(time.time())

        if rows_compared is not None:
            self.rows_compared_total.labels(
                table_name=table_name,
            ).inc(rows_compared)

            if duration > 0:
                rate = rows_compared / duration
                self.comparison_rate.labels(
                    table_name=table_name,
                ).set(rate)

        logger.info(
            f"Recorded reconciliation run: table={table_name}, "
            f"status={status}, duration={duration:.2f}s, "
            f"rows={rows_compared or 'N/A'}"
        )

    def record_row_count_mismatch(
        self,
        table_name: str,
        source_count: int,
        target_count: int,
    ) -> None:
        """
        Record a row count mismatch

        Args:
            table_name: Name of the table
            source_count: Row count in source database
            target_count: Row count in target database
        """
        self.row_count_mismatch_total.labels(
            table_name=table_name,
        ).inc()

        difference = source_count - target_count
        self.row_count_difference.labels(
            table_name=table_name,
        ).set(difference)

        logger.warning(
            f"Row count mismatch detected: table={table_name}, "
            f"source={source_count}, target={target_count}, "
            f"difference={difference}"
        )

    def record_checksum_mismatch(
        self,
        table_name: str,
    ) -> None:
        """
        Record a checksum mismatch

        Args:
            table_name: Name of the table
        """
        self.checksum_mismatch_total.labels(
            table_name=table_name,
        ).inc()

        logger.warning(f"Checksum mismatch detected: table={table_name}")

    def reset_difference(self, table_name: str) -> None:
        """
        Reset row count difference to zero

        Args:
            table_name: Name of the table
        """
        self.row_count_difference.labels(
            table_name=table_name,
        ).set(0)


class ConnectorMetrics:
    """
    Metrics for Kafka Connect connector operations

    Tracks connector deployments, failures, and operations.
    """

    def __init__(self, registry: Optional[CollectorRegistry] = None):
        """
        Initialize connector metrics

        Args:
            registry: Custom Prometheus registry (default: global REGISTRY)
        """
        self.registry = registry or REGISTRY

        # Deployment metrics
        self.connector_deployments_total = Counter(
            "connector_deployments_total",
            "Total number of connector deployments",
            ["connector_name", "status"],
            registry=self.registry,
        )

        self.connector_deployment_duration_seconds = Histogram(
            "connector_deployment_duration_seconds",
            "Duration of connector deployments in seconds",
            ["connector_name"],
            buckets=(1, 5, 10, 30, 60, 120, 300),
            registry=self.registry,
        )

        # Operation metrics
        self.connector_operations_total = Counter(
            "connector_operations_total",
            "Total number of connector operations",
            ["connector_name", "operation", "status"],
            registry=self.registry,
        )

        # State metrics
        self.connector_state = Gauge(
            "connector_state",
            "Current connector state (0=stopped, 1=running, 2=paused, 3=failed)",
            ["connector_name"],
            registry=self.registry,
        )

        self.connector_tasks_total = Gauge(
            "connector_tasks_total",
            "Total number of tasks for connector",
            ["connector_name"],
            registry=self.registry,
        )

        self.connector_tasks_running = Gauge(
            "connector_tasks_running",
            "Number of running tasks for connector",
            ["connector_name"],
            registry=self.registry,
        )

        self.connector_tasks_failed = Gauge(
            "connector_tasks_failed",
            "Number of failed tasks for connector",
            ["connector_name"],
            registry=self.registry,
        )

    def record_deployment(
        self,
        connector_name: str,
        success: bool,
        duration: float,
    ) -> None:
        """
        Record a connector deployment

        Args:
            connector_name: Name of the connector
            success: Whether deployment succeeded
            duration: Duration in seconds
        """
        status = "success" if success else "failed"

        self.connector_deployments_total.labels(
            connector_name=connector_name,
            status=status,
        ).inc()

        self.connector_deployment_duration_seconds.labels(
            connector_name=connector_name,
        ).observe(duration)

        logger.info(
            f"Recorded connector deployment: connector={connector_name}, "
            f"status={status}, duration={duration:.2f}s"
        )

    def record_operation(
        self,
        connector_name: str,
        operation: str,
        success: bool,
    ) -> None:
        """
        Record a connector operation (pause, resume, restart, etc.)

        Args:
            connector_name: Name of the connector
            operation: Operation type (pause, resume, restart, delete, etc.)
            success: Whether operation succeeded
        """
        status = "success" if success else "failed"

        self.connector_operations_total.labels(
            connector_name=connector_name,
            operation=operation,
            status=status,
        ).inc()

        logger.info(
            f"Recorded connector operation: connector={connector_name}, "
            f"operation={operation}, status={status}"
        )

    def update_connector_state(
        self,
        connector_name: str,
        state: str,
        tasks_total: int = 0,
        tasks_running: int = 0,
        tasks_failed: int = 0,
    ) -> None:
        """
        Update connector state metrics

        Args:
            connector_name: Name of the connector
            state: Connector state (RUNNING, PAUSED, FAILED, STOPPED, etc.)
            tasks_total: Total number of tasks
            tasks_running: Number of running tasks
            tasks_failed: Number of failed tasks
        """
        # Map state to numeric value
        state_map = {
            "STOPPED": 0,
            "RUNNING": 1,
            "PAUSED": 2,
            "FAILED": 3,
        }
        state_value = state_map.get(state.upper(), -1)

        self.connector_state.labels(
            connector_name=connector_name,
        ).set(state_value)

        self.connector_tasks_total.labels(
            connector_name=connector_name,
        ).set(tasks_total)

        self.connector_tasks_running.labels(
            connector_name=connector_name,
        ).set(tasks_running)

        self.connector_tasks_failed.labels(
            connector_name=connector_name,
        ).set(tasks_failed)

        logger.debug(
            f"Updated connector state: connector={connector_name}, "
            f"state={state}, tasks={tasks_running}/{tasks_total}, "
            f"failed={tasks_failed}"
        )


class VaultMetrics:
    """
    Metrics for HashiCorp Vault operations

    Tracks credential retrievals and Vault health.
    """

    def __init__(self, registry: Optional[CollectorRegistry] = None):
        """
        Initialize Vault metrics

        Args:
            registry: Custom Prometheus registry (default: global REGISTRY)
        """
        self.registry = registry or REGISTRY

        # Credential retrieval metrics
        self.credential_retrievals_total = Counter(
            "vault_credential_retrievals_total",
            "Total number of credential retrievals from Vault",
            ["secret_path", "status"],
            registry=self.registry,
        )

        self.credential_retrieval_duration_seconds = Histogram(
            "vault_credential_retrieval_duration_seconds",
            "Duration of credential retrievals in seconds",
            ["secret_path"],
            buckets=(0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10),
            registry=self.registry,
        )

        # Health check metrics
        self.vault_health_check_total = Counter(
            "vault_health_check_total",
            "Total number of Vault health checks",
            ["status"],
            registry=self.registry,
        )

        self.vault_is_sealed = Gauge(
            "vault_is_sealed",
            "Whether Vault is sealed (1=sealed, 0=unsealed)",
            registry=self.registry,
        )

    def record_credential_retrieval(
        self,
        secret_path: str,
        success: bool,
        duration: float,
    ) -> None:
        """
        Record a credential retrieval

        Args:
            secret_path: Path to the secret in Vault
            success: Whether retrieval succeeded
            duration: Duration in seconds
        """
        status = "success" if success else "failed"

        self.credential_retrievals_total.labels(
            secret_path=secret_path,
            status=status,
        ).inc()

        self.credential_retrieval_duration_seconds.labels(
            secret_path=secret_path,
        ).observe(duration)

        logger.debug(
            f"Recorded credential retrieval: path={secret_path}, "
            f"status={status}, duration={duration:.3f}s"
        )

    def record_health_check(
        self,
        is_healthy: bool,
        is_sealed: bool,
    ) -> None:
        """
        Record a Vault health check

        Args:
            is_healthy: Whether Vault is healthy
            is_sealed: Whether Vault is sealed
        """
        status = "healthy" if is_healthy else "unhealthy"

        self.vault_health_check_total.labels(
            status=status,
        ).inc()

        self.vault_is_sealed.set(1 if is_sealed else 0)

        logger.debug(
            f"Recorded Vault health check: status={status}, "
            f"sealed={is_sealed}"
        )


class ApplicationInfo:
    """
    Application metadata and version information

    Exposes application version, build info, and environment.
    """

    def __init__(
        self,
        app_name: str = "sqlserver-pg-cdc",
        version: str = "1.0.0",
        registry: Optional[CollectorRegistry] = None,
    ):
        """
        Initialize application info metrics

        Args:
            app_name: Application name
            version: Application version
            registry: Custom Prometheus registry (default: global REGISTRY)
        """
        self.registry = registry or REGISTRY

        # Application info
        self.info = Info(
            "application",
            "Application metadata",
            registry=self.registry,
        )

        self.info.info({
            "name": app_name,
            "version": version,
        })

        # Uptime metrics
        self._start_time = time.time()

        self.uptime_seconds = Gauge(
            "application_uptime_seconds",
            "Application uptime in seconds",
            registry=self.registry,
        )

    def update_uptime(self) -> None:
        """Update the uptime metric"""
        uptime = time.time() - self._start_time
        self.uptime_seconds.set(uptime)

    def get_uptime(self) -> float:
        """Get current uptime in seconds"""
        return time.time() - self._start_time


# Convenience function to initialize all metrics
def initialize_metrics(
    port: int = 9091,
    registry: Optional[CollectorRegistry] = None,
) -> Dict[str, Any]:
    """
    Initialize all metrics and start the metrics server

    Args:
        port: Port to expose metrics on (default: 9091)
        registry: Custom Prometheus registry (default: global REGISTRY)

    Returns:
        Dictionary containing all metrics objects:
        - publisher: MetricsPublisher
        - reconciliation: ReconciliationMetrics
        - connector: ConnectorMetrics
        - vault: VaultMetrics
        - app_info: ApplicationInfo
    """
    logger.info(f"Initializing metrics on port {port}")

    publisher = MetricsPublisher(port=port, registry=registry)
    publisher.start()

    return {
        "publisher": publisher,
        "reconciliation": ReconciliationMetrics(registry=registry),
        "connector": ConnectorMetrics(registry=registry),
        "vault": VaultMetrics(registry=registry),
        "app_info": ApplicationInfo(registry=registry),
    }


# Example usage
if __name__ == "__main__":
    import sys

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Initialize metrics
    metrics = initialize_metrics(port=9091)

    logger.info("Metrics server running. Press Ctrl+C to exit.")
    logger.info("Visit http://localhost:9091/metrics to view metrics")

    # Keep alive
    try:
        while True:
            # Update uptime
            metrics["app_info"].update_uptime()
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Shutting down metrics server")
        sys.exit(0)
