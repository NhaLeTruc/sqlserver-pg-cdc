"""
Metrics for CDC pipeline operations.

Tracks Kafka connector operations and Vault credential management
for monitoring the data pipeline infrastructure.
"""

import logging
from typing import Optional

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    CollectorRegistry,
    REGISTRY,
)

logger = logging.getLogger(__name__)


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
