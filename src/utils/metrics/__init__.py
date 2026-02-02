"""
Custom metrics publishing to Prometheus

This module provides utilities for publishing custom application metrics
to Prometheus for monitoring and alerting.

Usage:
    from utils.metrics import MetricsPublisher, ReconciliationMetrics

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
from typing import Any, Callable, TypeVar

from prometheus_client import CollectorRegistry, REGISTRY

from .pipeline import ConnectorMetrics, VaultMetrics
from .publisher import ApplicationInfo, MetricsPublisher
from .reconciliation import ReconciliationMetrics

logger = logging.getLogger(__name__)

# Type variable for metric types
T = TypeVar("T")


def get_or_create_metric(
    metric_factory: Callable[[], T],
    metric_name: str,
    registry: CollectorRegistry = REGISTRY,
) -> T:
    """
    INEFF-9: Utility function for safe metric registration.

    Creates a new metric or returns the existing one if already registered.
    Reduces duplicate try-except blocks throughout the codebase.

    Args:
        metric_factory: Callable that creates the metric (e.g., lambda: Counter(...))
        metric_name: Name of the metric for lookup if already registered
        registry: Prometheus registry to use (default: global REGISTRY)

    Returns:
        The metric instance (either newly created or existing)

    Example:
        REQUESTS_TOTAL = get_or_create_metric(
            lambda: Counter("requests_total", "Total requests", ["method"]),
            "requests_total"
        )
    """
    try:
        return metric_factory()
    except ValueError:
        # Metric already registered, get existing one
        existing = registry._names_to_collectors.get(metric_name)
        if existing is not None:
            return existing
        # If still not found, re-raise the original error
        raise


def initialize_metrics(
    port: int = 9091,
    registry: CollectorRegistry | None = None,
) -> dict[str, Any]:
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


__all__ = [
    "MetricsPublisher",
    "ReconciliationMetrics",
    "ConnectorMetrics",
    "VaultMetrics",
    "ApplicationInfo",
    "initialize_metrics",
    "get_or_create_metric",
]


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
