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
from typing import Any, Dict, Optional

from prometheus_client import CollectorRegistry

from .publisher import MetricsPublisher, ApplicationInfo
from .reconciliation import ReconciliationMetrics
from .pipeline import ConnectorMetrics, VaultMetrics

logger = logging.getLogger(__name__)


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


__all__ = [
    "MetricsPublisher",
    "ReconciliationMetrics",
    "ConnectorMetrics",
    "VaultMetrics",
    "ApplicationInfo",
    "initialize_metrics",
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
