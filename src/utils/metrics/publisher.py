"""
Metrics publisher for Prometheus HTTP server.

Provides base functionality for starting and managing the Prometheus
metrics HTTP server that exposes metrics on the /metrics endpoint.
"""

import logging
import time
from typing import Optional

from prometheus_client import (
    start_http_server,
    Gauge,
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
                logger.error(
                    f"CRITICAL: Port {self.port} already in use. Metrics server cannot start. "
                    f"This will prevent monitoring. Check for conflicting processes or change the port."
                )
                raise RuntimeError(
                    f"Metrics server port {self.port} is already in use. "
                    f"Cannot start metrics collection. Please stop conflicting process or use a different port."
                ) from e
            else:
                raise

    def is_started(self) -> bool:
        """Check if metrics server is running"""
        return self._server_started


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
