"""
Metrics for data reconciliation operations.

Tracks reconciliation runs, discrepancies, and performance metrics
for monitoring data consistency validation processes.
"""

import logging
import time
from typing import Optional

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    CollectorRegistry,
    REGISTRY,
)

logger = logging.getLogger(__name__)


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
