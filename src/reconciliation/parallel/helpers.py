"""
Helper functions and utilities for parallel reconciliation.

This module provides factory functions, worker estimation, and statistics
gathering for parallel reconciliation operations.
"""

import logging
from typing import Any, Callable, Dict, List

from prometheus_client import REGISTRY

from .reconciler import ParallelReconciler

logger = logging.getLogger(__name__)


def create_parallel_reconcile_job(
    reconcile_func: Callable,
    max_workers: int = 4,
    timeout_per_table: int = 3600,
    fail_fast: bool = False,
) -> Callable:
    """
    Factory function to create a parallel reconciliation job.

    Args:
        reconcile_func: Function to reconcile a single table
        max_workers: Maximum concurrent workers
        timeout_per_table: Timeout per table in seconds
        fail_fast: Stop on first error

    Returns:
        Callable that accepts tables list and kwargs

    Example:
        >>> def my_reconcile(table, validate_checksum=True):
        ...     # Reconcile single table
        ...     return {"table": table, "match": True}
        ...
        >>> parallel_job = create_parallel_reconcile_job(
        ...     my_reconcile,
        ...     max_workers=4
        ... )
        >>> results = parallel_job(['users', 'orders'], validate_checksum=True)
    """
    reconciler = ParallelReconciler(
        max_workers=max_workers,
        timeout_per_table=timeout_per_table,
        fail_fast=fail_fast,
    )

    def parallel_job(tables: List[str], **kwargs) -> Dict[str, Any]:
        """Execute parallel reconciliation job."""
        return reconciler.reconcile_tables(
            tables=tables,
            reconcile_func=reconcile_func,
            **kwargs,
        )

    return parallel_job


def estimate_optimal_workers(
    table_count: int,
    avg_table_time_seconds: float = 60.0,
    total_time_budget_seconds: float = 300.0,
    max_workers: int = 10,
) -> int:
    """
    Estimate optimal number of workers based on workload.

    Args:
        table_count: Number of tables to process
        avg_table_time_seconds: Average time per table
        total_time_budget_seconds: Desired total completion time
        max_workers: Maximum workers allowed

    Returns:
        Recommended worker count

    Example:
        >>> # 20 tables, 60s each, want done in 5 minutes
        >>> workers = estimate_optimal_workers(20, 60, 300, 10)
        >>> print(f"Use {workers} workers")
    """
    if table_count == 0:
        return 1

    # Calculate workers needed to meet time budget
    total_work_seconds = table_count * avg_table_time_seconds
    workers_needed = int(total_work_seconds / total_time_budget_seconds) + 1

    # Constrain to reasonable values
    workers = min(workers_needed, max_workers, table_count)
    workers = max(workers, 1)

    logger.info(
        f"Estimated optimal workers: {workers} "
        f"(tables={table_count}, avg_time={avg_table_time_seconds}s, "
        f"budget={total_time_budget_seconds}s)"
    )

    return workers


def get_parallel_reconciliation_stats() -> Dict[str, Any]:
    """
    Get current parallel reconciliation statistics.

    Returns:
        Dictionary with current metrics

    Example:
        >>> stats = get_parallel_reconciliation_stats()
        >>> print(f"Active workers: {stats['active_workers']}")
    """
    stats = {
        "active_workers": REGISTRY.get_sample_value("parallel_active_workers") or 0,
        "queue_size": REGISTRY.get_sample_value("parallel_queue_size") or 0,
        "total_processed": {
            "success": REGISTRY.get_sample_value(
                "parallel_tables_processed_total", {"status": "success"}
            )
            or 0,
            "failed": REGISTRY.get_sample_value(
                "parallel_tables_processed_total", {"status": "failed"}
            )
            or 0,
            "timeout": REGISTRY.get_sample_value(
                "parallel_tables_processed_total", {"status": "timeout"}
            )
            or 0,
        },
    }

    return stats
