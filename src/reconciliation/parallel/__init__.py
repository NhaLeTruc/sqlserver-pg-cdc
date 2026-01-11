"""
Parallel table reconciliation for improved performance.

Processes multiple tables concurrently using ThreadPoolExecutor to reduce
total reconciliation time by 3-5x.

Features:
- Configurable worker count
- Per-table timeout handling
- Error isolation (failures don't stop other tables)
- Result aggregation with detailed statistics
- Prometheus metrics for parallel operations
- Distributed tracing integration
- Resource-aware scheduling

Designed for tables up to 10M rows with safe concurrent database access.
"""

from .helpers import (
    create_parallel_reconcile_job,
    estimate_optimal_workers,
    get_parallel_reconciliation_stats,
)
from .reconciler import ParallelReconciler

__all__ = [
    'ParallelReconciler',
    'create_parallel_reconcile_job',
    'estimate_optimal_workers',
    'get_parallel_reconciliation_stats',
]
