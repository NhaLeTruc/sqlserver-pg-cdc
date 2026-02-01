"""
Parallel table reconciliation for improved performance.

BUG-7: This file now re-exports from the parallel/ submodule for backward compatibility.
New code should import directly from src.reconciliation.parallel instead.

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

# Re-export everything from the parallel submodule for backward compatibility
from src.reconciliation.parallel import (
    ParallelReconciler,
    create_parallel_reconcile_job,
    estimate_optimal_workers,
    get_parallel_reconciliation_stats,
)

__all__ = [
    'ParallelReconciler',
    'create_parallel_reconcile_job',
    'estimate_optimal_workers',
    'get_parallel_reconciliation_stats',
]
