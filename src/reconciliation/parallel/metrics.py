"""
Prometheus metrics for parallel reconciliation operations.

This module defines metrics to track parallel reconciliation performance
and throughput.
"""

from prometheus_client import Counter, Gauge, Histogram

from utils.metrics import get_or_create_metric

# INEFF-9: Use utility function to reduce duplicate try-except blocks
PARALLEL_TABLES_PROCESSED = get_or_create_metric(
    lambda: Counter(
        "parallel_tables_processed_total",
        "Total tables processed in parallel reconciliation",
        ["status"],  # success, failed, timeout
    ),
    "parallel_tables_processed_total",
)

PARALLEL_RECONCILIATION_TIME = get_or_create_metric(
    lambda: Histogram(
        "parallel_reconciliation_seconds",
        "Total time for parallel reconciliation job",
        ["worker_count"],
        buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600],
    ),
    "parallel_reconciliation_seconds",
)

PARALLEL_TABLE_TIME = get_or_create_metric(
    lambda: Histogram(
        "parallel_table_reconciliation_seconds",
        "Time to reconcile individual table in parallel job",
        ["table"],
        buckets=[0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600],
    ),
    "parallel_table_reconciliation_seconds",
)

PARALLEL_ACTIVE_WORKERS = get_or_create_metric(
    lambda: Gauge(
        "parallel_active_workers",
        "Number of active worker threads processing tables",
    ),
    "parallel_active_workers",
)

PARALLEL_QUEUE_SIZE = get_or_create_metric(
    lambda: Gauge(
        "parallel_queue_size",
        "Number of tables waiting to be processed",
    ),
    "parallel_queue_size",
)
