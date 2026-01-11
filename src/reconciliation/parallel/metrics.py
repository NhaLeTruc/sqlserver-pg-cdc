"""
Prometheus metrics for parallel reconciliation operations.

This module defines metrics to track parallel reconciliation performance
and throughput.
"""

from prometheus_client import REGISTRY, Counter, Gauge, Histogram

# Metrics
try:
    PARALLEL_TABLES_PROCESSED = Counter(
        "parallel_tables_processed_total",
        "Total tables processed in parallel reconciliation",
        ["status"],  # success, failed, timeout
        registry=REGISTRY
    )
except ValueError:
    # Metric already registered, get existing one
    PARALLEL_TABLES_PROCESSED = REGISTRY._names_to_collectors.get("parallel_tables_processed_total")

try:
    PARALLEL_RECONCILIATION_TIME = Histogram(
        "parallel_reconciliation_seconds",
        "Total time for parallel reconciliation job",
        ["worker_count"],
        buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600],
        registry=REGISTRY
    )
except ValueError:
    PARALLEL_RECONCILIATION_TIME = REGISTRY._names_to_collectors.get("parallel_reconciliation_seconds")

try:
    PARALLEL_TABLE_TIME = Histogram(
        "parallel_table_reconciliation_seconds",
        "Time to reconcile individual table in parallel job",
        ["table"],
        buckets=[0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600],
        registry=REGISTRY
    )
except ValueError:
    PARALLEL_TABLE_TIME = REGISTRY._names_to_collectors.get("parallel_table_reconciliation_seconds")

try:
    PARALLEL_ACTIVE_WORKERS = Gauge(
        "parallel_active_workers",
        "Number of active worker threads processing tables",
        registry=REGISTRY
    )
except ValueError:
    PARALLEL_ACTIVE_WORKERS = REGISTRY._names_to_collectors.get("parallel_active_workers")

try:
    PARALLEL_QUEUE_SIZE = Gauge(
        "parallel_queue_size",
        "Number of tables waiting to be processed",
        registry=REGISTRY
    )
except ValueError:
    PARALLEL_QUEUE_SIZE = REGISTRY._names_to_collectors.get("parallel_queue_size")
