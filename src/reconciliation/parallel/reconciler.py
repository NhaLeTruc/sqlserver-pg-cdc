"""
Parallel table reconciliation engine.

This module provides the ParallelReconciler class for processing multiple tables
concurrently using ThreadPoolExecutor.
"""

import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from datetime import UTC, datetime
from typing import Any

from opentelemetry import trace

from utils.tracing import trace_operation

from .metrics import (
    PARALLEL_ACTIVE_WORKERS,
    PARALLEL_QUEUE_SIZE,
    PARALLEL_RECONCILIATION_TIME,
    PARALLEL_TABLE_TIME,
    PARALLEL_TABLES_PROCESSED,
)

logger = logging.getLogger(__name__)


class CancellationError(Exception):
    """Raised when a task is cancelled via cancellation token."""

    pass


class ParallelReconciler:
    """
    Orchestrates parallel reconciliation of multiple tables.

    Uses ThreadPoolExecutor to process tables concurrently while maintaining
    safe database access and error isolation.
    """

    def __init__(
        self,
        max_workers: int = 4,
        timeout_per_table: int = 3600,
        fail_fast: bool = False,
    ):
        """
        Initialize parallel reconciler.

        Args:
            max_workers: Maximum concurrent workers (default: 4)
            timeout_per_table: Timeout in seconds for each table (default: 3600)
            fail_fast: If True, stop on first error (default: False)
        """
        self.max_workers = max_workers
        self.timeout_per_table = timeout_per_table
        self.fail_fast = fail_fast
        # CONC-5: Lock for thread-safe metric updates
        self._metrics_lock = threading.Lock()
        # CONC-6: Cancellation tokens for worker threads
        self._cancellation_tokens: dict[str, threading.Event] = {}

        logger.info(
            f"ParallelReconciler initialized: "
            f"max_workers={max_workers}, "
            f"timeout_per_table={timeout_per_table}s, "
            f"fail_fast={fail_fast}"
        )

    def reconcile_tables(
        self,
        tables: list[str],
        reconcile_func: Callable,
        **reconcile_kwargs,
    ) -> dict[str, Any]:
        """
        Reconcile multiple tables in parallel.

        Args:
            tables: List of table names to reconcile
            reconcile_func: Function to reconcile a single table
                           Must accept 'table' parameter and return dict
            **reconcile_kwargs: Additional keyword arguments for reconcile_func

        Returns:
            Aggregated results dictionary with structure:
            {
                'total_tables': int,
                'successful': int,
                'failed': int,
                'timeout': int,
                'results': List[Dict],
                'errors': List[Dict],
                'duration_seconds': float,
                'timestamp': str (ISO format),
                'max_workers': int
            }

        Example:
            >>> reconciler = ParallelReconciler(max_workers=4)
            >>> results = reconciler.reconcile_tables(
            ...     tables=['users', 'orders', 'products'],
            ...     reconcile_func=my_reconcile_function,
            ...     validate_checksum=True
            ... )
            >>> print(f"Reconciled {results['successful']}/{results['total_tables']} tables")
        """
        with trace_operation(
            "parallel_reconcile_tables",
            kind=trace.SpanKind.INTERNAL,
            table_count=len(tables),
            max_workers=self.max_workers,
        ):
            with PARALLEL_RECONCILIATION_TIME.labels(
                worker_count=self.max_workers
            ).time():
                start_time = datetime.now(UTC)

                # Initialize results
                results = {
                    "total_tables": len(tables),
                    "successful": 0,
                    "failed": 0,
                    "timeout": 0,
                    "results": [],
                    "errors": [],
                    "max_workers": self.max_workers,
                }

                if not tables:
                    logger.warning("No tables to reconcile")
                    results["duration_seconds"] = 0
                    results["timestamp"] = datetime.now(UTC).isoformat()
                    return results

                logger.info(
                    f"Starting parallel reconciliation of {len(tables)} tables "
                    f"with {self.max_workers} workers"
                )

                # CONC-5: Update queue size metric with lock for atomicity
                with self._metrics_lock:
                    PARALLEL_QUEUE_SIZE.set(len(tables))

                # CONC-6: Create cancellation tokens for each table
                self._cancellation_tokens = {
                    table: threading.Event() for table in tables
                }

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submit all tasks with their cancellation tokens
                    future_to_table = {
                        executor.submit(
                            self._reconcile_table_wrapper,
                            table,
                            reconcile_func,
                            self._cancellation_tokens[table],
                            **reconcile_kwargs,
                        ): table
                        for table in tables
                    }

                    # Track active workers
                    with self._metrics_lock:
                        PARALLEL_ACTIVE_WORKERS.set(min(self.max_workers, len(tables)))

                    # Collect results as they complete
                    completed_count = 0
                    for future in as_completed(future_to_table):
                        table = future_to_table[future]
                        completed_count += 1

                        # CONC-5: Update queue and worker metrics atomically
                        with self._metrics_lock:
                            PARALLEL_QUEUE_SIZE.set(len(tables) - completed_count)
                            PARALLEL_ACTIVE_WORKERS.set(
                                min(self.max_workers, len(tables) - completed_count)
                            )

                        try:
                            result = future.result(timeout=self.timeout_per_table)
                            results["results"].append(result)
                            results["successful"] += 1
                            PARALLEL_TABLES_PROCESSED.labels(status="success").inc()

                            logger.info(
                                f"✓ Table {table} reconciled successfully "
                                f"({completed_count}/{len(tables)})"
                            )

                        except TimeoutError:
                            # CONC-6: Signal cancellation to the worker thread
                            if table in self._cancellation_tokens:
                                self._cancellation_tokens[table].set()
                                logger.debug(f"Signaled cancellation for table {table}")

                            results["timeout"] += 1
                            results["errors"].append(
                                {
                                    "table": table,
                                    "error": f"Timeout after {self.timeout_per_table}s",
                                    "type": "TimeoutError",
                                }
                            )
                            PARALLEL_TABLES_PROCESSED.labels(status="timeout").inc()

                            logger.error(
                                f"✗ Table {table} reconciliation timeout "
                                f"after {self.timeout_per_table}s "
                                f"({completed_count}/{len(tables)})"
                            )

                            if self.fail_fast:
                                logger.warning("Fail-fast enabled, canceling remaining tasks")
                                # Signal cancellation for all remaining tasks
                                for t, token in self._cancellation_tokens.items():
                                    token.set()
                                break

                        except Exception as e:
                            results["failed"] += 1
                            results["errors"].append(
                                {
                                    "table": table,
                                    "error": str(e),
                                    "type": type(e).__name__,
                                }
                            )
                            PARALLEL_TABLES_PROCESSED.labels(status="failed").inc()

                            logger.error(
                                f"✗ Table {table} reconciliation failed: {e} "
                                f"({completed_count}/{len(tables)})",
                                exc_info=True,
                            )

                            if self.fail_fast:
                                logger.warning("Fail-fast enabled, canceling remaining tasks")
                                # Signal cancellation for all remaining tasks
                                for t, token in self._cancellation_tokens.items():
                                    token.set()
                                break

                # Clear cancellation tokens
                self._cancellation_tokens.clear()

                # Reset metrics atomically
                with self._metrics_lock:
                    PARALLEL_ACTIVE_WORKERS.set(0)
                    PARALLEL_QUEUE_SIZE.set(0)

                # Finalize results
                end_time = datetime.now(UTC)
                results["duration_seconds"] = (end_time - start_time).total_seconds()
                results["timestamp"] = end_time.isoformat()

                # Log summary
                logger.info(
                    f"Parallel reconciliation complete: "
                    f"{results['successful']} successful, "
                    f"{results['failed']} failed, "
                    f"{results['timeout']} timeout "
                    f"out of {results['total_tables']} tables "
                    f"in {results['duration_seconds']:.2f}s"
                )

                return results

    def _reconcile_table_wrapper(
        self,
        table: str,
        reconcile_func: Callable,
        cancellation_token: threading.Event,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Wrapper to add table context, timing, and error handling.

        Args:
            table: Table name
            reconcile_func: Function to reconcile the table
            cancellation_token: Event to signal cancellation
            **kwargs: Additional arguments for reconcile_func

        Returns:
            Result dictionary with 'table', 'duration_seconds', and function results
        """
        with trace_operation(
            "parallel_reconcile_single_table",
            kind=trace.SpanKind.INTERNAL,
            table=table,
        ):
            start_time = datetime.now(UTC)

            try:
                # CONC-6: Check cancellation before starting
                if cancellation_token.is_set():
                    raise CancellationError(f"Task cancelled before starting for table {table}")

                logger.debug(f"Starting reconciliation for table: {table}")

                # Call the reconciliation function with cancellation token if supported
                # This allows the reconcile_func to periodically check for cancellation
                # For backward compatibility, try without cancellation_token if function doesn't accept it
                try:
                    result = reconcile_func(
                        table=table,
                        cancellation_token=cancellation_token,
                        **kwargs,
                    )
                except TypeError as e:
                    if "cancellation_token" in str(e):
                        # Function doesn't accept cancellation_token, call without it
                        result = reconcile_func(table=table, **kwargs)
                    else:
                        raise

                # Ensure result is a dictionary
                if not isinstance(result, dict):
                    result = {"success": True, "data": result}

                # Add metadata
                result["table"] = table
                duration = (datetime.now(UTC) - start_time).total_seconds()
                result["duration_seconds"] = duration

                # Track per-table timing
                PARALLEL_TABLE_TIME.labels(table=table).observe(duration)

                logger.debug(
                    f"Completed reconciliation for table {table} in {duration:.2f}s"
                )

                return result

            except Exception as e:
                logger.error(
                    f"Error reconciling table {table}: {e}",
                    exc_info=True,
                )
                raise
