"""
Reconciliation-specific tracing decorators and utilities.

Provides specialized decorators and context managers for tracing
reconciliation operations with appropriate attributes.
"""

import logging
from functools import wraps

from .context import add_span_attributes, add_span_event, trace_operation
from .database import trace_database_query

logger = logging.getLogger(__name__)


def trace_reconciliation(func):
    """
    Decorator for tracing reconciliation functions.

    Automatically captures table names, counts, and results.

    Example:
        >>> @trace_reconciliation
        ... def reconcile_table(source_cursor, target_cursor, source_table, target_table):
        ...     # Implementation
        ...     pass
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract table names from kwargs or args
        source_table = kwargs.get('source_table', '')
        target_table = kwargs.get('target_table', '')

        if not source_table and len(args) >= 3:
            source_table = str(args[2])
        if not target_table and len(args) >= 4:
            target_table = str(args[3])

        with trace_operation(
            "reconcile_table",
            component="reconciliation",
            source_table=source_table,
            target_table=target_table
        ) as span:
            add_span_event("reconciliation_started")

            try:
                result = func(*args, **kwargs)

                # Add result attributes
                if isinstance(result, dict):
                    add_span_attributes(
                        source_count=result.get('source_count', 'unknown'),
                        target_count=result.get('target_count', 'unknown'),
                        match=str(result.get('match', False)),
                        difference=result.get('difference', 0)
                    )

                    if 'checksum_match' in result:
                        add_span_attributes(
                            checksum_match=str(result.get('checksum_match'))
                        )

                add_span_event("reconciliation_completed", status="success")
                return result

            except Exception as e:
                add_span_event("reconciliation_failed", error=str(e))
                raise

    return wrapper


def trace_checksum_calculation(func):
    """
    Decorator for tracing checksum calculation functions.

    Captures table name and row counts.

    Example:
        >>> @trace_checksum_calculation
        ... def calculate_checksum(cursor, table_name, columns=None):
        ...     # Implementation
        ...     pass
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract table name
        table_name = kwargs.get('table_name', '')
        if not table_name and len(args) >= 2:
            table_name = str(args[1])

        with trace_operation(
            "calculate_checksum",
            component="reconciliation",
            table=table_name
        ) as span:
            add_span_event("checksum_started")

            result = func(*args, **kwargs)

            add_span_attributes(checksum_length=len(result) if result else 0)
            add_span_event("checksum_completed")

            return result

    return wrapper


def trace_row_count(func):
    """
    Decorator for tracing row count queries.

    Example:
        >>> @trace_row_count
        ... def get_row_count(cursor, table_name):
        ...     # Implementation
        ...     pass
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract table name
        table_name = kwargs.get('table_name', '')
        if not table_name and len(args) >= 2:
            table_name = str(args[1])

        with trace_database_query("COUNT", table_name):
            add_span_event("count_query_started")

            result = func(*args, **kwargs)

            add_span_attributes(row_count=result if isinstance(result, int) else 0)
            add_span_event("count_query_completed")

            return result

    return wrapper


def trace_batch_operation(operation_type: str):
    """
    Decorator factory for tracing batch operations.

    Args:
        operation_type: Type of batch operation (e.g., "parallel_reconciliation")

    Example:
        >>> @trace_batch_operation("parallel_reconciliation")
        ... def reconcile_tables_parallel(tables):
        ...     # Implementation
        ...     pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Extract table list if available
            tables = kwargs.get('tables', [])
            if not tables and len(args) >= 1 and isinstance(args[0], list):
                tables = args[0]

            with trace_operation(
                operation_type,
                component="reconciliation",
                table_count=len(tables)
            ) as span:
                add_span_event("batch_started", table_count=len(tables))

                result = func(*args, **kwargs)

                # Extract success/failure counts if result is dict
                if isinstance(result, dict):
                    add_span_attributes(
                        successful=result.get('successful', 0),
                        failed=result.get('failed', 0),
                        total=result.get('total_tables', len(tables))
                    )

                add_span_event("batch_completed")
                return result

        return wrapper
    return decorator


# Context managers for manual tracing
class ReconciliationSpan:
    """
    Context manager for manually creating reconciliation spans.

    Provides fine-grained control over span lifecycle.

    Example:
        >>> with ReconciliationSpan("customers") as span:
        ...     span.add_count("source", 1000)
        ...     span.add_count("target", 1000)
        ...     span.set_match(True)
    """

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.span_context = None
        self.span = None

    def __enter__(self):
        self.span_context = trace_operation(
            "reconcile_table",
            component="reconciliation",
            table=self.table_name
        )
        self.span = self.span_context.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span_context:
            return self.span_context.__exit__(exc_type, exc_val, exc_tb)

    def add_count(self, source: str, count: int):
        """Add row count attribute."""
        add_span_attributes(**{f"{source}_count": count})

    def set_match(self, match: bool):
        """Set match status."""
        add_span_attributes(match=str(match))

    def add_checksum(self, source: str, checksum: str):
        """Add checksum attribute."""
        add_span_attributes(**{f"{source}_checksum": checksum[:16]})

    def mark_discrepancy(self, discrepancy_type: str, count: int = 0):
        """Mark a discrepancy found."""
        add_span_event(
            "discrepancy_found",
            type=discrepancy_type,
            count=count
        )


# Utility function to add tracing to existing scheduler
def wrap_reconciliation_job(job_func):
    """
    Wrap an existing reconciliation job function with tracing.

    Args:
        job_func: Original job function

    Returns:
        Wrapped function with tracing

    Example:
        >>> original_job = reconcile_job_wrapper
        >>> traced_job = wrap_reconciliation_job(original_job)
    """
    @wraps(job_func)
    def wrapper(*args, **kwargs):
        with trace_operation(
            "reconciliation_job",
            component="scheduler"
        ) as span:
            try:
                result = job_func(*args, **kwargs)
                add_span_attributes(job_status="success")
                return result
            except Exception as e:
                add_span_attributes(job_status="failed")
                add_span_event("job_failed", error=str(e))
                raise

    return wrapper
