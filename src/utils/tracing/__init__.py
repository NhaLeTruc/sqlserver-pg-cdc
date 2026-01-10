"""
Distributed tracing using OpenTelemetry and Jaeger.

Instruments:
- Database queries (PostgreSQL, SQL Server)
- Reconciliation operations
- HTTP requests
- Custom application spans

Provides end-to-end visibility across the CDC pipeline.
"""

from .context import add_span_attributes, add_span_event, trace_operation
from .database import trace_database_query, trace_http_request
from .decorators import trace_function
from .reconciliation import (
    ReconciliationSpan,
    trace_batch_operation,
    trace_checksum_calculation,
    trace_reconciliation,
    trace_row_count,
    wrap_reconciliation_job,
)
from .tracer import (
    get_tracer,
    initialize_tracing,
    instrument_psycopg2,
    instrument_requests,
    setup_auto_instrumentation,
    shutdown_tracing,
)

__all__ = [
    "initialize_tracing",
    "get_tracer",
    "shutdown_tracing",
    "trace_operation",
    "trace_function",
    "add_span_attributes",
    "add_span_event",
    "trace_database_query",
    "trace_http_request",
    "trace_reconciliation",
    "trace_checksum_calculation",
    "trace_row_count",
    "trace_batch_operation",
    "ReconciliationSpan",
    "wrap_reconciliation_job",
    "instrument_psycopg2",
    "instrument_requests",
    "setup_auto_instrumentation",
]


# Example usage and integration points
if __name__ == "__main__":
    import time

    # Example: Initialize tracing
    tracer = initialize_tracing(
        service_name="cdc-example",
        otlp_endpoint="localhost:4317",
        console_export=True
    )

    # Example: Trace an operation
    with trace_operation("example_operation", user="admin") as span:
        print("Doing work...")
        span.set_attribute("work.items", "42")
        add_span_event("work_started")

        # Simulate work
        time.sleep(0.1)

        add_span_event("work_completed")

    # Shutdown
    shutdown_tracing()
