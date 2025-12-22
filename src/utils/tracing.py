"""
Distributed tracing using OpenTelemetry and Jaeger.

Instruments:
- Database queries (PostgreSQL, SQL Server)
- Reconciliation operations
- HTTP requests
- Custom application spans

Provides end-to-end visibility across the CDC pipeline.
"""

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from contextlib import contextmanager
from typing import Any, Dict, Optional
import logging
import os

logger = logging.getLogger(__name__)

# Global tracer instance
_tracer: Optional[trace.Tracer] = None
_is_initialized = False


def initialize_tracing(
    service_name: str = "sqlserver-pg-cdc",
    otlp_endpoint: Optional[str] = None,
    console_export: bool = False,
    sampling_rate: float = 1.0
) -> trace.Tracer:
    """
    Initialize distributed tracing with OpenTelemetry.

    Args:
        service_name: Name of the service for identification
        otlp_endpoint: OTLP collector endpoint (e.g., "localhost:4317")
        console_export: If True, also export traces to console (debug)
        sampling_rate: Sampling rate 0.0-1.0 (1.0 = trace everything)

    Returns:
        Configured tracer instance

    Example:
        >>> tracer = initialize_tracing(
        ...     service_name="cdc-reconciliation",
        ...     otlp_endpoint="localhost:4317"
        ... )
    """
    global _tracer, _is_initialized

    if _is_initialized:
        logger.warning("Tracing already initialized, returning existing tracer")
        return _tracer

    # Create resource with service name
    resource = Resource(attributes={
        SERVICE_NAME: service_name
    })

    # Set up tracer provider
    provider = TracerProvider(resource=resource)

    # Configure exporters
    exporters = []

    # OTLP exporter (Jaeger, Tempo, etc.)
    if otlp_endpoint is None:
        otlp_endpoint = os.getenv("OTLP_ENDPOINT", "localhost:4317")

    if otlp_endpoint:
        try:
            otlp_exporter = OTLPSpanExporter(
                endpoint=otlp_endpoint,
                insecure=True  # Use insecure for local development
            )
            provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
            exporters.append("OTLP")
            logger.info(f"OTLP exporter configured: {otlp_endpoint}")
        except Exception as e:
            logger.warning(f"Failed to configure OTLP exporter: {e}")

    # Console exporter for debugging
    if console_export or os.getenv("TRACE_CONSOLE", "").lower() == "true":
        console_exporter = ConsoleSpanExporter()
        provider.add_span_processor(BatchSpanProcessor(console_exporter))
        exporters.append("Console")
        logger.info("Console exporter configured")

    if not exporters:
        logger.warning("No trace exporters configured, tracing will be a no-op")

    # Set global tracer provider
    trace.set_tracer_provider(provider)

    # Get tracer instance
    _tracer = trace.get_tracer(service_name)
    _is_initialized = True

    logger.info(
        f"Tracing initialized: {service_name} "
        f"(exporters: {', '.join(exporters)}, sampling: {sampling_rate})"
    )

    return _tracer


def get_tracer() -> trace.Tracer:
    """
    Get the global tracer instance.

    Initializes tracing with defaults if not already initialized.

    Returns:
        Configured tracer instance
    """
    global _tracer

    if _tracer is None:
        logger.info("Tracer not initialized, initializing with defaults")
        _tracer = initialize_tracing()

    return _tracer


@contextmanager
def trace_operation(
    operation_name: str,
    kind: trace.SpanKind = trace.SpanKind.INTERNAL,
    **attributes
):
    """
    Context manager for tracing operations.

    Automatically creates a span, adds attributes, handles errors,
    and ensures proper span completion.

    Args:
        operation_name: Name of the operation being traced
        kind: Span kind (INTERNAL, CLIENT, SERVER, etc.)
        **attributes: Custom attributes to add to the span

    Yields:
        Span instance for adding custom events/attributes

    Example:
        >>> with trace_operation("reconcile_table", table="customers") as span:
        ...     result = reconcile_table("customers")
        ...     span.set_attribute("rows_reconciled", result.count)
    """
    tracer = get_tracer()

    with tracer.start_as_current_span(
        operation_name,
        kind=kind
    ) as span:
        # Add custom attributes
        for key, value in attributes.items():
            # Convert value to string for compatibility
            span.set_attribute(key, str(value))

        try:
            yield span
        except Exception as e:
            # Record exception in span
            span.set_attribute("error", True)
            span.set_attribute("error.type", type(e).__name__)
            span.set_attribute("error.message", str(e))
            span.record_exception(e)
            raise


def trace_function(operation_name: Optional[str] = None, **default_attributes):
    """
    Decorator for tracing function calls.

    Args:
        operation_name: Optional custom operation name (defaults to function name)
        **default_attributes: Default attributes to add to all spans

    Example:
        >>> @trace_function(component="reconciliation")
        ... def reconcile_table(table_name: str):
        ...     # Function implementation
        ...     pass
    """
    def decorator(func):
        import functools

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Use custom name or function name
            name = operation_name or f"{func.__module__}.{func.__name__}"

            # Combine default attributes with function-specific ones
            attributes = default_attributes.copy()
            attributes["function"] = func.__name__

            with trace_operation(name, **attributes):
                return func(*args, **kwargs)

        return wrapper
    return decorator


def add_span_attributes(**attributes):
    """
    Add attributes to the current span.

    Convenient function to add attributes without needing span reference.

    Args:
        **attributes: Attributes to add to current span

    Example:
        >>> with trace_operation("process_data"):
        ...     data = load_data()
        ...     add_span_attributes(data_size=len(data))
    """
    current_span = trace.get_current_span()
    if current_span.is_recording():
        for key, value in attributes.items():
            current_span.set_attribute(key, str(value))


def add_span_event(name: str, **attributes):
    """
    Add an event to the current span.

    Events represent discrete moments in time during span execution.

    Args:
        name: Event name
        **attributes: Event attributes

    Example:
        >>> with trace_operation("reconcile_table"):
        ...     add_span_event("checksum_started")
        ...     checksum = calculate_checksum()
        ...     add_span_event("checksum_completed", checksum=checksum)
    """
    current_span = trace.get_current_span()
    if current_span.is_recording():
        # Convert attributes to proper types
        attrs = {k: str(v) for k, v in attributes.items()}
        current_span.add_event(name, attributes=attrs)


def trace_database_query(
    query_type: str,
    table: str,
    database: str = "unknown"
) -> Any:
    """
    Context manager for tracing database queries.

    Args:
        query_type: Type of query (SELECT, INSERT, UPDATE, etc.)
        table: Table name
        database: Database name

    Example:
        >>> with trace_database_query("SELECT", "customers", "warehouse_target"):
        ...     cursor.execute("SELECT COUNT(*) FROM customers")
        ...     count = cursor.fetchone()[0]
    """
    return trace_operation(
        f"db.{query_type.lower()}",
        kind=trace.SpanKind.CLIENT,
        **{
            "db.operation": query_type,
            "db.table": table,
            "db.system": database,
            "component": "database"
        }
    )


def trace_http_request(method: str, url: str, **extra_attrs):
    """
    Context manager for tracing HTTP requests.

    Args:
        method: HTTP method (GET, POST, etc.)
        url: Request URL
        **extra_attrs: Additional attributes

    Example:
        >>> with trace_http_request("GET", "http://localhost:8083/connectors"):
        ...     response = requests.get(url)
    """
    return trace_operation(
        f"http.{method.lower()}",
        kind=trace.SpanKind.CLIENT,
        **{
            "http.method": method,
            "http.url": url,
            "component": "http",
            **extra_attrs
        }
    )


def shutdown_tracing(timeout: int = 30):
    """
    Shutdown tracing and flush pending spans.

    Should be called before application exit.

    Args:
        timeout: Timeout in seconds for flushing spans
    """
    global _is_initialized

    if _is_initialized:
        try:
            provider = trace.get_tracer_provider()
            if hasattr(provider, 'shutdown'):
                provider.shutdown()
            logger.info("Tracing shutdown complete")
        except Exception as e:
            logger.error(f"Error during tracing shutdown: {e}")
        finally:
            _is_initialized = False


# Auto-instrumentation helpers
def instrument_psycopg2():
    """
    Automatically instrument psycopg2 for database tracing.

    Call this once at application startup.
    """
    try:
        from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
        Psycopg2Instrumentor().instrument()
        logger.info("psycopg2 instrumentation enabled")
    except ImportError:
        logger.warning("opentelemetry-instrumentation-psycopg2 not installed")
    except Exception as e:
        logger.error(f"Failed to instrument psycopg2: {e}")


def instrument_requests():
    """
    Automatically instrument requests library for HTTP tracing.

    Call this once at application startup.
    """
    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor
        RequestsInstrumentor().instrument()
        logger.info("requests instrumentation enabled")
    except ImportError:
        logger.warning("opentelemetry-instrumentation-requests not installed")
    except Exception as e:
        logger.error(f"Failed to instrument requests: {e}")


def setup_auto_instrumentation():
    """
    Setup all available auto-instrumentation.

    Convenience function to enable all supported auto-instrumentation.
    """
    logger.info("Setting up auto-instrumentation")

    # Database instrumentation
    instrument_psycopg2()

    # HTTP instrumentation
    instrument_requests()

    logger.info("Auto-instrumentation setup complete")


# Example usage and integration points
if __name__ == "__main__":
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
        import time
        time.sleep(0.1)

        add_span_event("work_completed")

    # Shutdown
    shutdown_tracing()
