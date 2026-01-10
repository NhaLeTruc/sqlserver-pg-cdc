"""
Context managers and utilities for span management.

Provides context managers for creating spans and adding attributes/events
to the current span without explicit span references.
"""

from contextlib import contextmanager

from opentelemetry import trace

from .tracer import get_tracer


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
