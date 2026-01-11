"""
Decorators for adding tracing to functions.

Provides decorators that automatically create spans for function calls
with appropriate attributes and error handling.
"""

import functools

from .context import trace_operation


def trace_function(operation_name: str | None = None, **default_attributes):
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
