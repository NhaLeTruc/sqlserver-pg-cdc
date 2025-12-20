"""
Retry decorator with exponential backoff for database operations

Provides resilient retry logic for transient failures with:
- Exponential backoff (base 2.0)
- Jitter to prevent thundering herd
- Configurable max retries
- Database-specific exception handling
- Callback support for metrics integration

Usage:
    from src.utils.retry import retry_with_backoff

    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def database_operation():
        # Database operation that may fail transiently
        cursor.execute("SELECT * FROM table")
"""

import time
import random
import logging
from typing import Callable, Any, Optional, Type, Tuple
from functools import wraps

logger = logging.getLogger(__name__)


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None
):
    """
    Decorator that retries a function with exponential backoff

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay in seconds (default: 1.0)
        max_delay: Maximum delay in seconds (default: 60.0)
        exponential_base: Base for exponential backoff (default: 2.0)
        jitter: Add random jitter to prevent thundering herd (default: True)
        retryable_exceptions: Tuple of exception types to retry (default: all exceptions)
        on_retry: Callback function(attempt, exception, delay) called on each retry

    Returns:
        Decorated function with retry logic

    Example:
        @retry_with_backoff(max_retries=3, base_delay=1.0)
        def query_database(cursor, query):
            cursor.execute(query)
            return cursor.fetchall()

        @retry_with_backoff(
            max_retries=5,
            retryable_exceptions=(ConnectionError, TimeoutError),
            on_retry=lambda attempt, exc, delay: print(f"Retry {attempt}: {exc}")
        )
        def connect_to_database():
            return pyodbc.connect(connection_string)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    last_exception = e

                    # Get function name safely
                    func_name = getattr(func, '__name__', 'function')

                    # Check if this exception type is retryable
                    if retryable_exceptions and not isinstance(e, retryable_exceptions):
                        # Non-retryable exception, raise immediately
                        logger.error(
                            f"Non-retryable exception in {func_name}: {type(e).__name__}: {e}"
                        )
                        raise

                    # If this was the last attempt, raise the exception
                    if attempt == max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) exceeded for {func_name}: "
                            f"{type(e).__name__}: {e}"
                        )
                        raise

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)

                    # Add jitter if enabled (±25% of delay)
                    if jitter:
                        jitter_amount = delay * 0.25
                        delay = delay + random.uniform(-jitter_amount, jitter_amount)
                        delay = max(0.1, delay)  # Ensure minimum delay

                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed for {func_name}: "
                        f"{type(e).__name__}: {e}. Retrying in {delay:.2f}s..."
                    )

                    # Call retry callback if provided
                    if on_retry:
                        try:
                            on_retry(attempt + 1, e, delay)
                        except Exception as callback_error:
                            logger.error(f"Error in retry callback: {callback_error}")

                    # Wait before retrying
                    time.sleep(delay)

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception
            else:
                raise RuntimeError(f"Unexpected error in retry logic for {func.__name__}")

        return wrapper
    return decorator


def is_retryable_db_exception(exception: Exception) -> bool:
    """
    Determine if a database exception is retryable

    Checks for common transient database errors that should be retried:
    - Connection errors
    - Timeout errors
    - Lock timeout errors
    - Deadlock errors

    Args:
        exception: The exception to check

    Returns:
        True if the exception is retryable, False otherwise
    """
    exception_str = str(exception).lower()
    exception_type = type(exception).__name__.lower()

    # Common retryable error patterns
    retryable_patterns = [
        "connection",
        "timeout",
        "deadlock",
        "lock wait timeout",
        "lost connection",
        "server has gone away",
        "can't connect",
        "unable to connect",
        "connection refused",
        "connection reset",
        "broken pipe",
        "network error",
        "communication link failure",
        "connection closed",
        "connection terminated",
    ]

    # Check exception message
    for pattern in retryable_patterns:
        if pattern in exception_str or pattern in exception_type:
            return True

    # Check specific exception types
    retryable_exception_names = [
        "connectionerror",
        "timeouterror",
        "operationalerror",  # Common database error for transient issues
        "interfaceerror",    # Database interface errors
    ]

    if exception_type in retryable_exception_names:
        return True

    return False


# Database-specific retry decorators for convenience
def retry_database_operation(
    max_retries: int = 3,
    base_delay: float = 1.0,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None
):
    """
    Convenience decorator for database operations with smart exception filtering

    Only retries on transient database errors (connection, timeout, deadlock, etc.)
    Non-retryable errors (syntax errors, constraint violations) fail immediately.

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay in seconds (default: 1.0)
        on_retry: Callback function(attempt, exception, delay) called on each retry

    Example:
        @retry_database_operation(max_retries=5)
        def execute_query(cursor, query):
            cursor.execute(query)
            return cursor.fetchall()
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    last_exception = e

                    # Get function name safely
                    func_name = getattr(func, '__name__', 'function')

                    # Check if this is a retryable database exception
                    if not is_retryable_db_exception(e):
                        # Non-retryable exception (e.g., syntax error), raise immediately
                        logger.error(
                            f"Non-retryable database error in {func_name}: "
                            f"{type(e).__name__}: {e}"
                        )
                        raise

                    # If this was the last attempt, raise the exception
                    if attempt == max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) exceeded for {func_name}: "
                            f"{type(e).__name__}: {e}"
                        )
                        raise

                    # Calculate delay with exponential backoff and jitter
                    delay = min(base_delay * (2.0 ** attempt), 60.0)
                    jitter_amount = delay * 0.25
                    delay = delay + random.uniform(-jitter_amount, jitter_amount)
                    delay = max(0.1, delay)

                    logger.warning(
                        f"Retryable database error in {func_name} "
                        f"(attempt {attempt + 1}/{max_retries}): "
                        f"{type(e).__name__}: {e}. Retrying in {delay:.2f}s..."
                    )

                    # Call retry callback if provided
                    if on_retry:
                        try:
                            on_retry(attempt + 1, e, delay)
                        except Exception as callback_error:
                            logger.error(f"Error in retry callback: {callback_error}")

                    # Wait before retrying
                    time.sleep(delay)

            # This should never be reached
            if last_exception:
                raise last_exception
            else:
                raise RuntimeError(f"Unexpected error in retry logic for {func.__name__}")

        return wrapper
    return decorator