"""
Structured logging configuration for CDC pipeline

Provides JSON-formatted logging with correlation IDs, contextual information,
and integration with monitoring systems.

Usage:
    from utils.logging import setup_logging, get_logger

    # Setup logging (call once at application startup)
    setup_logging(level="INFO", log_file="/var/log/cdc/app.log")

    # Get logger for your module
    logger = get_logger(__name__)

    # Log with context
    logger.info("Processing record", extra={
        "table_name": "customers",
        "record_id": 12345,
        "operation": "INSERT"
    })
"""

from .config import configure_from_env, get_logger, setup_logging
from .formatters import ConsoleFormatter, JSONFormatter
from .handlers import ContextLogger

__all__ = [
    "setup_logging",
    "get_logger",
    "configure_from_env",
    "JSONFormatter",
    "ConsoleFormatter",
    "ContextLogger",
]


# Example usage and testing
if __name__ == "__main__":
    # Setup logging
    setup_logging(
        level="DEBUG",
        console_output=True,
        json_format=False,
    )

    # Get logger
    logger = get_logger(__name__)

    # Basic logging
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")

    # Logging with context
    logger.info(
        "Processing customer record",
        extra={
            "table_name": "customers",
            "record_id": 12345,
            "operation": "INSERT",
        }
    )

    # Logging with exception
    try:
        raise ValueError("Example error")
    except ValueError:
        logger.error("An error occurred", exc_info=True)

    # Context logger
    context_logger = ContextLogger(
        "example",
        service="reconciliation",
        table_name="customers",
    )
    context_logger.info("Starting reconciliation", row_count=1000)
    context_logger.warning("Mismatch detected", difference=5)

    print("\n--- JSON Format ---\n")

    # Test JSON format
    setup_logging(
        level="INFO",
        console_output=True,
        json_format=True,
    )

    logger = get_logger(__name__)
    logger.info(
        "JSON formatted log",
        extra={
            "table_name": "orders",
            "duration_ms": 1234,
        }
    )
