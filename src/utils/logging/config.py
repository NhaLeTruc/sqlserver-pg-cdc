"""
Logging configuration for CDC pipeline.

Provides setup functions for configuring application-wide logging
with support for file rotation, console output, and JSON formatting.
"""

import logging
import logging.handlers
import os
import sys

from .formatters import ConsoleFormatter, JSONFormatter


def setup_logging(
    level: str = "INFO",
    log_file: str | None = None,
    console_output: bool = True,
    json_format: bool = False,
    app_name: str = "sqlserver-pg-cdc",
    max_bytes: int = 100 * 1024 * 1024,  # 100MB
    backup_count: int = 5,
) -> None:
    """
    Configure logging for the application

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (if None, file logging is disabled)
        console_output: Whether to output to console
        json_format: Use JSON format for both console and file logs
        app_name: Application name for log context
        max_bytes: Maximum log file size before rotation
        backup_count: Number of rotated log files to keep
    """
    # Convert level string to logging level
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Clear existing handlers
    root_logger.handlers.clear()

    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(numeric_level)

        if json_format:
            console_handler.setFormatter(
                JSONFormatter(
                    include_timestamp=True,
                    include_hostname=True,
                    app_name=app_name,
                )
            )
        else:
            console_handler.setFormatter(ConsoleFormatter(use_colors=True))

        root_logger.addHandler(console_handler)

    # File handler with rotation
    if log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(numeric_level)

        if json_format:
            file_handler.setFormatter(
                JSONFormatter(
                    include_timestamp=True,
                    include_hostname=True,
                    app_name=app_name,
                )
            )
        else:
            file_handler.setFormatter(
                logging.Formatter(
                    fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )

        root_logger.addHandler(file_handler)

    # Set levels for noisy third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("hvac").setLevel(logging.WARNING)

    # Log startup message
    logger = logging.getLogger(__name__)
    logger.info(
        f"Logging initialized: level={level}, file={log_file or 'none'}, "
        f"console={console_output}, json={json_format}"
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance

    Args:
        name: Logger name (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def shutdown_logging() -> None:
    """
    Shutdown logging and release all file handles.

    BUG-13: Provides explicit cleanup for RotatingFileHandler to prevent
    file handle leaks. Call this during application shutdown.

    Example:
        import atexit
        atexit.register(shutdown_logging)
    """
    root_logger = logging.getLogger()

    # Close and remove all handlers
    for handler in root_logger.handlers[:]:
        try:
            handler.close()
        except Exception:
            pass
        root_logger.removeHandler(handler)

    # Also call logging.shutdown() for complete cleanup
    logging.shutdown()


def configure_from_env() -> None:
    """
    Configure logging from environment variables

    Environment variables:
        LOG_LEVEL: Log level (default: INFO)
        LOG_FILE: Log file path (default: none)
        LOG_JSON: Use JSON format (default: false)
        LOG_CONSOLE: Enable console output (default: true)
    """
    level = os.getenv("LOG_LEVEL", "INFO")
    log_file = os.getenv("LOG_FILE")
    json_format = os.getenv("LOG_JSON", "false").lower() in ("true", "1", "yes")
    console_output = os.getenv("LOG_CONSOLE", "true").lower() in ("true", "1", "yes")

    setup_logging(
        level=level,
        log_file=log_file,
        console_output=console_output,
        json_format=json_format,
    )
