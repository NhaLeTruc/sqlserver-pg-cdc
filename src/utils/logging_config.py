"""
Structured logging configuration for CDC pipeline

Provides JSON-formatted logging with correlation IDs, contextual information,
and integration with monitoring systems.

Usage:
    from src.utils.logging_config import setup_logging, get_logger

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

import logging
import logging.config
import logging.handlers
import json
import sys
import os
from datetime import datetime
from typing import Optional, Dict, Any
import traceback


class JSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging

    Outputs log records as JSON with standard fields plus any extra context.
    """

    def __init__(
        self,
        include_timestamp: bool = True,
        include_hostname: bool = True,
        app_name: str = "sqlserver-pg-cdc",
    ):
        """
        Initialize JSON formatter

        Args:
            include_timestamp: Include ISO8601 timestamp
            include_hostname: Include hostname in log records
            app_name: Application name to include in logs
        """
        super().__init__()
        self.include_timestamp = include_timestamp
        self.include_hostname = include_hostname
        self.app_name = app_name
        self.hostname = os.uname().nodename if include_hostname else None

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON

        Args:
            record: Log record to format

        Returns:
            JSON string
        """
        # Base log structure
        log_data = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "app": self.app_name,
        }

        # Add timestamp
        if self.include_timestamp:
            log_data["timestamp"] = datetime.utcfromtimestamp(
                record.created
            ).isoformat() + "Z"

        # Add hostname
        if self.include_hostname and self.hostname:
            log_data["hostname"] = self.hostname

        # Add source location
        log_data["source"] = {
            "file": record.pathname,
            "line": record.lineno,
            "function": record.funcName,
        }

        # Add thread/process info
        log_data["process"] = {
            "pid": record.process,
            "thread": record.thread,
            "thread_name": record.threadName,
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info),
            }

        # Add extra context (from logger.info(..., extra={...}))
        # Skip internal logging fields
        skip_fields = {
            "name", "msg", "args", "created", "filename", "funcName",
            "levelname", "levelno", "lineno", "module", "msecs",
            "message", "pathname", "process", "processName", "relativeCreated",
            "thread", "threadName", "exc_info", "exc_text", "stack_info",
        }

        extra_data = {}
        for key, value in record.__dict__.items():
            if key not in skip_fields and not key.startswith("_"):
                extra_data[key] = value

        if extra_data:
            log_data["context"] = extra_data

        return json.dumps(log_data, default=str)


class ConsoleFormatter(logging.Formatter):
    """
    Human-readable console formatter with colors

    Provides colored output for different log levels.
    """

    # ANSI color codes
    COLORS = {
        "DEBUG": "\033[36m",      # Cyan
        "INFO": "\033[32m",       # Green
        "WARNING": "\033[33m",    # Yellow
        "ERROR": "\033[31m",      # Red
        "CRITICAL": "\033[35m",   # Magenta
    }
    RESET = "\033[0m"

    def __init__(self, use_colors: bool = True):
        """
        Initialize console formatter

        Args:
            use_colors: Whether to use ANSI color codes
        """
        super().__init__(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.use_colors = use_colors and sys.stderr.isatty()

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record for console output

        Args:
            record: Log record to format

        Returns:
            Formatted string
        """
        if self.use_colors:
            levelname = record.levelname
            if levelname in self.COLORS:
                record.levelname = (
                    f"{self.COLORS[levelname]}{levelname}{self.RESET}"
                )

        formatted = super().format(record)

        # Add extra context if present
        skip_fields = {
            "name", "msg", "args", "created", "filename", "funcName",
            "levelname", "levelno", "lineno", "module", "msecs",
            "message", "pathname", "process", "processName", "relativeCreated",
            "thread", "threadName", "exc_info", "exc_text", "stack_info",
        }

        extra_items = []
        for key, value in record.__dict__.items():
            if key not in skip_fields and not key.startswith("_"):
                extra_items.append(f"{key}={value}")

        if extra_items:
            formatted += f" [{', '.join(extra_items)}]"

        return formatted


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
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
        json_format: Use JSON format for file logs
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


class ContextLogger:
    """
    Logger wrapper that adds contextual information to all log messages

    Usage:
        logger = ContextLogger("my_module", table_name="customers")
        logger.info("Processing record", record_id=12345)
        # Output includes both table_name and record_id
    """

    def __init__(self, name: str, **context):
        """
        Initialize context logger

        Args:
            name: Logger name
            **context: Contextual key-value pairs to include in all logs
        """
        self.logger = logging.getLogger(name)
        self.context = context

    def _log(
        self,
        level: int,
        msg: str,
        *args,
        exc_info=None,
        **kwargs
    ) -> None:
        """
        Internal log method that merges context

        Args:
            level: Log level
            msg: Log message
            *args: Message format args
            exc_info: Exception info
            **kwargs: Additional context
        """
        # Merge context with kwargs
        extra = {**self.context, **kwargs}

        self.logger.log(
            level,
            msg,
            *args,
            exc_info=exc_info,
            extra=extra,
        )

    def debug(self, msg: str, *args, **kwargs) -> None:
        """Log debug message with context"""
        self._log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        """Log info message with context"""
        self._log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        """Log warning message with context"""
        self._log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: str, *args, exc_info=None, **kwargs) -> None:
        """Log error message with context"""
        self._log(logging.ERROR, msg, *args, exc_info=exc_info, **kwargs)

    def critical(self, msg: str, *args, exc_info=None, **kwargs) -> None:
        """Log critical message with context"""
        self._log(logging.CRITICAL, msg, *args, exc_info=exc_info, **kwargs)

    def update_context(self, **context) -> None:
        """
        Update the context for this logger

        Args:
            **context: New context key-value pairs
        """
        self.context.update(context)

    def get_context(self) -> Dict[str, Any]:
        """
        Get current context

        Returns:
            Dictionary of context key-value pairs
        """
        return self.context.copy()


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
