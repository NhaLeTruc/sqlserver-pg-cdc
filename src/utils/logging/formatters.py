"""
Custom log formatters for structured and console logging.

Provides JSON formatter for structured logs and console formatter
with color support for human-readable output.
"""

import json
import logging
import os
import sys
import traceback
from datetime import UTC, datetime


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
            log_data["timestamp"] = datetime.fromtimestamp(
                record.created, UTC
            ).isoformat()

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
            "taskName", "asctime",  # Skip internal fields
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
            "taskName", "asctime",  # Skip internal fields
        }

        extra_items = []
        for key, value in record.__dict__.items():
            if key not in skip_fields and not key.startswith("_"):
                extra_items.append(f"{key}={value}")

        if extra_items:
            formatted += f" [{', '.join(extra_items)}]"

        return formatted
