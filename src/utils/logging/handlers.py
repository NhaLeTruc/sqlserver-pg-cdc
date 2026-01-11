"""
Custom logging handlers and wrappers.

Provides ContextLogger for adding contextual information to log messages.
"""

import logging
from typing import Any


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

    def get_context(self) -> dict[str, Any]:
        """
        Get current context

        Returns:
            Dictionary of context key-value pairs
        """
        return self.context.copy()
