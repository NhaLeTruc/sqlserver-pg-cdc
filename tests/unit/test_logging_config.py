"""
Unit tests for src/utils/logging_config.py

This module provides comprehensive tests for structured logging configuration,
including JSON formatting, console formatting, context logging, and environment-based configuration.
"""

import json
import logging
import os
import sys
import tempfile
from unittest.mock import patch

from src.utils.logging import (
    ConsoleFormatter,
    ContextLogger,
    JSONFormatter,
    configure_from_env,
    setup_logging,
)


class TestJSONFormatter:
    """Test JSONFormatter class"""

    def test_init_with_defaults(self):
        """Test initialization with default parameters"""
        # Arrange & Act
        formatter = JSONFormatter()

        # Assert
        assert formatter.include_timestamp is True
        assert formatter.include_hostname is True
        assert formatter.app_name == "sqlserver-pg-cdc"
        assert formatter.hostname is not None

    def test_init_with_custom_values(self):
        """Test initialization with custom parameters"""
        # Arrange & Act
        formatter = JSONFormatter(
            include_timestamp=False,
            include_hostname=False,
            app_name="test-app"
        )

        # Assert
        assert formatter.include_timestamp is False
        assert formatter.include_hostname is False
        assert formatter.app_name == "test-app"
        assert formatter.hostname is None

    def test_format_basic_log_record(self):
        """Test formatting a basic log record"""
        # Arrange
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/path/to/test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None
        )

        # Act
        result = formatter.format(record)
        data = json.loads(result)

        # Assert
        assert data["level"] == "INFO"
        assert data["logger"] == "test_logger"
        assert data["message"] == "Test message"
        assert data["app"] == "sqlserver-pg-cdc"
        assert "timestamp" in data
        assert "hostname" in data
        assert "source" in data
        assert data["source"]["file"] == "/path/to/test.py"
        assert data["source"]["line"] == 42

    def test_format_without_timestamp(self):
        """Test formatting without timestamp"""
        # Arrange
        formatter = JSONFormatter(include_timestamp=False)
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/path/to/test.py",
            lineno=10,
            msg="Test",
            args=(),
            exc_info=None
        )

        # Act
        result = formatter.format(record)
        data = json.loads(result)

        # Assert
        assert "timestamp" not in data

    def test_format_without_hostname(self):
        """Test formatting without hostname"""
        # Arrange
        formatter = JSONFormatter(include_hostname=False)
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/path/to/test.py",
            lineno=10,
            msg="Test",
            args=(),
            exc_info=None
        )

        # Act
        result = formatter.format(record)
        data = json.loads(result)

        # Assert
        assert "hostname" not in data

    def test_format_with_exception_info(self):
        """Test formatting with exception information"""
        # Arrange
        formatter = JSONFormatter()

        try:
            raise ValueError("Test error")
        except ValueError:
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname="/path/to/test.py",
            lineno=50,
            msg="Error occurred",
            args=(),
            exc_info=exc_info
        )

        # Act
        result = formatter.format(record)
        data = json.loads(result)

        # Assert
        assert "exception" in data
        assert data["exception"]["type"] == "ValueError"
        assert "Test error" in data["exception"]["message"]
        assert "traceback" in data["exception"]
        assert isinstance(data["exception"]["traceback"], list)

    def test_format_with_extra_context(self):
        """Test formatting with extra context fields"""
        # Arrange
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/path/to/test.py",
            lineno=30,
            msg="Processing record",
            args=(),
            exc_info=None
        )
        # Add extra fields
        record.table_name = "customers"
        record.record_id = 12345
        record.operation = "INSERT"

        # Act
        result = formatter.format(record)
        data = json.loads(result)

        # Assert
        assert "context" in data
        assert data["context"]["table_name"] == "customers"
        assert data["context"]["record_id"] == 12345
        assert data["context"]["operation"] == "INSERT"

    def test_format_excludes_internal_fields(self):
        """Test that internal logging fields are excluded from context"""
        # Arrange
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/path/to/test.py",
            lineno=30,
            msg="Test",
            args=(),
            exc_info=None
        )
        # Add custom field
        record.custom_field = "value"

        # Act
        result = formatter.format(record)
        data = json.loads(result)

        # Assert
        # Internal fields should not be in context
        if "context" in data:
            assert "name" not in data["context"]
            assert "msg" not in data["context"]
            assert "pathname" not in data["context"]
            # Custom field should be in context
            assert data["context"]["custom_field"] == "value"




class TestConsoleFormatter:
    """Test ConsoleFormatter class"""

    def test_init_with_colors(self):
        """Test initialization with colors enabled"""
        # Arrange & Act
        formatter = ConsoleFormatter(use_colors=True)

        # Assert
        assert formatter is not None

    def test_init_without_colors(self):
        """Test initialization with colors disabled"""
        # Arrange & Act
        formatter = ConsoleFormatter(use_colors=False)

        # Assert
        assert formatter.use_colors is False

    @patch('sys.stderr.isatty', return_value=True)
    def test_format_with_colors_enabled(self, mock_isatty):
        """Test formatting with colors enabled"""
        # Arrange
        formatter = ConsoleFormatter(use_colors=True)
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/path/to/test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None
        )

        # Act
        result = formatter.format(record)

        # Assert
        assert "Test message" in result
        assert "test_logger" in result

    def test_format_without_colors(self):
        """Test formatting without colors"""
        # Arrange
        formatter = ConsoleFormatter(use_colors=False)
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/path/to/test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None
        )

        # Act
        result = formatter.format(record)

        # Assert
        assert "Test message" in result
        assert "test_logger" in result
        assert "[INFO]" in result

    def test_format_with_extra_context(self):
        """Test formatting with extra context"""
        # Arrange
        formatter = ConsoleFormatter(use_colors=False)
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="/path/to/test.py",
            lineno=10,
            msg="Processing",
            args=(),
            exc_info=None
        )
        record.table_name = "customers"
        record.record_id = 999

        # Act
        result = formatter.format(record)

        # Assert
        assert "table_name=customers" in result
        assert "record_id=999" in result

    def test_format_different_log_levels(self):
        """Test formatting different log levels"""
        # Arrange
        formatter = ConsoleFormatter(use_colors=False)

        for level_name, level_num in [
            ("DEBUG", logging.DEBUG),
            ("INFO", logging.INFO),
            ("WARNING", logging.WARNING),
            ("ERROR", logging.ERROR),
            ("CRITICAL", logging.CRITICAL),
        ]:
            record = logging.LogRecord(
                name="test_logger",
                level=level_num,
                pathname="/path/to/test.py",
                lineno=10,
                msg=f"{level_name} message",
                args=(),
                exc_info=None
            )

            # Act
            result = formatter.format(record)

            # Assert
            assert level_name in result
            assert f"{level_name} message" in result


    @patch('sys.stderr.isatty', return_value=True)
    def test_format_with_colors_level_not_in_colors(self, mock_isatty):
        """Test formatting with colors when level is not in COLORS dict"""
        # Arrange
        formatter = ConsoleFormatter(use_colors=True)
        # Create a custom level that's not in COLORS
        record = logging.LogRecord(
            name="test_logger",
            level=99,  # Custom level
            pathname="/path/to/test.py",
            lineno=10,
            msg="Custom level message",
            args=(),
            exc_info=None
        )
        record.levelname = "CUSTOM"

        # Act
        result = formatter.format(record)

        # Assert
        assert "CUSTOM" in result
        assert "Custom level message" in result


class TestSetupLogging:
    """Test setup_logging function"""

    def teardown_method(self):
        """Clean up logging handlers after each test"""
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.setLevel(logging.WARNING)

    def test_setup_logging_with_defaults(self):
        """Test setup with default parameters"""
        # Arrange & Act
        setup_logging()

        # Assert
        root_logger = logging.getLogger()
        assert root_logger.level == logging.INFO
        assert len(root_logger.handlers) > 0

    def test_setup_logging_with_custom_level(self):
        """Test setup with custom log level"""
        # Arrange & Act
        setup_logging(level="DEBUG")

        # Assert
        root_logger = logging.getLogger()
        assert root_logger.level == logging.DEBUG

    def test_setup_logging_with_invalid_level_defaults_to_info(self):
        """Test that invalid level defaults to INFO"""
        # Arrange & Act
        setup_logging(level="INVALID")

        # Assert
        root_logger = logging.getLogger()
        assert root_logger.level == logging.INFO

    def test_setup_logging_console_only(self):
        """Test setup with console output only"""
        # Arrange & Act
        setup_logging(console_output=True, log_file=None)

        # Assert
        root_logger = logging.getLogger()
        assert len(root_logger.handlers) == 1
        assert isinstance(root_logger.handlers[0], logging.StreamHandler)

    def test_setup_logging_with_file(self):
        """Test setup with file logging"""
        # Arrange
        with tempfile.NamedTemporaryFile(delete=False, suffix=".log") as tmp:
            log_file = tmp.name

        try:
            # Act
            setup_logging(log_file=log_file, console_output=False)

            # Assert
            root_logger = logging.getLogger()
            assert len(root_logger.handlers) == 1
            assert isinstance(root_logger.handlers[0], logging.handlers.RotatingFileHandler)
        finally:
            # Cleanup
            if os.path.exists(log_file):
                os.unlink(log_file)

    def test_setup_logging_with_json_format(self):
        """Test setup with JSON formatting"""
        # Arrange
        with tempfile.NamedTemporaryFile(delete=False, suffix=".log") as tmp:
            log_file = tmp.name

        try:
            # Act
            setup_logging(log_file=log_file, json_format=True, console_output=False)

            # Assert
            root_logger = logging.getLogger()
            file_handler = root_logger.handlers[0]
            assert isinstance(file_handler.formatter, JSONFormatter)
        finally:
            # Cleanup
            if os.path.exists(log_file):
                os.unlink(log_file)

    def test_setup_logging_creates_log_directory(self):
        """Test that log directory is created if it doesn't exist"""
        # Arrange
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "subdir", "test.log")

            # Act
            setup_logging(log_file=log_file, console_output=False)

            # Assert
            assert os.path.exists(os.path.dirname(log_file))
            assert os.path.exists(log_file)

    def test_setup_logging_clears_existing_handlers(self):
        """Test that existing handlers are cleared"""
        # Arrange
        root_logger = logging.getLogger()
        root_logger.addHandler(logging.StreamHandler())
        initial_count = len(root_logger.handlers)
        assert initial_count > 0

        # Act
        setup_logging(console_output=True)

        # Assert
        # Should have exactly one handler (the new console handler)
        assert len(root_logger.handlers) == 1


    def test_setup_logging_with_custom_max_bytes(self):
        """Test setup with custom max_bytes for rotation"""
        # Arrange
        with tempfile.NamedTemporaryFile(delete=False, suffix=".log") as tmp:
            log_file = tmp.name

        try:
            # Act
            setup_logging(
                log_file=log_file,
                max_bytes=1024,
                backup_count=3,
                console_output=False
            )

            # Assert
            root_logger = logging.getLogger()
            file_handler = root_logger.handlers[0]
            assert file_handler.maxBytes == 1024
            assert file_handler.backupCount == 3
        finally:
            # Cleanup
            if os.path.exists(log_file):
                os.unlink(log_file)

    def test_setup_logging_with_console_and_file(self):
        """Test setup with both console and file output"""
        # Arrange
        with tempfile.NamedTemporaryFile(delete=False, suffix=".log") as tmp:
            log_file = tmp.name

        try:
            # Act
            setup_logging(log_file=log_file, console_output=True)

            # Assert
            root_logger = logging.getLogger()
            assert len(root_logger.handlers) == 2
            handler_types = [type(h).__name__ for h in root_logger.handlers]
            assert "StreamHandler" in handler_types
            assert "RotatingFileHandler" in handler_types
        finally:
            # Cleanup
            if os.path.exists(log_file):
                os.unlink(log_file)


class TestContextLogger:
    """Test ContextLogger class"""

    def test_init_with_context(self):
        """Test initialization with context"""
        # Arrange & Act
        logger = ContextLogger("test_logger", table_name="customers", operation="INSERT")

        # Assert
        assert logger.context["table_name"] == "customers"
        assert logger.context["operation"] == "INSERT"

    def test_init_without_context(self):
        """Test initialization without context"""
        # Arrange & Act
        logger = ContextLogger("test_logger")

        # Assert
        assert logger.context == {}

    @patch('logging.Logger.log')
    def test_debug_adds_context(self, mock_log):
        """Test that debug method adds context"""
        # Arrange
        logger = ContextLogger("test_logger", service="reconciliation")

        # Act
        logger.debug("Debug message", custom_field="value")

        # Assert
        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][0] == logging.DEBUG
        assert call_args[0][1] == "Debug message"
        assert call_args[1]["extra"]["service"] == "reconciliation"
        assert call_args[1]["extra"]["custom_field"] == "value"

    @patch('logging.Logger.log')
    def test_info_adds_context(self, mock_log):
        """Test that info method adds context"""
        # Arrange
        logger = ContextLogger("test_logger", table_name="orders")

        # Act
        logger.info("Info message", record_id=123)

        # Assert
        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][0] == logging.INFO
        assert call_args[1]["extra"]["table_name"] == "orders"
        assert call_args[1]["extra"]["record_id"] == 123

    @patch('logging.Logger.log')
    def test_warning_adds_context(self, mock_log):
        """Test that warning method adds context"""
        # Arrange
        logger = ContextLogger("test_logger", component="validator")

        # Act
        logger.warning("Warning message")

        # Assert
        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][0] == logging.WARNING
        assert call_args[1]["extra"]["component"] == "validator"

    @patch('logging.Logger.log')
    def test_error_adds_context(self, mock_log):
        """Test that error method adds context"""
        # Arrange
        logger = ContextLogger("test_logger", module="processor")

        # Act
        logger.error("Error message", error_code=500)

        # Assert
        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][0] == logging.ERROR
        assert call_args[1]["extra"]["module"] == "processor"
        assert call_args[1]["extra"]["error_code"] == 500

    @patch('logging.Logger.log')
    def test_error_with_exc_info(self, mock_log):
        """Test error method with exception info"""
        # Arrange
        logger = ContextLogger("test_logger")

        # Act
        logger.error("Error occurred", exc_info=True)

        # Assert
        mock_log.assert_called_once()
        assert mock_log.call_args[1]["exc_info"] is True

    @patch('logging.Logger.log')
    def test_critical_adds_context(self, mock_log):
        """Test that critical method adds context"""
        # Arrange
        logger = ContextLogger("test_logger", severity="high")

        # Act
        logger.critical("Critical message")

        # Assert
        mock_log.assert_called_once()
        call_args = mock_log.call_args
        assert call_args[0][0] == logging.CRITICAL
        assert call_args[1]["extra"]["severity"] == "high"

    def test_update_context(self):
        """Test updating context"""
        # Arrange
        logger = ContextLogger("test_logger", field1="value1")

        # Act
        logger.update_context(field2="value2", field3="value3")

        # Assert
        assert logger.context["field1"] == "value1"
        assert logger.context["field2"] == "value2"
        assert logger.context["field3"] == "value3"

    def test_update_context_overwrites_existing(self):
        """Test that update_context overwrites existing values"""
        # Arrange
        logger = ContextLogger("test_logger", field1="old_value")

        # Act
        logger.update_context(field1="new_value")

        # Assert
        assert logger.context["field1"] == "new_value"

    def test_get_context_returns_copy(self):
        """Test that get_context returns a copy"""
        # Arrange
        logger = ContextLogger("test_logger", field1="value1")

        # Act
        context = logger.get_context()
        context["field2"] = "value2"

        # Assert
        assert "field2" not in logger.context
        assert logger.context["field1"] == "value1"

    def test_get_context_includes_all_fields(self):
        """Test that get_context returns all context fields"""
        # Arrange
        logger = ContextLogger("test_logger", field1="value1", field2="value2")

        # Act
        context = logger.get_context()

        # Assert
        assert context["field1"] == "value1"
        assert context["field2"] == "value2"


class TestConfigureFromEnv:
    """Test configure_from_env function"""

    def teardown_method(self):
        """Clean up after each test"""
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.setLevel(logging.WARNING)

    @patch.dict(os.environ, {
        "LOG_LEVEL": "DEBUG",
        "LOG_FILE": "/tmp/test.log",
        "LOG_JSON": "true",
        "LOG_CONSOLE": "false"
    })
    @patch('src.utils.logging.config.setup_logging')
    def test_configure_from_env_all_vars_set(self, mock_setup):
        """Test configuration from all environment variables"""
        # Arrange & Act
        configure_from_env()

        # Assert
        mock_setup.assert_called_once_with(
            level="DEBUG",
            log_file="/tmp/test.log",
            console_output=False,
            json_format=True
        )

    @patch.dict(os.environ, {}, clear=True)
    @patch('src.utils.logging.config.setup_logging')
    def test_configure_from_env_with_defaults(self, mock_setup):
        """Test configuration with default values"""
        # Arrange & Act
        configure_from_env()

        # Assert
        mock_setup.assert_called_once_with(
            level="INFO",
            log_file=None,
            console_output=True,
            json_format=False
        )

    @patch.dict(os.environ, {"LOG_LEVEL": "WARNING"})
    @patch('src.utils.logging.config.setup_logging')
    def test_configure_from_env_partial_vars(self, mock_setup):
        """Test configuration with partial environment variables"""
        # Arrange & Act
        configure_from_env()

        # Assert
        mock_setup.assert_called_once_with(
            level="WARNING",
            log_file=None,
            console_output=True,
            json_format=False
        )

    @patch.dict(os.environ, {"LOG_JSON": "1"})
    @patch('src.utils.logging.config.setup_logging')
    def test_configure_from_env_json_true_variations(self, mock_setup):
        """Test that various true values for LOG_JSON work"""
        # Arrange & Act
        configure_from_env()

        # Assert
        assert mock_setup.call_args[1]["json_format"] is True

    @patch.dict(os.environ, {"LOG_JSON": "yes"})
    @patch('src.utils.logging.config.setup_logging')
    def test_configure_from_env_json_yes(self, mock_setup):
        """Test LOG_JSON with 'yes' value"""
        # Arrange & Act
        configure_from_env()

        # Assert
        assert mock_setup.call_args[1]["json_format"] is True

    @patch.dict(os.environ, {"LOG_JSON": "false"})
    @patch('src.utils.logging.config.setup_logging')
    def test_configure_from_env_json_false(self, mock_setup):
        """Test LOG_JSON with 'false' value"""
        # Arrange & Act
        configure_from_env()

        # Assert
        assert mock_setup.call_args[1]["json_format"] is False

    @patch.dict(os.environ, {"LOG_CONSOLE": "0"})
    @patch('src.utils.logging.config.setup_logging')
    def test_configure_from_env_console_false_variations(self, mock_setup):
        """Test that false values for LOG_CONSOLE work"""
        # Arrange & Act
        configure_from_env()

        # Assert
        assert mock_setup.call_args[1]["console_output"] is False

    @patch.dict(os.environ, {"LOG_FILE": "/var/log/myapp.log"})
    @patch('src.utils.logging.config.setup_logging')
    def test_configure_from_env_with_log_file(self, mock_setup):
        """Test configuration with LOG_FILE set"""
        # Arrange & Act
        configure_from_env()

        # Assert
        assert mock_setup.call_args[1]["log_file"] == "/var/log/myapp.log"
