"""
Unit tests for retry logic with exponential backoff

Tests verify:
- Exponential backoff calculation
- Jitter implementation
- Exception filtering
- Retry callbacks
- Database-specific retry logic
"""

import pytest
import time
from unittest.mock import Mock, patch, call
from src.utils.retry import (
    retry_with_backoff,
    retry_database_operation,
    is_retryable_db_exception
)


class TestRetryWithBackoff:
    """Test retry_with_backoff decorator"""

    def test_success_on_first_attempt(self):
        """Test function succeeds on first attempt without retries"""
        mock_func = Mock(return_value="success")
        decorated = retry_with_backoff(max_retries=3)(mock_func)

        result = decorated()

        assert result == "success"
        assert mock_func.call_count == 1

    def test_success_after_retries(self):
        """Test function succeeds after transient failures"""
        mock_func = Mock(side_effect=[
            ConnectionError("Connection failed"),
            ConnectionError("Connection failed"),
            "success"
        ])
        decorated = retry_with_backoff(max_retries=3)(mock_func)

        result = decorated()

        assert result == "success"
        assert mock_func.call_count == 3

    def test_max_retries_exceeded(self):
        """Test function fails after max retries exceeded"""
        mock_func = Mock(side_effect=ConnectionError("Persistent error"))
        decorated = retry_with_backoff(max_retries=2)(mock_func)

        with pytest.raises(ConnectionError, match="Persistent error"):
            decorated()

        # Called 3 times: initial + 2 retries
        assert mock_func.call_count == 3

    def test_exponential_backoff_timing(self):
        """Test exponential backoff delays increase correctly"""
        mock_func = Mock(side_effect=[
            TimeoutError("Timeout"),
            TimeoutError("Timeout"),
            "success"
        ])

        with patch('time.sleep') as mock_sleep:
            decorated = retry_with_backoff(
                max_retries=2,
                base_delay=1.0,
                exponential_base=2.0,
                jitter=False
            )(mock_func)

            result = decorated()

            assert result == "success"
            # Should have 2 sleep calls (for 2 retries)
            assert mock_sleep.call_count == 2
            # First retry: 1.0 * 2^0 = 1.0
            # Second retry: 1.0 * 2^1 = 2.0
            delays = [call_args[0][0] for call_args in mock_sleep.call_args_list]
            assert delays[0] == 1.0
            assert delays[1] == 2.0

    def test_max_delay_cap(self):
        """Test delay is capped at max_delay"""
        mock_func = Mock(side_effect=[
            TimeoutError("Timeout"),
            TimeoutError("Timeout"),
            "success"
        ])

        with patch('time.sleep') as mock_sleep:
            decorated = retry_with_backoff(
                max_retries=5,
                base_delay=10.0,
                max_delay=15.0,
                exponential_base=2.0,
                jitter=False
            )(mock_func)

            result = decorated()

            # Check that all delays are capped at max_delay
            delays = [call_args[0][0] for call_args in mock_sleep.call_args_list]
            for delay in delays:
                assert delay <= 15.0

    def test_jitter_adds_randomness(self):
        """Test jitter adds randomness to delays"""
        mock_func = Mock(side_effect=[
            TimeoutError("Timeout"),
            TimeoutError("Timeout"),
            TimeoutError("Timeout"),
            "success"
        ])

        with patch('time.sleep') as mock_sleep:
            decorated = retry_with_backoff(
                max_retries=3,
                base_delay=1.0,
                exponential_base=2.0,
                jitter=True
            )(mock_func)

            result = decorated()

            # Get the actual delays
            delays = [call_args[0][0] for call_args in mock_sleep.call_args_list]

            # With jitter, delays should vary within ±25% of base delay
            # First delay base: 1.0, range: 0.75-1.25
            assert 0.75 <= delays[0] <= 1.25
            # Second delay base: 2.0, range: 1.5-2.5
            assert 1.5 <= delays[1] <= 2.5

    def test_retryable_exceptions_filter(self):
        """Test only specified exceptions are retried"""
        mock_func = Mock(side_effect=ValueError("Not retryable"))
        decorated = retry_with_backoff(
            max_retries=3,
            retryable_exceptions=(ConnectionError, TimeoutError)
        )(mock_func)

        # ValueError should not be retried
        with pytest.raises(ValueError, match="Not retryable"):
            decorated()

        # Should fail immediately without retries
        assert mock_func.call_count == 1

    def test_on_retry_callback_called(self):
        """Test on_retry callback is called for each retry"""
        mock_callback = Mock()
        mock_func = Mock(side_effect=[
            ConnectionError("Error 1"),
            ConnectionError("Error 2"),
            "success"
        ])

        with patch('time.sleep'):
            decorated = retry_with_backoff(
                max_retries=3,
                on_retry=mock_callback
            )(mock_func)

            result = decorated()

            assert result == "success"
            # Callback should be called twice (for 2 retries)
            assert mock_callback.call_count == 2

            # Check callback arguments: (attempt, exception, delay)
            first_call = mock_callback.call_args_list[0][0]
            assert first_call[0] == 1  # First retry attempt
            assert isinstance(first_call[1], ConnectionError)
            assert isinstance(first_call[2], float)  # delay

    def test_on_retry_callback_exception_handled(self):
        """Test exception in callback doesn't break retry logic"""
        mock_callback = Mock(side_effect=Exception("Callback error"))
        mock_func = Mock(side_effect=[
            ConnectionError("Error"),
            "success"
        ])

        with patch('time.sleep'):
            decorated = retry_with_backoff(
                max_retries=2,
                on_retry=mock_callback
            )(mock_func)

            # Should still succeed despite callback error
            result = decorated()
            assert result == "success"

    def test_function_with_arguments(self):
        """Test retry works with function arguments"""
        mock_func = Mock(side_effect=[
            ConnectionError("Error"),
            lambda x, y: x + y
        ])
        mock_func.side_effect = [
            ConnectionError("Error"),
            "result: arg1, arg2"
        ]

        with patch('time.sleep'):
            decorated = retry_with_backoff(max_retries=2)(mock_func)

            result = decorated("arg1", "arg2")

            assert result == "result: arg1, arg2"
            # Verify arguments passed correctly
            assert mock_func.call_count == 2


class TestIsRetryableDbException:
    """Test is_retryable_db_exception function"""

    def test_connection_errors_retryable(self):
        """Test connection errors are identified as retryable"""
        retryable_errors = [
            ConnectionError("Connection failed"),
            Exception("Lost connection to MySQL server"),
            Exception("Can't connect to database"),
            Exception("Connection refused"),
            Exception("Connection reset by peer"),
            Exception("Broken pipe"),
        ]

        for error in retryable_errors:
            assert is_retryable_db_exception(error) is True

    def test_timeout_errors_retryable(self):
        """Test timeout errors are identified as retryable"""
        retryable_errors = [
            TimeoutError("Operation timed out"),
            Exception("Connection timeout"),
            Exception("Lock wait timeout exceeded"),
            Exception("Query timeout"),
        ]

        for error in retryable_errors:
            assert is_retryable_db_exception(error) is True

    def test_deadlock_errors_retryable(self):
        """Test deadlock errors are identified as retryable"""
        retryable_errors = [
            Exception("Deadlock found when trying to get lock"),
            Exception("Lock wait timeout exceeded; try restarting transaction"),
        ]

        for error in retryable_errors:
            assert is_retryable_db_exception(error) is True

    def test_syntax_errors_not_retryable(self):
        """Test syntax errors are not retryable"""
        non_retryable_errors = [
            Exception("Syntax error in SQL statement"),
            ValueError("Invalid column name"),
            Exception("Table does not exist"),
            Exception("Column 'xyz' is not in list"),
        ]

        for error in non_retryable_errors:
            assert is_retryable_db_exception(error) is False

    def test_constraint_violations_not_retryable(self):
        """Test constraint violations are not retryable"""
        non_retryable_errors = [
            Exception("UNIQUE constraint failed"),
            Exception("FOREIGN KEY constraint failed"),
            Exception("NOT NULL constraint failed"),
            Exception("CHECK constraint violated"),
        ]

        for error in non_retryable_errors:
            assert is_retryable_db_exception(error) is False

    def test_case_insensitive_matching(self):
        """Test error matching is case-insensitive"""
        assert is_retryable_db_exception(Exception("CONNECTION FAILED")) is True
        assert is_retryable_db_exception(Exception("Timeout Error")) is True
        assert is_retryable_db_exception(Exception("DEADLOCK DETECTED")) is True


class TestRetryDatabaseOperation:
    """Test retry_database_operation decorator"""

    def test_retries_on_connection_error(self):
        """Test database operation retries on connection errors"""
        mock_func = Mock(side_effect=[
            ConnectionError("Connection lost"),
            "success"
        ])

        with patch('time.sleep'):
            decorated = retry_database_operation(max_retries=3)(mock_func)

            result = decorated()

            assert result == "success"
            assert mock_func.call_count == 2

    def test_retries_on_timeout_error(self):
        """Test database operation retries on timeout errors"""
        mock_func = Mock(side_effect=[
            TimeoutError("Query timeout"),
            "success"
        ])

        with patch('time.sleep'):
            decorated = retry_database_operation(max_retries=3)(mock_func)

            result = decorated()

            assert result == "success"
            assert mock_func.call_count == 2

    def test_does_not_retry_syntax_error(self):
        """Test database operation does not retry syntax errors"""
        mock_func = Mock(side_effect=Exception("Syntax error in SQL"))

        decorated = retry_database_operation(max_retries=3)(mock_func)

        with pytest.raises(Exception, match="Syntax error"):
            decorated()

        # Should fail immediately without retries
        assert mock_func.call_count == 1

    def test_does_not_retry_constraint_violation(self):
        """Test database operation does not retry constraint violations"""
        mock_func = Mock(side_effect=Exception("UNIQUE constraint failed"))

        decorated = retry_database_operation(max_retries=3)(mock_func)

        with pytest.raises(Exception, match="UNIQUE constraint"):
            decorated()

        # Should fail immediately
        assert mock_func.call_count == 1

    def test_callback_receives_retry_info(self):
        """Test callback receives retry information"""
        mock_callback = Mock()
        mock_func = Mock(side_effect=[
            ConnectionError("Connection failed"),
            "success"
        ])

        with patch('time.sleep'):
            decorated = retry_database_operation(
                max_retries=3,
                on_retry=mock_callback
            )(mock_func)

            result = decorated()

            assert result == "success"
            assert mock_callback.call_count == 1

            # Check callback received correct information
            attempt, exception, delay = mock_callback.call_args[0]
            assert attempt == 1
            assert isinstance(exception, ConnectionError)
            assert isinstance(delay, float)

    def test_max_retries_with_persistent_error(self):
        """Test max retries behavior with persistent connection error"""
        mock_func = Mock(side_effect=ConnectionError("Persistent connection issue"))

        with patch('time.sleep'):
            decorated = retry_database_operation(max_retries=2)(mock_func)

            with pytest.raises(ConnectionError, match="Persistent connection issue"):
                decorated()

            # Should be called 3 times: initial + 2 retries
            assert mock_func.call_count == 3

    def test_with_function_arguments_and_kwargs(self):
        """Test retry works with positional and keyword arguments"""
        call_count = [0]

        def database_query(table, limit=10):
            call_count[0] += 1
            if call_count[0] == 1:
                raise ConnectionError("Connection failed")
            return f"Query {table} with limit {limit}"

        with patch('time.sleep'):
            decorated = retry_database_operation(max_retries=2)(database_query)

            result = decorated("customers", limit=100)

            assert result == "Query customers with limit 100"
            assert call_count[0] == 2

    def test_preserves_function_metadata(self):
        """Test decorator preserves function name and docstring"""
        @retry_database_operation(max_retries=3)
        def my_database_function():
            """This is my database function"""
            return "result"

        assert my_database_function.__name__ == "my_database_function"
        assert my_database_function.__doc__ == "This is my database function"


class TestRetryIntegration:
    """Integration tests for retry logic"""

    def test_realistic_database_scenario(self):
        """Test realistic database scenario with transient failures"""
        attempts = []

        def flaky_database_operation():
            attempts.append(len(attempts) + 1)
            if len(attempts) < 3:
                raise ConnectionError(f"Transient connection error {len(attempts)}")
            return {"status": "success", "rows": 100}

        with patch('time.sleep'):
            decorated = retry_database_operation(max_retries=5, base_delay=0.5)(
                flaky_database_operation
            )

            result = decorated()

            assert result == {"status": "success", "rows": 100}
            assert len(attempts) == 3
            assert attempts == [1, 2, 3]

    def test_metrics_callback_integration(self):
        """Test integration with metrics callback"""
        metrics_data = []

        def metrics_callback(attempt, exception, delay):
            metrics_data.append({
                "attempt": attempt,
                "error_type": type(exception).__name__,
                "delay": delay
            })

        mock_func = Mock(side_effect=[
            ConnectionError("Error 1"),
            TimeoutError("Error 2"),
            "success"
        ])

        with patch('time.sleep'):
            decorated = retry_database_operation(
                max_retries=3,
                on_retry=metrics_callback
            )(mock_func)

            result = decorated()

            assert result == "success"
            assert len(metrics_data) == 2
            assert metrics_data[0]["error_type"] == "ConnectionError"
            assert metrics_data[1]["error_type"] == "TimeoutError"
            assert all(isinstance(m["delay"], float) for m in metrics_data)