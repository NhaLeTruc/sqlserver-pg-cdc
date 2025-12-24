"""
Unit tests for incremental checksum calculation.

Tests state management, incremental vs full checksums, and chunked processing.
"""

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from reconciliation.incremental import (
    IncrementalChecksumTracker,
    calculate_incremental_checksum,
    calculate_checksum_chunked,
    _calculate_delta_checksum,
    _calculate_full_checksum,
    _get_db_type,
    _quote_identifier,
)


class TestIncrementalChecksumTracker:
    """Test IncrementalChecksumTracker functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.tracker = IncrementalChecksumTracker(state_dir=self.temp_dir)

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization(self):
        """Test tracker initialization."""
        assert self.tracker.state_dir == Path(self.temp_dir)
        assert self.tracker.state_dir.exists()

    def test_save_and_load_state(self):
        """Test saving and loading checksum state."""
        table = "users"
        checksum = "abc123"
        row_count = 1000
        timestamp = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        # Save state
        self.tracker.save_checksum_state(table, checksum, row_count, timestamp, "full")

        # Load timestamp
        loaded_timestamp = self.tracker.get_last_checksum_timestamp(table)
        assert loaded_timestamp == timestamp

        # Load checksum
        loaded_checksum = self.tracker.get_last_checksum(table)
        assert loaded_checksum == checksum

    def test_get_last_checksum_timestamp_no_state(self):
        """Test getting timestamp when no state exists."""
        result = self.tracker.get_last_checksum_timestamp("nonexistent")
        assert result is None

    def test_get_last_checksum_no_state(self):
        """Test getting checksum when no state exists."""
        result = self.tracker.get_last_checksum("nonexistent")
        assert result is None

    def test_save_state_default_timestamp(self):
        """Test saving state with default timestamp."""
        before = datetime.now(timezone.utc)

        self.tracker.save_checksum_state("users", "checksum", 100)

        after = datetime.now(timezone.utc)

        loaded_timestamp = self.tracker.get_last_checksum_timestamp("users")
        assert before <= loaded_timestamp <= after

    def test_clear_state(self):
        """Test clearing saved state."""
        # Save state
        self.tracker.save_checksum_state("users", "checksum", 100)

        # Verify it exists
        assert self.tracker.get_last_checksum("users") is not None

        # Clear it
        self.tracker.clear_state("users")

        # Verify it's gone
        assert self.tracker.get_last_checksum("users") is None

    def test_clear_state_nonexistent(self):
        """Test clearing state that doesn't exist (should not error)."""
        # Should not raise exception
        self.tracker.clear_state("nonexistent")

    def test_list_tracked_tables(self):
        """Test listing all tracked tables."""
        # Save state for multiple tables
        self.tracker.save_checksum_state("users", "check1", 100)
        self.tracker.save_checksum_state("orders", "check2", 200)
        self.tracker.save_checksum_state("products", "check3", 300)

        # List tables
        tables = self.tracker.list_tracked_tables()

        assert len(tables) == 3
        assert "users" in tables
        assert "orders" in tables
        assert "products" in tables

    def test_list_tracked_tables_empty(self):
        """Test listing when no tables tracked."""
        tables = self.tracker.list_tracked_tables()
        assert tables == []

    def test_state_file_path_sanitization(self):
        """Test that table names are sanitized for filesystem."""
        # Table name with special characters
        table = "schema/table"

        self.tracker.save_checksum_state(table, "checksum", 100)

        # Should save successfully
        loaded = self.tracker.get_last_checksum(table)
        assert loaded == "checksum"

    def test_corrupted_state_file(self):
        """Test handling of corrupted state file."""
        # Create corrupted state file
        state_file = self.tracker._get_state_file("users")
        with open(state_file, "w") as f:
            f.write("invalid json{")

        # Should return None instead of raising exception
        assert self.tracker.get_last_checksum_timestamp("users") is None
        assert self.tracker.get_last_checksum("users") is None


class TestGetDbType:
    """Test database type detection."""

    def test_detect_postgresql(self):
        """Test PostgreSQL detection."""
        cursor = Mock()
        cursor.__class__.__name__ = "PostgreSQLCursor"

        db_type = _get_db_type(cursor)
        assert db_type == "postgresql"

    def test_detect_sqlserver(self):
        """Test SQL Server detection."""
        cursor = Mock()
        cursor.__class__.__name__ = "PyODBCCursor"

        db_type = _get_db_type(cursor)
        assert db_type == "sqlserver"

    def test_detect_unknown(self):
        """Test unknown database type."""
        cursor = Mock()
        cursor.__class__.__name__ = "UnknownCursor"

        db_type = _get_db_type(cursor)
        assert db_type == "unknown"


class TestQuoteIdentifier:
    """Test identifier quoting."""

    def test_quote_postgresql(self):
        """Test PostgreSQL identifier quoting."""
        cursor = Mock()
        quoted = _quote_identifier(cursor, "table_name", "postgresql")
        assert quoted == '"table_name"'

    def test_quote_sqlserver(self):
        """Test SQL Server identifier quoting."""
        cursor = Mock()
        quoted = _quote_identifier(cursor, "table_name", "sqlserver")
        assert quoted == "[table_name]"

    def test_quote_unknown(self):
        """Test unknown database type (no quoting)."""
        cursor = Mock()
        quoted = _quote_identifier(cursor, "table_name", "unknown")
        assert quoted == "table_name"


class TestCalculateFullChecksum:
    """Test full checksum calculation."""

    def test_calculate_full_checksum(self):
        """Test calculating checksum for all rows."""
        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter([
            (1, "Alice", 25),
            (2, "Bob", 30),
            (3, "Charlie", 35),
        ]))

        checksum, row_count = _calculate_full_checksum(
            cursor, '"users"', '"id"', "postgresql"
        )

        assert isinstance(checksum, str)
        assert len(checksum) == 64  # SHA256 hex digest
        assert row_count == 3

        cursor.execute.assert_called_once()

    def test_calculate_full_checksum_empty_table(self):
        """Test checksum for empty table."""
        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter([]))

        checksum, row_count = _calculate_full_checksum(
            cursor, '"users"', '"id"', "postgresql"
        )

        assert isinstance(checksum, str)
        assert row_count == 0

    def test_calculate_full_checksum_with_nulls(self):
        """Test checksum with NULL values."""
        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter([
            (1, "Alice", None),
            (2, None, 30),
            (3, "Charlie", 35),
        ]))

        checksum, row_count = _calculate_full_checksum(
            cursor, '"users"', '"id"', "postgresql"
        )

        assert isinstance(checksum, str)
        assert row_count == 3


class TestCalculateDeltaChecksum:
    """Test incremental checksum calculation."""

    def test_calculate_delta_checksum_postgresql(self):
        """Test delta checksum for PostgreSQL."""
        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter([
            (1, "Alice", 25),
            (2, "Bob", 30),
        ]))

        since = datetime(2025, 1, 1, tzinfo=timezone.utc)

        checksum, row_count = _calculate_delta_checksum(
            cursor, '"users"', '"id"', '"updated_at"', since, "postgresql"
        )

        assert isinstance(checksum, str)
        assert row_count == 2

        # Verify query used parameterized placeholder
        cursor.execute.assert_called_once()
        call_args = cursor.execute.call_args
        assert "%s" in call_args[0][0]
        assert call_args[0][1] == (since,)

    def test_calculate_delta_checksum_sqlserver(self):
        """Test delta checksum for SQL Server."""
        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter([(1, "Alice", 25)]))

        since = datetime(2025, 1, 1, tzinfo=timezone.utc)

        checksum, row_count = _calculate_delta_checksum(
            cursor, "[users]", "[id]", "[updated_at]", since, "sqlserver"
        )

        assert isinstance(checksum, str)
        assert row_count == 1

        # Verify query used question mark placeholder
        call_args = cursor.execute.call_args
        assert "?" in call_args[0][0]

    def test_calculate_delta_checksum_no_changes(self):
        """Test delta checksum when no rows changed."""
        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter([]))

        since = datetime(2025, 1, 1, tzinfo=timezone.utc)

        checksum, row_count = _calculate_delta_checksum(
            cursor, '"users"', '"id"', '"updated_at"', since, "postgresql"
        )

        assert isinstance(checksum, str)
        assert row_count == 0


class TestCalculateIncrementalChecksum:
    """Test main incremental checksum function."""

    def test_full_checksum_mode(self):
        """Test full checksum when no previous timestamp."""
        cursor = Mock()
        cursor.__class__.__name__ = "PostgreSQLCursor"
        cursor.__iter__ = Mock(return_value=iter([
            (1, "Alice", 25),
            (2, "Bob", 30),
        ]))

        checksum, row_count = calculate_incremental_checksum(
            cursor, "users", "id", last_checksum_time=None
        )

        assert isinstance(checksum, str)
        assert row_count == 2

    def test_incremental_mode(self):
        """Test incremental checksum with previous timestamp."""
        cursor = Mock()
        cursor.__class__.__name__ = "PostgreSQLCursor"
        cursor.__iter__ = Mock(return_value=iter([(3, "Charlie", 35)]))

        last_time = datetime(2025, 1, 1, tzinfo=timezone.utc)

        checksum, row_count = calculate_incremental_checksum(
            cursor, "users", "id", last_checksum_time=last_time
        )

        assert isinstance(checksum, str)
        assert row_count == 1

    def test_with_tracker(self):
        """Test checksum calculation with state tracking."""
        temp_dir = tempfile.mkdtemp()
        tracker = IncrementalChecksumTracker(state_dir=temp_dir)

        cursor = Mock()
        cursor.__class__.__name__ = "PostgreSQLCursor"
        cursor.__iter__ = Mock(return_value=iter([(1, "Alice", 25)]))

        checksum, row_count = calculate_incremental_checksum(
            cursor, "users", "id", tracker=tracker
        )

        # Verify state was saved
        saved_checksum = tracker.get_last_checksum("users")
        assert saved_checksum == checksum

        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)


class TestCalculateChecksumChunked:
    """Test chunked checksum calculation."""

    def test_single_chunk(self):
        """Test checksum with data fitting in single chunk."""
        cursor = Mock()
        cursor.__class__.__name__ = "PostgreSQLCursor"

        # Single chunk of 3 rows
        cursor.fetchall.return_value = [
            (1, "Alice", 25),
            (2, "Bob", 30),
            (3, "Charlie", 35),
        ]

        checksum = calculate_checksum_chunked(cursor, "users", "id", chunk_size=10)

        assert isinstance(checksum, str)
        assert len(checksum) == 64

    def test_multiple_chunks(self):
        """Test checksum with data spanning multiple chunks."""
        cursor = Mock()
        cursor.__class__.__name__ = "PostgreSQLCursor"

        # Simulate 2 chunks
        cursor.fetchall.side_effect = [
            [(1, "Alice", 25), (2, "Bob", 30)],  # First chunk (size=2)
            [(3, "Charlie", 35)],  # Second chunk (size=1, last)
        ]

        checksum = calculate_checksum_chunked(cursor, "users", "id", chunk_size=2)

        assert isinstance(checksum, str)
        # Should have been called twice (2 chunks)
        assert cursor.execute.call_count == 2

    def test_empty_table(self):
        """Test chunked checksum for empty table."""
        cursor = Mock()
        cursor.__class__.__name__ = "PostgreSQLCursor"
        cursor.fetchall.return_value = []

        checksum = calculate_checksum_chunked(cursor, "users", "id", chunk_size=10)

        assert isinstance(checksum, str)

    def test_exact_chunk_boundary(self):
        """Test when data ends exactly on chunk boundary."""
        cursor = Mock()
        cursor.__class__.__name__ = "PostgreSQLCursor"

        # Exactly 2 chunks of size 2
        cursor.fetchall.side_effect = [
            [(1, "Alice", 25), (2, "Bob", 30)],  # First chunk
            [(3, "Charlie", 35), (4, "David", 40)],  # Second chunk
            [],  # No more data
        ]

        checksum = calculate_checksum_chunked(cursor, "users", "id", chunk_size=2)

        assert isinstance(checksum, str)
        # Should have been called 3 times (2 full chunks + empty check)
        assert cursor.execute.call_count == 3


class TestDeterministicChecksums:
    """Test that checksums are deterministic."""

    def test_same_data_same_checksum(self):
        """Test that same data produces same checksum."""
        cursor1 = Mock()
        cursor1.__iter__ = Mock(return_value=iter([
            (1, "Alice", 25),
            (2, "Bob", 30),
        ]))

        cursor2 = Mock()
        cursor2.__iter__ = Mock(return_value=iter([
            (1, "Alice", 25),
            (2, "Bob", 30),
        ]))

        checksum1, _ = _calculate_full_checksum(cursor1, "users", "id", "postgresql")
        checksum2, _ = _calculate_full_checksum(cursor2, "users", "id", "postgresql")

        assert checksum1 == checksum2

    def test_different_data_different_checksum(self):
        """Test that different data produces different checksum."""
        cursor1 = Mock()
        cursor1.__iter__ = Mock(return_value=iter([(1, "Alice", 25)]))

        cursor2 = Mock()
        cursor2.__iter__ = Mock(return_value=iter([(1, "Bob", 25)]))

        checksum1, _ = _calculate_full_checksum(cursor1, "users", "id", "postgresql")
        checksum2, _ = _calculate_full_checksum(cursor2, "users", "id", "postgresql")

        assert checksum1 != checksum2

    def test_order_matters(self):
        """Test that row order affects checksum."""
        cursor1 = Mock()
        cursor1.__iter__ = Mock(return_value=iter([
            (1, "Alice", 25),
            (2, "Bob", 30),
        ]))

        cursor2 = Mock()
        cursor2.__iter__ = Mock(return_value=iter([
            (2, "Bob", 30),
            (1, "Alice", 25),
        ]))

        checksum1, _ = _calculate_full_checksum(cursor1, "users", "id", "postgresql")
        checksum2, _ = _calculate_full_checksum(cursor2, "users", "id", "postgresql")

        # Different order should produce different checksum
        assert checksum1 != checksum2
