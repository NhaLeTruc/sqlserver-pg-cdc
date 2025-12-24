"""
Unit tests for row-level reconciliation.

Tests row-by-row comparison, discrepancy detection, and repair script generation.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock

import pytest

from reconciliation.row_level import (
    RowDiscrepancy,
    RowLevelReconciler,
    generate_repair_script,
    _format_value,
    _generate_delete_sql,
    _generate_insert_sql,
    _generate_update_sql,
)


class TestRowDiscrepancy:
    """Test RowDiscrepancy dataclass."""

    def test_initialization(self):
        """Test basic initialization."""
        disc = RowDiscrepancy(
            table="users",
            primary_key={"id": 1},
            discrepancy_type="MISSING",
            source_data={"id": 1, "name": "John"},
            target_data=None,
        )

        assert disc.table == "users"
        assert disc.primary_key == {"id": 1}
        assert disc.discrepancy_type == "MISSING"
        assert disc.source_data == {"id": 1, "name": "John"}
        assert disc.target_data is None
        assert disc.modified_columns is None
        assert isinstance(disc.timestamp, datetime)

    def test_to_dict(self):
        """Test conversion to dictionary."""
        disc = RowDiscrepancy(
            table="users",
            primary_key={"id": 1},
            discrepancy_type="MODIFIED",
            source_data={"id": 1, "name": "John"},
            target_data={"id": 1, "name": "Jane"},
            modified_columns=["name"],
        )

        result = disc.to_dict()

        assert result["table"] == "users"
        assert result["primary_key"] == {"id": 1}
        assert result["discrepancy_type"] == "MODIFIED"
        assert result["source_data"] == {"id": 1, "name": "John"}
        assert result["target_data"] == {"id": 1, "name": "Jane"}
        assert result["modified_columns"] == ["name"]
        assert "timestamp" in result


class TestRowLevelReconciler:
    """Test RowLevelReconciler functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.source_cursor = Mock()
        self.target_cursor = Mock()
        self.source_cursor.__class__.__name__ = "PostgreSQLCursor"
        self.target_cursor.__class__.__name__ = "PostgreSQLCursor"
        # Initialize description as empty list to avoid iteration errors
        self.source_cursor.description = []
        self.target_cursor.description = []

    def test_get_db_type_postgresql(self):
        """Test database type detection for PostgreSQL."""
        reconciler = RowLevelReconciler(
            source_cursor=self.source_cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
        )

        db_type = reconciler._get_db_type(self.source_cursor)
        assert db_type == "postgresql"

    def test_get_db_type_sqlserver(self):
        """Test database type detection for SQL Server."""
        cursor = Mock()
        cursor.__class__.__name__ = "PyODBCCursor"

        reconciler = RowLevelReconciler(
            source_cursor=cursor,
            target_cursor=cursor,
            pk_columns=["id"],
        )

        db_type = reconciler._get_db_type(cursor)
        assert db_type == "sqlserver"

    def test_quote_identifier_postgresql(self):
        """Test identifier quoting for PostgreSQL."""
        reconciler = RowLevelReconciler(
            source_cursor=self.source_cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
        )

        quoted = reconciler._quote_identifier(self.source_cursor, "table_name")
        assert quoted == '"table_name"'

    def test_quote_identifier_sqlserver(self):
        """Test identifier quoting for SQL Server."""
        cursor = Mock()
        cursor.__class__.__name__ = "PyODBCCursor"

        reconciler = RowLevelReconciler(
            source_cursor=cursor,
            target_cursor=cursor,
            pk_columns=["id"],
        )

        quoted = reconciler._quote_identifier(cursor, "table_name")
        assert quoted == "[table_name]"

    def test_get_placeholder_postgresql(self):
        """Test parameter placeholder for PostgreSQL."""
        reconciler = RowLevelReconciler(
            source_cursor=self.source_cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
        )

        assert reconciler._get_placeholder(self.source_cursor, 0) == "$1"
        assert reconciler._get_placeholder(self.source_cursor, 1) == "$2"

    def test_get_placeholder_sqlserver(self):
        """Test parameter placeholder for SQL Server."""
        cursor = Mock()
        cursor.__class__.__name__ = "PyODBCCursor"

        reconciler = RowLevelReconciler(
            source_cursor=cursor,
            target_cursor=cursor,
            pk_columns=["id"],
        )

        assert reconciler._get_placeholder(cursor, 0) == "?"
        assert reconciler._get_placeholder(cursor, 1) == "?"

    def test_pk_tuple_to_dict_single_column(self):
        """Test PK tuple to dict conversion with single column."""
        reconciler = RowLevelReconciler(
            source_cursor=self.source_cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
        )

        result = reconciler._pk_tuple_to_dict((123,))
        assert result == {"id": 123}

    def test_pk_tuple_to_dict_multiple_columns(self):
        """Test PK tuple to dict conversion with multiple columns."""
        reconciler = RowLevelReconciler(
            source_cursor=self.source_cursor,
            target_cursor=self.target_cursor,
            pk_columns=["user_id", "org_id"],
        )

        result = reconciler._pk_tuple_to_dict((123, 456))
        assert result == {"user_id": 123, "org_id": 456}

    def test_compare_rows_identical(self):
        """Test comparing identical rows."""
        reconciler = RowLevelReconciler(
            source_cursor=self.source_cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
        )

        source = {"id": 1, "name": "John", "age": 30}
        target = {"id": 1, "name": "John", "age": 30}

        modified = reconciler._compare_rows(source, target)
        assert modified == []

    def test_compare_rows_modified_string(self):
        """Test comparing rows with modified string column."""
        reconciler = RowLevelReconciler(
            source_cursor=self.source_cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
        )

        source = {"id": 1, "name": "John", "age": 30}
        target = {"id": 1, "name": "Jane", "age": 30}

        modified = reconciler._compare_rows(source, target)
        assert modified == ["name"]

    def test_compare_rows_modified_number(self):
        """Test comparing rows with modified numeric column."""
        reconciler = RowLevelReconciler(
            source_cursor=self.source_cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
        )

        source = {"id": 1, "name": "John", "age": 30}
        target = {"id": 1, "name": "John", "age": 31}

        modified = reconciler._compare_rows(source, target)
        assert modified == ["age"]

    def test_compare_rows_float_tolerance(self):
        """Test float comparison with tolerance."""
        reconciler = RowLevelReconciler(
            source_cursor=self.source_cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
            float_tolerance=0.01,
        )

        source = {"id": 1, "price": 10.001}
        target = {"id": 1, "price": 10.002}

        # Within tolerance
        modified = reconciler._compare_rows(source, target)
        assert modified == []

        # Outside tolerance
        source["price"] = 10.0
        target["price"] = 10.02
        modified = reconciler._compare_rows(source, target)
        assert modified == ["price"]

    def test_compare_rows_null_handling(self):
        """Test NULL value handling in row comparison."""
        reconciler = RowLevelReconciler(
            source_cursor=self.source_cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
        )

        # Both NULL
        source = {"id": 1, "name": None}
        target = {"id": 1, "name": None}
        modified = reconciler._compare_rows(source, target)
        assert modified == []

        # Source NULL, target not
        source = {"id": 1, "name": None}
        target = {"id": 1, "name": "John"}
        modified = reconciler._compare_rows(source, target)
        assert modified == ["name"]

        # Source not NULL, target NULL
        source = {"id": 1, "name": "John"}
        target = {"id": 1, "name": None}
        modified = reconciler._compare_rows(source, target)
        assert modified == ["name"]

    def test_compare_rows_whitespace_trimming(self):
        """Test string whitespace trimming in comparison."""
        reconciler = RowLevelReconciler(
            source_cursor=self.source_cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
        )

        source = {"id": 1, "name": "John  "}
        target = {"id": 1, "name": "  John"}

        # Should be considered equal after trimming
        modified = reconciler._compare_rows(source, target)
        assert modified == []

    def test_get_all_primary_keys(self):
        """Test fetching all primary keys."""
        cursor = Mock()
        cursor.__class__.__name__ = "PostgreSQLCursor"
        cursor.fetchall.return_value = [(1,), (2,), (3,)]

        reconciler = RowLevelReconciler(
            source_cursor=cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
        )

        pks = reconciler._get_all_primary_keys(cursor, "users")

        assert pks == {(1,), (2,), (3,)}
        cursor.execute.assert_called_once()

    def test_get_all_primary_keys_composite(self):
        """Test fetching composite primary keys."""
        cursor = Mock()
        cursor.__class__.__name__ = "PostgreSQLCursor"
        cursor.fetchall.return_value = [(1, 10), (2, 20), (3, 30)]

        reconciler = RowLevelReconciler(
            source_cursor=cursor,
            target_cursor=self.target_cursor,
            pk_columns=["user_id", "org_id"],
        )

        pks = reconciler._get_all_primary_keys(cursor, "user_orgs")

        assert pks == {(1, 10), (2, 20), (3, 30)}

    def test_get_row_data(self):
        """Test fetching row data by primary key."""
        cursor = Mock()
        cursor.__class__.__name__ = "PostgreSQLCursor"
        cursor.fetchone.return_value = (1, "John", 30)
        cursor.description = [("id",), ("name",), ("age",)]

        reconciler = RowLevelReconciler(
            source_cursor=cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
        )

        data = reconciler._get_row_data(cursor, "users", (1,))

        assert data == {"id": 1, "name": "John", "age": 30}
        cursor.execute.assert_called_once()

    def test_reconcile_table_missing_rows(self):
        """Test reconciling with missing rows."""
        # Source has rows 1, 2, 3
        # Target has rows 1, 2
        # Expected: row 3 is missing

        self.source_cursor.fetchall.return_value = [(1,), (2,), (3,)]
        self.target_cursor.fetchall.return_value = [(1,), (2,)]

        # Row data for missing row and common rows
        # Order: missing rows first, then common rows (source then target for each)
        self.source_cursor.fetchone.side_effect = [
            (3, "Alice", 25),  # PK=3 (missing) - processed first
            (1, "User1", 20),  # PK=1 (common) - source side
            (2, "User2", 30),  # PK=2 (common) - source side
        ]
        self.source_cursor.description = [("id",), ("name",), ("age",)]

        self.target_cursor.fetchone.side_effect = [
            (1, "User1", 20),  # PK=1 (common) - target side
            (2, "User2", 30),  # PK=2 (common) - target side
        ]
        self.target_cursor.description = [("id",), ("name",), ("age",)]

        reconciler = RowLevelReconciler(
            source_cursor=self.source_cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
        )

        discrepancies = reconciler.reconcile_table("users", "users")

        # Should find 1 missing row
        missing = [d for d in discrepancies if d.discrepancy_type == "MISSING"]
        assert len(missing) == 1
        assert missing[0].primary_key == {"id": 3}
        assert missing[0].source_data == {"id": 3, "name": "Alice", "age": 25}

    def test_reconcile_table_extra_rows(self):
        """Test reconciling with extra rows."""
        # Source has rows 1, 2
        # Target has rows 1, 2, 3
        # Expected: row 3 is extra

        self.source_cursor.fetchall.return_value = [(1,), (2,)]
        self.target_cursor.fetchall.return_value = [(1,), (2,), (3,)]

        # Row data for extra row and common rows
        # Order: extra rows first, then common rows (source then target for each)
        self.source_cursor.fetchone.side_effect = [
            (1, "User1", 20),  # PK=1 (common) - source side
            (2, "User2", 30),  # PK=2 (common) - source side
        ]
        self.source_cursor.description = [("id",), ("name",), ("age",)]

        self.target_cursor.fetchone.side_effect = [
            (3, "Bob", 35),    # PK=3 (extra) - processed first
            (1, "User1", 20),  # PK=1 (common) - target side
            (2, "User2", 30),  # PK=2 (common) - target side
        ]
        self.target_cursor.description = [("id",), ("name",), ("age",)]

        reconciler = RowLevelReconciler(
            source_cursor=self.source_cursor,
            target_cursor=self.target_cursor,
            pk_columns=["id"],
        )

        discrepancies = reconciler.reconcile_table("users", "users")

        # Should find 1 extra row
        extra = [d for d in discrepancies if d.discrepancy_type == "EXTRA"]
        assert len(extra) == 1
        assert extra[0].primary_key == {"id": 3}
        assert extra[0].target_data == {"id": 3, "name": "Bob", "age": 35}


class TestFormatValue:
    """Test value formatting for SQL."""

    def test_format_null(self):
        """Test NULL value formatting."""
        assert _format_value(None, "postgresql") == "NULL"
        assert _format_value(None, "sqlserver") == "NULL"

    def test_format_string(self):
        """Test string value formatting."""
        assert _format_value("test", "postgresql") == "'test'"
        assert _format_value("test", "sqlserver") == "'test'"

    def test_format_string_with_quotes(self):
        """Test string with single quotes."""
        assert _format_value("it's", "postgresql") == "'it''s'"
        assert _format_value("it's", "sqlserver") == "'it''s'"

    def test_format_integer(self):
        """Test integer formatting."""
        assert _format_value(42, "postgresql") == "42"
        assert _format_value(42, "sqlserver") == "42"

    def test_format_float(self):
        """Test float formatting."""
        assert _format_value(3.14, "postgresql") == "3.14"
        assert _format_value(3.14, "sqlserver") == "3.14"

    def test_format_boolean_postgresql(self):
        """Test boolean formatting for PostgreSQL."""
        assert _format_value(True, "postgresql") == "TRUE"
        assert _format_value(False, "postgresql") == "FALSE"

    def test_format_boolean_sqlserver(self):
        """Test boolean formatting for SQL Server."""
        assert _format_value(True, "sqlserver") == "1"
        assert _format_value(False, "sqlserver") == "0"

    def test_format_datetime(self):
        """Test datetime formatting."""
        dt = datetime(2025, 1, 15, 10, 30, 0)

        pg_result = _format_value(dt, "postgresql")
        assert "'2025-01-15" in pg_result

        sql_result = _format_value(dt, "sqlserver")
        assert "'2025-01-15" in sql_result


class TestGenerateInsertSQL:
    """Test INSERT statement generation."""

    def test_generate_insert_postgresql(self):
        """Test INSERT generation for PostgreSQL."""
        data = {"id": 1, "name": "John", "age": 30}
        sql = _generate_insert_sql("users", data, "postgresql")

        assert 'INSERT INTO "users"' in sql
        assert '"id"' in sql
        assert '"name"' in sql
        assert '"age"' in sql
        assert "VALUES" in sql
        assert "1" in sql
        assert "'John'" in sql
        assert "30" in sql

    def test_generate_insert_sqlserver(self):
        """Test INSERT generation for SQL Server."""
        data = {"id": 1, "name": "John", "age": 30}
        sql = _generate_insert_sql("users", data, "sqlserver")

        assert "INSERT INTO [users]" in sql
        assert "[id]" in sql
        assert "[name]" in sql
        assert "[age]" in sql
        assert "VALUES" in sql

    def test_generate_insert_empty_data(self):
        """Test INSERT with no data."""
        sql = _generate_insert_sql("users", {}, "postgresql")
        assert "Cannot generate INSERT" in sql


class TestGenerateDeleteSQL:
    """Test DELETE statement generation."""

    def test_generate_delete_postgresql(self):
        """Test DELETE generation for PostgreSQL."""
        pk = {"id": 1}
        sql = _generate_delete_sql("users", pk, "postgresql")

        assert 'DELETE FROM "users"' in sql
        assert '"id" = 1' in sql

    def test_generate_delete_sqlserver(self):
        """Test DELETE generation for SQL Server."""
        pk = {"id": 1}
        sql = _generate_delete_sql("users", pk, "sqlserver")

        assert "DELETE FROM [users]" in sql
        assert "[id] = 1" in sql

    def test_generate_delete_composite_key(self):
        """Test DELETE with composite primary key."""
        pk = {"user_id": 1, "org_id": 10}
        sql = _generate_delete_sql("user_orgs", pk, "postgresql")

        assert 'DELETE FROM "user_orgs"' in sql
        assert '"user_id" = 1' in sql
        assert '"org_id" = 10' in sql
        assert "AND" in sql

    def test_generate_delete_empty_pk(self):
        """Test DELETE with no primary key."""
        sql = _generate_delete_sql("users", {}, "postgresql")
        assert "Cannot generate DELETE" in sql


class TestGenerateUpdateSQL:
    """Test UPDATE statement generation."""

    def test_generate_update_postgresql(self):
        """Test UPDATE generation for PostgreSQL."""
        pk = {"id": 1}
        data = {"id": 1, "name": "Jane", "age": 31}
        modified_cols = ["name", "age"]

        sql = _generate_update_sql("users", pk, data, modified_cols, "postgresql")

        assert 'UPDATE "users"' in sql
        assert 'SET "name" = ' in sql
        assert 'SET' in sql and '"age" = ' in sql
        assert 'WHERE "id" = 1' in sql

    def test_generate_update_sqlserver(self):
        """Test UPDATE generation for SQL Server."""
        pk = {"id": 1}
        data = {"id": 1, "name": "Jane", "age": 31}
        modified_cols = ["name", "age"]

        sql = _generate_update_sql("users", pk, data, modified_cols, "sqlserver")

        assert "UPDATE [users]" in sql
        assert "SET [name] = " in sql
        assert "[age] = " in sql
        assert "WHERE [id] = 1" in sql

    def test_generate_update_empty_modified_cols(self):
        """Test UPDATE with no modified columns."""
        pk = {"id": 1}
        data = {"id": 1, "name": "Jane"}

        sql = _generate_update_sql("users", pk, data, [], "postgresql")
        assert "Cannot generate UPDATE" in sql


class TestGenerateRepairScript:
    """Test repair script generation."""

    def test_generate_repair_script_all_types(self):
        """Test repair script with all discrepancy types."""
        discrepancies = [
            RowDiscrepancy(
                table="users",
                primary_key={"id": 1},
                discrepancy_type="MISSING",
                source_data={"id": 1, "name": "John"},
                target_data=None,
            ),
            RowDiscrepancy(
                table="users",
                primary_key={"id": 2},
                discrepancy_type="EXTRA",
                source_data=None,
                target_data={"id": 2, "name": "Bob"},
            ),
            RowDiscrepancy(
                table="users",
                primary_key={"id": 3},
                discrepancy_type="MODIFIED",
                source_data={"id": 3, "name": "Jane"},
                target_data={"id": 3, "name": "Joan"},
                modified_columns=["name"],
            ),
        ]

        script = generate_repair_script(discrepancies, "users", "postgresql")

        assert "Repair script for users" in script
        assert "Total discrepancies: 3" in script
        assert "BEGIN;" in script
        assert "COMMIT;" in script
        assert "INSERT INTO" in script
        assert "DELETE FROM" in script
        assert "UPDATE" in script
        assert "1 missing row" in script
        assert "1 extra row" in script
        assert "1 modified row" in script

    def test_generate_repair_script_sqlserver(self):
        """Test repair script for SQL Server."""
        discrepancies = [
            RowDiscrepancy(
                table="users",
                primary_key={"id": 1},
                discrepancy_type="MISSING",
                source_data={"id": 1, "name": "John"},
                target_data=None,
            ),
        ]

        script = generate_repair_script(discrepancies, "users", "sqlserver")

        assert "BEGIN TRANSACTION;" in script
        assert "COMMIT;" in script
        assert "INSERT INTO [users]" in script

    def test_generate_repair_script_empty(self):
        """Test repair script with no discrepancies."""
        script = generate_repair_script([], "users", "postgresql")

        assert "Total discrepancies: 0" in script
        assert "BEGIN;" in script
        assert "COMMIT;" in script
