"""
Unit tests for SQL injection prevention in compare.py

Tests database-native identifier quoting mechanisms to prevent SQL injection:
- PostgreSQL: psycopg2.sql.Identifier
- SQL Server: Bracket quoting [schema].[table]
"""

import pytest
from unittest.mock import Mock, MagicMock
from src.reconciliation.compare import (
    _quote_postgres_identifier,
    _quote_sqlserver_identifier,
    _get_db_type,
    _quote_identifier,
    get_row_count,
    calculate_checksum
)


class TestPostgreSQLQuoting:
    """Test PostgreSQL identifier quoting"""

    def test_quote_simple_table_name(self):
        """Test quoting simple table name"""
        result = _quote_postgres_identifier("customers")
        # psycopg2 sql.Identifier quotes with double quotes
        assert '"customers"' in result

    def test_quote_schema_table(self):
        """Test quoting schema.table format"""
        result = _quote_postgres_identifier("public.customers")
        # Should quote schema and table separately
        assert '"public"' in result
        assert '"customers"' in result

    def test_reject_sql_injection_attempt(self):
        """Test rejection of SQL injection attempts"""
        malicious_inputs = [
            "customers; DROP TABLE users--",
            "customers' OR '1'='1",
            "customers/**/UNION/**/SELECT",
            "../etc/passwd",
            "customers\x00malicious"
        ]

        for malicious_input in malicious_inputs:
            with pytest.raises(ValueError, match="Invalid identifier format"):
                _quote_postgres_identifier(malicious_input)

    def test_reject_multiple_dots(self):
        """Test rejection of invalid schema.table.extra format"""
        with pytest.raises(ValueError, match="Invalid schema.table format"):
            _quote_postgres_identifier("public.dbo.customers")

    def test_allow_underscores_and_numbers(self):
        """Test that valid identifiers with underscores and numbers are allowed"""
        result = _quote_postgres_identifier("table_123")
        assert '"table_123"' in result


class TestSQLServerQuoting:
    """Test SQL Server identifier quoting"""

    def test_quote_simple_table_name(self):
        """Test quoting simple table name"""
        result = _quote_sqlserver_identifier("customers")
        assert result == "[customers]"

    def test_quote_schema_table(self):
        """Test quoting schema.table format"""
        result = _quote_sqlserver_identifier("dbo.customers")
        assert result == "[dbo].[customers]"

    def test_quote_already_bracketed_name(self):
        """Test quoting already bracketed identifiers"""
        result = _quote_sqlserver_identifier("[dbo].[customers]")
        # Should remove existing brackets and re-quote
        assert result == "[dbo].[customers]"

    def test_reject_sql_injection_attempt(self):
        """Test rejection of SQL injection attempts"""
        malicious_inputs = [
            "customers; DROP TABLE users--",
            "customers' OR '1'='1",
            "customers/**/UNION/**/SELECT",
            "../etc/passwd",
            "customers\x00malicious"
        ]

        for malicious_input in malicious_inputs:
            with pytest.raises(ValueError, match="Invalid identifier format"):
                _quote_sqlserver_identifier(malicious_input)

    def test_reject_multiple_dots(self):
        """Test rejection of invalid schema.table.extra format"""
        with pytest.raises(ValueError, match="Invalid schema.table format"):
            _quote_sqlserver_identifier("master.dbo.customers")

    def test_allow_underscores_and_numbers(self):
        """Test that valid identifiers with underscores and numbers are allowed"""
        result = _quote_sqlserver_identifier("table_123")
        assert result == "[table_123]"


class TestDatabaseTypeDetection:
    """Test database type detection from cursor"""

    def test_detect_postgresql_cursor(self):
        """Test detection of PostgreSQL cursor"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'psycopg2.extensions'

        result = _get_db_type(mock_cursor)
        assert result == 'postgresql'

    def test_detect_pyodbc_cursor(self):
        """Test detection of pyodbc (SQL Server) cursor"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'pyodbc'

        result = _get_db_type(mock_cursor)
        assert result == 'sqlserver'

    def test_default_to_sqlserver(self):
        """Test default to SQL Server for unknown cursor types"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'unknown_db_driver'

        result = _get_db_type(mock_cursor)
        assert result == 'sqlserver'


class TestQuoteIdentifier:
    """Test database-specific identifier quoting"""

    def test_quote_postgresql_identifier(self):
        """Test PostgreSQL identifier quoting via _quote_identifier"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'psycopg2.extensions'

        result = _quote_identifier(mock_cursor, "customers")
        assert '"customers"' in result

    def test_quote_sqlserver_identifier(self):
        """Test SQL Server identifier quoting via _quote_identifier"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'pyodbc'

        result = _quote_identifier(mock_cursor, "customers")
        assert result == "[customers]"

    def test_quote_schema_table_postgresql(self):
        """Test schema.table quoting for PostgreSQL"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'psycopg2.extensions'

        result = _quote_identifier(mock_cursor, "public.customers")
        assert '"public"' in result
        assert '"customers"' in result

    def test_quote_schema_table_sqlserver(self):
        """Test schema.table quoting for SQL Server"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'pyodbc'

        result = _quote_identifier(mock_cursor, "dbo.customers")
        assert result == "[dbo].[customers]"


class TestGetRowCountSecurity:
    """Test SQL injection prevention in get_row_count"""

    def test_legitimate_table_name(self):
        """Test get_row_count with legitimate table name"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'pyodbc'
        mock_cursor.fetchone.return_value = [100]

        result = get_row_count(mock_cursor, "customers")

        assert result == 100
        # Verify query uses bracketed identifier
        call_args = mock_cursor.execute.call_args[0][0]
        assert "[customers]" in call_args

    def test_schema_qualified_table_name(self):
        """Test get_row_count with schema.table format"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'pyodbc'
        mock_cursor.fetchone.return_value = [250]

        result = get_row_count(mock_cursor, "dbo.orders")

        assert result == 250
        # Verify query uses bracketed identifiers
        call_args = mock_cursor.execute.call_args[0][0]
        assert "[dbo].[orders]" in call_args

    def test_reject_sql_injection_in_table_name(self):
        """Test get_row_count rejects SQL injection attempts"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'pyodbc'

        malicious_inputs = [
            "customers; DROP TABLE users--",
            "customers' OR '1'='1",
            "customers UNION SELECT * FROM passwords",
            "customers/**/WHERE/**/'1'='1"
        ]

        for malicious_input in malicious_inputs:
            with pytest.raises(ValueError, match="Invalid identifier format"):
                get_row_count(mock_cursor, malicious_input)

    def test_postgresql_identifier_quoting(self):
        """Test PostgreSQL uses psycopg2 identifier quoting"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'psycopg2.extensions'
        mock_cursor.fetchone.return_value = [150]

        result = get_row_count(mock_cursor, "products")

        assert result == 150
        # Verify query was executed (quoting verified in _quote_identifier tests)
        assert mock_cursor.execute.called


class TestCalculateChecksumSecurity:
    """Test SQL injection prevention in calculate_checksum"""

    def test_legitimate_table_name(self):
        """Test calculate_checksum with legitimate table name"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'pyodbc'
        # Set description to None to use fallback path
        mock_cursor.description = None
        mock_cursor.__iter__ = Mock(return_value=iter([[1, "test", 100]]))

        result = calculate_checksum(mock_cursor, "customers")

        # Should return a SHA256 hash
        assert isinstance(result, str)
        assert len(result) == 64  # SHA256 hex digest length

        # Verify table name was quoted in queries
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        # Should have bracketed table name
        assert any("[customers]" in call for call in execute_calls)

    def test_reject_sql_injection_in_table_name(self):
        """Test calculate_checksum rejects SQL injection in table name"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'pyodbc'

        with pytest.raises(ValueError, match="Invalid identifier format"):
            calculate_checksum(mock_cursor, "customers; DROP TABLE users--")

    def test_reject_sql_injection_in_column_names(self):
        """Test calculate_checksum rejects SQL injection in column names"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'pyodbc'

        malicious_columns = [
            "id",
            "name; DROP TABLE users--"
        ]

        with pytest.raises(ValueError, match="Invalid identifier format"):
            calculate_checksum(mock_cursor, "customers", columns=malicious_columns)

    def test_column_name_quoting(self):
        """Test that column names are properly quoted"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'pyodbc'
        mock_cursor.description = None
        mock_cursor.__iter__ = Mock(return_value=iter([[1, "test"]]))

        columns = ["customer_id", "customer_name"]
        result = calculate_checksum(mock_cursor, "customers", columns=columns)

        # Verify column names were quoted
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        final_query = execute_calls[-1]  # Last query is the SELECT
        assert "[customer_id]" in final_query
        assert "[customer_name]" in final_query

    def test_schema_qualified_table_with_columns(self):
        """Test schema.table with column list"""
        mock_cursor = Mock()
        mock_cursor.__class__.__module__ = 'pyodbc'
        mock_cursor.description = None
        mock_cursor.__iter__ = Mock(return_value=iter([[1, "test"]]))

        columns = ["id", "name"]
        result = calculate_checksum(mock_cursor, "dbo.customers", columns=columns)

        # Verify schema.table was quoted
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        final_query = execute_calls[-1]
        assert "[dbo].[customers]" in final_query
        assert "[id]" in final_query
        assert "[name]" in final_query


class TestSecurityEdgeCases:
    """Test edge cases for security"""

    def test_empty_string_table_name(self):
        """Test that empty string is rejected"""
        with pytest.raises(ValueError):
            _quote_sqlserver_identifier("")

    def test_special_characters_in_identifier(self):
        """Test various special characters are rejected"""
        invalid_chars = [
            "table@name",
            "table#name",
            "table$name",
            "table%name",
            "table&name",
            "table*name",
            "table(name)",
            "table+name",
            "table=name",
            "table{name}",
            "table name",  # space
            "table\tname",  # tab
            "table\nname",  # newline
        ]

        for invalid_name in invalid_chars:
            with pytest.raises(ValueError, match="Invalid identifier format"):
                _quote_sqlserver_identifier(invalid_name)

    def test_unicode_characters_rejected(self):
        """Test that Unicode special characters are rejected"""
        unicode_attacks = [
            "table™name",
            "table™️name",
            "table\u200bname",  # zero-width space
            "table\ufeffname",  # zero-width no-break space
        ]

        for invalid_name in unicode_attacks:
            with pytest.raises(ValueError, match="Invalid identifier format"):
                _quote_sqlserver_identifier(invalid_name)

    def test_sql_keywords_are_allowed(self):
        """Test that SQL keywords are allowed as they will be quoted"""
        sql_keywords = [
            "SELECT",
            "DROP",
            "INSERT",
            "DELETE",
            "UPDATE",
            "WHERE"
        ]

        for keyword in sql_keywords:
            # Should not raise - quoting makes them safe
            result = _quote_sqlserver_identifier(keyword)
            assert result == f"[{keyword}]"

    def test_very_long_identifier(self):
        """Test that very long but valid identifiers are accepted"""
        long_name = "a" * 128  # SQL Server max identifier length
        result = _quote_sqlserver_identifier(long_name)
        assert result == f"[{long_name}]"