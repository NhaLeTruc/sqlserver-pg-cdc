"""
Unit tests for reconciliation tool

Tests verify:
- T074: Row count comparison logic
- T075: Checksum validation logic
- T076: Discrepancy reporting

These tests follow TDD - they should FAIL until implementation is complete.
"""

import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime
from typing import Dict, List, Any


# T074: Unit test for row count comparison
class TestRowCountComparison:
    """Test row count comparison functionality"""

    def test_row_count_match(self):
        """Test row count comparison when counts match"""
        # Import will fail until implementation exists
        from src.reconciliation.compare import compare_row_counts

        source_count = 1000
        target_count = 1000
        table_name = "customers"

        result = compare_row_counts(table_name, source_count, target_count)

        assert result["table"] == table_name
        assert result["source_count"] == source_count
        assert result["target_count"] == target_count
        assert result["match"] is True
        assert result["difference"] == 0
        assert "timestamp" in result

    def test_row_count_mismatch_target_less(self):
        """Test row count comparison when target has fewer rows"""
        from src.reconciliation.compare import compare_row_counts

        source_count = 1000
        target_count = 950
        table_name = "orders"

        result = compare_row_counts(table_name, source_count, target_count)

        assert result["table"] == table_name
        assert result["source_count"] == source_count
        assert result["target_count"] == target_count
        assert result["match"] is False
        assert result["difference"] == -50  # Target is missing 50 rows
        assert result["status"] == "MISMATCH"

    def test_row_count_mismatch_target_more(self):
        """Test row count comparison when target has more rows (data quality issue)"""
        from src.reconciliation.compare import compare_row_counts

        source_count = 1000
        target_count = 1020
        table_name = "line_items"

        result = compare_row_counts(table_name, source_count, target_count)

        assert result["table"] == table_name
        assert result["match"] is False
        assert result["difference"] == 20  # Target has 20 extra rows
        assert result["status"] == "MISMATCH"

    def test_row_count_with_zero_rows(self):
        """Test row count comparison when both tables are empty"""
        from src.reconciliation.compare import compare_row_counts

        result = compare_row_counts("empty_table", 0, 0)

        assert result["match"] is True
        assert result["difference"] == 0

    def test_row_count_invalid_negative(self):
        """Test row count comparison rejects negative counts"""
        from src.reconciliation.compare import compare_row_counts

        with pytest.raises(ValueError, match="Row counts cannot be negative"):
            compare_row_counts("invalid_table", -1, 100)

        with pytest.raises(ValueError, match="Row counts cannot be negative"):
            compare_row_counts("invalid_table", 100, -1)


# T075: Unit test for checksum validation
class TestChecksumValidation:
    """Test checksum validation functionality"""

    def test_checksum_match(self):
        """Test checksum comparison when checksums match"""
        from src.reconciliation.compare import compare_checksums

        table_name = "customers"
        source_checksum = "abc123def456"
        target_checksum = "abc123def456"

        result = compare_checksums(table_name, source_checksum, target_checksum)

        assert result["table"] == table_name
        assert result["source_checksum"] == source_checksum
        assert result["target_checksum"] == target_checksum
        assert result["match"] is True
        assert result["status"] == "MATCH"
        assert "timestamp" in result

    def test_checksum_mismatch(self):
        """Test checksum comparison when checksums differ (data corruption)"""
        from src.reconciliation.compare import compare_checksums

        table_name = "orders"
        source_checksum = "abc123def456"
        target_checksum = "xyz789ghi012"

        result = compare_checksums(table_name, source_checksum, target_checksum)

        assert result["table"] == table_name
        assert result["source_checksum"] == source_checksum
        assert result["target_checksum"] == target_checksum
        assert result["match"] is False
        assert result["status"] == "MISMATCH"

    def test_checksum_with_empty_string(self):
        """Test checksum comparison handles empty checksums"""
        from src.reconciliation.compare import compare_checksums

        # Both empty should match
        result = compare_checksums("empty_table", "", "")
        assert result["match"] is True

        # One empty, one not should mismatch
        result = compare_checksums("partial_table", "abc123", "")
        assert result["match"] is False

    def test_checksum_null_values(self):
        """Test checksum comparison rejects None values"""
        from src.reconciliation.compare import compare_checksums

        with pytest.raises(ValueError, match="Checksums cannot be None"):
            compare_checksums("invalid_table", None, "abc123")

        with pytest.raises(ValueError, match="Checksums cannot be None"):
            compare_checksums("invalid_table", "abc123", None)

    def test_checksum_case_sensitivity(self):
        """Test checksum comparison is case-sensitive"""
        from src.reconciliation.compare import compare_checksums

        result = compare_checksums("case_table", "ABC123", "abc123")

        assert result["match"] is False
        assert result["status"] == "MISMATCH"


# T076: Unit test for discrepancy reporting
class TestDiscrepancyReporting:
    """Test discrepancy reporting functionality"""

    def test_generate_report_all_match(self):
        """Test report generation when all tables match"""
        from src.reconciliation.report import generate_report

        comparison_results = [
            {
                "table": "customers",
                "source_count": 1000,
                "target_count": 1000,
                "match": True,
                "difference": 0,
                "source_checksum": "abc123",
                "target_checksum": "abc123",
                "checksum_match": True
            },
            {
                "table": "orders",
                "source_count": 5000,
                "target_count": 5000,
                "match": True,
                "difference": 0,
                "source_checksum": "def456",
                "target_checksum": "def456",
                "checksum_match": True
            }
        ]

        report = generate_report(comparison_results)

        assert report["status"] == "PASS"
        assert report["total_tables"] == 2
        assert report["tables_matched"] == 2
        assert report["tables_mismatched"] == 0
        assert len(report["discrepancies"]) == 0
        assert "timestamp" in report
        assert "summary" in report

    def test_generate_report_with_discrepancies(self):
        """Test report generation with row count and checksum mismatches"""
        from src.reconciliation.report import generate_report

        comparison_results = [
            {
                "table": "customers",
                "source_count": 1000,
                "target_count": 950,
                "match": False,
                "difference": -50,
                "source_checksum": "abc123",
                "target_checksum": "abc123",
                "checksum_match": True
            },
            {
                "table": "orders",
                "source_count": 5000,
                "target_count": 5000,
                "match": True,
                "difference": 0,
                "source_checksum": "def456",
                "target_checksum": "xyz789",
                "checksum_match": False
            }
        ]

        report = generate_report(comparison_results)

        assert report["status"] == "FAIL"
        assert report["total_tables"] == 2
        assert report["tables_matched"] == 0
        assert report["tables_mismatched"] == 2
        assert len(report["discrepancies"]) == 2

        # Verify discrepancy details
        discrepancies = report["discrepancies"]

        # First discrepancy: row count mismatch
        customers_disc = next(d for d in discrepancies if d["table"] == "customers")
        assert customers_disc["issue_type"] == "ROW_COUNT_MISMATCH"
        assert customers_disc["severity"] == "HIGH"
        assert customers_disc["details"]["missing_rows"] == 50

        # Second discrepancy: checksum mismatch
        orders_disc = next(d for d in discrepancies if d["table"] == "orders")
        assert orders_disc["issue_type"] == "CHECKSUM_MISMATCH"
        assert orders_disc["severity"] == "CRITICAL"

    def test_generate_report_empty_results(self):
        """Test report generation with no comparison results"""
        from src.reconciliation.report import generate_report

        report = generate_report([])

        assert report["status"] == "NO_DATA"
        assert report["total_tables"] == 0
        assert len(report["discrepancies"]) == 0

    def test_report_json_serializable(self):
        """Test that generated report is JSON serializable"""
        import json
        from src.reconciliation.report import generate_report

        comparison_results = [
            {
                "table": "test_table",
                "source_count": 100,
                "target_count": 100,
                "match": True,
                "difference": 0,
                "source_checksum": "abc",
                "target_checksum": "abc",
                "checksum_match": True
            }
        ]

        report = generate_report(comparison_results)

        # Should not raise exception
        json_str = json.dumps(report)
        assert isinstance(json_str, str)
        assert len(json_str) > 0

    def test_report_includes_recommendations(self):
        """Test report includes actionable recommendations"""
        from src.reconciliation.report import generate_report

        comparison_results = [
            {
                "table": "customers",
                "source_count": 1000,
                "target_count": 950,
                "match": False,
                "difference": -50,
                "source_checksum": "abc123",
                "target_checksum": "abc123",
                "checksum_match": True
            }
        ]

        report = generate_report(comparison_results)

        assert "recommendations" in report
        assert len(report["recommendations"]) > 0
        assert any("replication lag" in rec.lower() for rec in report["recommendations"])

    def test_report_severity_levels(self):
        """Test report assigns correct severity levels"""
        from src.reconciliation.report import generate_report

        comparison_results = [
            {
                "table": "low_priority",
                "source_count": 1000,
                "target_count": 999,
                "match": False,
                "difference": -1,  # Small difference
                "source_checksum": "abc",
                "target_checksum": "abc",
                "checksum_match": True
            },
            {
                "table": "high_priority",
                "source_count": 1000,
                "target_count": 500,
                "match": False,
                "difference": -500,  # Large difference
                "source_checksum": "abc",
                "target_checksum": "abc",
                "checksum_match": True
            }
        ]

        report = generate_report(comparison_results)

        discrepancies = report["discrepancies"]

        low_disc = next(d for d in discrepancies if d["table"] == "low_priority")
        high_disc = next(d for d in discrepancies if d["table"] == "high_priority")

        # Small differences should be MEDIUM severity
        # Large differences (>10%) should be HIGH or CRITICAL
        assert low_disc["severity"] in ["LOW", "MEDIUM"]
        assert high_disc["severity"] in ["HIGH", "CRITICAL"]


# Additional utility tests
class TestReconciliationUtilities:
    """Test reconciliation utility functions"""

    def test_calculate_checksum_for_table(self):
        """Test checksum calculation for a table"""
        from src.reconciliation.compare import calculate_checksum

        # Mock database connection with description
        mock_cursor = Mock()
        mock_cursor.description = [("id",), ("name",), ("email",)]
        mock_cursor.execute.return_value = None
        mock_cursor.__iter__ = Mock(return_value=iter([
            (1, "Customer 1", "customer1@example.com"),
            (2, "Customer 2", "customer2@example.com")
        ]))

        table_name = "customers"
        checksum = calculate_checksum(mock_cursor, table_name, columns=["id", "name", "email"])

        assert isinstance(checksum, str)
        assert len(checksum) > 0
        # MD5 hash should be 32 characters
        assert len(checksum) == 32

    def test_get_row_count_for_table(self):
        """Test row count retrieval for a table"""
        from src.reconciliation.compare import get_row_count

        # Mock database connection
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1234,)

        table_name = "orders"
        count = get_row_count(mock_cursor, table_name)

        assert count == 1234
        assert isinstance(count, int)

    def test_format_timestamp(self):
        """Test timestamp formatting in reports"""
        from src.reconciliation.report import format_timestamp

        timestamp = datetime(2025, 12, 2, 15, 30, 45)
        formatted = format_timestamp(timestamp)

        assert isinstance(formatted, str)
        assert "2025" in formatted
        assert "12" in formatted
        assert "02" in formatted


# ============================================================================
# PHASE 1 ENHANCEMENTS: Additional Coverage for compare.py
# ============================================================================

class TestCompareEnhanced:
    """Enhanced tests for compare.py edge cases and error paths"""

    def test_get_row_count_with_empty_table(self):
        """Test get_row_count returns 0 for empty tables"""
        from src.reconciliation.compare import get_row_count

        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (0,)

        count = get_row_count(mock_cursor, "empty_table")

        assert count == 0
        mock_cursor.execute.assert_called_once()
        assert "SELECT COUNT(*)" in mock_cursor.execute.call_args[0][0]

    def test_get_row_count_with_schema_qualified_name(self):
        """Test get_row_count handles schema.table format"""
        from src.reconciliation.compare import get_row_count

        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (500,)

        count = get_row_count(mock_cursor, "dbo.customers")

        assert count == 500
        assert "dbo.customers" in mock_cursor.execute.call_args[0][0]

    def test_get_row_count_executes_correct_query(self):
        """Test get_row_count generates correct SQL query"""
        from src.reconciliation.compare import get_row_count

        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (100,)

        get_row_count(mock_cursor, "test_table")

        expected_query = "SELECT COUNT(*) FROM test_table"
        mock_cursor.execute.assert_called_once_with(expected_query)

    def test_get_row_count_with_database_exception(self):
        """Test get_row_count propagates database exceptions"""
        from src.reconciliation.compare import get_row_count

        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Database connection lost")

        with pytest.raises(Exception, match="Database connection lost"):
            get_row_count(mock_cursor, "test_table")

    def test_calculate_checksum_with_empty_table(self):
        """Test calculate_checksum for empty table"""
        from src.reconciliation.compare import calculate_checksum

        mock_cursor = Mock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.execute.return_value = None
        mock_cursor.__iter__ = Mock(return_value=iter([]))

        checksum = calculate_checksum(mock_cursor, "empty_table", columns=["id", "name"])

        # Empty table should still produce a valid MD5 hash
        assert isinstance(checksum, str)
        assert len(checksum) == 32

    def test_calculate_checksum_with_null_values(self):
        """Test calculate_checksum handles NULL values correctly"""
        from src.reconciliation.compare import calculate_checksum

        mock_cursor = Mock()
        mock_cursor.description = [("id",), ("value",)]
        mock_cursor.execute.return_value = None
        mock_cursor.__iter__ = Mock(return_value=iter([
            (1, None),
            (2, "test"),
            (3, None)
        ]))

        checksum = calculate_checksum(mock_cursor, "null_table", columns=["id", "value"])

        assert isinstance(checksum, str)
        assert len(checksum) == 32

    def test_calculate_checksum_with_special_characters(self):
        """Test calculate_checksum handles special characters and unicode"""
        from src.reconciliation.compare import calculate_checksum

        mock_cursor = Mock()
        mock_cursor.description = [("id",), ("text",)]
        mock_cursor.execute.return_value = None
        mock_cursor.__iter__ = Mock(return_value=iter([
            (1, "Test with 'quotes'"),
            (2, "Test with \"double quotes\""),
            (3, "Test with | pipe"),
            (4, "Test with émojis 🚀"),
            (5, "Test\nwith\nnewlines")
        ]))

        checksum = calculate_checksum(mock_cursor, "special_table", columns=["id", "text"])

        assert isinstance(checksum, str)
        assert len(checksum) == 32

    def test_calculate_checksum_deterministic(self):
        """Test calculate_checksum produces same result for same data"""
        from src.reconciliation.compare import calculate_checksum

        mock_cursor1 = Mock()
        mock_cursor1.description = [("id",), ("name",)]
        mock_cursor1.execute.return_value = None
        mock_cursor1.__iter__ = Mock(return_value=iter([
            (1, "Alice"),
            (2, "Bob")
        ]))

        mock_cursor2 = Mock()
        mock_cursor2.description = [("id",), ("name",)]
        mock_cursor2.execute.return_value = None
        mock_cursor2.__iter__ = Mock(return_value=iter([
            (1, "Alice"),
            (2, "Bob")
        ]))

        checksum1 = calculate_checksum(mock_cursor1, "table1", columns=["id", "name"])
        checksum2 = calculate_checksum(mock_cursor2, "table2", columns=["id", "name"])

        assert checksum1 == checksum2

    def test_calculate_checksum_different_order_different_hash(self):
        """Test calculate_checksum produces different hash for different row order"""
        from src.reconciliation.compare import calculate_checksum

        mock_cursor1 = Mock()
        mock_cursor1.description = [("id",), ("name",)]
        mock_cursor1.execute.return_value = None
        mock_cursor1.__iter__ = Mock(return_value=iter([
            (1, "Alice"),
            (2, "Bob")
        ]))

        mock_cursor2 = Mock()
        mock_cursor2.description = [("id",), ("name",)]
        mock_cursor2.execute.return_value = None
        mock_cursor2.__iter__ = Mock(return_value=iter([
            (2, "Bob"),
            (1, "Alice")
        ]))

        checksum1 = calculate_checksum(mock_cursor1, "table1", columns=["id", "name"])
        checksum2 = calculate_checksum(mock_cursor2, "table2", columns=["id", "name"])

        # Different order should produce different checksums
        assert checksum1 != checksum2

    def test_calculate_checksum_without_explicit_columns(self):
        """Test calculate_checksum auto-detects columns from cursor description"""
        from src.reconciliation.compare import calculate_checksum

        mock_cursor = Mock()
        mock_cursor.description = [("id",), ("name",), ("email",)]
        mock_cursor.execute.return_value = None
        mock_cursor.__iter__ = Mock(return_value=iter([
            (1, "Alice", "alice@example.com")
        ]))

        checksum = calculate_checksum(mock_cursor, "auto_table")

        assert isinstance(checksum, str)
        assert len(checksum) == 32
        # Should have called execute twice: once for LIMIT 0, once for actual query
        assert mock_cursor.execute.call_count == 2

    def test_calculate_checksum_without_description_attribute(self):
        """Test calculate_checksum fallback when cursor lacks description"""
        from src.reconciliation.compare import calculate_checksum

        # Create a cursor-like object without description attribute
        mock_cursor = Mock(spec=['execute', '__iter__'])
        mock_cursor.execute.return_value = None
        mock_cursor.__iter__ = Mock(return_value=iter([
            (1, "Alice"),
            (2, "Bob")
        ]))

        checksum = calculate_checksum(mock_cursor, "no_desc_table")

        assert isinstance(checksum, str)
        assert len(checksum) == 32
        # Should use SELECT * FROM ... ORDER BY 1
        assert "SELECT * FROM no_desc_table ORDER BY 1" in str(mock_cursor.execute.call_args)

    def test_reconcile_table_row_count_only(self):
        """Test reconcile_table with checksum validation disabled"""
        from src.reconciliation.compare import reconcile_table

        source_cursor = Mock()
        source_cursor.fetchone.return_value = (1000,)

        target_cursor = Mock()
        target_cursor.fetchone.return_value = (1000,)

        result = reconcile_table(
            source_cursor,
            target_cursor,
            "source_table",
            "target_table",
            validate_checksum=False
        )

        assert result["table"] == "target_table"
        assert result["source_count"] == 1000
        assert result["target_count"] == 1000
        assert result["match"] is True
        assert result["difference"] == 0
        assert "source_checksum" not in result
        assert "target_checksum" not in result
        assert "checksum_match" not in result
        assert "timestamp" in result

    def test_reconcile_table_with_checksum_validation(self):
        """Test reconcile_table with checksum validation enabled"""
        from src.reconciliation.compare import reconcile_table

        source_cursor = Mock()
        source_cursor.fetchone.return_value = (100,)
        source_cursor.description = [("id",), ("name",)]
        source_cursor.execute.return_value = None
        source_cursor.__iter__ = Mock(return_value=iter([
            (1, "Alice"),
            (2, "Bob")
        ]))

        target_cursor = Mock()
        target_cursor.fetchone.return_value = (100,)
        target_cursor.description = [("id",), ("name",)]
        target_cursor.execute.return_value = None
        target_cursor.__iter__ = Mock(return_value=iter([
            (1, "Alice"),
            (2, "Bob")
        ]))

        result = reconcile_table(
            source_cursor,
            target_cursor,
            "source_table",
            "target_table",
            validate_checksum=True,
            columns=["id", "name"]
        )

        assert result["table"] == "target_table"
        assert result["match"] is True
        assert "source_checksum" in result
        assert "target_checksum" in result
        assert result["checksum_match"] is True

    def test_reconcile_table_row_count_match_checksum_mismatch(self):
        """Test reconcile_table when counts match but checksums differ"""
        from src.reconciliation.compare import reconcile_table

        source_cursor = Mock()
        source_cursor.fetchone.return_value = (2,)
        source_cursor.description = [("id",), ("name",)]
        source_cursor.execute.return_value = None
        source_cursor.__iter__ = Mock(return_value=iter([
            (1, "Alice"),
            (2, "Bob")
        ]))

        target_cursor = Mock()
        target_cursor.fetchone.return_value = (2,)
        target_cursor.description = [("id",), ("name",)]
        target_cursor.execute.return_value = None
        target_cursor.__iter__ = Mock(return_value=iter([
            (1, "Alice"),
            (2, "Charlie")  # Different data
        ]))

        result = reconcile_table(
            source_cursor,
            target_cursor,
            "source_table",
            "target_table",
            validate_checksum=True,
            columns=["id", "name"]
        )

        assert result["source_count"] == 2
        assert result["target_count"] == 2
        assert result["difference"] == 0
        assert result["checksum_match"] is False
        assert result["match"] is False  # Overall match should be False

    def test_reconcile_table_with_empty_tables(self):
        """Test reconcile_table with both tables empty"""
        from src.reconciliation.compare import reconcile_table

        source_cursor = Mock()
        source_cursor.fetchone.return_value = (0,)

        target_cursor = Mock()
        target_cursor.fetchone.return_value = (0,)

        result = reconcile_table(
            source_cursor,
            target_cursor,
            "empty_source",
            "empty_target",
            validate_checksum=False
        )

        assert result["source_count"] == 0
        assert result["target_count"] == 0
        assert result["match"] is True
        assert result["difference"] == 0

    def test_reconcile_table_propagates_exceptions(self):
        """Test reconcile_table propagates database exceptions"""
        from src.reconciliation.compare import reconcile_table

        source_cursor = Mock()
        source_cursor.execute.side_effect = Exception("Connection timeout")

        target_cursor = Mock()

        with pytest.raises(Exception, match="Connection timeout"):
            reconcile_table(
                source_cursor,
                target_cursor,
                "source_table",
                "target_table",
                validate_checksum=False
            )


# ============================================================================
# PHASE 1 ENHANCEMENTS: Additional Coverage for report.py
# ============================================================================

class TestReportEnhanced:
    """Enhanced tests for report.py edge cases and error paths"""

    def test_calculate_severity_zero_source_count_zero_difference(self):
        """Test _calculate_severity with zero source and zero difference"""
        from src.reconciliation.report import _calculate_severity

        severity = _calculate_severity(0, 0)
        assert severity == "LOW"

    def test_calculate_severity_zero_source_count_with_difference(self):
        """Test _calculate_severity with zero source and non-zero difference"""
        from src.reconciliation.report import _calculate_severity

        severity = _calculate_severity(0, 100)
        assert severity == "CRITICAL"

    def test_calculate_severity_less_than_point_one_percent(self):
        """Test _calculate_severity for difference < 0.1%"""
        from src.reconciliation.report import _calculate_severity

        # 0.05% difference
        severity = _calculate_severity(10000, 5)
        assert severity == "LOW"

    def test_calculate_severity_less_than_one_percent(self):
        """Test _calculate_severity for difference < 1%"""
        from src.reconciliation.report import _calculate_severity

        # 0.5% difference
        severity = _calculate_severity(10000, 50)
        assert severity == "MEDIUM"

    def test_calculate_severity_less_than_ten_percent(self):
        """Test _calculate_severity for difference < 10%"""
        from src.reconciliation.report import _calculate_severity

        # 5% difference
        severity = _calculate_severity(10000, 500)
        assert severity == "HIGH"

    def test_calculate_severity_greater_than_ten_percent(self):
        """Test _calculate_severity for difference >= 10%"""
        from src.reconciliation.report import _calculate_severity

        # 50% difference
        severity = _calculate_severity(10000, 5000)
        assert severity == "CRITICAL"

    def test_calculate_severity_boundary_values(self):
        """Test _calculate_severity at exact boundary values"""
        from src.reconciliation.report import _calculate_severity

        # Exactly 0.1%
        assert _calculate_severity(1000, 1) in ["LOW", "MEDIUM"]

        # Exactly 1%
        assert _calculate_severity(1000, 10) in ["MEDIUM", "HIGH"]

        # Exactly 10%
        assert _calculate_severity(1000, 100) in ["HIGH", "CRITICAL"]

    def test_generate_summary_all_matched(self):
        """Test _generate_summary when all tables match"""
        from src.reconciliation.report import _generate_summary

        summary = _generate_summary(10, 10, 0)
        assert "All 10 tables passed" in summary
        assert "consistent" in summary.lower()

    def test_generate_summary_with_discrepancies(self):
        """Test _generate_summary with some mismatched tables"""
        from src.reconciliation.report import _generate_summary

        summary = _generate_summary(10, 7, 3)
        assert "3" in summary
        assert "10" in summary
        assert "7" in summary
        assert "discrepancies" in summary.lower()

    def test_generate_recommendations_no_discrepancies(self):
        """Test _generate_recommendations with no issues"""
        from src.reconciliation.report import _generate_recommendations

        recommendations = _generate_recommendations([], [])
        assert len(recommendations) > 0
        assert any("consistent" in rec.lower() for rec in recommendations)

    def test_generate_recommendations_missing_rows(self):
        """Test _generate_recommendations for missing rows"""
        from src.reconciliation.report import _generate_recommendations

        discrepancies = [
            {
                "issue_type": "ROW_COUNT_MISMATCH",
                "details": {"missing_rows": 100, "extra_rows": 0}
            }
        ]

        recommendations = _generate_recommendations(discrepancies, [])
        assert any("missing" in rec.lower() for rec in recommendations)
        assert any("replication lag" in rec.lower() for rec in recommendations)

    def test_generate_recommendations_extra_rows(self):
        """Test _generate_recommendations for extra rows"""
        from src.reconciliation.report import _generate_recommendations

        discrepancies = [
            {
                "issue_type": "ROW_COUNT_MISMATCH",
                "details": {"missing_rows": 0, "extra_rows": 50}
            }
        ]

        recommendations = _generate_recommendations(discrepancies, [])
        assert any("extra" in rec.lower() for rec in recommendations)
        assert any("duplicate" in rec.lower() or "data quality" in rec.lower() for rec in recommendations)

    def test_generate_recommendations_checksum_mismatch(self):
        """Test _generate_recommendations for checksum issues"""
        from src.reconciliation.report import _generate_recommendations

        discrepancies = [
            {
                "issue_type": "CHECKSUM_MISMATCH",
                "details": {}
            }
        ]

        recommendations = _generate_recommendations(discrepancies, [])
        assert any("corruption" in rec.lower() for rec in recommendations)
        assert any("row-by-row" in rec.lower() for rec in recommendations)

    def test_generate_recommendations_many_discrepancies(self):
        """Test _generate_recommendations with many affected tables"""
        from src.reconciliation.report import _generate_recommendations

        discrepancies = [
            {"issue_type": "ROW_COUNT_MISMATCH", "details": {"missing_rows": 10, "extra_rows": 0}}
            for _ in range(10)
        ]

        recommendations = _generate_recommendations(discrepancies, [])
        assert any("multiple tables" in rec.lower() or "full resync" in rec.lower()
                   for rec in recommendations)

    def test_generate_recommendations_includes_troubleshooting_reference(self):
        """Test _generate_recommendations includes documentation reference"""
        from src.reconciliation.report import _generate_recommendations

        discrepancies = [
            {"issue_type": "ROW_COUNT_MISMATCH", "details": {"missing_rows": 10, "extra_rows": 0}}
        ]

        recommendations = _generate_recommendations(discrepancies, [])
        assert any("troubleshooting" in rec.lower() for rec in recommendations)

    def test_export_report_json_creates_file(self, tmp_path):
        """Test export_report_json creates valid JSON file"""
        from src.reconciliation.report import export_report_json

        report = {
            "status": "PASS",
            "total_tables": 5,
            "timestamp": "2025-12-04T10:00:00"
        }

        output_file = tmp_path / "report.json"
        export_report_json(report, str(output_file))

        assert output_file.exists()

        # Verify JSON is valid
        import json
        with open(output_file) as f:
            loaded = json.load(f)

        assert loaded["status"] == "PASS"
        assert loaded["total_tables"] == 5

    def test_export_report_json_with_complex_data(self, tmp_path):
        """Test export_report_json handles complex nested data"""
        from src.reconciliation.report import export_report_json, generate_report

        comparison_results = [
            {
                "table": "test_table",
                "source_count": 1000,
                "target_count": 950,
                "match": False,
                "difference": -50,
                "source_checksum": "abc123",
                "target_checksum": "abc123",
                "checksum_match": True,
                "timestamp": "2025-12-04T10:00:00"
            }
        ]

        report = generate_report(comparison_results)
        output_file = tmp_path / "complex_report.json"
        export_report_json(report, str(output_file))

        assert output_file.exists()

        import json
        with open(output_file) as f:
            loaded = json.load(f)

        assert loaded["status"] == "FAIL"
        assert len(loaded["discrepancies"]) > 0

    def test_export_report_csv_creates_file(self, tmp_path):
        """Test export_report_csv creates valid CSV file"""
        from src.reconciliation.report import export_report_csv, generate_report

        comparison_results = [
            {
                "table": "test_table",
                "source_count": 1000,
                "target_count": 950,
                "match": False,
                "difference": -50,
                "source_checksum": "abc",
                "target_checksum": "abc",
                "checksum_match": True,
                "timestamp": "2025-12-04T10:00:00"
            }
        ]

        report = generate_report(comparison_results)
        output_file = tmp_path / "report.csv"
        export_report_csv(report, str(output_file))

        assert output_file.exists()

        # Verify CSV structure
        import csv
        with open(output_file) as f:
            reader = csv.reader(f)
            headers = next(reader)
            assert "Table" in headers
            assert "Status" in headers
            assert "Severity" in headers

    def test_export_report_csv_with_empty_discrepancies(self, tmp_path):
        """Test export_report_csv handles reports with no discrepancies"""
        from src.reconciliation.report import export_report_csv

        report = {
            "status": "PASS",
            "discrepancies": []
        }

        output_file = tmp_path / "empty_report.csv"
        export_report_csv(report, str(output_file))

        assert output_file.exists()

        # Should have headers only
        import csv
        with open(output_file) as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 1  # Just headers

    def test_export_report_csv_with_special_characters(self, tmp_path):
        """Test export_report_csv handles special characters in table names"""
        from src.reconciliation.report import export_report_csv

        report = {
            "status": "FAIL",
            "discrepancies": [
                {
                    "table": "table_with_'quotes'",
                    "issue_type": "ROW_COUNT_MISMATCH",
                    "severity": "HIGH",
                    "details": {
                        "source_count": 100,
                        "target_count": 90,
                        "missing_rows": 10,
                        "extra_rows": 0
                    }
                }
            ]
        }

        output_file = tmp_path / "special_chars.csv"
        export_report_csv(report, str(output_file))

        assert output_file.exists()

    def test_format_report_console_basic_structure(self):
        """Test format_report_console produces readable output"""
        from src.reconciliation.report import format_report_console, generate_report

        comparison_results = [
            {
                "table": "test_table",
                "source_count": 1000,
                "target_count": 1000,
                "match": True,
                "difference": 0,
                "source_checksum": "abc",
                "target_checksum": "abc",
                "checksum_match": True,
                "timestamp": "2025-12-04T10:00:00"
            }
        ]

        report = generate_report(comparison_results)
        output = format_report_console(report)

        assert "RECONCILIATION REPORT" in output
        assert "Status: PASS" in output
        assert "Total Tables: 1" in output
        assert "SUMMARY" in output

    def test_format_report_console_with_discrepancies(self):
        """Test format_report_console shows discrepancy details"""
        from src.reconciliation.report import format_report_console, generate_report

        comparison_results = [
            {
                "table": "problem_table",
                "source_count": 1000,
                "target_count": 950,
                "match": False,
                "difference": -50,
                "source_checksum": "abc",
                "target_checksum": "abc",
                "checksum_match": True,
                "timestamp": "2025-12-04T10:00:00"
            }
        ]

        report = generate_report(comparison_results)
        output = format_report_console(report)

        assert "DISCREPANCIES" in output
        assert "problem_table" in output
        assert "ROW_COUNT_MISMATCH" in output

    def test_format_report_console_with_recommendations(self):
        """Test format_report_console includes recommendations"""
        from src.reconciliation.report import format_report_console, generate_report

        comparison_results = [
            {
                "table": "test_table",
                "source_count": 1000,
                "target_count": 950,
                "match": False,
                "difference": -50,
                "source_checksum": "abc",
                "target_checksum": "abc",
                "checksum_match": True,
                "timestamp": "2025-12-04T10:00:00"
            }
        ]

        report = generate_report(comparison_results)
        output = format_report_console(report)

        assert "RECOMMENDATIONS" in output
        assert any(char.isdigit() for char in output)  # Numbered list

    def test_format_report_console_with_long_table_names(self):
        """Test format_report_console handles very long table names"""
        from src.reconciliation.report import format_report_console, generate_report

        long_table_name = "very_long_table_name_" * 10

        # Create a mismatch so the table name appears in discrepancies
        comparison_results = [
            {
                "table": long_table_name,
                "source_count": 100,
                "target_count": 90,
                "match": False,
                "difference": -10,
                "source_checksum": "abc",
                "target_checksum": "abc",
                "checksum_match": True,
                "timestamp": "2025-12-04T10:00:00"
            }
        ]

        report = generate_report(comparison_results)
        output = format_report_console(report)

        # Long table name should appear in discrepancies section
        assert long_table_name in output
        assert "DISCREPANCIES" in output

    def test_generate_report_calculates_totals(self):
        """Test generate_report calculates source and target total rows"""
        from src.reconciliation.report import generate_report

        comparison_results = [
            {
                "table": "table1",
                "source_count": 1000,
                "target_count": 1000,
                "match": True,
                "difference": 0,
                "checksum_match": True
            },
            {
                "table": "table2",
                "source_count": 2000,
                "target_count": 1950,
                "match": False,
                "difference": -50,
                "checksum_match": True
            }
        ]

        report = generate_report(comparison_results)

        assert report["source_total_rows"] == 3000
        assert report["target_total_rows"] == 2950

    def test_generate_report_handles_missing_checksum_match(self):
        """Test generate_report defaults checksum_match to True if not present"""
        from src.reconciliation.report import generate_report

        comparison_results = [
            {
                "table": "table1",
                "source_count": 1000,
                "target_count": 1000,
                "match": True,
                "difference": 0
                # checksum_match intentionally missing
            }
        ]

        report = generate_report(comparison_results)

        # Should still work and consider it matched
        assert report["status"] == "PASS"
        assert report["tables_matched"] == 1

    def test_generate_report_multiple_issues_same_table(self):
        """Test generate_report handles both row count and checksum mismatch"""
        from src.reconciliation.report import generate_report

        comparison_results = [
            {
                "table": "problematic_table",
                "source_count": 1000,
                "target_count": 950,
                "match": False,
                "difference": -50,
                "source_checksum": "abc123",
                "target_checksum": "xyz789",
                "checksum_match": False,
                "timestamp": "2025-12-04T10:00:00"
            }
        ]

        report = generate_report(comparison_results)

        # Should have both ROW_COUNT_MISMATCH and CHECKSUM_MISMATCH
        assert len(report["discrepancies"]) == 2
        issue_types = [d["issue_type"] for d in report["discrepancies"]]
        assert "ROW_COUNT_MISMATCH" in issue_types
        assert "CHECKSUM_MISMATCH" in issue_types

    def test_format_report_console_with_no_recommendations(self):
        """Test format_report_console when recommendations list is empty"""
        from src.reconciliation.report import format_report_console

        report = {
            "status": "PASS",
            "timestamp": "2025-12-04T10:00:00",
            "total_tables": 5,
            "tables_matched": 5,
            "tables_mismatched": 0,
            "source_total_rows": 1000,
            "target_total_rows": 1000,
            "summary": "All tables passed",
            "discrepancies": [],
            "recommendations": []  # Empty recommendations
        }

        result = format_report_console(report)

        # Should not include RECOMMENDATIONS section when list is empty
        assert "RECOMMENDATIONS" not in result
        assert "All tables passed" in result
        assert "PASS" in result
