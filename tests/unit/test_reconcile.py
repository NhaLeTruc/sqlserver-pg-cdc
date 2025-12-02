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
