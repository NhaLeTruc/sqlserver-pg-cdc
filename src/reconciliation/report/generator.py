"""
Report generation logic for reconciliation results.

This module generates comprehensive reports from comparison results,
including discrepancy analysis and actionable recommendations.
"""

from datetime import UTC, datetime
from typing import Any


# CQ-4: Discrepancy type handlers for cleaner dispatch pattern
class DiscrepancyType:
    """Constants for discrepancy types."""

    ROW_COUNT_MISMATCH = "ROW_COUNT_MISMATCH"
    CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"


def _create_row_count_discrepancy(
    result: dict[str, Any],
    severity_func: callable,
) -> dict[str, Any]:
    """
    Create a row count mismatch discrepancy record.

    Args:
        result: Comparison result dictionary
        severity_func: Function to calculate severity

    Returns:
        Discrepancy dictionary
    """
    difference = result.get("difference", 0)
    severity = severity_func(
        result.get("source_count", 0),
        abs(difference)
    )

    return {
        "table": result["table"],
        "issue_type": DiscrepancyType.ROW_COUNT_MISMATCH,
        "severity": severity,
        "details": {
            "source_count": result.get("source_count", 0),
            "target_count": result.get("target_count", 0),
            "missing_rows": abs(difference) if difference < 0 else 0,
            "extra_rows": difference if difference > 0 else 0
        },
        "timestamp": result.get("timestamp", datetime.now(UTC).isoformat())
    }


def _create_checksum_discrepancy(result: dict[str, Any]) -> dict[str, Any]:
    """
    Create a checksum mismatch discrepancy record.

    Args:
        result: Comparison result dictionary

    Returns:
        Discrepancy dictionary
    """
    return {
        "table": result["table"],
        "issue_type": DiscrepancyType.CHECKSUM_MISMATCH,
        "severity": "CRITICAL",
        "details": {
            "source_checksum": result.get("source_checksum", ""),
            "target_checksum": result.get("target_checksum", ""),
            "description": "Data corruption or modification detected"
        },
        "timestamp": result.get("timestamp", datetime.now(UTC).isoformat())
    }


def format_timestamp(timestamp: datetime) -> str:
    """
    Format timestamp for reports

    Args:
        timestamp: DateTime object

    Returns:
        ISO 8601 formatted timestamp string
    """
    return timestamp.isoformat()


def generate_report(comparison_results: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Generate reconciliation report from comparison results

    Args:
        comparison_results: List of comparison result dictionaries

    Returns:
        Dictionary containing:
        - status: PASS, FAIL, or NO_DATA
        - total_tables: Number of tables compared
        - tables_matched: Number of tables with matching data
        - tables_mismatched: Number of tables with discrepancies
        - discrepancies: List of discrepancy details
        - summary: Human-readable summary
        - recommendations: List of recommended actions
        - timestamp: Report generation timestamp
        - source_total_rows: Total rows in source
        - target_total_rows: Total rows in target
    """
    if not comparison_results:
        return {
            "status": "NO_DATA",
            "total_tables": 0,
            "tables_matched": 0,
            "tables_mismatched": 0,
            "discrepancies": [],
            "summary": "No comparison data available",
            "recommendations": [],
            "timestamp": datetime.now(UTC).isoformat(),
            "source_total_rows": 0,
            "target_total_rows": 0
        }

    total_tables = len(comparison_results)
    tables_matched = 0
    tables_mismatched = 0
    discrepancies = []
    source_total_rows = 0
    target_total_rows = 0

    for result in comparison_results:
        source_total_rows += result.get("source_count", 0)
        target_total_rows += result.get("target_count", 0)

        row_count_match = result.get("match", False)
        checksum_match = result.get("checksum_match", True)  # Default True if not checked

        # Table matches only if both row count and checksum match
        if row_count_match and checksum_match:
            tables_matched += 1
        else:
            tables_mismatched += 1

            # CQ-4: Use helper functions for cleaner discrepancy creation
            if not row_count_match:
                discrepancies.append(
                    _create_row_count_discrepancy(result, _calculate_severity)
                )

            if not checksum_match:
                discrepancies.append(_create_checksum_discrepancy(result))

    # Determine overall status
    status = "PASS" if tables_mismatched == 0 else "FAIL"

    # Generate summary
    summary = _generate_summary(total_tables, tables_matched, tables_mismatched)

    # Generate recommendations
    recommendations = _generate_recommendations(discrepancies, comparison_results)

    report = {
        "status": status,
        "total_tables": total_tables,
        "tables_matched": tables_matched,
        "tables_mismatched": tables_mismatched,
        "discrepancies": discrepancies,
        "summary": summary,
        "recommendations": recommendations,
        "timestamp": datetime.now(UTC).isoformat(),
        "source_total_rows": source_total_rows,
        "target_total_rows": target_total_rows
    }

    return report


def _calculate_severity(source_count: int, difference: int) -> str:
    """
    Calculate severity level based on row count difference

    Args:
        source_count: Number of rows in source
        difference: Absolute difference in row counts

    Returns:
        Severity level: LOW, MEDIUM, HIGH, or CRITICAL
    """
    if source_count == 0:
        return "LOW" if difference == 0 else "CRITICAL"

    # Calculate percentage difference
    percentage_diff = (difference / source_count) * 100

    if percentage_diff < 0.1:  # Less than 0.1%
        return "LOW"
    elif percentage_diff < 1.0:  # Less than 1%
        return "MEDIUM"
    elif percentage_diff < 10.0:  # Less than 10%
        return "HIGH"
    else:
        return "CRITICAL"


def _generate_summary(total_tables: int, matched: int, mismatched: int) -> str:
    """
    Generate human-readable summary

    Args:
        total_tables: Total number of tables compared
        matched: Number of tables that matched
        mismatched: Number of tables with discrepancies

    Returns:
        Summary string
    """
    if mismatched == 0:
        return f"All {total_tables} tables passed reconciliation. Data is consistent."
    else:
        return (
            f"Reconciliation found discrepancies in {mismatched} of {total_tables} tables. "
            f"{matched} tables are consistent."
        )


def _generate_recommendations(
    discrepancies: list[dict[str, Any]],
    comparison_results: list[dict[str, Any]]
) -> list[str]:
    """
    Generate actionable recommendations based on discrepancies

    Args:
        discrepancies: List of discrepancy details
        comparison_results: Original comparison results

    Returns:
        List of recommendation strings
    """
    recommendations = []

    if not discrepancies:
        recommendations.append(
            "Data is consistent. Continue monitoring replication lag and pipeline health."
        )
        return recommendations

    # Check for row count mismatches
    row_count_issues = [
        d for d in discrepancies if d["issue_type"] == DiscrepancyType.ROW_COUNT_MISMATCH
    ]

    if row_count_issues:
        # Check if target has fewer rows (replication lag)
        missing_rows = sum(
            d["details"].get("missing_rows", 0) for d in row_count_issues
        )

        if missing_rows > 0:
            recommendations.append(
                f"Target database is missing {missing_rows} rows. "
                "Check replication lag and connector status."
            )
            recommendations.append(
                "Review Kafka Connect logs for errors or backpressure."
            )

        # Check if target has extra rows (data quality issue)
        extra_rows = sum(
            d["details"].get("extra_rows", 0) for d in row_count_issues
        )

        if extra_rows > 0:
            recommendations.append(
                f"Target database has {extra_rows} extra rows. "
                "Investigate for duplicate inserts or data quality issues."
            )

    # Check for checksum mismatches
    checksum_issues = [
        d for d in discrepancies if d["issue_type"] == DiscrepancyType.CHECKSUM_MISMATCH
    ]

    if checksum_issues:
        recommendations.append(
            f"Data corruption detected in {len(checksum_issues)} table(s). "
            "Run detailed row-by-row comparison to identify corrupted records."
        )
        recommendations.append(
            "Check for schema evolution or type conversion issues in Kafka Connect."
        )

    # General recommendations
    if len(discrepancies) > 5:
        recommendations.append(
            "Multiple tables affected. Consider pausing replication and "
            "performing full resync."
        )

    recommendations.append(
        "Consult docs/troubleshooting.md for detailed resolution steps."
    )

    return recommendations
