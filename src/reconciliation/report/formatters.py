"""
Report formatting and export utilities.

This module provides functions to export reconciliation reports
in various formats: JSON, CSV, and console/terminal output.
"""

import csv
import json
from typing import Any


def export_report_json(report: dict[str, Any], output_path: str) -> None:
    """
    Export report to JSON file

    Args:
        report: Report dictionary
        output_path: Path to output file
    """
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)


def export_report_csv(report: dict[str, Any], output_path: str) -> None:
    """
    Export report to CSV file

    Args:
        report: Report dictionary
        output_path: Path to output file
    """
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)

        # Write header
        writer.writerow([
            "Table",
            "Status",
            "Source Count",
            "Target Count",
            "Difference",
            "Issue Type",
            "Severity"
        ])

        # Write rows
        for discrepancy in report.get("discrepancies", []):
            writer.writerow([
                discrepancy.get("table", ""),
                "FAIL",
                discrepancy.get("details", {}).get("source_count", ""),
                discrepancy.get("details", {}).get("target_count", ""),
                (discrepancy.get("details", {}).get("missing_rows", 0) +
                 discrepancy.get("details", {}).get("extra_rows", 0)),
                discrepancy.get("issue_type", ""),
                discrepancy.get("severity", "")
            ])


def format_report_console(report: dict[str, Any]) -> str:
    """
    Format report for console output

    Args:
        report: Report dictionary

    Returns:
        Formatted string for console display
    """
    lines = []

    lines.append("=" * 80)
    lines.append("RECONCILIATION REPORT")
    lines.append("=" * 80)
    lines.append(f"Status: {report['status']}")
    lines.append(f"Timestamp: {report['timestamp']}")
    lines.append(f"Total Tables: {report['total_tables']}")
    lines.append(f"Tables Matched: {report['tables_matched']}")
    lines.append(f"Tables Mismatched: {report['tables_mismatched']}")
    lines.append(f"Source Total Rows: {report['source_total_rows']:,}")
    lines.append(f"Target Total Rows: {report['target_total_rows']:,}")
    lines.append("")

    lines.append("SUMMARY")
    lines.append("-" * 80)
    lines.append(report['summary'])
    lines.append("")

    if report['discrepancies']:
        lines.append("DISCREPANCIES")
        lines.append("-" * 80)

        for disc in report['discrepancies']:
            lines.append(f"Table: {disc['table']}")
            lines.append(f"  Issue: {disc['issue_type']}")
            lines.append(f"  Severity: {disc['severity']}")
            lines.append(f"  Details: {disc['details']}")
            lines.append("")

    if report['recommendations']:
        lines.append("RECOMMENDATIONS")
        lines.append("-" * 80)
        for i, rec in enumerate(report['recommendations'], 1):
            lines.append(f"{i}. {rec}")
        lines.append("")

    lines.append("=" * 80)

    return "\n".join(lines)
