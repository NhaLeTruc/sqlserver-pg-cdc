"""
Reconciliation module for SQL Server to PostgreSQL CDC pipeline

This module provides functionality to validate data consistency between
SQL Server (source) and PostgreSQL (target) databases.

Components:
- compare: Row count and checksum comparison logic
- report: Reconciliation report generation
- scheduler: Automated reconciliation scheduling

Usage:
    from src.reconciliation.compare import compare_row_counts, compare_checksums
    from src.reconciliation.report import generate_report
    from src.reconciliation.scheduler import ReconciliationScheduler
"""

__version__ = "1.0.0"
__all__ = ["compare", "report", "scheduler"]
