"""
Row count and checksum comparison logic for data reconciliation.

BUG-7: This file now re-exports from the compare/ submodule for backward compatibility.
New code should import directly from src.reconciliation.compare instead.

This module provides functions to compare data between source and target databases:
- Row count comparison
- Checksum validation
- Table-level data integrity checks
"""

# Re-export everything from the compare submodule for backward compatibility
from src.reconciliation.compare import (
    _get_db_type,
    _quote_identifier,
    _quote_postgres_identifier,
    _quote_sqlserver_identifier,
    calculate_checksum,
    calculate_checksum_chunked,
    compare_checksums,
    compare_row_counts,
    get_row_count,
    reconcile_table,
)

__all__ = [
    'reconcile_table',
    'get_row_count',
    'calculate_checksum',
    'calculate_checksum_chunked',
    'compare_row_counts',
    'compare_checksums',
    '_quote_identifier',
    '_quote_postgres_identifier',
    '_quote_sqlserver_identifier',
    '_get_db_type',
]
