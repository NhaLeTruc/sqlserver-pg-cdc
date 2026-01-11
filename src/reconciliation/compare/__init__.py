"""
Row count and checksum comparison logic for data reconciliation.

This submodule provides functions to compare data between source and target databases:
- Row count comparison
- Checksum validation
- Table-level data integrity checks
- SQL injection protection via identifier quoting
"""

from .checksum import calculate_checksum, calculate_checksum_chunked
from .counts import compare_checksums, compare_row_counts, get_row_count, reconcile_table
from .quoting import (
    _get_db_type,
    _quote_identifier,
    _quote_postgres_identifier,
    _quote_sqlserver_identifier,
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
