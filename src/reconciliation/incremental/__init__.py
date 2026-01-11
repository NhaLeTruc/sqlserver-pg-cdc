"""
Incremental checksum calculation using CDC metadata.

Only checksums rows modified since last reconciliation run.
Stores checksum state for incremental updates to achieve 10-100x speedup
on large tables with few changes.
"""

from .checksum import (
    _calculate_delta_checksum,
    _calculate_full_checksum,
    _get_db_type,
    _quote_identifier,
    calculate_checksum_chunked,
    calculate_incremental_checksum,
)
from .state import IncrementalChecksumTracker

__all__ = [
    'calculate_incremental_checksum',
    'calculate_checksum_chunked',
    'IncrementalChecksumTracker',
    '_calculate_delta_checksum',
    '_calculate_full_checksum',
    '_get_db_type',
    '_quote_identifier',
]
