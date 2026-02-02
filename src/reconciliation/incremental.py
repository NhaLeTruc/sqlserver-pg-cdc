"""
Incremental checksum calculation using CDC metadata.

BUG-7: This file now re-exports from the incremental/ submodule for backward compatibility.
New code should import directly from reconciliation.incremental instead.

Only checksums rows modified since last reconciliation run.
Stores checksum state for incremental updates to achieve 10-100x speedup
on large tables with few changes.
"""

# Re-export everything from the incremental submodule for backward compatibility
from reconciliation.incremental import (
    IncrementalChecksumTracker,
    _calculate_delta_checksum,
    _calculate_full_checksum,
    _get_db_type,
    _quote_identifier,
    calculate_checksum_chunked,
    calculate_incremental_checksum,
)

__all__ = [
    'calculate_incremental_checksum',
    'calculate_checksum_chunked',
    'IncrementalChecksumTracker',
    '_calculate_delta_checksum',
    '_calculate_full_checksum',
    '_get_db_type',
    '_quote_identifier',
]
