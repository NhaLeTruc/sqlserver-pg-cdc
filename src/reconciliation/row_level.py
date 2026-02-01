"""
Row-level reconciliation for detailed discrepancy detection.

BUG-7: This file now re-exports from the row_level/ submodule for backward compatibility.
New code should import directly from src.reconciliation.row_level instead.

Identifies:
- Missing rows (in source but not target)
- Extra rows (in target but not source)
- Modified rows (different values)
- Generates repair SQL scripts

Performance optimized for tables up to 10M rows using batching and streaming.
"""

# Re-export everything from the row_level submodule for backward compatibility
from src.reconciliation.row_level import (
    RowDiscrepancy,
    RowLevelReconciler,
    _format_value,
    _generate_delete_sql,
    _generate_insert_sql,
    _generate_update_sql,
    generate_repair_script,
)

__all__ = [
    'RowLevelReconciler',
    'RowDiscrepancy',
    'generate_repair_script',
    '_format_value',
    '_generate_delete_sql',
    '_generate_insert_sql',
    '_generate_update_sql',
]
