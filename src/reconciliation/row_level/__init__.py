"""
Row-level reconciliation for detailed discrepancy detection.

Identifies:
- Missing rows (in source but not target)
- Extra rows (in target but not source)
- Modified rows (different values)
- Generates repair SQL scripts

Performance optimized for tables up to 10M rows using batching and streaming.
"""

from .reconciler import RowDiscrepancy, RowLevelReconciler
from .repair import (
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
