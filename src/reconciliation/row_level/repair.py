"""
SQL repair script generation from row-level discrepancies.

This module generates executable SQL scripts to fix discrepancies found during
row-level reconciliation, including INSERTs for missing rows, DELETEs for extra rows,
and UPDATEs for modified rows.
"""

from datetime import UTC, datetime
from typing import Any

from opentelemetry import trace

from utils.tracing import trace_operation

from .reconciler import RowDiscrepancy


def generate_repair_script(
    discrepancies: list[RowDiscrepancy], target_table: str, database_type: str = "postgresql"
) -> str:
    """
    Generate SQL repair script from discrepancies.

    Args:
        discrepancies: List of row discrepancies
        target_table: Target table name
        database_type: 'postgresql' or 'sqlserver'

    Returns:
        SQL script to fix all discrepancies
    """
    with trace_operation(
        "generate_repair_script",
        kind=trace.SpanKind.INTERNAL,
        table=target_table,
        discrepancy_count=len(discrepancies),
    ):
        script_lines = [
            f"-- Repair script for {target_table}",
            f"-- Generated: {datetime.now(UTC).isoformat()}",
            f"-- Total discrepancies: {len(discrepancies)}",
            f"-- Database type: {database_type}",
            "",
        ]

        if database_type == "postgresql":
            script_lines.append("BEGIN;")
        else:
            script_lines.append("BEGIN TRANSACTION;")

        script_lines.append("")

        # Group by type for better organization
        missing = [d for d in discrepancies if d.discrepancy_type == "MISSING"]
        extra = [d for d in discrepancies if d.discrepancy_type == "EXTRA"]
        modified = [d for d in discrepancies if d.discrepancy_type == "MODIFIED"]

        # Generate INSERTs for missing rows
        if missing:
            script_lines.append(f"-- Insert {len(missing)} missing rows")
            script_lines.append("")
            for disc in missing:
                script_lines.append(f"-- Missing row: {disc.primary_key}")
                script_lines.append(
                    _generate_insert_sql(target_table, disc.source_data, database_type)
                )
                script_lines.append("")

        # Generate DELETEs for extra rows
        if extra:
            script_lines.append(f"-- Delete {len(extra)} extra rows")
            script_lines.append("")
            for disc in extra:
                script_lines.append(f"-- Extra row: {disc.primary_key}")
                script_lines.append(
                    _generate_delete_sql(target_table, disc.primary_key, database_type)
                )
                script_lines.append("")

        # Generate UPDATEs for modified rows
        if modified:
            script_lines.append(f"-- Update {len(modified)} modified rows")
            script_lines.append("")
            for disc in modified:
                script_lines.append(f"-- Modified row: {disc.primary_key}")
                script_lines.append(
                    f"-- Modified columns: {', '.join(disc.modified_columns)}"
                )
                script_lines.append(
                    _generate_update_sql(
                        target_table,
                        disc.primary_key,
                        disc.source_data,
                        disc.modified_columns,
                        database_type,
                    )
                )
                script_lines.append("")

        script_lines.append("COMMIT;")

        return "\n".join(script_lines)


def _generate_insert_sql(
    table: str, data: dict[str, Any], database_type: str = "postgresql"
) -> str:
    """Generate INSERT statement."""
    if not data:
        return "-- Cannot generate INSERT: no data"

    columns = list(data.keys())
    values = [_format_value(data[col], database_type) for col in columns]

    # Quote identifiers
    if database_type == "postgresql":
        quoted_table = f'"{table}"'
        quoted_cols = [f'"{col}"' for col in columns]
    else:
        quoted_table = f"[{table}]"
        quoted_cols = [f"[{col}]" for col in columns]

    columns_str = ", ".join(quoted_cols)
    values_str = ", ".join(values)

    return f"INSERT INTO {quoted_table} ({columns_str}) VALUES ({values_str});"


def _generate_delete_sql(
    table: str, pk: dict[str, Any], database_type: str = "postgresql"
) -> str:
    """Generate DELETE statement."""
    if not pk:
        return "-- Cannot generate DELETE: no primary key"

    # Quote identifiers
    if database_type == "postgresql":
        quoted_table = f'"{table}"'
        conditions = [f'"{k}" = {_format_value(v, database_type)}' for k, v in pk.items()]
    else:
        quoted_table = f"[{table}]"
        conditions = [f"[{k}] = {_format_value(v, database_type)}" for k, v in pk.items()]

    where_clause = " AND ".join(conditions)

    return f"DELETE FROM {quoted_table} WHERE {where_clause};"


def _generate_update_sql(
    table: str,
    pk: dict[str, Any],
    data: dict[str, Any],
    modified_cols: list[str],
    database_type: str = "postgresql",
) -> str:
    """Generate UPDATE statement."""
    if not pk or not modified_cols:
        return "-- Cannot generate UPDATE: no primary key or modified columns"

    # Quote identifiers and build SET clause
    if database_type == "postgresql":
        quoted_table = f'"{table}"'
        set_parts = [f'"{col}" = {_format_value(data[col], database_type)}' for col in modified_cols]
        where_parts = [f'"{k}" = {_format_value(v, database_type)}' for k, v in pk.items()]
    else:
        quoted_table = f"[{table}]"
        set_parts = [f"[{col}] = {_format_value(data[col], database_type)}" for col in modified_cols]
        where_parts = [f"[{k}] = {_format_value(v, database_type)}" for k, v in pk.items()]

    set_clause = ", ".join(set_parts)
    where_clause = " AND ".join(where_parts)

    return f"UPDATE {quoted_table} SET {set_clause} WHERE {where_clause};"


def _format_value(value: Any, database_type: str = "postgresql") -> str:
    """Format value for SQL statement."""
    if value is None:
        return "NULL"

    if isinstance(value, str):
        # Escape single quotes
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    if isinstance(value, bool):
        if database_type == "postgresql":
            return "TRUE" if value else "FALSE"
        else:
            return "1" if value else "0"

    if isinstance(value, (int, float)):
        return str(value)

    if isinstance(value, datetime):
        if database_type == "postgresql":
            return f"'{value.isoformat()}'"
        else:
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"

    # Fallback: convert to string
    return f"'{str(value)}'"
