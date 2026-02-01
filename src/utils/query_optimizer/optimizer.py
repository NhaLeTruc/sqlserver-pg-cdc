"""
Query optimization strategies for reconciliation operations.

Provides optimized query generation for common reconciliation patterns
including row counts and checksum calculations.
"""

from src.utils.sql_safety import quote_identifier, quote_schema_table, validate_identifier


class QueryOptimizer:
    """
    Query optimizer for database reconciliation operations.

    Provides methods to generate optimized queries for common operations.
    """

    @staticmethod
    def optimize_row_count_query(table_name: str, database_type: str = "postgresql") -> str:
        """
        Generate optimized row count query.

        Args:
            table_name: Name of the table
            database_type: 'postgresql' or 'sqlserver'

        Returns:
            Optimized SQL query

        Raises:
            ValueError: If table_name contains invalid characters
        """
        # Validate table name to prevent SQL injection
        validate_identifier(table_name)

        if database_type == "postgresql":
            # Use PostgreSQL statistics for approximate count (fast)
            # For exact count, falls back to COUNT(*)
            quoted_table = quote_identifier(table_name, "postgresql")
            # Use escaped literal for relname comparison (validated above)
            escaped_name = table_name.replace("'", "''")
            return f"""
SELECT
    CASE
        WHEN n_live_tup > 0 THEN n_live_tup
        ELSE (SELECT COUNT(*) FROM {quoted_table})
    END AS row_count
FROM pg_stat_user_tables
WHERE schemaname = 'public' AND relname = '{escaped_name}';
"""
        elif database_type == "sqlserver":
            # Use SQL Server statistics for approximate count
            # Use escaped literal for t.name comparison (validated above)
            escaped_name = table_name.replace("'", "''")
            return f"""
SELECT
    SUM(p.rows) AS row_count
FROM sys.partitions p
INNER JOIN sys.tables t ON p.object_id = t.object_id
WHERE t.name = '{escaped_name}'
    AND p.index_id IN (0, 1);
"""
        else:
            # Generic fallback
            quoted_table = quote_identifier(table_name, "postgresql")
            return f"SELECT COUNT(*) AS row_count FROM {quoted_table};"

    @staticmethod
    def optimize_checksum_query(
        table_name: str,
        columns: list[str],
        database_type: str = "postgresql",
    ) -> str:
        """
        Generate optimized checksum aggregation query.

        Args:
            table_name: Name of the table
            columns: List of columns to include in checksum
            database_type: 'postgresql' or 'sqlserver'

        Returns:
            Optimized SQL query

        Raises:
            ValueError: If table_name or any column name contains invalid characters
        """
        # Validate all identifiers to prevent SQL injection
        validate_identifier(table_name)
        for col in columns:
            validate_identifier(col)

        if database_type == "postgresql":
            # Use PostgreSQL's MD5 hash aggregation
            quoted_table = quote_identifier(table_name, "postgresql")
            quoted_cols = [quote_identifier(col, "postgresql") for col in columns]
            columns_concat = " || ".join(
                [f"COALESCE(CAST({col} AS TEXT), '')" for col in quoted_cols]
            )
            return f"""
SELECT
    MD5(string_agg(row_hash, '' ORDER BY row_hash)) AS table_checksum
FROM (
    SELECT MD5({columns_concat}) AS row_hash
    FROM {quoted_table}
) t;
"""
        elif database_type == "sqlserver":
            # Use SQL Server's CHECKSUM aggregation
            quoted_table = quote_identifier(table_name, "sqlserver")
            quoted_cols = [quote_identifier(col, "sqlserver") for col in columns]
            columns_list = ", ".join(quoted_cols)
            return f"""
SELECT
    CHECKSUM_AGG(CAST(CHECKSUM({columns_list}) AS BIGINT)) AS table_checksum
FROM {quoted_table};
"""
        else:
            raise ValueError(f"Unsupported database type: {database_type}")
