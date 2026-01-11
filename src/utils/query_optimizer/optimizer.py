"""
Query optimization strategies for reconciliation operations.

Provides optimized query generation for common reconciliation patterns
including row counts and checksum calculations.
"""



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
        """
        if database_type == "postgresql":
            # Use PostgreSQL statistics for approximate count (fast)
            # For exact count, falls back to COUNT(*)
            return f"""
SELECT
    CASE
        WHEN n_live_tup > 0 THEN n_live_tup
        ELSE (SELECT COUNT(*) FROM {table_name})
    END AS row_count
FROM pg_stat_user_tables
WHERE schemaname = 'public' AND relname = '{table_name}';
"""
        elif database_type == "sqlserver":
            # Use SQL Server statistics for approximate count
            return f"""
SELECT
    SUM(p.rows) AS row_count
FROM sys.partitions p
INNER JOIN sys.tables t ON p.object_id = t.object_id
WHERE t.name = '{table_name}'
    AND p.index_id IN (0, 1);
"""
        else:
            # Generic fallback
            return f"SELECT COUNT(*) AS row_count FROM {table_name};"

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
        """
        if database_type == "postgresql":
            # Use PostgreSQL's MD5 hash aggregation
            columns_concat = " || ".join([f"COALESCE(CAST({col} AS TEXT), '')" for col in columns])
            return f"""
SELECT
    MD5(string_agg(row_hash, '' ORDER BY row_hash)) AS table_checksum
FROM (
    SELECT MD5({columns_concat}) AS row_hash
    FROM {table_name}
) t;
"""
        elif database_type == "sqlserver":
            # Use SQL Server's CHECKSUM aggregation
            columns_list = ", ".join(columns)
            return f"""
SELECT
    CHECKSUM_AGG(CAST(CHECKSUM({columns_list}) AS BIGINT)) AS table_checksum
FROM {table_name};
"""
        else:
            raise ValueError(f"Unsupported database type: {database_type}")
