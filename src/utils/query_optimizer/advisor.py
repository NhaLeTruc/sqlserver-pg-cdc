"""
Index recommendation and DDL generation utilities.

Provides tools for recommending indexes based on common reconciliation patterns
and generating database-specific DDL statements for creating those indexes.
"""

from dataclasses import dataclass

from utils.sql_safety import quote_identifier, validate_identifier


@dataclass
class IndexRecommendation:
    """Index recommendation for query optimization."""

    table_name: str
    column_names: list[str]
    index_type: str  # 'btree', 'hash', 'gin', etc.
    include_columns: list[str] | None = None
    where_clause: str | None = None  # For partial indexes
    reason: str = ""
    estimated_impact: str = "medium"  # 'low', 'medium', 'high'


class IndexAdvisor:
    """
    Index advisor for reconciliation operations.

    Provides methods to recommend indexes and generate DDL statements.
    """

    @staticmethod
    def recommend_indexes_for_reconciliation(
        table_name: str,
        primary_keys: list[str],
        timestamp_column: str | None = None,
        checksum_column: str | None = None,
        status_column: str | None = None,
    ) -> list[IndexRecommendation]:
        """
        Generate index recommendations for common reconciliation patterns.

        Args:
            table_name: Name of the table to optimize
            primary_keys: List of primary key columns
            timestamp_column: Column tracking last update time
            checksum_column: Column containing checksum value
            status_column: Column indicating record status

        Returns:
            List of index recommendations
        """
        recommendations = []

        # 1. Primary key index (usually already exists, but included for completeness)
        recommendations.append(
            IndexRecommendation(
                table_name=table_name,
                column_names=primary_keys,
                index_type="btree",
                reason="Primary key lookup optimization for row-level reconciliation",
                estimated_impact="high",
            )
        )

        # 2. Timestamp index for change tracking
        if timestamp_column:
            recommendations.append(
                IndexRecommendation(
                    table_name=table_name,
                    column_names=[timestamp_column],
                    index_type="btree",
                    include_columns=primary_keys,
                    reason="Optimize incremental reconciliation queries filtering by timestamp",
                    estimated_impact="high",
                )
            )

        # 3. Checksum index for validation queries
        if checksum_column:
            recommendations.append(
                IndexRecommendation(
                    table_name=table_name,
                    column_names=[checksum_column],
                    index_type="hash",  # Hash index for exact match in PostgreSQL
                    include_columns=primary_keys,
                    reason="Optimize checksum validation queries",
                    estimated_impact="medium",
                )
            )

        # 4. Composite index for filtered queries
        if status_column and timestamp_column:
            recommendations.append(
                IndexRecommendation(
                    table_name=table_name,
                    column_names=[status_column, timestamp_column],
                    index_type="btree",
                    include_columns=primary_keys + ([checksum_column] if checksum_column else []),
                    reason="Optimize queries filtering by status and timestamp",
                    estimated_impact="medium",
                )
            )

        # 5. Partial index for active records only
        if status_column:
            recommendations.append(
                IndexRecommendation(
                    table_name=table_name,
                    column_names=primary_keys + ([timestamp_column] if timestamp_column else []),
                    index_type="btree",
                    where_clause=f"{status_column} = 'active'",
                    reason="Partial index for active records reduces index size",
                    estimated_impact="medium",
                )
            )

        return recommendations

    @staticmethod
    def generate_index_ddl(
        recommendation: IndexRecommendation, database_type: str = "postgresql"
    ) -> str:
        """
        Generate DDL statement for creating an index.

        Args:
            recommendation: Index recommendation
            database_type: 'postgresql' or 'sqlserver'

        Returns:
            DDL statement as string
        """
        if database_type == "postgresql":
            return IndexAdvisor._generate_postgres_index_ddl(recommendation)
        elif database_type == "sqlserver":
            return IndexAdvisor._generate_sqlserver_index_ddl(recommendation)
        else:
            raise ValueError(f"Unsupported database type: {database_type}")

    @staticmethod
    def _generate_postgres_index_ddl(recommendation: IndexRecommendation) -> str:
        """Generate PostgreSQL index DDL."""
        # Validate all identifiers
        validate_identifier(recommendation.table_name)
        for col in recommendation.column_names:
            validate_identifier(col)
        if recommendation.include_columns:
            for col in recommendation.include_columns:
                validate_identifier(col)
        if recommendation.index_type not in ("btree", "hash", "gin", "gist", "brin"):
            raise ValueError(f"Invalid index type: {recommendation.index_type}")

        # Index name (validated identifiers are safe for concatenation)
        columns_str = "_".join(recommendation.column_names)
        index_name = f"ix_{recommendation.table_name}_{columns_str}"
        validate_identifier(index_name)

        # Quote identifiers
        quoted_table = quote_identifier(recommendation.table_name, "postgresql")
        quoted_index = quote_identifier(index_name, "postgresql")
        quoted_columns = [quote_identifier(col, "postgresql") for col in recommendation.column_names]
        columns = ", ".join(quoted_columns)

        # Index type
        index_type_clause = ""
        if recommendation.index_type != "btree":
            index_type_clause = f" USING {recommendation.index_type}"

        # INCLUDE clause
        include_clause = ""
        if recommendation.include_columns:
            quoted_include = [quote_identifier(col, "postgresql") for col in recommendation.include_columns]
            include_cols = ", ".join(quoted_include)
            include_clause = f" INCLUDE ({include_cols})"

        # WHERE clause for partial indexes
        # Note: where_clause is passed through as-is; callers must ensure it's safe
        where_clause = ""
        if recommendation.where_clause:
            where_clause = f" WHERE {recommendation.where_clause}"

        ddl = (
            f"CREATE INDEX CONCURRENTLY {quoted_index}\n"
            f"ON {quoted_table}{index_type_clause} ({columns})"
            f"{include_clause}{where_clause};"
        )

        return ddl

    @staticmethod
    def _generate_sqlserver_index_ddl(recommendation: IndexRecommendation) -> str:
        """Generate SQL Server index DDL."""
        # Validate all identifiers
        validate_identifier(recommendation.table_name)
        for col in recommendation.column_names:
            validate_identifier(col)
        if recommendation.include_columns:
            for col in recommendation.include_columns:
                validate_identifier(col)

        # Index name (validated identifiers are safe for concatenation)
        columns_str = "_".join(recommendation.column_names)
        index_name = f"IX_{recommendation.table_name}_{columns_str}"
        validate_identifier(index_name)

        # Quote identifiers
        quoted_table = quote_identifier(recommendation.table_name, "sqlserver")
        quoted_index = quote_identifier(index_name, "sqlserver")
        quoted_columns = [quote_identifier(col, "sqlserver") for col in recommendation.column_names]
        columns = ", ".join(quoted_columns)

        # INCLUDE clause
        include_clause = ""
        if recommendation.include_columns:
            quoted_include = [quote_identifier(col, "sqlserver") for col in recommendation.include_columns]
            include_cols = ", ".join(quoted_include)
            include_clause = f"\nINCLUDE ({include_cols})"

        # WHERE clause for filtered indexes
        # Note: where_clause is passed through as-is; callers must ensure it's safe
        where_clause = ""
        if recommendation.where_clause:
            where_clause = f"\nWHERE {recommendation.where_clause}"

        ddl = (
            f"CREATE NONCLUSTERED INDEX {quoted_index}\n"
            f"ON [dbo].{quoted_table} ({columns})"
            f"{include_clause}{where_clause}\n"
            f"WITH (ONLINE = ON, FILLFACTOR = 90);"
        )

        return ddl
