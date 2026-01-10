"""
Index recommendation and DDL generation utilities.

Provides tools for recommending indexes based on common reconciliation patterns
and generating database-specific DDL statements for creating those indexes.
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class IndexRecommendation:
    """Index recommendation for query optimization."""

    table_name: str
    column_names: List[str]
    index_type: str  # 'btree', 'hash', 'gin', etc.
    include_columns: Optional[List[str]] = None
    where_clause: Optional[str] = None  # For partial indexes
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
        primary_keys: List[str],
        timestamp_column: Optional[str] = None,
        checksum_column: Optional[str] = None,
        status_column: Optional[str] = None,
    ) -> List[IndexRecommendation]:
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
        # Index name
        columns_str = "_".join(recommendation.column_names)
        index_name = f"ix_{recommendation.table_name}_{columns_str}"

        # Column list
        columns = ", ".join(recommendation.column_names)

        # Index type
        index_type_clause = ""
        if recommendation.index_type != "btree":
            index_type_clause = f" USING {recommendation.index_type}"

        # INCLUDE clause
        include_clause = ""
        if recommendation.include_columns:
            include_cols = ", ".join(recommendation.include_columns)
            include_clause = f" INCLUDE ({include_cols})"

        # WHERE clause for partial indexes
        where_clause = ""
        if recommendation.where_clause:
            where_clause = f" WHERE {recommendation.where_clause}"

        ddl = (
            f"CREATE INDEX CONCURRENTLY {index_name}\n"
            f"ON {recommendation.table_name}{index_type_clause} ({columns})"
            f"{include_clause}{where_clause};"
        )

        return ddl

    @staticmethod
    def _generate_sqlserver_index_ddl(recommendation: IndexRecommendation) -> str:
        """Generate SQL Server index DDL."""
        # Index name
        columns_str = "_".join(recommendation.column_names)
        index_name = f"IX_{recommendation.table_name}_{columns_str}"

        # Column list
        columns = ", ".join(recommendation.column_names)

        # INCLUDE clause
        include_clause = ""
        if recommendation.include_columns:
            include_cols = ", ".join(recommendation.include_columns)
            include_clause = f"\nINCLUDE ({include_cols})"

        # WHERE clause for filtered indexes
        where_clause = ""
        if recommendation.where_clause:
            where_clause = f"\nWHERE {recommendation.where_clause}"

        ddl = (
            f"CREATE NONCLUSTERED INDEX {index_name}\n"
            f"ON dbo.{recommendation.table_name} ({columns})"
            f"{include_clause}{where_clause}\n"
            f"WITH (ONLINE = ON, FILLFACTOR = 90);"
        )

        return ddl
