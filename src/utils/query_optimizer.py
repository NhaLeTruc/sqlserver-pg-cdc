"""
Query optimization utilities for reconciliation operations.

Provides tools for analyzing query performance, generating execution plans,
and recommending indexes for common reconciliation patterns.
"""

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extensions
import pyodbc
from prometheus_client import Histogram

from utils.tracing import get_tracer, trace_operation

logger = logging.getLogger(__name__)
tracer = get_tracer(__name__)


# Metrics
QUERY_EXECUTION_TIME = Histogram(
    "query_execution_seconds",
    "Query execution time in seconds",
    ["database_type", "query_type"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
)

QUERY_PLAN_ANALYSIS_TIME = Histogram(
    "query_plan_analysis_seconds",
    "Time to analyze query execution plan",
    ["database_type"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)


@dataclass
class ExecutionPlanMetrics:
    """Metrics extracted from query execution plan."""

    estimated_rows: Optional[int] = None
    actual_rows: Optional[int] = None
    execution_time_ms: Optional[float] = None
    cpu_time_ms: Optional[float] = None
    logical_reads: Optional[int] = None
    physical_reads: Optional[int] = None
    scan_count: Optional[int] = None
    has_index_scan: bool = False
    has_table_scan: bool = False
    has_nested_loop: bool = False
    has_hash_join: bool = False
    warnings: List[str] = None

    def __post_init__(self) -> None:
        if self.warnings is None:
            self.warnings = []


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


class QueryOptimizer:
    """
    Query optimizer for database reconciliation operations.

    Provides methods to analyze query performance, generate execution plans,
    and recommend indexes.
    """

    @staticmethod
    def analyze_postgres_query(
        conn: psycopg2.extensions.connection,
        query: str,
        params: Optional[Tuple] = None,
        execute: bool = False,
    ) -> Tuple[Optional[ExecutionPlanMetrics], Optional[str]]:
        """
        Analyze PostgreSQL query and return execution plan metrics.

        Args:
            conn: PostgreSQL database connection
            query: SQL query to analyze
            params: Query parameters
            execute: Whether to actually execute the query (EXPLAIN ANALYZE)

        Returns:
            Tuple of (metrics, plan_text)
        """
        with trace_operation(
            "analyze_postgres_query",
            kind=tracer.SpanKind.CLIENT,
            execute=execute,
        ):
            with QUERY_PLAN_ANALYSIS_TIME.labels(database_type="postgresql").time():
                cursor = conn.cursor()

                try:
                    # Get execution plan
                    if execute:
                        explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
                    else:
                        explain_query = f"EXPLAIN (FORMAT JSON) {query}"

                    if params:
                        cursor.execute(explain_query, params)
                    else:
                        cursor.execute(explain_query)

                    plan_result = cursor.fetchone()[0]

                    # Extract metrics from plan
                    metrics = QueryOptimizer._parse_postgres_plan(plan_result, execute)

                    # Get text format for logging
                    if execute:
                        explain_text_query = f"EXPLAIN (ANALYZE, BUFFERS) {query}"
                    else:
                        explain_text_query = f"EXPLAIN {query}"

                    if params:
                        cursor.execute(explain_text_query, params)
                    else:
                        cursor.execute(explain_text_query)

                    plan_text = "\n".join([row[0] for row in cursor.fetchall()])

                    logger.info(f"PostgreSQL query plan:\n{plan_text}")

                    return metrics, plan_text

                except Exception as e:
                    logger.error(f"Failed to analyze PostgreSQL query: {e}")
                    return None, None

                finally:
                    cursor.close()

    @staticmethod
    def _parse_postgres_plan(
        plan_json: List[Dict[str, Any]], executed: bool
    ) -> ExecutionPlanMetrics:
        """Parse PostgreSQL JSON execution plan."""
        metrics = ExecutionPlanMetrics()

        if not plan_json or not isinstance(plan_json, list):
            return metrics

        plan = plan_json[0].get("Plan", {})

        # Extract basic metrics
        metrics.estimated_rows = plan.get("Plan Rows")

        if executed:
            metrics.actual_rows = plan.get("Actual Rows")
            metrics.execution_time_ms = plan.get("Actual Total Time")

        # Check for scan types
        node_type = plan.get("Node Type", "")
        if "Seq Scan" in node_type or "Table Scan" in node_type:
            metrics.has_table_scan = True
            metrics.warnings.append(f"Table scan detected on {plan.get('Relation Name', 'unknown')}")

        if "Index Scan" in node_type or "Index Only Scan" in node_type:
            metrics.has_index_scan = True

        # Check join types
        if "Nested Loop" in node_type:
            metrics.has_nested_loop = True
        elif "Hash Join" in node_type:
            metrics.has_hash_join = True

        # Recursively check child plans
        QueryOptimizer._check_plan_nodes(plan, metrics)

        return metrics

    @staticmethod
    def _check_plan_nodes(plan: Dict[str, Any], metrics: ExecutionPlanMetrics) -> None:
        """Recursively check plan nodes for optimization opportunities."""
        # Check for table scans
        node_type = plan.get("Node Type", "")
        if "Seq Scan" in node_type:
            relation = plan.get("Relation Name", "unknown")
            if relation != "unknown":
                metrics.warnings.append(f"Sequential scan on table '{relation}'")

        # Check for sort operations
        if "Sort" in node_type:
            sort_key = plan.get("Sort Key", [])
            if sort_key:
                metrics.warnings.append(f"Sort operation on columns: {sort_key}")

        # Recursively check child plans
        for child_plan in plan.get("Plans", []):
            QueryOptimizer._check_plan_nodes(child_plan, metrics)

    @staticmethod
    def analyze_sqlserver_query(
        conn: pyodbc.Connection,
        query: str,
        params: Optional[Tuple] = None,
        execute: bool = False,
    ) -> Tuple[Optional[ExecutionPlanMetrics], Optional[str]]:
        """
        Analyze SQL Server query and return execution plan metrics.

        Args:
            conn: SQL Server database connection
            query: SQL query to analyze
            params: Query parameters
            execute: Whether to actually execute the query and get actual stats

        Returns:
            Tuple of (metrics, plan_text)
        """
        with trace_operation(
            "analyze_sqlserver_query",
            kind=tracer.SpanKind.CLIENT,
            execute=execute,
        ):
            with QUERY_PLAN_ANALYSIS_TIME.labels(database_type="sqlserver").time():
                cursor = conn.cursor()

                try:
                    # Enable execution plan
                    cursor.execute("SET SHOWPLAN_TEXT ON")

                    # Get estimated plan
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)

                    plan_rows = cursor.fetchall()
                    plan_text = "\n".join([row[0] for row in plan_rows])

                    cursor.execute("SET SHOWPLAN_TEXT OFF")

                    # Get actual execution statistics if requested
                    if execute:
                        cursor.execute("SET STATISTICS TIME ON")
                        cursor.execute("SET STATISTICS IO ON")

                        if params:
                            cursor.execute(query, params)
                        else:
                            cursor.execute(query)

                        # Fetch results to ensure query executes
                        cursor.fetchall()

                        cursor.execute("SET STATISTICS TIME OFF")
                        cursor.execute("SET STATISTICS IO OFF")

                    # Parse plan metrics
                    metrics = QueryOptimizer._parse_sqlserver_plan(plan_text)

                    logger.info(f"SQL Server query plan:\n{plan_text}")

                    return metrics, plan_text

                except Exception as e:
                    logger.error(f"Failed to analyze SQL Server query: {e}")
                    return None, None

                finally:
                    cursor.close()

    @staticmethod
    def _parse_sqlserver_plan(plan_text: str) -> ExecutionPlanMetrics:
        """Parse SQL Server text execution plan."""
        metrics = ExecutionPlanMetrics()

        if not plan_text:
            return metrics

        # Check for table scans
        if "Table Scan" in plan_text:
            metrics.has_table_scan = True
            # Extract table name
            table_match = re.search(r"Table Scan.*?\[(\w+)\]", plan_text)
            if table_match:
                table_name = table_match.group(1)
                metrics.warnings.append(f"Table scan detected on table '{table_name}'")

        # Check for index scans
        if "Index Scan" in plan_text or "Index Seek" in plan_text:
            metrics.has_index_scan = True

        # Check for nested loops
        if "Nested Loops" in plan_text:
            metrics.has_nested_loop = True

        # Check for hash joins
        if "Hash Match" in plan_text:
            metrics.has_hash_join = True

        # Look for sort operations
        if "Sort" in plan_text:
            metrics.warnings.append("Sort operation detected")

        # Extract estimated rows
        rows_match = re.search(r"EstimateRows = (\d+)", plan_text)
        if rows_match:
            metrics.estimated_rows = int(rows_match.group(1))

        return metrics

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
            return QueryOptimizer._generate_postgres_index_ddl(recommendation)
        elif database_type == "sqlserver":
            return QueryOptimizer._generate_sqlserver_index_ddl(recommendation)
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
        columns: List[str],
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
