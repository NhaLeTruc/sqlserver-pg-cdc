"""
Query analysis utilities for examining execution plans.

Provides tools for analyzing query performance, generating execution plans,
and extracting metrics from PostgreSQL and SQL Server databases.
"""

import logging
import re
from dataclasses import dataclass
from typing import Any

import psycopg2
import psycopg2.extensions
import pyodbc
from opentelemetry import trace
from prometheus_client import Histogram

from src.utils.tracing import trace_operation

logger = logging.getLogger(__name__)


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

    estimated_rows: int | None = None
    actual_rows: int | None = None
    execution_time_ms: float | None = None
    cpu_time_ms: float | None = None
    logical_reads: int | None = None
    physical_reads: int | None = None
    scan_count: int | None = None
    has_index_scan: bool = False
    has_table_scan: bool = False
    has_nested_loop: bool = False
    has_hash_join: bool = False
    warnings: list[str] = None

    def __post_init__(self) -> None:
        if self.warnings is None:
            self.warnings = []


class QueryAnalyzer:
    """
    Query analyzer for database operations.

    Provides methods to analyze query performance and generate execution plans.
    """

    @staticmethod
    def analyze_postgres_query(
        conn: psycopg2.extensions.connection,
        query: str,
        params: tuple | None = None,
        execute: bool = False,
    ) -> tuple[ExecutionPlanMetrics | None, str | None]:
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
            kind=trace.SpanKind.CLIENT,
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
                    metrics = QueryAnalyzer._parse_postgres_plan(plan_result, execute)

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
        plan_json: list[dict[str, Any]], executed: bool
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
        QueryAnalyzer._check_plan_nodes(plan, metrics)

        return metrics

    @staticmethod
    def _check_plan_nodes(plan: dict[str, Any], metrics: ExecutionPlanMetrics) -> None:
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
            QueryAnalyzer._check_plan_nodes(child_plan, metrics)

    @staticmethod
    def analyze_sqlserver_query(
        conn: pyodbc.Connection,
        query: str,
        params: tuple | None = None,
        execute: bool = False,
    ) -> tuple[ExecutionPlanMetrics | None, str | None]:
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
            kind=trace.SpanKind.CLIENT,
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
                    metrics = QueryAnalyzer._parse_sqlserver_plan(plan_text)

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
            # Extract table name (handles both [table] and [schema].[table] formats)
            table_match = re.search(r"Table Scan.*?\[(?:\w+\]\.\[)?(\w+)\]", plan_text)
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
