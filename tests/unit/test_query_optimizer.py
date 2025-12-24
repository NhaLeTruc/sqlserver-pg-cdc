"""
Unit tests for query optimizer module.

Tests query analysis, index recommendations, and DDL generation.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

from utils.query_optimizer import (
    ExecutionPlanMetrics,
    IndexRecommendation,
    QueryOptimizer,
)


class TestExecutionPlanMetrics:
    """Test ExecutionPlanMetrics dataclass."""

    def test_initialization(self):
        """Test basic initialization."""
        metrics = ExecutionPlanMetrics()
        assert metrics.estimated_rows is None
        assert metrics.actual_rows is None
        assert metrics.warnings == []

    def test_initialization_with_values(self):
        """Test initialization with values."""
        metrics = ExecutionPlanMetrics(
            estimated_rows=1000,
            actual_rows=950,
            execution_time_ms=123.45,
            has_table_scan=True,
        )
        assert metrics.estimated_rows == 1000
        assert metrics.actual_rows == 950
        assert metrics.execution_time_ms == 123.45
        assert metrics.has_table_scan is True
        assert metrics.warnings == []


class TestIndexRecommendation:
    """Test IndexRecommendation dataclass."""

    def test_basic_recommendation(self):
        """Test basic index recommendation."""
        rec = IndexRecommendation(
            table_name="users",
            column_names=["id"],
            index_type="btree",
            reason="Primary key lookup",
        )
        assert rec.table_name == "users"
        assert rec.column_names == ["id"]
        assert rec.index_type == "btree"
        assert rec.reason == "Primary key lookup"

    def test_recommendation_with_include_columns(self):
        """Test recommendation with INCLUDE columns."""
        rec = IndexRecommendation(
            table_name="users",
            column_names=["created_at"],
            index_type="btree",
            include_columns=["id", "email"],
            reason="Covering index",
        )
        assert rec.include_columns == ["id", "email"]

    def test_recommendation_with_where_clause(self):
        """Test partial index recommendation."""
        rec = IndexRecommendation(
            table_name="users",
            column_names=["id"],
            index_type="btree",
            where_clause="status = 'active'",
            reason="Partial index for active users",
        )
        assert rec.where_clause == "status = 'active'"


class TestQueryOptimizer:
    """Test QueryOptimizer functionality."""

    def test_parse_postgres_plan_with_table_scan(self):
        """Test parsing PostgreSQL plan with table scan."""
        plan_json = [
            {
                "Plan": {
                    "Node Type": "Seq Scan",
                    "Relation Name": "users",
                    "Plan Rows": 1000,
                }
            }
        ]

        metrics = QueryOptimizer._parse_postgres_plan(plan_json, executed=False)

        assert metrics.estimated_rows == 1000
        assert metrics.has_table_scan is True
        assert any("users" in warning for warning in metrics.warnings)

    def test_parse_postgres_plan_with_index_scan(self):
        """Test parsing PostgreSQL plan with index scan."""
        plan_json = [
            {
                "Plan": {
                    "Node Type": "Index Scan",
                    "Relation Name": "users",
                    "Plan Rows": 100,
                }
            }
        ]

        metrics = QueryOptimizer._parse_postgres_plan(plan_json, executed=False)

        assert metrics.estimated_rows == 100
        assert metrics.has_index_scan is True
        assert metrics.has_table_scan is False

    def test_parse_postgres_plan_with_hash_join(self):
        """Test parsing PostgreSQL plan with hash join."""
        plan_json = [
            {
                "Plan": {
                    "Node Type": "Hash Join",
                    "Plan Rows": 500,
                }
            }
        ]

        metrics = QueryOptimizer._parse_postgres_plan(plan_json, executed=False)

        assert metrics.has_hash_join is True

    def test_parse_postgres_plan_with_nested_loop(self):
        """Test parsing PostgreSQL plan with nested loop."""
        plan_json = [
            {
                "Plan": {
                    "Node Type": "Nested Loop",
                    "Plan Rows": 200,
                }
            }
        ]

        metrics = QueryOptimizer._parse_postgres_plan(plan_json, executed=False)

        assert metrics.has_nested_loop is True

    def test_parse_postgres_plan_executed(self):
        """Test parsing PostgreSQL plan with actual execution data."""
        plan_json = [
            {
                "Plan": {
                    "Node Type": "Index Scan",
                    "Plan Rows": 100,
                    "Actual Rows": 95,
                    "Actual Total Time": 12.345,
                }
            }
        ]

        metrics = QueryOptimizer._parse_postgres_plan(plan_json, executed=True)

        assert metrics.estimated_rows == 100
        assert metrics.actual_rows == 95
        assert metrics.execution_time_ms == 12.345

    def test_parse_sqlserver_plan_with_table_scan(self):
        """Test parsing SQL Server plan with table scan."""
        plan_text = """
        |--Table Scan(OBJECT:([dbo].[users]))
           EstimateRows = 1000
        """

        metrics = QueryOptimizer._parse_sqlserver_plan(plan_text)

        assert metrics.has_table_scan is True
        assert metrics.estimated_rows == 1000
        assert any("users" in warning for warning in metrics.warnings)

    def test_parse_sqlserver_plan_with_index_scan(self):
        """Test parsing SQL Server plan with index scan."""
        plan_text = """
        |--Index Scan(OBJECT:([dbo].[users].[IX_users_email]))
           EstimateRows = 100
        """

        metrics = QueryOptimizer._parse_sqlserver_plan(plan_text)

        assert metrics.has_index_scan is True
        assert metrics.estimated_rows == 100

    def test_parse_sqlserver_plan_with_hash_match(self):
        """Test parsing SQL Server plan with hash join."""
        plan_text = """
        |--Hash Match(Inner Join)
           EstimateRows = 500
        """

        metrics = QueryOptimizer._parse_sqlserver_plan(plan_text)

        assert metrics.has_hash_join is True
        assert metrics.estimated_rows == 500

    def test_parse_sqlserver_plan_with_nested_loops(self):
        """Test parsing SQL Server plan with nested loops."""
        plan_text = """
        |--Nested Loops(Inner Join)
           EstimateRows = 200
        """

        metrics = QueryOptimizer._parse_sqlserver_plan(plan_text)

        assert metrics.has_nested_loop is True

    def test_parse_sqlserver_plan_with_sort(self):
        """Test parsing SQL Server plan with sort operation."""
        plan_text = """
        |--Sort(ORDER BY:([users].[created_at] DESC))
           EstimateRows = 1000
        """

        metrics = QueryOptimizer._parse_sqlserver_plan(plan_text)

        assert any("Sort" in warning for warning in metrics.warnings)

    def test_recommend_indexes_basic(self):
        """Test basic index recommendations."""
        recommendations = QueryOptimizer.recommend_indexes_for_reconciliation(
            table_name="users",
            primary_keys=["id"],
        )

        assert len(recommendations) >= 1
        # Should include primary key recommendation
        pk_rec = recommendations[0]
        assert pk_rec.table_name == "users"
        assert pk_rec.column_names == ["id"]

    def test_recommend_indexes_with_timestamp(self):
        """Test recommendations with timestamp column."""
        recommendations = QueryOptimizer.recommend_indexes_for_reconciliation(
            table_name="users",
            primary_keys=["id"],
            timestamp_column="updated_at",
        )

        # Should include timestamp index
        ts_recs = [r for r in recommendations if "updated_at" in r.column_names]
        assert len(ts_recs) >= 1

    def test_recommend_indexes_with_checksum(self):
        """Test recommendations with checksum column."""
        recommendations = QueryOptimizer.recommend_indexes_for_reconciliation(
            table_name="users",
            primary_keys=["id"],
            checksum_column="row_checksum",
        )

        # Should include checksum index
        checksum_recs = [r for r in recommendations if "row_checksum" in r.column_names]
        assert len(checksum_recs) >= 1

    def test_recommend_indexes_with_status(self):
        """Test recommendations with status column."""
        recommendations = QueryOptimizer.recommend_indexes_for_reconciliation(
            table_name="users",
            primary_keys=["id"],
            status_column="status",
            timestamp_column="updated_at",
        )

        # Should include composite index
        composite_recs = [
            r for r in recommendations if "status" in r.column_names and len(r.column_names) > 1
        ]
        assert len(composite_recs) >= 1

    def test_recommend_indexes_partial_index(self):
        """Test partial index recommendation."""
        recommendations = QueryOptimizer.recommend_indexes_for_reconciliation(
            table_name="users",
            primary_keys=["id"],
            status_column="status",
        )

        # Should include partial index with WHERE clause
        partial_recs = [r for r in recommendations if r.where_clause is not None]
        assert len(partial_recs) >= 1

    def test_generate_postgres_index_ddl_basic(self):
        """Test PostgreSQL DDL generation for basic index."""
        rec = IndexRecommendation(
            table_name="users",
            column_names=["id"],
            index_type="btree",
        )

        ddl = QueryOptimizer.generate_index_ddl(rec, database_type="postgresql")

        assert "CREATE INDEX CONCURRENTLY" in ddl
        assert "ix_users_id" in ddl
        assert "ON users" in ddl
        assert "(id)" in ddl

    def test_generate_postgres_index_ddl_with_include(self):
        """Test PostgreSQL DDL with INCLUDE columns."""
        rec = IndexRecommendation(
            table_name="users",
            column_names=["created_at"],
            index_type="btree",
            include_columns=["id", "email"],
        )

        ddl = QueryOptimizer.generate_index_ddl(rec, database_type="postgresql")

        assert "INCLUDE (id, email)" in ddl

    def test_generate_postgres_index_ddl_with_where(self):
        """Test PostgreSQL DDL with WHERE clause."""
        rec = IndexRecommendation(
            table_name="users",
            column_names=["id"],
            index_type="btree",
            where_clause="status = 'active'",
        )

        ddl = QueryOptimizer.generate_index_ddl(rec, database_type="postgresql")

        assert "WHERE status = 'active'" in ddl

    def test_generate_postgres_index_ddl_hash(self):
        """Test PostgreSQL DDL for hash index."""
        rec = IndexRecommendation(
            table_name="users",
            column_names=["email_hash"],
            index_type="hash",
        )

        ddl = QueryOptimizer.generate_index_ddl(rec, database_type="postgresql")

        assert "USING hash" in ddl

    def test_generate_sqlserver_index_ddl_basic(self):
        """Test SQL Server DDL generation for basic index."""
        rec = IndexRecommendation(
            table_name="users",
            column_names=["id"],
            index_type="btree",
        )

        ddl = QueryOptimizer.generate_index_ddl(rec, database_type="sqlserver")

        assert "CREATE NONCLUSTERED INDEX" in ddl
        assert "IX_users_id" in ddl
        assert "ON dbo.users" in ddl
        assert "(id)" in ddl
        assert "WITH (ONLINE = ON, FILLFACTOR = 90)" in ddl

    def test_generate_sqlserver_index_ddl_with_include(self):
        """Test SQL Server DDL with INCLUDE columns."""
        rec = IndexRecommendation(
            table_name="users",
            column_names=["created_at"],
            index_type="btree",
            include_columns=["id", "email"],
        )

        ddl = QueryOptimizer.generate_index_ddl(rec, database_type="sqlserver")

        assert "INCLUDE (id, email)" in ddl

    def test_generate_sqlserver_index_ddl_with_where(self):
        """Test SQL Server DDL with WHERE clause."""
        rec = IndexRecommendation(
            table_name="users",
            column_names=["id"],
            index_type="btree",
            where_clause="status = 'active'",
        )

        ddl = QueryOptimizer.generate_index_ddl(rec, database_type="sqlserver")

        assert "WHERE status = 'active'" in ddl

    def test_generate_index_ddl_invalid_database_type(self):
        """Test DDL generation with invalid database type."""
        rec = IndexRecommendation(
            table_name="users",
            column_names=["id"],
            index_type="btree",
        )

        with pytest.raises(ValueError, match="Unsupported database type"):
            QueryOptimizer.generate_index_ddl(rec, database_type="mysql")

    def test_optimize_row_count_query_postgres(self):
        """Test row count query optimization for PostgreSQL."""
        query = QueryOptimizer.optimize_row_count_query("users", database_type="postgresql")

        assert "pg_stat_user_tables" in query
        assert "n_live_tup" in query
        assert "users" in query

    def test_optimize_row_count_query_sqlserver(self):
        """Test row count query optimization for SQL Server."""
        query = QueryOptimizer.optimize_row_count_query("users", database_type="sqlserver")

        assert "sys.partitions" in query
        assert "sys.tables" in query
        assert "users" in query

    def test_optimize_row_count_query_generic(self):
        """Test row count query for generic database."""
        query = QueryOptimizer.optimize_row_count_query("users", database_type="unknown")

        assert "SELECT COUNT(*)" in query
        assert "FROM users" in query

    def test_optimize_checksum_query_postgres(self):
        """Test checksum query optimization for PostgreSQL."""
        query = QueryOptimizer.optimize_checksum_query(
            "users", columns=["id", "email", "name"], database_type="postgresql"
        )

        assert "MD5" in query
        assert "string_agg" in query
        assert "id" in query
        assert "email" in query
        assert "name" in query

    def test_optimize_checksum_query_sqlserver(self):
        """Test checksum query optimization for SQL Server."""
        query = QueryOptimizer.optimize_checksum_query(
            "users", columns=["id", "email", "name"], database_type="sqlserver"
        )

        assert "CHECKSUM_AGG" in query
        assert "CHECKSUM" in query
        assert "id" in query
        assert "email" in query
        assert "name" in query

    def test_optimize_checksum_query_invalid_database_type(self):
        """Test checksum query with invalid database type."""
        with pytest.raises(ValueError, match="Unsupported database type"):
            QueryOptimizer.optimize_checksum_query(
                "users", columns=["id"], database_type="mysql"
            )

    @patch("psycopg2.extensions.connection")
    def test_analyze_postgres_query_without_execution(self, mock_conn):
        """Test PostgreSQL query analysis without execution."""
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Mock EXPLAIN JSON result
        mock_cursor.fetchone.return_value = (
            [
                {
                    "Plan": {
                        "Node Type": "Index Scan",
                        "Plan Rows": 100,
                    }
                }
            ],
        )

        # Mock EXPLAIN text result
        mock_cursor.fetchall.return_value = [("Index Scan on users",), ("Rows: 100",)]

        metrics, plan_text = QueryOptimizer.analyze_postgres_query(
            mock_conn, "SELECT * FROM users WHERE id = 1", execute=False
        )

        assert metrics is not None
        assert metrics.estimated_rows == 100
        assert plan_text is not None
        assert "Index Scan" in plan_text

    @patch("pyodbc.Connection")
    def test_analyze_sqlserver_query_without_execution(self, mock_conn):
        """Test SQL Server query analysis without execution."""
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Mock SHOWPLAN_TEXT result
        mock_cursor.fetchall.return_value = [
            ("|--Index Seek(OBJECT:([dbo].[users].[IX_users_id]))",),
            ("   EstimateRows = 100",),
        ]

        metrics, plan_text = QueryOptimizer.analyze_sqlserver_query(
            mock_conn, "SELECT * FROM users WHERE id = 1", execute=False
        )

        assert metrics is not None
        assert metrics.estimated_rows == 100
        assert plan_text is not None
        assert "Index Seek" in plan_text
