#!/usr/bin/env python3
"""
Query Performance Analysis Tool

Analyzes reconciliation queries and provides optimization recommendations.

Usage:
    python analyze_query_performance.py --config config.yml
    python analyze_query_performance.py --table users --database postgresql
    python analyze_query_performance.py --recommend-indexes --table users
"""

import argparse
import logging
import sys
from pathlib import Path

import psycopg2
import pyodbc
import yaml

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from utils.query_optimizer import QueryOptimizer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file."""
    with open(config_path) as f:
        return yaml.safe_load(f)


def connect_postgres(config: dict) -> psycopg2.extensions.connection:
    """Connect to PostgreSQL database."""
    return psycopg2.connect(
        host=config["host"],
        port=config.get("port", 5432),
        database=config["database"],
        user=config["user"],
        password=config["password"],
    )


def connect_sqlserver(config: dict) -> pyodbc.Connection:
    """Connect to SQL Server database."""
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={config['host']},{config.get('port', 1433)};"
        f"DATABASE={config['database']};"
        f"UID={config['user']};"
        f"PWD={config['password']};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def analyze_query(
    database_type: str,
    config: dict,
    query: str,
    execute: bool = False,
) -> None:
    """
    Analyze a query and display execution plan.

    Args:
        database_type: 'postgresql' or 'sqlserver'
        config: Database configuration
        query: SQL query to analyze
        execute: Whether to execute the query for actual statistics
    """
    logger.info(f"Analyzing query on {database_type}...")

    if database_type == "postgresql":
        conn = connect_postgres(config)
        metrics, plan_text = QueryOptimizer.analyze_postgres_query(conn, query, execute=execute)
        conn.close()
    elif database_type == "sqlserver":
        conn = connect_sqlserver(config)
        metrics, plan_text = QueryOptimizer.analyze_sqlserver_query(conn, query, execute=execute)
        conn.close()
    else:
        logger.error(f"Unsupported database type: {database_type}")
        return

    if metrics is None or plan_text is None:
        logger.error("Failed to analyze query")
        return

    # Display results
    print("\n" + "=" * 80)
    print("QUERY ANALYSIS RESULTS")
    print("=" * 80)

    print("\nExecution Plan:")
    print("-" * 80)
    print(plan_text)

    print("\nMetrics:")
    print("-" * 80)
    if metrics.estimated_rows:
        print(f"  Estimated Rows: {metrics.estimated_rows:,}")
    if metrics.actual_rows is not None:
        print(f"  Actual Rows: {metrics.actual_rows:,}")
    if metrics.execution_time_ms:
        print(f"  Execution Time: {metrics.execution_time_ms:.2f} ms")
    if metrics.has_table_scan:
        print("  ⚠ Table Scan Detected: YES")
    if metrics.has_index_scan:
        print("  ✓ Index Scan Used: YES")
    if metrics.has_hash_join:
        print("  Hash Join: YES")
    if metrics.has_nested_loop:
        print("  Nested Loop: YES")

    if metrics.warnings:
        print("\nWarnings:")
        print("-" * 80)
        for warning in metrics.warnings:
            print(f"  ⚠ {warning}")

    print("\n" + "=" * 80)


def recommend_indexes(
    table_name: str,
    primary_keys: list[str],
    timestamp_column: str | None = None,
    checksum_column: str | None = None,
    status_column: str | None = None,
    database_type: str = "postgresql",
) -> None:
    """
    Generate and display index recommendations.

    Args:
        table_name: Name of the table
        primary_keys: List of primary key columns
        timestamp_column: Timestamp column for change tracking
        checksum_column: Checksum column
        status_column: Status column
        database_type: 'postgresql' or 'sqlserver'
    """
    logger.info(f"Generating index recommendations for table '{table_name}'...")

    recommendations = QueryOptimizer.recommend_indexes_for_reconciliation(
        table_name=table_name,
        primary_keys=primary_keys,
        timestamp_column=timestamp_column,
        checksum_column=checksum_column,
        status_column=status_column,
    )

    print("\n" + "=" * 80)
    print(f"INDEX RECOMMENDATIONS FOR '{table_name}'")
    print("=" * 80)

    for i, rec in enumerate(recommendations, 1):
        print(f"\n{i}. {rec.index_type.upper()} Index")
        print("-" * 80)
        print(f"  Columns: {', '.join(rec.column_names)}")
        if rec.include_columns:
            print(f"  Include Columns: {', '.join(rec.include_columns)}")
        if rec.where_clause:
            print(f"  Where: {rec.where_clause}")
        print(f"  Reason: {rec.reason}")
        print(f"  Estimated Impact: {rec.estimated_impact.upper()}")

        print(f"\n  DDL ({database_type}):")
        ddl = QueryOptimizer.generate_index_ddl(rec, database_type=database_type)
        print(f"  {ddl}")

    print("\n" + "=" * 80)
    print("\nNOTE: Review existing indexes before creating new ones to avoid duplicates.")
    print("=" * 80 + "\n")


def test_row_count_optimization(
    database_type: str,
    config: dict,
    table_name: str,
) -> None:
    """
    Test optimized row count query.

    Args:
        database_type: 'postgresql' or 'sqlserver'
        config: Database configuration
        table_name: Name of the table
    """
    logger.info(f"Testing row count optimization for '{table_name}'...")

    optimized_query = QueryOptimizer.optimize_row_count_query(table_name, database_type)

    print("\n" + "=" * 80)
    print(f"OPTIMIZED ROW COUNT QUERY FOR '{table_name}'")
    print("=" * 80)
    print(optimized_query)
    print("=" * 80 + "\n")

    # Execute and display result
    try:
        if database_type == "postgresql":
            conn = connect_postgres(config)
            cursor = conn.cursor()
            cursor.execute(optimized_query)
            result = cursor.fetchone()
            cursor.close()
            conn.close()
        elif database_type == "sqlserver":
            conn = connect_sqlserver(config)
            cursor = conn.cursor()
            cursor.execute(optimized_query)
            result = cursor.fetchone()
            cursor.close()
            conn.close()
        else:
            logger.error(f"Unsupported database type: {database_type}")
            return

        print(f"Row Count: {result[0]:,}\n")

    except Exception as e:
        logger.error(f"Failed to execute row count query: {e}")


def test_checksum_optimization(
    database_type: str,
    config: dict,
    table_name: str,
    columns: list[str],
) -> None:
    """
    Test optimized checksum query.

    Args:
        database_type: 'postgresql' or 'sqlserver'
        config: Database configuration
        table_name: Name of the table
        columns: Columns to include in checksum
    """
    logger.info(f"Testing checksum optimization for '{table_name}'...")

    optimized_query = QueryOptimizer.optimize_checksum_query(
        table_name, columns, database_type
    )

    print("\n" + "=" * 80)
    print(f"OPTIMIZED CHECKSUM QUERY FOR '{table_name}'")
    print("=" * 80)
    print(optimized_query)
    print("=" * 80 + "\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Analyze query performance and recommend indexes"
    )

    parser.add_argument(
        "--config",
        help="Path to database configuration file (YAML)",
    )

    parser.add_argument(
        "--database",
        choices=["postgresql", "sqlserver"],
        help="Database type",
    )

    parser.add_argument(
        "--table",
        help="Table name to analyze",
    )

    parser.add_argument(
        "--query",
        help="SQL query to analyze",
    )

    parser.add_argument(
        "--query-file",
        help="File containing SQL query to analyze",
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute query to get actual statistics",
    )

    parser.add_argument(
        "--recommend-indexes",
        action="store_true",
        help="Generate index recommendations",
    )

    parser.add_argument(
        "--primary-keys",
        nargs="+",
        help="Primary key columns (for index recommendations)",
    )

    parser.add_argument(
        "--timestamp-column",
        help="Timestamp column for change tracking",
    )

    parser.add_argument(
        "--checksum-column",
        help="Checksum column",
    )

    parser.add_argument(
        "--status-column",
        help="Status column",
    )

    parser.add_argument(
        "--test-row-count",
        action="store_true",
        help="Test optimized row count query",
    )

    parser.add_argument(
        "--test-checksum",
        action="store_true",
        help="Test optimized checksum query",
    )

    parser.add_argument(
        "--checksum-columns",
        nargs="+",
        help="Columns for checksum calculation",
    )

    args = parser.parse_args()

    # Load configuration
    config = {}
    if args.config:
        config = load_config(args.config)
        if "database" in config and not args.database:
            args.database = config["database"].get("type", "postgresql")

    # Validate required arguments
    if not args.database:
        parser.error("--database is required")

    # Execute requested operation
    if args.recommend_indexes:
        if not args.table or not args.primary_keys:
            parser.error("--table and --primary-keys are required for index recommendations")

        recommend_indexes(
            table_name=args.table,
            primary_keys=args.primary_keys,
            timestamp_column=args.timestamp_column,
            checksum_column=args.checksum_column,
            status_column=args.status_column,
            database_type=args.database,
        )

    elif args.test_row_count:
        if not args.table or not args.config:
            parser.error("--table and --config are required for row count test")

        db_config = config.get("database", config)
        test_row_count_optimization(args.database, db_config, args.table)

    elif args.test_checksum:
        if not args.table or not args.checksum_columns or not args.config:
            parser.error("--table, --checksum-columns, and --config are required for checksum test")

        db_config = config.get("database", config)
        test_checksum_optimization(
            args.database, db_config, args.table, args.checksum_columns
        )

    elif args.query or args.query_file:
        if not args.config:
            parser.error("--config is required for query analysis")

        # Get query
        if args.query_file:
            with open(args.query_file) as f:
                query = f.read()
        else:
            query = args.query

        db_config = config.get("database", config)
        analyze_query(args.database, db_config, query, execute=args.execute)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
