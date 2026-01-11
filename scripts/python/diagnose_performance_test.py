#!/usr/bin/env python3
"""
Diagnose performance test instability issues.

This script helps identify why test_measure_replication_throughput is unstable.
"""

import os
import time

import psycopg2
import pyodbc
import requests


def check_connector_health(kafka_connect_url: str) -> dict:
    """Check connector health and return status."""
    results = {}

    try:
        # Get connector list
        response = requests.get(f"{kafka_connect_url}/connectors", timeout=5)
        response.raise_for_status()
        connectors = response.json()
        results["connectors_available"] = connectors

        # Check each connector
        for connector in ["sqlserver-cdc-source", "postgresql-jdbc-sink"]:
            if connector in connectors:
                status_response = requests.get(
                    f"{kafka_connect_url}/connectors/{connector}/status",
                    timeout=5
                )
                status_response.raise_for_status()
                status = status_response.json()
                results[connector] = {
                    "state": status.get("connector", {}).get("state"),
                    "tasks": [
                        {
                            "id": t.get("id"),
                            "state": t.get("state"),
                            "worker_id": t.get("worker_id")
                        }
                        for t in status.get("tasks", [])
                    ]
                }
            else:
                results[connector] = {"state": "MISSING"}

    except Exception as e:
        results["error"] = str(e)

    return results


def check_database_state(sqlserver_conn, postgres_conn) -> dict:
    """Check database state for stale data."""
    results = {}

    try:
        # Check SQL Server
        with sqlserver_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM dbo.customers WHERE name LIKE 'Perf Test %'")
            results["sqlserver_perf_test_rows"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM dbo.customers")
            results["sqlserver_total_rows"] = cursor.fetchone()[0]

        # Check PostgreSQL
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM customers WHERE name LIKE 'Perf Test %'")
            results["postgres_perf_test_rows"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM customers")
            results["postgres_total_rows"] = cursor.fetchone()[0]

    except Exception as e:
        results["error"] = str(e)

    return results


def check_cdc_lag(sqlserver_conn) -> dict:
    """Check CDC capture job lag."""
    results = {}

    try:
        with sqlserver_conn.cursor() as cursor:
            # Check if CDC capture job is running
            cursor.execute("""
                SELECT
                    name,
                    is_cdc_enabled
                FROM sys.databases
                WHERE name = 'warehouse_source'
            """)
            row = cursor.fetchone()
            results["cdc_enabled"] = bool(row[1]) if row else False

            # Check CDC scan status
            cursor.execute("""
                SELECT
                    COUNT(*) as pending_changes
                FROM cdc.lsn_time_mapping
            """)
            results["cdc_lsn_entries"] = cursor.fetchone()[0]

    except Exception as e:
        results["error"] = str(e)

    return results


def simulate_test_scenario():
    """Simulate the test scenario and identify issues."""
    print("="*70)
    print("PERFORMANCE TEST DIAGNOSTIC")
    print("="*70)
    print()

    # Connect to databases
    print("Connecting to databases...")
    sqlserver_conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('SQLSERVER_HOST', 'localhost')},1433;"
        f"DATABASE={os.getenv('SQLSERVER_DATABASE', 'warehouse_source')};"
        f"UID={os.getenv('SQLSERVER_USER', 'sa')};"
        f"PWD={os.getenv('SQLSERVER_PASSWORD', 'YourStrong!Passw0rd')};"
        f"TrustServerCertificate=yes;",
        autocommit=False
    )

    postgres_conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "warehouse_target"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres_secure_password"),
    )
    postgres_conn.autocommit = True

    print("✓ Connected to databases")
    print()

    # Check connector health
    print("Checking connector health...")
    kafka_connect_url = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
    connector_health = check_connector_health(kafka_connect_url)

    print("Connector Status:")
    for key, value in connector_health.items():
        if key.endswith("-source") or key.endswith("-sink"):
            print(f"  {key}: {value.get('state', 'UNKNOWN')}")
            if value.get("tasks"):
                for task in value["tasks"]:
                    print(f"    Task {task['id']}: {task['state']}")
    print()

    # Check database state
    print("Checking database state...")
    db_state = check_database_state(sqlserver_conn, postgres_conn)
    print("Database State:")
    print("  SQL Server:")
    print(f"    Total rows: {db_state.get('sqlserver_total_rows', 'N/A')}")
    print(f"    Perf Test rows: {db_state.get('sqlserver_perf_test_rows', 'N/A')}")
    print("  PostgreSQL:")
    print(f"    Total rows: {db_state.get('postgres_total_rows', 'N/A')}")
    print(f"    Perf Test rows: {db_state.get('postgres_perf_test_rows', 'N/A')}")
    print()

    # Check for stale data
    if db_state.get('sqlserver_perf_test_rows', 0) > 0 or db_state.get('postgres_perf_test_rows', 0) > 0:
        print("⚠️  WARNING: Stale performance test data found!")
        print("   This could cause test instability.")
        print("   Run: make reset-test-env")
        print()

    # Check CDC lag
    print("Checking CDC capture lag...")
    cdc_lag = check_cdc_lag(sqlserver_conn)
    print("CDC Status:")
    print(f"  CDC Enabled: {cdc_lag.get('cdc_enabled', 'N/A')}")
    print(f"  LSN Entries: {cdc_lag.get('cdc_lsn_entries', 'N/A')}")
    print()

    # Test clear_tables operation
    print("Testing clear_tables operation...")
    print("  Deleting from PostgreSQL...")
    with postgres_conn.cursor() as cursor:
        cursor.execute("DELETE FROM customers WHERE name LIKE 'Perf Test %'")

    print("  Deleting from SQL Server...")
    with sqlserver_conn.cursor() as cursor:
        cursor.execute("DELETE FROM dbo.customers WHERE name LIKE 'Perf Test %'")
    sqlserver_conn.commit()

    print("  Waiting 3 seconds for CDC to process deletes...")
    time.sleep(3)

    # Verify cleanup
    db_state_after = check_database_state(sqlserver_conn, postgres_conn)
    print("\nDatabase State After Cleanup:")
    print(f"  SQL Server Perf Test rows: {db_state_after.get('sqlserver_perf_test_rows', 'N/A')}")
    print(f"  PostgreSQL Perf Test rows: {db_state_after.get('postgres_perf_test_rows', 'N/A')}")

    if db_state_after.get('postgres_perf_test_rows', 0) > 0:
        print("\n⚠️  WARNING: DELETE operations replicated to PostgreSQL!")
        print("   This means DELETE CDC events are being captured and replicated.")
        print("   The test's clear_tables() method may leave tombstone records.")
        print()
        print("ISSUE IDENTIFIED:")
        print("  The test uses DELETE to clear data, but CDC captures these as tombstone")
        print("  records. When the test runs again, it may count these tombstone records")
        print("  or experience timing issues due to extra CDC events.")
        print()
        print("SOLUTION:")
        print("  1. Use TRUNCATE instead of DELETE (doesn't trigger CDC)")
        print("  2. Use the new reset-test-env feature before tests")
        print("  3. Filter out __deleted='true' rows when counting")

    print()
    print("="*70)
    print("DIAGNOSTIC COMPLETE")
    print("="*70)

    sqlserver_conn.close()
    postgres_conn.close()


if __name__ == "__main__":
    simulate_test_scenario()
