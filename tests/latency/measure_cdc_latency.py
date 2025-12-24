#!/usr/bin/env python3
"""
Measure CDC pipeline latency for INSERT, UPDATE, DELETE operations.
"""

import time
import pyodbc
import psycopg2
from datetime import datetime

# Connection strings
SQLSERVER_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=warehouse_source;"
    "UID=sa;"
    "PWD=YourStrong!Passw0rd;"
    "TrustServerCertificate=yes;"
)

POSTGRES_CONN = {
    "host": "localhost",
    "port": 5432,
    "database": "warehouse_target",
    "user": "postgres",
    "password": "postgres_secure_password"
}

def measure_insert_latency():
    """Measure latency for INSERT operation."""
    print("\n=== Testing INSERT Latency ===")

    # Connect to databases
    sqlserver_conn = pyodbc.connect(SQLSERVER_CONN_STR, autocommit=True)
    postgres_conn = psycopg2.connect(**POSTGRES_CONN)
    postgres_conn.autocommit = True

    try:
        # Generate unique test data
        test_id = int(time.time() * 1000)
        test_email = f"latency_test_{test_id}@example.com"

        # Record start time and insert into SQL Server
        start_time = time.time()
        with sqlserver_conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO dbo.customers (name, email) VALUES (?, ?)",
                f"Latency Test {test_id}", test_email
            )
        insert_time = time.time()

        print(f"Inserted into SQL Server at: {insert_time:.3f}")

        # Poll PostgreSQL for the record
        max_wait = 30  # 30 seconds timeout
        poll_interval = 0.1  # 100ms
        found = False

        with postgres_conn.cursor() as cursor:
            while (time.time() - start_time) < max_wait:
                cursor.execute(
                    "SELECT id, name, email FROM customers WHERE email = %s",
                    (test_email,)
                )
                result = cursor.fetchone()

                if result:
                    found = True
                    replicated_time = time.time()
                    latency_ms = (replicated_time - insert_time) * 1000
                    total_latency_ms = (replicated_time - start_time) * 1000

                    print(f"Found in PostgreSQL at: {replicated_time:.3f}")
                    print(f"✓ INSERT Latency: {latency_ms:.0f}ms (total: {total_latency_ms:.0f}ms)")
                    return latency_ms

                time.sleep(poll_interval)

        if not found:
            print(f"✗ INSERT did not replicate within {max_wait}s")
            return None

    finally:
        sqlserver_conn.close()
        postgres_conn.close()

def measure_update_latency():
    """Measure latency for UPDATE operation."""
    print("\n=== Testing UPDATE Latency ===")

    sqlserver_conn = pyodbc.connect(SQLSERVER_CONN_STR, autocommit=True)
    postgres_conn = psycopg2.connect(**POSTGRES_CONN)
    postgres_conn.autocommit = True

    try:
        # First, create a record to update
        test_id = int(time.time() * 1000)
        test_email = f"update_test_{test_id}@example.com"

        with sqlserver_conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO dbo.customers (name, email) VALUES (?, ?)",
                f"Update Test {test_id}", test_email
            )

        # Wait for initial insert to replicate
        print("Waiting for initial INSERT to replicate...")
        time.sleep(3)

        # Verify it's in PostgreSQL
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT id FROM customers WHERE email = %s", (test_email,))
            result = cursor.fetchone()
            if not result:
                print("✗ Initial record not found, skipping UPDATE test")
                return None
            pg_id = result[0]

        # Now measure UPDATE latency
        updated_name = f"UPDATED {test_id}"
        start_time = time.time()

        with sqlserver_conn.cursor() as cursor:
            cursor.execute(
                "UPDATE dbo.customers SET name = ? WHERE email = ?",
                updated_name, test_email
            )
        update_time = time.time()
        print(f"Updated in SQL Server at: {update_time:.3f}")

        # Poll PostgreSQL for the update
        max_wait = 30
        poll_interval = 0.1
        found = False

        with postgres_conn.cursor() as cursor:
            while (time.time() - start_time) < max_wait:
                cursor.execute(
                    "SELECT name FROM customers WHERE id = %s",
                    (pg_id,)
                )
                result = cursor.fetchone()

                if result and result[0] == updated_name:
                    found = True
                    replicated_time = time.time()
                    latency_ms = (replicated_time - update_time) * 1000
                    total_latency_ms = (replicated_time - start_time) * 1000

                    print(f"Update found in PostgreSQL at: {replicated_time:.3f}")
                    print(f"✓ UPDATE Latency: {latency_ms:.0f}ms (total: {total_latency_ms:.0f}ms)")
                    return latency_ms

                time.sleep(poll_interval)

        if not found:
            print(f"✗ UPDATE did not replicate within {max_wait}s")
            return None

    finally:
        sqlserver_conn.close()
        postgres_conn.close()

def measure_delete_latency():
    """Measure latency for DELETE operation."""
    print("\n=== Testing DELETE Latency ===")

    sqlserver_conn = pyodbc.connect(SQLSERVER_CONN_STR, autocommit=True)
    postgres_conn = psycopg2.connect(**POSTGRES_CONN)
    postgres_conn.autocommit = True

    try:
        # First, create a record to delete
        test_id = int(time.time() * 1000)
        test_email = f"delete_test_{test_id}@example.com"

        with sqlserver_conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO dbo.customers (name, email) VALUES (?, ?)",
                f"Delete Test {test_id}", test_email
            )

        # Wait for initial insert to replicate
        print("Waiting for initial INSERT to replicate...")
        time.sleep(3)

        # Verify it's in PostgreSQL
        with postgres_conn.cursor() as cursor:
            cursor.execute("SELECT id FROM customers WHERE email = %s", (test_email,))
            result = cursor.fetchone()
            if not result:
                print("✗ Initial record not found, skipping DELETE test")
                return None
            pg_id = result[0]

        # Now measure DELETE latency
        start_time = time.time()

        with sqlserver_conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM dbo.customers WHERE email = ?",
                test_email
            )
        delete_time = time.time()
        print(f"Deleted from SQL Server at: {delete_time:.3f}")

        # Poll PostgreSQL for the deletion
        # Note: The sink connector is configured with delete.handling.mode=rewrite
        # which adds a "deleted" column instead of removing the row
        max_wait = 30
        poll_interval = 0.1
        found = False

        with postgres_conn.cursor() as cursor:
            while (time.time() - start_time) < max_wait:
                cursor.execute(
                    "SELECT __deleted FROM customers WHERE id = %s",
                    (pg_id,)
                )
                result = cursor.fetchone()

                # Check if deleted flag is set to 'true'
                if result and result[0] == 'true':
                    found = True
                    replicated_time = time.time()
                    latency_ms = (replicated_time - delete_time) * 1000
                    total_latency_ms = (replicated_time - start_time) * 1000

                    print(f"Delete found in PostgreSQL at: {replicated_time:.3f}")
                    print(f"✓ DELETE Latency: {latency_ms:.0f}ms (total: {total_latency_ms:.0f}ms)")
                    return latency_ms

                time.sleep(poll_interval)

        if not found:
            print(f"✗ DELETE did not replicate within {max_wait}s")
            return None

    finally:
        sqlserver_conn.close()
        postgres_conn.close()

if __name__ == "__main__":
    print("CDC Pipeline Latency Measurement")
    print("=" * 60)

    # Run multiple iterations for better statistics
    iterations = 3

    insert_latencies = []
    update_latencies = []
    delete_latencies = []

    for i in range(iterations):
        print(f"\n--- Iteration {i+1}/{iterations} ---")

        # Measure INSERT
        insert_lat = measure_insert_latency()
        if insert_lat:
            insert_latencies.append(insert_lat)
        time.sleep(2)  # Brief pause between operations

        # Measure UPDATE
        update_lat = measure_update_latency()
        if update_lat:
            update_latencies.append(update_lat)
        time.sleep(2)

        # Measure DELETE
        delete_lat = measure_delete_latency()
        if delete_lat:
            delete_latencies.append(delete_lat)
        time.sleep(2)

    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    if insert_latencies:
        avg_insert = sum(insert_latencies) / len(insert_latencies)
        min_insert = min(insert_latencies)
        max_insert = max(insert_latencies)
        print(f"INSERT: avg={avg_insert:.0f}ms, min={min_insert:.0f}ms, max={max_insert:.0f}ms")

    if update_latencies:
        avg_update = sum(update_latencies) / len(update_latencies)
        min_update = min(update_latencies)
        max_update = max(update_latencies)
        print(f"UPDATE: avg={avg_update:.0f}ms, min={min_update:.0f}ms, max={max_update:.0f}ms")

    if delete_latencies:
        avg_delete = sum(delete_latencies) / len(delete_latencies)
        min_delete = min(delete_latencies)
        max_delete = max(delete_latencies)
        print(f"DELETE: avg={avg_delete:.0f}ms, min={min_delete:.0f}ms, max={max_delete:.0f}ms")
