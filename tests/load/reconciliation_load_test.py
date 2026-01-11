"""
Reconciliation-specific load testing.

Simulates realistic reconciliation workloads:
- Parallel table reconciliation
- Incremental checksum updates
- Row-level reconciliation under load
- Large table reconciliation
"""

import logging
import random
import time
from typing import Any

from locust import User, between, events, task

logger = logging.getLogger(__name__)


class ReconciliationLoadSimulator:
    """
    Simulates reconciliation operations without actual database access.

    Useful for testing reconciliation logic performance.
    """

    def __init__(self):
        self.tables = [
            {"name": "customers", "rows": 100000},
            {"name": "orders", "rows": 500000},
            {"name": "products", "rows": 10000},
            {"name": "order_items", "rows": 1000000},
            {"name": "categories", "rows": 100},
        ]

    def simulate_row_count(self, table_name: str) -> int:
        """Simulate row count query with realistic delay."""
        # Find table
        table = next((t for t in self.tables if t["name"] == table_name), None)
        if not table:
            raise ValueError(f"Table {table_name} not found")

        # Simulate query time based on table size
        rows = table["rows"]
        query_time = 0.001 + (rows / 1000000) * 0.5  # Base + linear scaling
        time.sleep(query_time)

        return rows

    def simulate_checksum(self, table_name: str) -> str:
        """Simulate checksum calculation with realistic delay."""
        table = next((t for t in self.tables if t["name"] == table_name), None)
        if not table:
            raise ValueError(f"Table {table_name} not found")

        # Simulate checksum time (more expensive than row count)
        rows = table["rows"]
        checksum_time = 0.01 + (rows / 100000) * 1.0  # Base + linear scaling
        time.sleep(checksum_time)

        # Generate pseudo-checksum
        import hashlib
        return hashlib.sha256(f"{table_name}_{rows}_{time.time()}".encode()).hexdigest()

    def simulate_row_level_reconciliation(self, table_name: str, sample_size: int = 1000) -> dict[str, Any]:
        """Simulate row-level reconciliation with sampling."""
        table = next((t for t in self.tables if t["name"] == table_name), None)
        if not table:
            raise ValueError(f"Table {table_name} not found")

        # Simulate time based on sample size
        recon_time = 0.1 + (sample_size / 10000) * 5.0
        time.sleep(recon_time)

        # Simulate finding discrepancies
        missing = random.randint(0, int(sample_size * 0.01))
        extra = random.randint(0, int(sample_size * 0.005))
        modified = random.randint(0, int(sample_size * 0.02))

        return {
            "table": table_name,
            "sampled_rows": sample_size,
            "missing": missing,
            "extra": extra,
            "modified": modified,
            "total_discrepancies": missing + extra + modified
        }


class ReconciliationUser(User):
    """
    Simulates user performing reconciliation operations.

    Uses custom tasks instead of HTTP requests.
    """

    wait_time = between(2, 10)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.simulator = ReconciliationLoadSimulator()

    @task(3)
    def reconcile_small_table(self):
        """Reconcile small table (<10k rows)."""
        small_tables = [t["name"] for t in self.simulator.tables if t["rows"] < 10000]
        if not small_tables:
            return

        table = random.choice(small_tables)
        start_time = time.time()

        try:
            # Simulate reconciliation steps
            source_count = self.simulator.simulate_row_count(table)
            target_count = self.simulator.simulate_row_count(table)
            source_checksum = self.simulator.simulate_checksum(table)
            target_checksum = self.simulator.simulate_checksum(table)

            total_time = int((time.time() - start_time) * 1000)

            # Record custom event
            events.request.fire(
                request_type="RECONCILE",
                name=f"Small Table: {table}",
                response_time=total_time,
                response_length=len(source_checksum) + len(target_checksum),
                exception=None,
                context={}
            )

        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="RECONCILE",
                name=f"Small Table: {table}",
                response_time=total_time,
                response_length=0,
                exception=e,
                context={}
            )

    @task(2)
    def reconcile_medium_table(self):
        """Reconcile medium table (10k-100k rows)."""
        medium_tables = [
            t["name"] for t in self.simulator.tables
            if 10000 <= t["rows"] <= 100000
        ]
        if not medium_tables:
            return

        table = random.choice(medium_tables)
        start_time = time.time()

        try:
            source_count = self.simulator.simulate_row_count(table)
            target_count = self.simulator.simulate_row_count(table)

            # Only checksum if counts match (optimization)
            if source_count == target_count:
                source_checksum = self.simulator.simulate_checksum(table)
                target_checksum = self.simulator.simulate_checksum(table)

            total_time = int((time.time() - start_time) * 1000)

            events.request.fire(
                request_type="RECONCILE",
                name=f"Medium Table: {table}",
                response_time=total_time,
                response_length=source_count + target_count,
                exception=None,
                context={}
            )

        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="RECONCILE",
                name=f"Medium Table: {table}",
                response_time=total_time,
                response_length=0,
                exception=e,
                context={}
            )

    @task(1)
    def reconcile_large_table(self):
        """Reconcile large table (>100k rows)."""
        large_tables = [t["name"] for t in self.simulator.tables if t["rows"] > 100000]
        if not large_tables:
            return

        table = random.choice(large_tables)
        start_time = time.time()

        try:
            # For large tables, use chunked processing
            source_count = self.simulator.simulate_row_count(table)
            target_count = self.simulator.simulate_row_count(table)

            # Simulate chunked checksum
            chunks = max(1, source_count // 10000)
            for _ in range(min(chunks, 5)):  # Limit to 5 chunks for simulation
                time.sleep(0.05)

            total_time = int((time.time() - start_time) * 1000)

            events.request.fire(
                request_type="RECONCILE",
                name=f"Large Table: {table}",
                response_time=total_time,
                response_length=source_count + target_count,
                exception=None,
                context={}
            )

        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="RECONCILE",
                name=f"Large Table: {table}",
                response_time=total_time,
                response_length=0,
                exception=e,
                context={}
            )

    @task(1)
    def row_level_reconciliation(self):
        """Perform row-level reconciliation on sampled data."""
        table = random.choice([t["name"] for t in self.simulator.tables])
        start_time = time.time()

        try:
            # Sample 1000 rows for row-level comparison
            result = self.simulator.simulate_row_level_reconciliation(table, sample_size=1000)

            total_time = int((time.time() - start_time) * 1000)

            events.request.fire(
                request_type="ROW_LEVEL",
                name=f"Row-Level: {table}",
                response_time=total_time,
                response_length=result["total_discrepancies"],
                exception=None,
                context={}
            )

        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="ROW_LEVEL",
                name=f"Row-Level: {table}",
                response_time=total_time,
                response_length=0,
                exception=e,
                context={}
            )


class ParallelReconciliationUser(User):
    """
    Simulates parallel reconciliation of multiple tables.

    Tests system under realistic parallel load.
    """

    wait_time = between(5, 15)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.simulator = ReconciliationLoadSimulator()

    @task
    def parallel_reconcile_batch(self):
        """Reconcile multiple tables in parallel."""
        # Select random batch of tables
        batch_size = random.randint(2, 5)
        tables = random.sample(
            [t["name"] for t in self.simulator.tables],
            min(batch_size, len(self.simulator.tables))
        )

        start_time = time.time()

        try:
            # Simulate parallel execution (simplified)
            for table in tables:
                self.simulator.simulate_row_count(table)

            # Parallel checksum would be concurrent, simulate with reduced time
            time.sleep(0.5)

            total_time = int((time.time() - start_time) * 1000)

            events.request.fire(
                request_type="PARALLEL",
                name=f"Parallel Batch ({len(tables)} tables)",
                response_time=total_time,
                response_length=len(tables),
                exception=None,
                context={}
            )

        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="PARALLEL",
                name=f"Parallel Batch ({len(tables)} tables)",
                response_time=total_time,
                response_length=0,
                exception=e,
                context={}
            )


# Event handlers for reconciliation-specific metrics
@events.init.add_listener
def on_locust_init(environment, **kwargs):
    """Initialize reconciliation load testing."""
    logger.info("Initializing reconciliation load tests")


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Called when test starts."""
    logger.info("Starting reconciliation load test")
    logger.info("Simulating reconciliation operations for CDC pipeline")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Called when test stops - print summary."""
    logger.info("Reconciliation load test completed")

    stats = environment.stats

    logger.info(f"Total reconciliation operations: {stats.total.num_requests}")
    logger.info(f"Failed operations: {stats.total.num_failures}")
    logger.info(f"Average response time: {stats.total.avg_response_time:.2f}ms")
    logger.info(f"Requests per second: {stats.total.current_rps:.2f}")
