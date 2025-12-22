"""
Database-specific load testing for CDC pipeline.

Tests database operations under load:
- Connection pool exhaustion
- Concurrent reconciliation
- Query performance under load
- Checksum calculation performance

Note: This requires database access and is not run through Locust's HTTP framework.
Use pytest-benchmark or custom runner.
"""

import time
import threading
import statistics
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class LoadTestResult:
    """Results from a load test run."""
    test_name: str
    total_operations: int
    successful_operations: int
    failed_operations: int
    total_time_seconds: float
    operations_per_second: float
    min_latency_ms: float
    max_latency_ms: float
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    errors: List[str]


class DatabaseLoadTester:
    """
    Load tester for database operations.

    Simulates concurrent reconciliation and query load.
    """

    def __init__(
        self,
        source_connection_factory,
        target_connection_factory,
        max_workers: int = 10
    ):
        """
        Initialize load tester.

        Args:
            source_connection_factory: Factory function to create source connections
            target_connection_factory: Factory function to create target connections
            max_workers: Maximum concurrent workers
        """
        self.source_connection_factory = source_connection_factory
        self.target_connection_factory = target_connection_factory
        self.max_workers = max_workers

    def test_concurrent_row_counts(
        self,
        table_name: str,
        num_operations: int = 100
    ) -> LoadTestResult:
        """
        Test concurrent row count queries.

        Args:
            table_name: Table to query
            num_operations: Number of concurrent operations

        Returns:
            Load test results
        """
        start_time = time.time()
        latencies: List[float] = []
        errors: List[str] = []
        successful = 0
        failed = 0

        def get_row_count():
            """Single row count operation."""
            op_start = time.time()
            try:
                source_conn = self.source_connection_factory()
                source_cursor = source_conn.cursor()

                source_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = source_cursor.fetchone()[0]

                source_cursor.close()
                source_conn.close()

                latency = (time.time() - op_start) * 1000  # Convert to ms
                return True, latency, None

            except Exception as e:
                latency = (time.time() - op_start) * 1000
                return False, latency, str(e)

        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(get_row_count) for _ in range(num_operations)]

            for future in as_completed(futures):
                success, latency, error = future.result()
                latencies.append(latency)

                if success:
                    successful += 1
                else:
                    failed += 1
                    if error:
                        errors.append(error)

        total_time = time.time() - start_time
        ops_per_second = num_operations / total_time if total_time > 0 else 0

        # Calculate percentiles
        sorted_latencies = sorted(latencies)
        p95_index = int(len(sorted_latencies) * 0.95)
        p99_index = int(len(sorted_latencies) * 0.99)

        return LoadTestResult(
            test_name="concurrent_row_counts",
            total_operations=num_operations,
            successful_operations=successful,
            failed_operations=failed,
            total_time_seconds=total_time,
            operations_per_second=ops_per_second,
            min_latency_ms=min(latencies),
            max_latency_ms=max(latencies),
            avg_latency_ms=statistics.mean(latencies),
            p95_latency_ms=sorted_latencies[p95_index] if sorted_latencies else 0,
            p99_latency_ms=sorted_latencies[p99_index] if sorted_latencies else 0,
            errors=errors[:10]  # Keep first 10 errors
        )

    def test_concurrent_checksums(
        self,
        table_name: str,
        num_operations: int = 50
    ) -> LoadTestResult:
        """
        Test concurrent checksum calculations.

        Args:
            table_name: Table to checksum
            num_operations: Number of concurrent operations

        Returns:
            Load test results
        """
        start_time = time.time()
        latencies: List[float] = []
        errors: List[str] = []
        successful = 0
        failed = 0

        def calculate_checksum():
            """Single checksum operation."""
            op_start = time.time()
            try:
                source_conn = self.source_connection_factory()
                source_cursor = source_conn.cursor()

                # Simplified checksum calculation
                import hashlib
                source_cursor.execute(f"SELECT * FROM {table_name} ORDER BY 1")

                hasher = hashlib.sha256()
                for row in source_cursor:
                    row_str = "|".join(str(val) if val is not None else "NULL" for val in row)
                    hasher.update(row_str.encode('utf-8'))

                checksum = hasher.hexdigest()

                source_cursor.close()
                source_conn.close()

                latency = (time.time() - op_start) * 1000
                return True, latency, None

            except Exception as e:
                latency = (time.time() - op_start) * 1000
                return False, latency, str(e)

        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(calculate_checksum) for _ in range(num_operations)]

            for future in as_completed(futures):
                success, latency, error = future.result()
                latencies.append(latency)

                if success:
                    successful += 1
                else:
                    failed += 1
                    if error:
                        errors.append(error)

        total_time = time.time() - start_time
        ops_per_second = num_operations / total_time if total_time > 0 else 0

        sorted_latencies = sorted(latencies)
        p95_index = int(len(sorted_latencies) * 0.95)
        p99_index = int(len(sorted_latencies) * 0.99)

        return LoadTestResult(
            test_name="concurrent_checksums",
            total_operations=num_operations,
            successful_operations=successful,
            failed_operations=failed,
            total_time_seconds=total_time,
            operations_per_second=ops_per_second,
            min_latency_ms=min(latencies),
            max_latency_ms=max(latencies),
            avg_latency_ms=statistics.mean(latencies),
            p95_latency_ms=sorted_latencies[p95_index] if sorted_latencies else 0,
            p99_latency_ms=sorted_latencies[p99_index] if sorted_latencies else 0,
            errors=errors[:10]
        )

    def test_connection_pool_exhaustion(
        self,
        pool_size: int = 10,
        num_requests: int = 100
    ) -> LoadTestResult:
        """
        Test connection pool behavior under load.

        Simulates scenario where requests exceed pool size.

        Args:
            pool_size: Size of connection pool
            num_requests: Number of concurrent requests

        Returns:
            Load test results
        """
        start_time = time.time()
        latencies: List[float] = []
        errors: List[str] = []
        successful = 0
        failed = 0
        semaphore = threading.Semaphore(pool_size)

        def get_connection():
            """Simulate connection acquisition."""
            op_start = time.time()
            acquired = False

            try:
                # Try to acquire connection from pool
                acquired = semaphore.acquire(timeout=5.0)

                if not acquired:
                    raise TimeoutError("Pool exhausted")

                # Simulate query execution
                time.sleep(0.1)

                latency = (time.time() - op_start) * 1000
                return True, latency, None

            except Exception as e:
                latency = (time.time() - op_start) * 1000
                return False, latency, str(e)

            finally:
                if acquired:
                    semaphore.release()

        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=num_requests) as executor:
            futures = [executor.submit(get_connection) for _ in range(num_requests)]

            for future in as_completed(futures):
                success, latency, error = future.result()
                latencies.append(latency)

                if success:
                    successful += 1
                else:
                    failed += 1
                    if error:
                        errors.append(error)

        total_time = time.time() - start_time
        ops_per_second = num_requests / total_time if total_time > 0 else 0

        sorted_latencies = sorted(latencies)
        p95_index = int(len(sorted_latencies) * 0.95)
        p99_index = int(len(sorted_latencies) * 0.99)

        return LoadTestResult(
            test_name="connection_pool_exhaustion",
            total_operations=num_requests,
            successful_operations=successful,
            failed_operations=failed,
            total_time_seconds=total_time,
            operations_per_second=ops_per_second,
            min_latency_ms=min(latencies) if latencies else 0,
            max_latency_ms=max(latencies) if latencies else 0,
            avg_latency_ms=statistics.mean(latencies) if latencies else 0,
            p95_latency_ms=sorted_latencies[p95_index] if sorted_latencies else 0,
            p99_latency_ms=sorted_latencies[p99_index] if sorted_latencies else 0,
            errors=errors[:10]
        )

    def print_results(self, result: LoadTestResult):
        """
        Print formatted load test results.

        Args:
            result: Load test result to print
        """
        print(f"\n{'='*60}")
        print(f"Load Test Results: {result.test_name}")
        print(f"{'='*60}")
        print(f"Total Operations:    {result.total_operations}")
        print(f"Successful:          {result.successful_operations}")
        print(f"Failed:              {result.failed_operations}")
        print(f"Total Time:          {result.total_time_seconds:.2f}s")
        print(f"Throughput:          {result.operations_per_second:.2f} ops/sec")
        print(f"\nLatency Statistics (ms):")
        print(f"  Min:               {result.min_latency_ms:.2f}")
        print(f"  Average:           {result.avg_latency_ms:.2f}")
        print(f"  P95:               {result.p95_latency_ms:.2f}")
        print(f"  P99:               {result.p99_latency_ms:.2f}")
        print(f"  Max:               {result.max_latency_ms:.2f}")

        if result.errors:
            print(f"\nErrors (first 10):")
            for i, error in enumerate(result.errors, 1):
                print(f"  {i}. {error}")
        print(f"{'='*60}\n")


# Example usage function
def run_load_tests():
    """
    Example function to run database load tests.

    This is a template - actual implementation requires database connections.
    """
    print("Database Load Testing")
    print("Note: Configure database connections before running")

    # Placeholder for connection factories
    def mock_connection_factory():
        """Mock connection factory for testing."""
        import sqlite3
        return sqlite3.connect(':memory:')

    # Initialize tester
    tester = DatabaseLoadTester(
        source_connection_factory=mock_connection_factory,
        target_connection_factory=mock_connection_factory,
        max_workers=10
    )

    # Run connection pool test
    pool_result = tester.test_connection_pool_exhaustion(
        pool_size=10,
        num_requests=50
    )
    tester.print_results(pool_result)


if __name__ == "__main__":
    run_load_tests()
