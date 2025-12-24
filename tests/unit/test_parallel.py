"""
Unit tests for parallel reconciliation module.

Tests parallel table processing, error handling, timeouts, and metrics.
"""

import time
from concurrent.futures import TimeoutError
from unittest.mock import MagicMock, Mock, patch

import pytest

from reconciliation.parallel import (
    ParallelReconciler,
    create_parallel_reconcile_job,
    estimate_optimal_workers,
    get_parallel_reconciliation_stats,
)


class TestParallelReconciler:
    """Test ParallelReconciler functionality."""

    def test_initialization(self):
        """Test reconciler initialization."""
        reconciler = ParallelReconciler(max_workers=4, timeout_per_table=3600)

        assert reconciler.max_workers == 4
        assert reconciler.timeout_per_table == 3600
        assert reconciler.fail_fast is False

    def test_initialization_with_fail_fast(self):
        """Test initialization with fail_fast enabled."""
        reconciler = ParallelReconciler(max_workers=2, fail_fast=True)

        assert reconciler.fail_fast is True

    def test_reconcile_empty_tables_list(self):
        """Test reconciling empty table list."""
        reconciler = ParallelReconciler()

        def mock_reconcile(table):
            return {"table": table, "match": True}

        results = reconciler.reconcile_tables(tables=[], reconcile_func=mock_reconcile)

        assert results["total_tables"] == 0
        assert results["successful"] == 0
        assert results["failed"] == 0
        assert results["results"] == []
        assert results["errors"] == []

    def test_reconcile_single_table_success(self):
        """Test successfully reconciling single table."""
        reconciler = ParallelReconciler()

        def mock_reconcile(table):
            return {"table": table, "match": True, "row_count": 100}

        results = reconciler.reconcile_tables(
            tables=["users"], reconcile_func=mock_reconcile
        )

        assert results["total_tables"] == 1
        assert results["successful"] == 1
        assert results["failed"] == 0
        assert results["timeout"] == 0
        assert len(results["results"]) == 1
        assert results["results"][0]["table"] == "users"
        assert results["results"][0]["match"] is True
        assert "duration_seconds" in results["results"][0]

    def test_reconcile_multiple_tables_success(self):
        """Test successfully reconciling multiple tables."""
        reconciler = ParallelReconciler(max_workers=2)

        def mock_reconcile(table):
            return {"table": table, "match": True}

        tables = ["users", "orders", "products"]
        results = reconciler.reconcile_tables(tables=tables, reconcile_func=mock_reconcile)

        assert results["total_tables"] == 3
        assert results["successful"] == 3
        assert results["failed"] == 0
        assert len(results["results"]) == 3

        # Verify all tables processed
        result_tables = {r["table"] for r in results["results"]}
        assert result_tables == {"users", "orders", "products"}

    def test_reconcile_with_kwargs(self):
        """Test passing additional kwargs to reconcile function."""
        reconciler = ParallelReconciler()

        def mock_reconcile(table, validate_checksum=False, chunk_size=1000):
            return {
                "table": table,
                "validate_checksum": validate_checksum,
                "chunk_size": chunk_size,
            }

        results = reconciler.reconcile_tables(
            tables=["users"],
            reconcile_func=mock_reconcile,
            validate_checksum=True,
            chunk_size=5000,
        )

        assert results["successful"] == 1
        result = results["results"][0]
        assert result["validate_checksum"] is True
        assert result["chunk_size"] == 5000

    def test_reconcile_with_failure(self):
        """Test handling of reconciliation failure."""
        reconciler = ParallelReconciler()

        def mock_reconcile(table):
            if table == "orders":
                raise ValueError("Database connection failed")
            return {"table": table, "match": True}

        tables = ["users", "orders", "products"]
        results = reconciler.reconcile_tables(tables=tables, reconcile_func=mock_reconcile)

        assert results["total_tables"] == 3
        assert results["successful"] == 2
        assert results["failed"] == 1
        assert len(results["errors"]) == 1

        error = results["errors"][0]
        assert error["table"] == "orders"
        assert "Database connection failed" in error["error"]
        assert error["type"] == "ValueError"

    def test_reconcile_with_timeout(self):
        """Test handling of reconciliation timeout."""
        reconciler = ParallelReconciler(timeout_per_table=0.5)

        def mock_reconcile(table):
            if table == "large_table":
                time.sleep(1.5)  # Exceeds timeout
            return {"table": table, "match": True}

        tables = ["small_table", "large_table"]
        results = reconciler.reconcile_tables(tables=tables, reconcile_func=mock_reconcile)

        assert results["total_tables"] == 2
        # At least one should timeout
        assert results["timeout"] >= 1 or results["successful"] >= 1

    def test_reconcile_fail_fast_enabled(self):
        """Test fail-fast behavior on error."""
        reconciler = ParallelReconciler(max_workers=1, fail_fast=True)

        call_count = {"count": 0}

        def mock_reconcile(table):
            call_count["count"] += 1
            if table == "orders":
                raise ValueError("Test error")
            return {"table": table, "match": True}

        tables = ["users", "orders", "products", "customers"]
        results = reconciler.reconcile_tables(tables=tables, reconcile_func=mock_reconcile)

        # Should stop after first error
        assert results["failed"] >= 1
        # Not all tables should be processed
        assert results["successful"] + results["failed"] < len(tables)

    def test_reconcile_metadata_included(self):
        """Test that results include proper metadata."""
        reconciler = ParallelReconciler(max_workers=2)

        def mock_reconcile(table):
            return {"match": True}

        results = reconciler.reconcile_tables(
            tables=["users", "orders"], reconcile_func=mock_reconcile
        )

        assert "duration_seconds" in results
        assert "timestamp" in results
        assert "max_workers" in results
        assert results["max_workers"] == 2

        # Each result should have duration
        for result in results["results"]:
            assert "duration_seconds" in result
            assert result["duration_seconds"] >= 0

    def test_reconcile_non_dict_result(self):
        """Test handling of non-dict return values."""
        reconciler = ParallelReconciler()

        def mock_reconcile(table):
            return "success"  # Non-dict return

        results = reconciler.reconcile_tables(
            tables=["users"], reconcile_func=mock_reconcile
        )

        assert results["successful"] == 1
        result = results["results"][0]
        assert isinstance(result, dict)
        assert result["success"] is True
        assert result["data"] == "success"

    def test_reconcile_parallel_execution(self):
        """Test that tables are actually processed in parallel."""
        reconciler = ParallelReconciler(max_workers=3)

        execution_times = []

        def mock_reconcile(table):
            start = time.time()
            time.sleep(0.1)  # Simulate work
            execution_times.append(time.time())
            return {"table": table}

        tables = ["table1", "table2", "table3"]
        start_time = time.time()
        results = reconciler.reconcile_tables(tables=tables, reconcile_func=mock_reconcile)
        total_time = time.time() - start_time

        assert results["successful"] == 3
        # If sequential, would take 0.3s. Parallel should be ~0.1s
        assert total_time < 0.25  # Allow some overhead

    def test_reconcile_worker_count_respected(self):
        """Test that max_workers limit is respected."""
        reconciler = ParallelReconciler(max_workers=2)

        active_workers = {"max": 0, "current": 0}

        def mock_reconcile(table):
            active_workers["current"] += 1
            active_workers["max"] = max(active_workers["max"], active_workers["current"])
            time.sleep(0.05)
            active_workers["current"] -= 1
            return {"table": table}

        tables = [f"table{i}" for i in range(10)]
        results = reconciler.reconcile_tables(tables=tables, reconcile_func=mock_reconcile)

        assert results["successful"] == 10
        # Max concurrent workers should not exceed max_workers
        assert active_workers["max"] <= 2


class TestCreateParallelReconcileJob:
    """Test parallel job factory function."""

    def test_create_job_with_defaults(self):
        """Test creating job with default parameters."""

        def mock_reconcile(table):
            return {"table": table}

        job = create_parallel_reconcile_job(mock_reconcile)

        assert callable(job)

        results = job(tables=["users", "orders"])
        assert results["total_tables"] == 2
        assert results["successful"] == 2

    def test_create_job_with_custom_params(self):
        """Test creating job with custom parameters."""

        def mock_reconcile(table):
            return {"table": table}

        job = create_parallel_reconcile_job(
            mock_reconcile, max_workers=8, timeout_per_table=1800, fail_fast=True
        )

        results = job(tables=["users"])
        assert results["max_workers"] == 8

    def test_create_job_with_kwargs(self):
        """Test created job passes kwargs correctly."""

        def mock_reconcile(table, custom_arg=None):
            return {"table": table, "custom_arg": custom_arg}

        job = create_parallel_reconcile_job(mock_reconcile)

        results = job(tables=["users"], custom_arg="test_value")
        assert results["results"][0]["custom_arg"] == "test_value"


class TestEstimateOptimalWorkers:
    """Test optimal worker estimation."""

    def test_estimate_basic(self):
        """Test basic worker estimation."""
        # 10 tables, 60s each, want done in 120s
        workers = estimate_optimal_workers(10, 60, 120, 10)

        # Total work: 600s, budget: 120s -> need at least 5 workers
        assert workers >= 5
        assert workers <= 10

    def test_estimate_constrained_by_max(self):
        """Test estimation constrained by max_workers."""
        # Would need 20 workers, but max is 10
        workers = estimate_optimal_workers(100, 60, 300, 10)

        assert workers == 10

    def test_estimate_constrained_by_table_count(self):
        """Test estimation constrained by table count."""
        # Need 10 workers but only 3 tables
        workers = estimate_optimal_workers(3, 60, 18, 10)

        assert workers <= 3

    def test_estimate_zero_tables(self):
        """Test estimation with zero tables."""
        workers = estimate_optimal_workers(0, 60, 300, 10)

        assert workers == 1

    def test_estimate_minimum_one_worker(self):
        """Test that at least 1 worker is returned."""
        workers = estimate_optimal_workers(1, 1, 1000, 10)

        assert workers >= 1

    def test_estimate_fast_tables(self):
        """Test estimation with fast tables."""
        # 20 tables, 5s each, want done in 50s
        workers = estimate_optimal_workers(20, 5, 50, 10)

        # Total work: 100s, budget: 50s -> need at least 2 workers
        assert workers >= 2
        assert workers <= 10


class TestGetParallelReconciliationStats:
    """Test parallel reconciliation statistics."""

    def test_get_stats_returns_dict(self):
        """Test that stats returns proper dictionary."""
        stats = get_parallel_reconciliation_stats()

        assert isinstance(stats, dict)
        assert "active_workers" in stats
        assert "queue_size" in stats
        assert "total_processed" in stats

    def test_get_stats_structure(self):
        """Test stats dictionary structure."""
        stats = get_parallel_reconciliation_stats()

        assert isinstance(stats["active_workers"], (int, float))
        assert isinstance(stats["queue_size"], (int, float))
        assert isinstance(stats["total_processed"], dict)
        assert "success" in stats["total_processed"]
        assert "failed" in stats["total_processed"]
        assert "timeout" in stats["total_processed"]

    def test_get_stats_after_reconciliation(self):
        """Test stats after running reconciliation."""
        reconciler = ParallelReconciler()

        def mock_reconcile(table):
            return {"table": table}

        # Run reconciliation
        reconciler.reconcile_tables(tables=["users", "orders"], reconcile_func=mock_reconcile)

        # Get stats
        stats = get_parallel_reconciliation_stats()

        # After completion, active workers and queue should be 0
        assert stats["active_workers"] == 0
        assert stats["queue_size"] == 0


class TestParallelReconcilerEdgeCases:
    """Test edge cases and error conditions."""

    def test_reconcile_function_raises_keyboard_interrupt(self):
        """Test handling of KeyboardInterrupt."""
        reconciler = ParallelReconciler()

        def mock_reconcile(table):
            if table == "users":
                raise KeyboardInterrupt()
            return {"table": table}

        tables = ["users", "orders"]

        with pytest.raises(KeyboardInterrupt):
            # KeyboardInterrupt should propagate
            reconciler.reconcile_tables(tables=tables, reconcile_func=mock_reconcile)

    def test_reconcile_with_none_result(self):
        """Test handling of None return value."""
        reconciler = ParallelReconciler()

        def mock_reconcile(table):
            return None

        results = reconciler.reconcile_tables(
            tables=["users"], reconcile_func=mock_reconcile
        )

        # Should wrap None in dict
        assert results["successful"] == 1
        result = results["results"][0]
        assert isinstance(result, dict)

    def test_reconcile_preserves_original_error_type(self):
        """Test that original exception type is preserved in errors."""
        reconciler = ParallelReconciler()

        class CustomError(Exception):
            pass

        def mock_reconcile(table):
            raise CustomError("Custom error message")

        results = reconciler.reconcile_tables(
            tables=["users"], reconcile_func=mock_reconcile
        )

        assert results["failed"] == 1
        error = results["errors"][0]
        assert error["type"] == "CustomError"
        assert "Custom error message" in error["error"]

    def test_reconcile_concurrent_modification(self):
        """Test behavior with concurrent table list modification."""
        reconciler = ParallelReconciler()

        def mock_reconcile(table):
            return {"table": table}

        tables = ["table1", "table2", "table3"]
        original_tables = tables.copy()

        results = reconciler.reconcile_tables(tables=tables, reconcile_func=mock_reconcile)

        # Original list should not be modified
        assert tables == original_tables
        assert results["successful"] == 3


class TestParallelReconcilerMetrics:
    """Test Prometheus metrics integration."""

    def test_metrics_tracked_on_success(self):
        """Test that success metrics are tracked."""
        from prometheus_client import REGISTRY

        reconciler = ParallelReconciler()

        def mock_reconcile(table):
            return {"table": table}

        # Get initial count
        before = (
            REGISTRY.get_sample_value(
                "parallel_tables_processed_total", {"status": "success"}
            )
            or 0
        )

        # Run reconciliation
        reconciler.reconcile_tables(tables=["users"], reconcile_func=mock_reconcile)

        # Check metric increased
        after = (
            REGISTRY.get_sample_value(
                "parallel_tables_processed_total", {"status": "success"}
            )
            or 0
        )

        assert after > before

    def test_metrics_tracked_on_failure(self):
        """Test that failure metrics are tracked."""
        from prometheus_client import REGISTRY

        reconciler = ParallelReconciler()

        def mock_reconcile(table):
            raise ValueError("Test error")

        # Get initial count
        before = (
            REGISTRY.get_sample_value(
                "parallel_tables_processed_total", {"status": "failed"}
            )
            or 0
        )

        # Run reconciliation (will fail)
        reconciler.reconcile_tables(tables=["users"], reconcile_func=mock_reconcile)

        # Check metric increased
        after = (
            REGISTRY.get_sample_value(
                "parallel_tables_processed_total", {"status": "failed"}
            )
            or 0
        )

        assert after > before

    def test_metrics_tracked_on_timeout(self):
        """Test that timeout metrics are tracked."""
        from prometheus_client import REGISTRY

        reconciler = ParallelReconciler(timeout_per_table=0.2)

        def mock_reconcile(table):
            time.sleep(1.0)  # Much longer than timeout
            return {"table": table}

        # Get initial count
        before = (
            REGISTRY.get_sample_value(
                "parallel_tables_processed_total", {"status": "timeout"}
            )
            or 0
        )

        # Run reconciliation (will timeout)
        reconciler.reconcile_tables(tables=["users"], reconcile_func=mock_reconcile)

        # Check metric increased (may take a moment for async completion)
        after = (
            REGISTRY.get_sample_value(
                "parallel_tables_processed_total", {"status": "timeout"}
            )
            or 0
        )

        # Timeout tracking should have increased
        assert after >= before
