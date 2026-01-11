"""
Unit tests for __init__.py files

This module provides tests for package initialization files to ensure:
- Version attributes are defined
- __all__ exports are correct
- Module imports work without errors
"""

import importlib

import pytest


class TestReconciliationInit:
    """Test src/reconciliation/__init__.py"""

    def test_version_attribute_exists(self):
        """Test that __version__ attribute is defined"""
        # Arrange & Act
        import src.reconciliation

        # Assert
        assert hasattr(src.reconciliation, "__version__")
        assert isinstance(src.reconciliation.__version__, str)
        assert src.reconciliation.__version__ == "1.0.0"

    def test_all_attribute_exists_and_correct(self):
        """Test that __all__ attribute contains expected modules"""
        # Arrange & Act
        import src.reconciliation

        # Assert
        assert hasattr(src.reconciliation, "__all__")
        assert isinstance(src.reconciliation.__all__, list)
        assert "compare" in src.reconciliation.__all__
        assert "report" in src.reconciliation.__all__
        assert "scheduler" in src.reconciliation.__all__
        assert len(src.reconciliation.__all__) == 3

    def test_module_imports_without_errors(self):
        """Test that module can be imported without errors"""
        # Arrange & Act & Assert
        try:
            import src.reconciliation
            # Reload to ensure fresh import
            importlib.reload(src.reconciliation)
        except ImportError as e:
            pytest.fail(f"Failed to import src.reconciliation: {e}")

    def test_submodules_can_be_imported(self):
        """Test that submodules listed in __all__ can be imported"""
        # Arrange
        import src.reconciliation

        # Act & Assert
        for module_name in src.reconciliation.__all__:
            try:
                module = importlib.import_module(f"src.reconciliation.{module_name}")
                assert module is not None
            except ImportError as e:
                pytest.fail(f"Failed to import src.reconciliation.{module_name}: {e}")

    def test_compare_module_accessible(self):
        """Test that compare module is accessible"""
        # Arrange & Act
        from src.reconciliation import compare

        # Assert
        assert compare is not None
        assert hasattr(compare, "compare_row_counts")
        assert hasattr(compare, "compare_checksums")
        assert hasattr(compare, "reconcile_table")

    def test_report_module_accessible(self):
        """Test that report module is accessible"""
        # Arrange & Act
        from src.reconciliation import report

        # Assert
        assert report is not None
        assert hasattr(report, "generate_report")
        assert hasattr(report, "format_report_console")
        assert hasattr(report, "export_report_json")

    def test_scheduler_module_accessible(self):
        """Test that scheduler module is accessible"""
        # Arrange & Act
        from src.reconciliation import scheduler

        # Assert
        assert scheduler is not None
        assert hasattr(scheduler, "ReconciliationScheduler")
        assert hasattr(scheduler, "reconcile_job_wrapper")


class TestUtilsInit:
    """Test src/utils/__init__.py"""

    def test_version_attribute_exists(self):
        """Test that __version__ attribute is defined"""
        # Arrange & Act
        import src.utils

        # Assert
        assert hasattr(src.utils, "__version__")
        assert isinstance(src.utils.__version__, str)
        assert src.utils.__version__ == "1.0.0"

    def test_all_attribute_exists_and_correct(self):
        """Test that __all__ attribute contains expected modules"""
        # Arrange & Act
        import src.utils

        # Assert
        assert hasattr(src.utils, "__all__")
        assert isinstance(src.utils.__all__, list)
        assert "vault_client" in src.utils.__all__
        assert "metrics" in src.utils.__all__
        assert len(src.utils.__all__) == 2

    def test_module_imports_without_errors(self):
        """Test that module can be imported without errors"""
        # Arrange & Act & Assert
        try:
            import src.utils
            # Reload to ensure fresh import
            importlib.reload(src.utils)
        except ImportError as e:
            pytest.fail(f"Failed to import src.utils: {e}")

    def test_submodules_can_be_imported(self):
        """Test that submodules listed in __all__ can be imported"""
        # Arrange
        import src.utils

        # Act & Assert
        for module_name in src.utils.__all__:
            try:
                module = importlib.import_module(f"src.utils.{module_name}")
                assert module is not None
            except ImportError as e:
                pytest.fail(f"Failed to import src.utils.{module_name}: {e}")

    def test_vault_client_module_accessible(self):
        """Test that vault_client module is accessible"""
        # Arrange & Act
        from src.utils import vault_client

        # Assert
        assert vault_client is not None
        assert hasattr(vault_client, "VaultClient")
        assert hasattr(vault_client, "get_credentials_from_vault")

    def test_metrics_module_accessible(self):
        """Test that metrics module is accessible"""
        # Arrange & Act
        from src.utils import metrics

        # Assert
        assert metrics is not None
        assert hasattr(metrics, "MetricsPublisher")
        assert hasattr(metrics, "ReconciliationMetrics")
        assert hasattr(metrics, "ConnectorMetrics")
        assert hasattr(metrics, "VaultMetrics")
        assert hasattr(metrics, "ApplicationInfo")
        assert hasattr(metrics, "initialize_metrics")

    def test_logging_module_can_be_imported(self):
        """Test that logging module (not in __all__) can still be imported"""
        # Arrange & Act
        try:
            from src.utils import logging
            assert logging is not None
            assert hasattr(logging, "setup_logging")
            assert hasattr(logging, "get_logger")
        except ImportError as e:
            pytest.fail(f"Failed to import src.utils.logging: {e}")
