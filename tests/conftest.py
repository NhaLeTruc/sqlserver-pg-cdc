"""
Pytest configuration and fixtures for CDC pipeline tests.
Provides shared fixtures for database connections and test setup.
"""

import os
from pathlib import Path

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "contract: mark test as contract test")
    config.addinivalue_line("markers", "performance: mark test as performance test")
    config.addinivalue_line("markers", "slow: mark test as slow running")


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Get project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def specs_dir(project_root: Path) -> Path:
    """Get specs directory."""
    return project_root / "specs" / "001-sqlserver-pg-cdc"


@pytest.fixture(scope="session")
def contracts_dir(specs_dir: Path) -> Path:
    """Get contracts directory."""
    return specs_dir / "contracts"


@pytest.fixture(scope="session")
def docker_configs_dir(project_root: Path) -> Path:
    """Get Docker configs directory."""
    return project_root / "docker" / "configs"


@pytest.fixture(autouse=True)
def set_test_env_vars() -> None:
    """Set default test environment variables if not already set."""
    defaults = {
        "SQLSERVER_HOST": "localhost",
        "SQLSERVER_DATABASE": "warehouse_source",
        "SQLSERVER_USER": "sa",
        "SQLSERVER_PASSWORD": "YourStrong!Passw0rd",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "warehouse_target",
        "POSTGRES_USER": "postgres",
        "POSTGRES_PASSWORD": "postgres_secure_password",
        "KAFKA_BROKER": "localhost:29092",
        "KAFKA_CONNECT_URL": "http://localhost:8083",
        "VAULT_ADDR": "http://localhost:8200",
        "VAULT_TOKEN": "dev-root-token",
    }

    for key, value in defaults.items():
        if key not in os.environ:
            os.environ[key] = value
