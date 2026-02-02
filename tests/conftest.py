"""
Pytest configuration and fixtures for CDC pipeline tests.
Provides shared fixtures for database connections and test setup.

Fixtures:
- T102: Pytest with Testcontainers integration
- T103: SQL Server connection fixture
- T104: PostgreSQL connection fixture
- T105: Kafka Connect API client fixture
"""

import os
import subprocess
import sys
import time
from collections.abc import Generator
from pathlib import Path

# Add src directory to Python path for imports
_src_path = Path(__file__).parent.parent / "src"
if str(_src_path) not in sys.path:
    sys.path.insert(0, str(_src_path))

import psycopg2
import pyodbc
import pytest
import requests
from hypothesis import Phase, Verbosity, settings
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =============================================================================
# Disable OpenTelemetry Tracing for Unit Tests
# =============================================================================

# Set this BEFORE pytest starts to prevent any tracing initialization
os.environ["OTEL_SDK_DISABLED"] = "true"


@pytest.fixture(scope="session", autouse=True)
def disable_tracing_for_tests():
    """
    Disable OpenTelemetry tracing during test runs.

    This prevents tests from trying to export traces to a collector
    that isn't running, which causes error messages and delays.
    """
    # Already set above at module level
    yield
    # Clean up after all tests
    os.environ.pop("OTEL_SDK_DISABLED", None)


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "contract: mark test as contract test")
    config.addinivalue_line("markers", "performance: mark test as performance test")
    config.addinivalue_line("markers", "slow: mark test as slow running")

    # Register Hypothesis profiles
    settings.register_profile(
        "thorough",
        max_examples=500,
        deadline=2000,
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target, Phase.shrink],
        verbosity=Verbosity.verbose,
    )
    settings.register_profile(
        "dev",
        max_examples=50,
        deadline=1000,
        verbosity=Verbosity.normal,
    )


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


@pytest.fixture(scope="session", autouse=True)
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


# =============================================================================
# T102: Testcontainers Integration Configuration
# =============================================================================

@pytest.fixture(scope="session")
def use_testcontainers() -> bool:
    """
    Determine if tests should use Testcontainers or local Docker services.

    Set USE_TESTCONTAINERS=true to spin up isolated containers for tests.
    By default, tests use the local docker-compose environment.
    """
    return os.environ.get("USE_TESTCONTAINERS", "false").lower() == "true"


@pytest.fixture(scope="session")
def wait_for_services() -> None:
    """
    Wait for Docker Compose services to be healthy before running tests.

    This fixture ensures all services are ready before test execution begins.
    """
    # Check if services are already running
    result = os.system("docker ps --filter 'name=cdc-' --format '{{.Names}}' | wc -l")

    if result == 0:
        # Wait for services to be healthy
        print("\nWaiting for CDC services to be healthy...")
        wait_script = Path(__file__).parent.parent / "docker" / "wait-for-services.sh"

        if wait_script.exists():
            result = os.system(f"{wait_script} 300")

            if result != 0:
                pytest.fail("Services are not healthy. Please start services with: cd docker && docker-compose up -d")


# =============================================================================
# T103: SQL Server Connection Fixture
# =============================================================================

@pytest.fixture(scope="session")
def sqlserver_connection_string() -> str:
    """Get SQL Server connection string from environment"""
    return (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.environ['SQLSERVER_HOST']},1433;"
        f"DATABASE={os.environ['SQLSERVER_DATABASE']};"
        f"UID={os.environ['SQLSERVER_USER']};"
        f"PWD={os.environ['SQLSERVER_PASSWORD']}"
    )


@pytest.fixture
def sqlserver_connection(
    sqlserver_connection_string: str,
    wait_for_services: None
) -> Generator[pyodbc.Connection, None, None]:
    """
    Provide SQL Server database connection for tests.

    Yields:
        pyodbc.Connection: Active database connection

    Example:
        def test_query(sqlserver_connection):
            cursor = sqlserver_connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1
    """
    max_retries = 5
    retry_delay = 2

    conn = None
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(sqlserver_connection_string, timeout=10)
            break
        except pyodbc.Error as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                pytest.fail(f"Failed to connect to SQL Server after {max_retries} attempts: {e}")

    try:
        yield conn
    finally:
        if conn:
            conn.close()


@pytest.fixture
def sqlserver_cursor(
    sqlserver_connection: pyodbc.Connection
) -> Generator[pyodbc.Cursor, None, None]:
    """
    Provide SQL Server cursor for tests.

    Yields:
        pyodbc.Cursor: Database cursor

    Example:
        def test_insert(sqlserver_cursor):
            sqlserver_cursor.execute("INSERT INTO test_table VALUES (1, 'test')")
            sqlserver_cursor.connection.commit()
    """
    cursor = sqlserver_connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


# =============================================================================
# T104: PostgreSQL Connection Fixture
# =============================================================================

@pytest.fixture(scope="session")
def postgres_connection_params() -> dict:
    """Get PostgreSQL connection parameters from environment"""
    return {
        "host": os.environ["POSTGRES_HOST"],
        "port": int(os.environ["POSTGRES_PORT"]),
        "database": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"]
    }


@pytest.fixture
def postgres_connection(
    postgres_connection_params: dict,
    wait_for_services: None
) -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Provide PostgreSQL database connection for tests.

    Yields:
        psycopg2.connection: Active database connection

    Example:
        def test_query(postgres_connection):
            cursor = postgres_connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1
    """
    max_retries = 5
    retry_delay = 2

    conn = None
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**postgres_connection_params, connect_timeout=10)
            conn.set_session(autocommit=False)
            break
        except psycopg2.Error as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                pytest.fail(f"Failed to connect to PostgreSQL after {max_retries} attempts: {e}")

    try:
        yield conn
    finally:
        if conn:
            conn.close()


@pytest.fixture
def postgres_cursor(
    postgres_connection: psycopg2.extensions.connection
) -> Generator[psycopg2.extensions.cursor, None, None]:
    """
    Provide PostgreSQL cursor for tests.

    Yields:
        psycopg2.cursor: Database cursor

    Example:
        def test_insert(postgres_cursor):
            postgres_cursor.execute("INSERT INTO test_table VALUES (1, 'test')")
            postgres_cursor.connection.commit()
    """
    cursor = postgres_connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


# =============================================================================
# T105: Kafka Connect API Client Fixture
# =============================================================================

class KafkaConnectClient:
    """
    Kafka Connect REST API client for tests.

    Provides methods to interact with Kafka Connect REST API:
    - List connectors
    - Get connector status
    - Deploy connector
    - Delete connector
    - Restart connector
    - Get connector config
    """

    def __init__(self, base_url: str):
        """
        Initialize Kafka Connect client.

        Args:
            base_url: Kafka Connect REST API URL (e.g., http://localhost:8083)
        """
        self.base_url = base_url.rstrip("/")

        # Configure session with retries
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def list_connectors(self) -> list:
        """List all connectors"""
        response = self.session.get(f"{self.base_url}/connectors", timeout=10)
        response.raise_for_status()
        return response.json()

    def get_connector_status(self, connector_name: str) -> dict:
        """Get connector status"""
        response = self.session.get(
            f"{self.base_url}/connectors/{connector_name}/status",
            timeout=10
        )
        response.raise_for_status()
        return response.json()

    def get_connector_config(self, connector_name: str) -> dict:
        """Get connector configuration"""
        response = self.session.get(
            f"{self.base_url}/connectors/{connector_name}/config",
            timeout=10
        )
        response.raise_for_status()
        return response.json()

    def deploy_connector(self, connector_config: dict) -> dict:
        """Deploy or update a connector"""
        connector_name = connector_config.get("name")

        if not connector_name:
            raise ValueError("Connector config must have 'name' field")

        # Check if connector exists
        try:
            existing = self.get_connector_config(connector_name)
            # Update existing connector
            response = self.session.put(
                f"{self.base_url}/connectors/{connector_name}/config",
                json=connector_config["config"],
                timeout=30
            )
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Create new connector
                response = self.session.post(
                    f"{self.base_url}/connectors",
                    json=connector_config,
                    timeout=30
                )
            else:
                raise

        response.raise_for_status()
        return response.json()

    def delete_connector(self, connector_name: str) -> None:
        """Delete a connector"""
        response = self.session.delete(
            f"{self.base_url}/connectors/{connector_name}",
            timeout=30
        )
        response.raise_for_status()

    def restart_connector(self, connector_name: str) -> None:
        """Restart a connector"""
        response = self.session.post(
            f"{self.base_url}/connectors/{connector_name}/restart",
            timeout=30
        )
        response.raise_for_status()

    def pause_connector(self, connector_name: str) -> None:
        """Pause a connector"""
        response = self.session.put(
            f"{self.base_url}/connectors/{connector_name}/pause",
            timeout=30
        )
        response.raise_for_status()

    def resume_connector(self, connector_name: str) -> None:
        """Resume a connector"""
        response = self.session.put(
            f"{self.base_url}/connectors/{connector_name}/resume",
            timeout=30
        )
        response.raise_for_status()

    def wait_for_connector_state(
        self,
        connector_name: str,
        expected_state: str,
        timeout: int = 60
    ) -> bool:
        """
        Wait for connector to reach expected state.

        Args:
            connector_name: Name of connector
            expected_state: Expected state (RUNNING, PAUSED, FAILED)
            timeout: Maximum wait time in seconds

        Returns:
            True if connector reached expected state, False otherwise
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                status = self.get_connector_status(connector_name)
                current_state = status.get("connector", {}).get("state")

                if current_state == expected_state:
                    return True

                time.sleep(2)
            except Exception:
                time.sleep(2)

        return False


@pytest.fixture(scope="session")
def kafka_connect_url() -> str:
    """Get Kafka Connect URL from environment"""
    return os.environ["KAFKA_CONNECT_URL"]


@pytest.fixture
def kafka_connect_client(
    kafka_connect_url: str,
    wait_for_services: None
) -> Generator[KafkaConnectClient, None, None]:
    """
    Provide Kafka Connect API client for tests.

    Yields:
        KafkaConnectClient: API client instance

    Example:
        def test_list_connectors(kafka_connect_client):
            connectors = kafka_connect_client.list_connectors()
            assert isinstance(connectors, list)

        def test_deploy_connector(kafka_connect_client):
            config = {
                "name": "test-connector",
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                    "tasks.max": "1",
                    # ... other config
                }
            }
            result = kafka_connect_client.deploy_connector(config)
            assert result["name"] == "test-connector"
    """
    client = KafkaConnectClient(kafka_connect_url)

    # Wait for Kafka Connect to be ready
    max_retries = 10
    for attempt in range(max_retries):
        try:
            client.list_connectors()
            break
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(3)
            else:
                pytest.fail(f"Kafka Connect not ready after {max_retries} attempts: {e}")

    yield client


# =============================================================================
# Test Data Cleanup Fixtures
# =============================================================================

@pytest.fixture
def cleanup_test_connectors(kafka_connect_client: KafkaConnectClient):
    """
    Cleanup test connectors after test execution.

    Automatically removes any connectors with 'test-' prefix after tests.
    """
    yield

    # Cleanup after test
    try:
        connectors = kafka_connect_client.list_connectors()
        for connector in connectors:
            if connector.startswith("test-"):
                kafka_connect_client.delete_connector(connector)
    except Exception:
        pass  # Best effort cleanup


@pytest.fixture
def cleanup_test_tables(sqlserver_cursor: pyodbc.Cursor, postgres_cursor: psycopg2.extensions.cursor):
    """
    Cleanup test tables after test execution.

    Automatically removes any tables with 'test_' prefix after tests.
    """
    yield

    # Cleanup SQL Server test tables
    try:
        sqlserver_cursor.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME LIKE 'test_%'
        """)

        for row in sqlserver_cursor.fetchall():
            table_name = row[0]
            sqlserver_cursor.execute(f"DROP TABLE IF EXISTS dbo.{table_name}")

        sqlserver_cursor.connection.commit()
    except Exception:
        pass

    # Cleanup PostgreSQL test tables
    try:
        postgres_cursor.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE tablename LIKE 'test_%' AND schemaname = 'public'
        """)

        for row in postgres_cursor.fetchall():
            table_name = row[0]
            postgres_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        postgres_cursor.connection.commit()
    except Exception:
        pass


# =============================================================================
# Test Environment Reset Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def reset_script_path(project_root: Path) -> Path:
    """Get path to reset test environment script."""
    return project_root / "scripts" / "bash" / "reset-test-environment.sh"


@pytest.fixture(scope="session")
def clean_test_environment(project_root: Path, reset_script_path: Path):
    """
    Reset test environment to clean state before performance and e2e tests.

    This fixture:
    - Truncates all tables
    - Clears Kafka topics
    - Resets connector offsets (unless QUICK_RESET=1)

    Set SKIP_RESET=1 to skip the reset (faster but may have stale data).
    Set QUICK_RESET=1 to skip connector restart (faster partial reset).
    """
    if os.environ.get("SKIP_RESET") == "1":
        print("\n" + "="*70)
        print("⚠️  SKIPPING TEST ENVIRONMENT RESET")
        print("   Tests may run with stale data!")
        print("="*70 + "\n")
        yield
        return

    print("\n" + "="*70)
    print("🧹 RESETTING TEST ENVIRONMENT TO CLEAN STATE")
    print("="*70)

    # Determine reset mode
    quick_mode = os.environ.get("QUICK_RESET") == "1"
    cmd = [str(reset_script_path)]
    if quick_mode:
        cmd.append("--quick")
        print("   Using quick mode (skip connector restart)")
    else:
        print("   Using full mode (includes connector restart)")

    # Run reset script
    try:
        result = subprocess.run(
            cmd,
            cwd=project_root,
            capture_output=False,  # Show output to user
            check=True
        )

        print("="*70)
        print("✅ TEST ENVIRONMENT RESET COMPLETE")
        print("="*70 + "\n")

    except subprocess.CalledProcessError as e:
        print(f"\n❌ Reset script failed with exit code {e.returncode}")
        print("   Tests may fail due to stale data!")
        print("="*70 + "\n")
        # Don't fail - allow tests to run anyway
    except Exception as e:
        print(f"\n❌ Unexpected error during reset: {e}")
        print("   Tests may fail due to stale data!")
        print("="*70 + "\n")

    yield


@pytest.fixture(scope="session", autouse=True)
def reset_test_environment(project_root: Path):
    """
    Reset test environment before running integration tests.

    - With FULL_RESET=1: Complete Docker environment reset (slow but thorough)
    - Without FULL_RESET: Uses existing environment (fast but may have stale state)

    This ensures tests run in a clean, predictable state.
    """
    if os.environ.get("FULL_RESET") == "1":
        print("\n" + "="*70)
        print("🔄 FULL ENVIRONMENT RESET - This will take ~2 minutes")
        print("="*70)

        # Stop and clean everything
        print("📦 Stopping Docker services and clearing volumes...")
        subprocess.run(
            ["make", "stop"],
            cwd=project_root,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(5)

        # Start fresh
        print("🚀 Starting Docker services...")
        subprocess.run(
            ["make", "start"],
            cwd=project_root,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        print("⏳ Waiting for services to be ready (30s)...")
        time.sleep(30)

        # Initialize
        print("🔧 Initializing databases and Vault...")
        subprocess.run(
            ["make", "init"],
            cwd=project_root,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(5)

        # Deploy connectors
        print("🔌 Deploying Kafka connectors...")
        subprocess.run(
            ["make", "deploy"],
            cwd=project_root,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        print("⏳ Waiting for connectors to be ready (10s)...")
        time.sleep(10)

        print("="*70)
        print("✅ Environment reset complete - tests starting")
        print("="*70 + "\n")
    else:
        print("\n" + "="*70)
        print("⚡ Using existing Docker environment")
        print("   Set FULL_RESET=1 for complete environment reset")
        print("="*70 + "\n")

    yield
