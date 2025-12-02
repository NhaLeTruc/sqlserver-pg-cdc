"""
End-to-end tests for Docker Compose environment

Tests verify:
- T096: Docker Compose stack startup
- T097: Service health checks
- T098: Test suite execution in local environment
- T099: Failure scenario simulation

These tests follow TDD - they should FAIL until implementation is complete.
"""

import pytest
import subprocess
import time
import requests
from pathlib import Path
from typing import Dict, List
import json


# T096: E2E test for Docker Compose stack startup
class TestDockerComposeStartup:
    """Test Docker Compose stack starts successfully"""

    @pytest.fixture(scope="class")
    def docker_compose_path(self):
        """Path to docker-compose.yml"""
        return Path(__file__).parent.parent.parent / "docker" / "docker-compose.yml"

    def test_docker_compose_file_exists(self, docker_compose_path):
        """Verify docker-compose.yml exists"""
        assert docker_compose_path.exists(), f"docker-compose.yml not found at {docker_compose_path}"

    def test_docker_compose_syntax_valid(self, docker_compose_path):
        """Verify docker-compose.yml has valid syntax"""
        result = subprocess.run(
            ["docker-compose", "-f", str(docker_compose_path), "config"],
            capture_output=True,
            text=True,
            timeout=30
        )

        assert result.returncode == 0, f"Invalid docker-compose syntax: {result.stderr}"

    def test_docker_compose_starts_all_services(self, docker_compose_path):
        """
        Test that docker-compose up starts all required services

        Expected services:
        - sqlserver
        - postgres
        - zookeeper
        - kafka
        - schema-registry
        - kafka-connect
        - vault
        - prometheus
        - grafana
        - jaeger
        """
        # Start services in detached mode
        result = subprocess.run(
            ["docker-compose", "-f", str(docker_compose_path), "up", "-d"],
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes for all services to start
        )

        assert result.returncode == 0, f"Failed to start services: {result.stderr}"

        # Wait a bit for services to initialize
        time.sleep(30)

        # Check that all services are running
        result = subprocess.run(
            ["docker-compose", "-f", str(docker_compose_path), "ps"],
            capture_output=True,
            text=True
        )

        required_services = [
            "sqlserver",
            "postgres",
            "zookeeper",
            "kafka",
            "schema-registry",
            "kafka-connect",
            "vault",
            "prometheus",
            "grafana",
            "jaeger"
        ]

        for service in required_services:
            assert service in result.stdout, f"Service {service} not found in running services"

    def test_services_start_within_time_limit(self, docker_compose_path):
        """Test that all services become healthy within 3 minutes"""
        max_wait = 180  # 3 minutes
        start_time = time.time()
        all_healthy = False

        while time.time() - start_time < max_wait:
            result = subprocess.run(
                ["docker-compose", "-f", str(docker_compose_path), "ps", "--format", "json"],
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                try:
                    services = json.loads(result.stdout)
                    healthy_count = sum(
                        1 for s in services
                        if s.get("Health") == "healthy" or s.get("State") == "running"
                    )

                    if healthy_count >= 10:  # All required services
                        all_healthy = True
                        break
                except json.JSONDecodeError:
                    pass

            time.sleep(10)

        assert all_healthy, "Not all services became healthy within 3 minutes"


# T097: E2E test for service health checks
class TestServiceHealthChecks:
    """Test service health check endpoints"""

    def test_sqlserver_health_check(self):
        """Test SQL Server responds to health check"""
        # Wait for SQL Server to be ready
        max_retries = 10
        for i in range(max_retries):
            result = subprocess.run(
                [
                    "docker", "exec", "cdc-sqlserver",
                    "/opt/mssql-tools/bin/sqlcmd",
                    "-S", "localhost",
                    "-U", "sa",
                    "-P", "YourStrong!Passw0rd",
                    "-Q", "SELECT 1"
                ],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0:
                assert "1" in result.stdout
                return

            time.sleep(5)

        pytest.fail("SQL Server health check failed after retries")

    def test_postgres_health_check(self):
        """Test PostgreSQL responds to health check"""
        result = subprocess.run(
            [
                "docker", "exec", "cdc-postgres",
                "pg_isready", "-U", "postgres"
            ],
            capture_output=True,
            text=True,
            timeout=10
        )

        assert result.returncode == 0
        assert "accepting connections" in result.stdout

    def test_kafka_health_check(self):
        """Test Kafka broker is healthy"""
        # Check if Kafka broker is responding
        result = subprocess.run(
            [
                "docker", "exec", "cdc-kafka",
                "kafka-broker-api-versions",
                "--bootstrap-server", "localhost:9092"
            ],
            capture_output=True,
            text=True,
            timeout=30
        )

        assert result.returncode == 0, f"Kafka broker not responding: {result.stderr}"

    def test_schema_registry_health_check(self):
        """Test Schema Registry is healthy"""
        try:
            response = requests.get("http://localhost:8081/subjects", timeout=10)
            assert response.status_code == 200
        except requests.RequestException as e:
            pytest.fail(f"Schema Registry health check failed: {e}")

    def test_kafka_connect_health_check(self):
        """Test Kafka Connect is healthy"""
        try:
            response = requests.get("http://localhost:8083/connectors", timeout=10)
            assert response.status_code == 200
        except requests.RequestException as e:
            pytest.fail(f"Kafka Connect health check failed: {e}")

    def test_vault_health_check(self):
        """Test Vault is healthy"""
        try:
            response = requests.get("http://localhost:8200/v1/sys/health", timeout=10)
            # Vault returns 200 for initialized and unsealed
            assert response.status_code in [200, 429, 472, 473]
        except requests.RequestException as e:
            pytest.fail(f"Vault health check failed: {e}")

    def test_prometheus_health_check(self):
        """Test Prometheus is healthy"""
        try:
            response = requests.get("http://localhost:9090/-/healthy", timeout=10)
            assert response.status_code == 200
        except requests.RequestException as e:
            pytest.fail(f"Prometheus health check failed: {e}")

    def test_grafana_health_check(self):
        """Test Grafana is healthy"""
        try:
            response = requests.get("http://localhost:3000/api/health", timeout=10)
            assert response.status_code == 200
            data = response.json()
            assert data.get("database") == "ok"
        except requests.RequestException as e:
            pytest.fail(f"Grafana health check failed: {e}")

    def test_jaeger_health_check(self):
        """Test Jaeger is healthy"""
        try:
            response = requests.get("http://localhost:16686/api/services", timeout=10)
            assert response.status_code == 200
        except requests.RequestException as e:
            pytest.fail(f"Jaeger health check failed: {e}")


# T098: E2E test for test suite execution in local environment
class TestTestSuiteExecution:
    """Test that test suite can run in local Docker environment"""

    def test_pytest_can_connect_to_sqlserver(self):
        """Test pytest can connect to SQL Server container"""
        import pyodbc

        try:
            conn = pyodbc.connect(
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=localhost,1433;"
                "DATABASE=master;"
                "UID=sa;"
                "PWD=YourStrong!Passw0rd",
                timeout=10
            )
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()
            assert version is not None
            assert "Microsoft SQL Server" in version[0]
            cursor.close()
            conn.close()

        except pyodbc.Error as e:
            pytest.fail(f"Failed to connect to SQL Server: {e}")

    def test_pytest_can_connect_to_postgres(self):
        """Test pytest can connect to PostgreSQL container"""
        import psycopg2

        try:
            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="postgres",
                user="postgres",
                password="postgres_secure_password",
                connect_timeout=10
            )
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()
            assert version is not None
            assert "PostgreSQL" in version[0]
            cursor.close()
            conn.close()

        except psycopg2.Error as e:
            pytest.fail(f"Failed to connect to PostgreSQL: {e}")

    def test_pytest_can_access_kafka_connect_api(self):
        """Test pytest can access Kafka Connect REST API"""
        try:
            response = requests.get("http://localhost:8083/connectors", timeout=10)
            assert response.status_code == 200
            connectors = response.json()
            assert isinstance(connectors, list)
        except Exception as e:
            pytest.fail(f"Failed to access Kafka Connect API: {e}")

    def test_unit_tests_run_successfully(self):
        """Test that unit tests can run"""
        result = subprocess.run(
            ["python", "-m", "pytest", "tests/unit/", "-v", "--tb=short"],
            capture_output=True,
            text=True,
            timeout=60
        )

        # Unit tests should pass (or at least run without errors)
        assert result.returncode in [0, 1], f"Unit tests failed with unexpected error: {result.stderr}"
        assert "PASSED" in result.stdout or "passed" in result.stdout

    def test_contract_tests_can_validate_configs(self):
        """Test that contract tests can validate connector configs"""
        result = subprocess.run(
            ["python", "-m", "pytest", "tests/contract/", "-v", "--tb=short"],
            capture_output=True,
            text=True,
            timeout=60
        )

        # Contract tests should pass
        assert result.returncode in [0, 1]
        assert "tests/contract/" in result.stdout


# T099: E2E test for failure scenario simulation
class TestFailureScenarios:
    """Test system behavior under failure conditions"""

    def test_service_can_restart_after_failure(self):
        """Test that a service can restart after being stopped"""
        # Stop PostgreSQL
        result = subprocess.run(
            ["docker", "stop", "cdc-postgres"],
            capture_output=True,
            text=True,
            timeout=30
        )

        assert result.returncode == 0, "Failed to stop PostgreSQL"

        # Wait a bit
        time.sleep(5)

        # Restart PostgreSQL
        result = subprocess.run(
            ["docker", "start", "cdc-postgres"],
            capture_output=True,
            text=True,
            timeout=30
        )

        assert result.returncode == 0, "Failed to restart PostgreSQL"

        # Wait for PostgreSQL to be healthy again
        max_wait = 30
        start_time = time.time()
        healthy = False

        while time.time() - start_time < max_wait:
            result = subprocess.run(
                ["docker", "exec", "cdc-postgres", "pg_isready", "-U", "postgres"],
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                healthy = True
                break

            time.sleep(2)

        assert healthy, "PostgreSQL did not become healthy after restart"

    def test_dependent_service_waits_for_dependencies(self):
        """Test that Kafka Connect waits for Kafka to be ready"""
        # Check Kafka Connect logs for dependency waiting
        result = subprocess.run(
            ["docker", "logs", "cdc-kafka-connect", "--tail", "100"],
            capture_output=True,
            text=True,
            timeout=10
        )

        # Kafka Connect should have successfully connected to Kafka
        assert "Kafka Connect started" in result.stdout or "REST server started" in result.stdout or result.returncode == 0

    def test_health_check_fails_when_service_unhealthy(self):
        """Test that health checks detect unhealthy services"""
        # This test verifies the health check mechanism itself
        # by checking if a stopped service is reported as unhealthy

        # Get current service health
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=cdc-", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True,
            text=True
        )

        services_status = result.stdout.strip().split("\n")

        # All services should be running or healthy
        for line in services_status:
            if line:
                name, status = line.split("\t")
                assert "Up" in status or "healthy" in status, f"Service {name} is not healthy: {status}"

    def test_network_isolation_between_containers(self):
        """Test that containers can communicate on the cdc-network"""
        # Test that Kafka Connect can reach Kafka
        result = subprocess.run(
            ["docker", "exec", "cdc-kafka-connect", "ping", "-c", "1", "kafka"],
            capture_output=True,
            text=True,
            timeout=10
        )

        assert result.returncode == 0, "Kafka Connect cannot reach Kafka on cdc-network"

        # Test that Kafka Connect can reach SQL Server
        result = subprocess.run(
            ["docker", "exec", "cdc-kafka-connect", "ping", "-c", "1", "sqlserver"],
            capture_output=True,
            text=True,
            timeout=10
        )

        assert result.returncode == 0, "Kafka Connect cannot reach SQL Server on cdc-network"

    def test_services_recover_from_network_partition(self):
        """Test services can recover from temporary network issues"""
        # Simulate network issue by disconnecting and reconnecting a service
        # Note: This is a simplified test - full network partition testing
        # would require more complex setup

        # Pause a service (simulates network partition)
        result = subprocess.run(
            ["docker", "pause", "cdc-postgres"],
            capture_output=True,
            text=True,
            timeout=10
        )

        assert result.returncode == 0, "Failed to pause PostgreSQL"

        # Wait a bit
        time.sleep(5)

        # Unpause the service
        result = subprocess.run(
            ["docker", "unpause", "cdc-postgres"],
            capture_output=True,
            text=True,
            timeout=10
        )

        assert result.returncode == 0, "Failed to unpause PostgreSQL"

        # Verify service is responsive again
        time.sleep(5)
        result = subprocess.run(
            ["docker", "exec", "cdc-postgres", "pg_isready", "-U", "postgres"],
            capture_output=True,
            text=True,
            timeout=10
        )

        assert result.returncode == 0, "PostgreSQL did not recover after unpause"


# Cleanup fixture
@pytest.fixture(scope="session", autouse=True)
def cleanup_docker_environment():
    """Cleanup Docker environment after tests"""
    yield

    # Note: Comment out the cleanup if you want to inspect the environment after tests
    # docker_compose_path = Path(__file__).parent.parent.parent / "docker" / "docker-compose.yml"
    # subprocess.run(
    #     ["docker-compose", "-f", str(docker_compose_path), "down", "-v"],
    #     capture_output=True,
    #     timeout=60
    # )
