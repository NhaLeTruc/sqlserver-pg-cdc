"""
Load testing for CDC pipeline using Locust.

Tests:
- Kafka Connect API load
- Connector status endpoints
- Configuration management endpoints
- Reconciliation throughput

Usage:
    # Web UI mode
    locust -f tests/load/locustfile.py --host=http://localhost:8083

    # Headless mode
    locust -f tests/load/locustfile.py \
        --host=http://localhost:8083 \
        --users 100 \
        --spawn-rate 10 \
        --run-time 5m \
        --headless
"""

from locust import HttpUser, task, between, events, constant
import random
import logging

logger = logging.getLogger(__name__)


class KafkaConnectAPIUser(HttpUser):
    """
    Simulates users interacting with Kafka Connect REST API.

    Tests API performance under load with various endpoint patterns.
    """

    wait_time = between(1, 5)  # Wait 1-5 seconds between tasks

    # Known connector names (update based on your setup)
    connector_names = [
        "sqlserver-cdc-source",
        "postgresql-jdbc-sink",
        "test-connector"
    ]

    def on_start(self):
        """Called when a user starts. Can be used for setup."""
        logger.info(f"User {self.environment.runner.user_count} started")

    @task(5)
    def get_connectors_list(self):
        """
        GET /connectors - List all connectors.

        High weight (5) as this is a common operation.
        """
        with self.client.get(
            "/connectors",
            catch_response=True,
            name="GET /connectors"
        ) as response:
            if response.status_code == 200:
                try:
                    connectors = response.json()
                    if isinstance(connectors, list):
                        response.success()
                    else:
                        response.failure("Response is not a list")
                except Exception as e:
                    response.failure(f"JSON parse error: {e}")
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(3)
    def get_connector_status(self):
        """
        GET /connectors/{name}/status - Get connector status.

        Medium weight (3) as status checks are frequent.
        """
        connector = random.choice(self.connector_names)
        with self.client.get(
            f"/connectors/{connector}/status",
            catch_response=True,
            name="GET /connectors/{name}/status"
        ) as response:
            if response.status_code == 200:
                try:
                    status = response.json()
                    if "connector" in status and "tasks" in status:
                        response.success()
                    else:
                        response.failure("Missing expected fields in status")
                except Exception as e:
                    response.failure(f"JSON parse error: {e}")
            elif response.status_code == 404:
                # Connector doesn't exist, this is expected
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(2)
    def get_connector_config(self):
        """
        GET /connectors/{name}/config - Get connector configuration.

        Lower weight (2) as config checks are less frequent.
        """
        connector = random.choice(self.connector_names)
        with self.client.get(
            f"/connectors/{connector}/config",
            catch_response=True,
            name="GET /connectors/{name}/config"
        ) as response:
            if response.status_code == 200:
                try:
                    config = response.json()
                    if isinstance(config, dict):
                        response.success()
                    else:
                        response.failure("Config is not a dictionary")
                except Exception as e:
                    response.failure(f"JSON parse error: {e}")
            elif response.status_code == 404:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(2)
    def get_connector_tasks(self):
        """
        GET /connectors/{name}/tasks - Get connector tasks.

        Lower weight (2) for task status checks.
        """
        connector = random.choice(self.connector_names)
        with self.client.get(
            f"/connectors/{connector}/tasks",
            catch_response=True,
            name="GET /connectors/{name}/tasks"
        ) as response:
            if response.status_code == 200:
                try:
                    tasks = response.json()
                    if isinstance(tasks, list):
                        response.success()
                    else:
                        response.failure("Tasks response is not a list")
                except Exception as e:
                    response.failure(f"JSON parse error: {e}")
            elif response.status_code == 404:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(1)
    def get_connector_plugins(self):
        """
        GET /connector-plugins - List available connector plugins.

        Low weight (1) as this is rarely called.
        """
        with self.client.get(
            "/connector-plugins",
            catch_response=True,
            name="GET /connector-plugins"
        ) as response:
            if response.status_code == 200:
                try:
                    plugins = response.json()
                    if isinstance(plugins, list):
                        response.success()
                    else:
                        response.failure("Plugins response is not a list")
                except Exception as e:
                    response.failure(f"JSON parse error: {e}")
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(1)
    def get_root(self):
        """
        GET / - Kafka Connect root endpoint.

        Low weight (1) for health checks.
        """
        with self.client.get(
            "/",
            catch_response=True,
            name="GET /"
        ) as response:
            if response.status_code == 200:
                try:
                    info = response.json()
                    if "version" in info or "commit" in info:
                        response.success()
                    else:
                        response.failure("Missing version info")
                except Exception as e:
                    response.failure(f"JSON parse error: {e}")
            else:
                response.failure(f"Status code: {response.status_code}")


class HighThroughputUser(HttpUser):
    """
    Simulates high-throughput monitoring scenarios.

    Uses constant pacing for predictable load.
    """

    wait_time = constant(0.5)  # Constant 0.5 second wait

    connector_names = [
        "sqlserver-cdc-source",
        "postgresql-jdbc-sink"
    ]

    @task
    def rapid_status_check(self):
        """Rapid status polling scenario."""
        connector = random.choice(self.connector_names)
        self.client.get(
            f"/connectors/{connector}/status",
            name="Rapid Status Check"
        )


class SpikeLoadUser(HttpUser):
    """
    Simulates spike load scenarios.

    Tests system behavior under sudden traffic increases.
    """

    wait_time = between(0.1, 1.0)  # Very short wait times

    @task
    def burst_request(self):
        """Burst of rapid requests."""
        self.client.get("/connectors", name="Burst Request")


# Custom event handlers for detailed logging
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Called when test starts."""
    logger.info("Load test starting...")
    logger.info(f"Target host: {environment.host}")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Called when test stops."""
    logger.info("Load test completed")
    logger.info(f"Total requests: {environment.stats.total.num_requests}")
    logger.info(f"Total failures: {environment.stats.total.num_failures}")


@events.request.add_listener
def on_request(request_type, name, response_time, response_length, exception, **kwargs):
    """Called for every request."""
    if exception:
        logger.error(f"Request failed: {name} - {exception}")


# User classes to run (can be configured)
# Default: Only KafkaConnectAPIUser
# To include all: locust -f locustfile.py --users 100 --user-classes KafkaConnectAPIUser,HighThroughputUser


# Performance thresholds (for validation)
PERFORMANCE_THRESHOLDS = {
    "max_response_time_ms": 1000,  # Max acceptable response time
    "max_failure_rate": 0.01,  # Max 1% failure rate
    "min_requests_per_second": 10  # Minimum throughput
}
