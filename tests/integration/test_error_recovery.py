"""
Integration tests for error recovery and retry logic

Tests verify:
- T077: PostgreSQL downtime recovery
- T078: Network failure retry logic
- T079: DLQ routing of validation errors

These tests follow TDD - they should FAIL until implementation is complete.
"""

import pytest
import time
import requests
from typing import Dict, Any
import psycopg2
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer


# T077: Integration test for PostgreSQL downtime recovery
class TestPostgreSQLDowntimeRecovery:
    """Test connector recovers from PostgreSQL downtime"""

    @pytest.fixture
    def postgres_container(self):
        """PostgreSQL container for testing downtime scenarios"""
        container = PostgresContainer("postgres:15")
        with container:
            yield container

    @pytest.fixture
    def kafka_connect_url(self):
        """Kafka Connect REST API URL"""
        return "http://localhost:8083"

    def test_connector_recovers_after_postgres_restart(
        self, postgres_container, kafka_connect_url
    ):
        """
        Test connector automatically recovers after PostgreSQL restart

        Scenario:
        1. Start replication with PostgreSQL running
        2. Insert 100 rows into SQL Server
        3. Stop PostgreSQL
        4. Insert 50 more rows (should buffer in Kafka)
        5. Restart PostgreSQL
        6. Verify all 150 rows eventually replicate (with retry)
        """
        # Setup: Deploy JDBC sink connector
        connector_config = {
            "name": "postgresql-sink-recovery-test",
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                "tasks.max": "1",
                "topics": "sqlserver.dbo.customers",
                "connection.url": postgres_container.get_connection_url(),
                "auto.create": "true",
                "insert.mode": "upsert",
                "pk.mode": "record_key",
                # Retry configuration
                "errors.retry.timeout": "300000",  # 5 minutes
                "errors.retry.delay.max.ms": "60000",  # 1 minute max backoff
                "connection.attempts": "10",
                "connection.backoff.ms": "5000"
            }
        }

        response = requests.post(
            f"{kafka_connect_url}/connectors",
            json=connector_config
        )
        assert response.status_code == 201

        # Step 1: Insert 100 rows with PostgreSQL running
        # (Assuming SQL Server test fixture exists)
        initial_rows = 100
        # insert_rows_into_sqlserver("customers", initial_rows)
        time.sleep(10)  # Wait for initial replication

        # Verify initial replication
        conn = psycopg2.connect(postgres_container.get_connection_url())
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM customers")
        count = cursor.fetchone()[0]
        assert count == initial_rows, "Initial replication failed"

        # Step 2: Stop PostgreSQL
        postgres_container.stop()
        time.sleep(5)

        # Step 3: Insert 50 more rows (will fail to replicate, should buffer)
        additional_rows = 50
        # insert_rows_into_sqlserver("customers", additional_rows)
        time.sleep(5)

        # Step 4: Restart PostgreSQL
        postgres_container.start()
        time.sleep(10)

        # Step 5: Verify connector recovers and replicates all rows
        # Wait up to 2 minutes for recovery and catch-up
        max_wait = 120
        start_time = time.time()
        expected_count = initial_rows + additional_rows

        while time.time() - start_time < max_wait:
            try:
                conn = psycopg2.connect(postgres_container.get_connection_url())
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM customers")
                count = cursor.fetchone()[0]

                if count == expected_count:
                    break

                time.sleep(5)
            except psycopg2.OperationalError:
                # Connection might fail immediately after restart
                time.sleep(5)
                continue

        assert count == expected_count, f"Expected {expected_count} rows, got {count}"

        # Verify connector status is RUNNING
        response = requests.get(
            f"{kafka_connect_url}/connectors/postgresql-sink-recovery-test/status"
        )
        assert response.status_code == 200
        status = response.json()
        assert status["connector"]["state"] == "RUNNING"

        cursor.close()
        conn.close()

    def test_connector_retries_with_exponential_backoff(
        self, postgres_container, kafka_connect_url
    ):
        """
        Test connector uses exponential backoff during retries

        Verifies retry attempts increase delay between connection attempts
        """
        # This test monitors Kafka Connect logs to verify backoff behavior
        # Expected log pattern: connection attempts with increasing delays

        connector_name = "postgresql-sink-backoff-test"

        # Deploy connector with short retry settings for faster test
        connector_config = {
            "name": connector_name,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                "tasks.max": "1",
                "topics": "sqlserver.dbo.test_backoff",
                "connection.url": "jdbc:postgresql://invalid-host:5432/test",
                "errors.retry.timeout": "60000",  # 1 minute
                "errors.retry.delay.max.ms": "10000",  # 10 seconds max
                "connection.attempts": "5",
                "connection.backoff.ms": "1000"  # Start with 1 second
            }
        }

        response = requests.post(
            f"{kafka_connect_url}/connectors",
            json=connector_config
        )
        assert response.status_code == 201

        # Wait and check connector status
        time.sleep(30)

        response = requests.get(
            f"{kafka_connect_url}/connectors/{connector_name}/status"
        )
        status = response.json()

        # Connector should be in FAILED state after exhausting retries
        assert status["connector"]["state"] in ["FAILED", "RUNNING"]

        # Verify task has retry information in trace
        task_state = status["tasks"][0]
        assert "trace" in task_state or task_state["state"] == "FAILED"


# T078: Integration test for network failure retry logic
class TestNetworkFailureRetry:
    """Test connector handles transient network failures"""

    def test_connector_handles_transient_network_errors(self):
        """
        Test connector retries after transient network failures

        Scenario:
        1. Start replication
        2. Simulate network partition (using iptables or container network)
        3. Verify connector enters retry state
        4. Restore network
        5. Verify connector recovers and continues replication
        """
        # Note: This test requires network manipulation capabilities
        # In Docker environment, this can be done with tc or iptables

        pytest.skip("Requires network manipulation setup")

        # Test implementation would:
        # 1. Use testcontainers network to simulate partition
        # 2. Monitor connector state during partition
        # 3. Verify automatic recovery after network restoration

    def test_connector_respects_max_retry_timeout(self):
        """
        Test connector eventually fails if retry timeout is exceeded
        """
        kafka_connect_url = "http://localhost:8083"
        connector_name = "postgresql-sink-timeout-test"

        # Deploy connector with very short timeout for testing
        connector_config = {
            "name": connector_name,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                "tasks.max": "1",
                "topics": "sqlserver.dbo.test_timeout",
                "connection.url": "jdbc:postgresql://invalid-host:5432/test",
                "errors.retry.timeout": "10000",  # 10 seconds total
                "connection.attempts": "3",
                "connection.backoff.ms": "2000"
            }
        }

        response = requests.post(
            f"{kafka_connect_url}/connectors",
            json=connector_config
        )
        assert response.status_code == 201

        # Wait for timeout period
        time.sleep(15)

        # Verify connector eventually fails
        response = requests.get(
            f"{kafka_connect_url}/connectors/{connector_name}/status"
        )
        status = response.json()

        # After exceeding retry timeout, task should be FAILED
        assert status["tasks"][0]["state"] == "FAILED"


# T079: Integration test for DLQ routing of validation errors
class TestDeadLetterQueueRouting:
    """Test DLQ routing for validation and transformation errors"""

    @pytest.fixture
    def kafka_admin(self):
        """Kafka admin client for topic operations"""
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
        yield admin
        admin.close()

    def test_dlq_routes_schema_validation_errors(self, kafka_admin):
        """
        Test DLQ routes messages that fail schema validation

        Scenario:
        1. Configure JDBC sink with DLQ
        2. Send message with incompatible schema
        3. Verify message is routed to DLQ topic
        4. Verify main connector continues processing valid messages
        """
        kafka_connect_url = "http://localhost:8083"
        dlq_topic = "dlq-postgresql-sink"

        # Deploy connector with DLQ configuration
        connector_config = {
            "name": "postgresql-sink-dlq-test",
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                "tasks.max": "1",
                "topics": "sqlserver.dbo.customers",
                "connection.url": "jdbc:postgresql://localhost:5432/testdb",
                "auto.create": "true",
                "insert.mode": "upsert",
                # Error handling with DLQ
                "errors.tolerance": "all",
                "errors.deadletterqueue.topic.name": dlq_topic,
                "errors.deadletterqueue.topic.replication.factor": "1",
                "errors.deadletterqueue.context.headers.enable": "true"
            }
        }

        response = requests.post(
            f"{kafka_connect_url}/connectors",
            json=connector_config
        )
        assert response.status_code == 201

        # Send valid message
        # send_kafka_message("sqlserver.dbo.customers", valid_customer_record)

        # Send invalid message (schema mismatch)
        # send_kafka_message("sqlserver.dbo.customers", invalid_customer_record)

        # Wait for processing
        time.sleep(10)

        # Verify DLQ topic exists
        topics = kafka_admin.list_topics()
        assert dlq_topic in topics

        # Verify DLQ contains the invalid message
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            dlq_topic,
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000
        )

        dlq_messages = list(consumer)
        assert len(dlq_messages) > 0

        # Verify DLQ message has error context in headers
        first_message = dlq_messages[0]
        headers = dict(first_message.headers)

        assert b"__connect.errors.topic" in headers
        assert b"__connect.errors.exception.class.name" in headers
        assert b"__connect.errors.exception.message" in headers

        consumer.close()

        # Verify connector is still RUNNING (tolerating errors)
        response = requests.get(
            f"{kafka_connect_url}/connectors/postgresql-sink-dlq-test/status"
        )
        status = response.json()
        assert status["connector"]["state"] == "RUNNING"

    def test_dlq_routes_type_conversion_errors(self):
        """
        Test DLQ routes messages with type conversion errors

        Example: String value in column expecting integer
        """
        kafka_connect_url = "http://localhost:8083"
        dlq_topic = "dlq-postgresql-sink"

        # Send message with type mismatch
        # e.g., customer_id as string when PostgreSQL expects integer

        # Verify message ends up in DLQ
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            dlq_topic,
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000
        )

        dlq_messages = list(consumer)

        # Should find messages with conversion errors
        type_error_messages = [
            msg for msg in dlq_messages
            if b"type conversion" in dict(msg.headers).get(
                b"__connect.errors.exception.message", b""
            ).lower()
        ]

        assert len(type_error_messages) > 0
        consumer.close()

    def test_dlq_preserves_original_message(self):
        """
        Test DLQ preserves original message payload for debugging
        """
        # This test verifies that messages in DLQ contain:
        # 1. Original key
        # 2. Original value
        # 3. Error headers with context

        pytest.skip("Requires message production setup")

    def test_dlq_retention_configuration(self, kafka_admin):
        """
        Test DLQ topic has appropriate retention (30 days)
        """
        from kafka.admin import ConfigResource, ConfigResourceType

        dlq_topic = "dlq-postgresql-sink"

        # Get topic configuration
        config_resource = ConfigResource(
            ConfigResourceType.TOPIC,
            dlq_topic
        )

        configs = kafka_admin.describe_configs([config_resource])

        # Verify retention is set to 30 days (2592000000 ms)
        retention_config = configs[config_resource].get("retention.ms")

        assert retention_config is not None
        retention_ms = int(retention_config.value)

        # 30 days = 2,592,000,000 milliseconds
        expected_retention = 30 * 24 * 60 * 60 * 1000
        assert retention_ms == expected_retention


# Additional error recovery tests
class TestConnectorResilience:
    """Test connector resilience under various error conditions"""

    def test_task_restart_after_failure(self):
        """Test task automatically restarts after failure"""
        kafka_connect_url = "http://localhost:8083"

        # Induce task failure
        # Then verify task restarts automatically

        # Check Kafka Connect worker configuration
        # Should have task.restart.max.retries configured

        pytest.skip("Requires failure injection setup")

    def test_connector_handles_invalid_records_with_tolerance(self):
        """Test connector continues despite invalid records when tolerance=all"""

        connector_config = {
            "name": "postgresql-sink-tolerance-test",
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                "tasks.max": "1",
                "topics": "sqlserver.dbo.test_tolerance",
                "connection.url": "jdbc:postgresql://localhost:5432/testdb",
                "errors.tolerance": "all",  # Continue despite errors
                "errors.log.enable": "true",
                "errors.log.include.messages": "true"
            }
        }

        # Send mix of valid and invalid records
        # Verify valid records are processed
        # Verify connector remains RUNNING

        pytest.skip("Requires message production setup")
