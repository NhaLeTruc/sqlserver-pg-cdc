"""
Contract tests for Confluent JDBC PostgreSQL sink connector configuration.
Validates that connector configs conform to the JSON schema specification.
"""

import json
from pathlib import Path
from typing import Any, Dict

import jsonschema
import pytest


class TestJdbcSinkConnectorContract:
    """Contract tests for JDBC PostgreSQL sink connector."""

    @pytest.fixture
    def schema_path(self) -> Path:
        """Path to JDBC sink connector JSON schema."""
        return (
            Path(__file__).parent.parent.parent
            / "specs"
            / "001-sqlserver-pg-cdc"
            / "contracts"
            / "jdbc-postgresql-sink.json"
        )

    @pytest.fixture
    def config_path(self) -> Path:
        """Path to actual JDBC sink connector configuration."""
        return (
            Path(__file__).parent.parent.parent
            / "docker"
            / "configs"
            / "kafka-connect"
            / "postgresql-sink.json"
        )

    @pytest.fixture
    def schema(self, schema_path: Path) -> Dict[str, Any]:
        """Load JSON schema for JDBC sink connector."""
        with open(schema_path) as f:
            return json.load(f)

    @pytest.fixture
    def config(self, config_path: Path) -> Dict[str, Any]:
        """Load actual JDBC sink connector configuration."""
        with open(config_path) as f:
            return json.load(f)

    def test_schema_file_exists(self, schema_path: Path) -> None:
        """Verify JSON schema file exists."""
        assert schema_path.exists(), f"Schema file not found: {schema_path}"

    def test_config_file_exists(self, config_path: Path) -> None:
        """Verify connector config file exists."""
        assert config_path.exists(), f"Config file not found: {config_path}"

    def test_config_validates_against_schema(
        self, config: Dict[str, Any], schema: Dict[str, Any]
    ) -> None:
        """Verify connector config validates against JSON schema."""
        try:
            jsonschema.validate(instance=config, schema=schema)
        except jsonschema.exceptions.ValidationError as e:
            pytest.fail(f"Configuration validation failed: {e.message}")

    def test_required_fields_present(self, config: Dict[str, Any]) -> None:
        """Verify all required fields are present in config."""
        assert "name" in config, "Missing 'name' field"
        assert "config" in config, "Missing 'config' field"

        config_section = config["config"]
        required_fields = [
            "connector.class",
            "connection.url",
            "connection.user",
            "connection.password",
            "topics",
            "insert.mode",
        ]

        for field in required_fields:
            assert field in config_section, f"Missing required field: {field}"

    def test_connector_class_is_jdbc_sink(self, config: Dict[str, Any]) -> None:
        """Verify connector class is Confluent JDBC Sink connector."""
        connector_class = config["config"]["connector.class"]
        assert (
            connector_class == "io.confluent.connect.jdbc.JdbcSinkConnector"
        ), f"Invalid connector class: {connector_class}"

    def test_insert_mode_is_upsert(self, config: Dict[str, Any]) -> None:
        """Verify insert mode is upsert for idempotency."""
        insert_mode = config["config"]["insert.mode"]
        assert insert_mode == "upsert", (
            f"Insert mode should be 'upsert' for idempotency, got '{insert_mode}'"
        )

    def test_upsert_mode_requires_pk_fields(self, config: Dict[str, Any]) -> None:
        """Verify pk.mode and pk.fields are configured for upsert mode."""
        config_section = config["config"]

        if config_section.get("insert.mode") == "upsert":
            assert "pk.mode" in config_section, (
                "upsert mode requires pk.mode to be specified"
            )
            assert "pk.fields" in config_section, (
                "upsert mode requires pk.fields to be specified"
            )

            pk_mode = config_section["pk.mode"]
            assert pk_mode == "record_value", (
                f"Expected pk.mode='record_value', got '{pk_mode}'"
            )

    def test_vault_references_in_credentials(self, config: Dict[str, Any]) -> None:
        """Verify credentials use Vault references (no plaintext passwords)."""
        config_section = config["config"]

        url = config_section.get("connection.url", "")
        user = config_section.get("connection.user", "")
        password = config_section.get("connection.password", "")

        # At least password should use Vault
        assert "${vault:" in password or "${vault:" in url or "${vault:" in user, (
            "Credentials should use Vault references for security"
        )

    def test_auto_create_disabled(self, config: Dict[str, Any]) -> None:
        """Verify auto.create is disabled for production safety."""
        auto_create = config["config"].get("auto.create", "true")
        assert auto_create == "false", (
            "auto.create should be false in production to prevent accidental table creation"
        )

    def test_auto_evolve_enabled(self, config: Dict[str, Any]) -> None:
        """Verify auto.evolve is enabled for schema evolution."""
        auto_evolve = config["config"].get("auto.evolve", "false")
        assert auto_evolve == "true", (
            "auto.evolve should be true to handle schema changes automatically"
        )

    def test_batch_size_configured(self, config: Dict[str, Any]) -> None:
        """Verify batch size is configured for performance."""
        batch_size = config["config"].get("batch.size", "0")
        batch_size_int = int(batch_size)
        assert batch_size_int > 0, f"Batch size should be > 0, got {batch_size}"
        assert batch_size_int <= 5000, (
            f"Batch size should be <= 5000 for safety, got {batch_size}"
        )

    def test_connection_pool_configured(self, config: Dict[str, Any]) -> None:
        """Verify connection pool is configured."""
        pool_size = config["config"].get("connection.pool.size", "0")
        pool_size_int = int(pool_size)
        assert pool_size_int > 0, f"Connection pool size should be > 0, got {pool_size}"

    def test_retry_configuration(self, config: Dict[str, Any]) -> None:
        """Verify retry configuration is set for resilience."""
        config_section = config["config"]

        connection_attempts = int(config_section.get("connection.attempts", "0"))
        assert connection_attempts > 0, (
            f"connection.attempts should be > 0, got {connection_attempts}"
        )

        backoff_ms = int(config_section.get("connection.backoff.ms", "0"))
        assert backoff_ms > 0, f"connection.backoff.ms should be > 0, got {backoff_ms}"

    def test_dead_letter_queue_configured(self, config: Dict[str, Any]) -> None:
        """Verify Dead Letter Queue is configured for error handling."""
        config_section = config["config"]

        errors_tolerance = config_section.get("errors.tolerance", "")
        assert errors_tolerance == "all", (
            f"errors.tolerance should be 'all' to use DLQ, got '{errors_tolerance}'"
        )

        dlq_topic = config_section.get("errors.deadletterqueue.topic.name", "")
        assert dlq_topic, "DLQ topic name should be configured"
        assert "dlq" in dlq_topic.lower(), (
            f"DLQ topic should contain 'dlq' in name, got '{dlq_topic}'"
        )

        dlq_context_headers = config_section.get(
            "errors.deadletterqueue.context.headers.enable", "false"
        )
        assert dlq_context_headers == "true", (
            "DLQ context headers should be enabled for debugging"
        )

    def test_converters_use_avro(self, config: Dict[str, Any]) -> None:
        """Verify converters are configured to use Avro with Schema Registry."""
        config_section = config["config"]

        key_converter = config_section.get("key.converter", "")
        value_converter = config_section.get("value.converter", "")

        assert "AvroConverter" in key_converter, f"Key converter not Avro: {key_converter}"
        assert "AvroConverter" in value_converter, (
            f"Value converter not Avro: {value_converter}"
        )

        # Verify Schema Registry URLs are configured
        assert "key.converter.schema.registry.url" in config_section, (
            "Missing Schema Registry URL for key converter"
        )
        assert "value.converter.schema.registry.url" in config_section, (
            "Missing Schema Registry URL for value converter"
        )

    def test_tasks_max_configured(self, config: Dict[str, Any]) -> None:
        """Verify tasks.max is configured for parallelism."""
        tasks_max = int(config["config"].get("tasks.max", "0"))
        assert tasks_max > 0, f"tasks.max should be > 0, got {tasks_max}"
        assert tasks_max <= 10, (
            f"tasks.max should be reasonable (<= 10), got {tasks_max}"
        )

    def test_error_logging_enabled(self, config: Dict[str, Any]) -> None:
        """Verify error logging is enabled."""
        config_section = config["config"]

        assert config_section.get("errors.log.enable", "false") == "true", (
            "Error logging should be enabled"
        )
        assert config_section.get("errors.log.include.messages", "false") == "true", (
            "Error messages should be included in logs"
        )
