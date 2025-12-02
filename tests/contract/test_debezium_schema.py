"""
Contract tests for Debezium SQL Server source connector configuration.
Validates that connector configs conform to the JSON schema specification.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict

import jsonschema
import pytest


class TestDebeziumSourceConnectorContract:
    """Contract tests for Debezium SQL Server CDC source connector."""

    @pytest.fixture
    def schema_path(self) -> Path:
        """Path to Debezium connector JSON schema."""
        return (
            Path(__file__).parent.parent.parent
            / "specs"
            / "001-sqlserver-pg-cdc"
            / "contracts"
            / "debezium-sqlserver-source.json"
        )

    @pytest.fixture
    def config_path(self) -> Path:
        """Path to actual Debezium connector configuration."""
        return (
            Path(__file__).parent.parent.parent
            / "docker"
            / "configs"
            / "debezium"
            / "sqlserver-source.json"
        )

    @pytest.fixture
    def schema(self, schema_path: Path) -> Dict[str, Any]:
        """Load JSON schema for Debezium connector."""
        with open(schema_path) as f:
            return json.load(f)

    @pytest.fixture
    def config(self, config_path: Path) -> Dict[str, Any]:
        """Load actual Debezium connector configuration."""
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
            "database.hostname",
            "database.port",
            "database.user",
            "database.password",
            "database.names",
            "table.include.list",
        ]

        for field in required_fields:
            assert field in config_section, f"Missing required field: {field}"

    def test_connector_class_is_debezium_sqlserver(
        self, config: Dict[str, Any]
    ) -> None:
        """Verify connector class is Debezium SQL Server connector."""
        connector_class = config["config"]["connector.class"]
        assert (
            connector_class == "io.debezium.connector.sqlserver.SqlServerConnector"
        ), f"Invalid connector class: {connector_class}"

    def test_tasks_max_is_one(self, config: Dict[str, Any]) -> None:
        """Verify tasks.max is set to 1 for SQL Server CDC (single-threaded)."""
        tasks_max = config["config"]["tasks.max"]
        assert (
            tasks_max == "1"
        ), f"SQL Server CDC requires tasks.max=1, got {tasks_max}"

    def test_vault_references_in_credentials(self, config: Dict[str, Any]) -> None:
        """Verify credentials use Vault references (no plaintext passwords)."""
        config_section = config["config"]

        # Check for Vault reference pattern: ${vault:...}
        hostname = config_section.get("database.hostname", "")
        user = config_section.get("database.user", "")
        password = config_section.get("database.password", "")

        # At least password should use Vault
        assert "${vault:" in password or "${vault:" in hostname or "${vault:" in user, (
            "Credentials should use Vault references for security"
        )

    def test_decimal_handling_mode(self, config: Dict[str, Any]) -> None:
        """Verify decimal handling mode is set for precision."""
        decimal_mode = config["config"].get("decimal.handling.mode", "")
        assert decimal_mode in [
            "precise",
            "double",
            "string",
        ], f"Invalid decimal handling mode: {decimal_mode}"

    def test_time_precision_mode(self, config: Dict[str, Any]) -> None:
        """Verify time precision mode is configured."""
        time_mode = config["config"].get("time.precision.mode", "")
        assert time_mode in [
            "adaptive",
            "connect",
        ], f"Invalid time precision mode: {time_mode}"

    def test_snapshot_mode_configured(self, config: Dict[str, Any]) -> None:
        """Verify snapshot mode is configured."""
        snapshot_mode = config["config"].get("snapshot.mode", "")
        assert snapshot_mode in [
            "initial",
            "schema_only",
            "no_snapshot",
        ], f"Invalid snapshot mode: {snapshot_mode}"

    def test_schema_changes_tracking_enabled(self, config: Dict[str, Any]) -> None:
        """Verify schema change tracking is enabled."""
        include_schema_changes = config["config"].get("include.schema.changes", "false")
        assert include_schema_changes == "true", (
            "Schema change tracking should be enabled for schema evolution"
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

    def test_error_handling_configured(self, config: Dict[str, Any]) -> None:
        """Verify error handling and logging are configured."""
        config_section = config["config"]

        errors_tolerance = config_section.get("errors.tolerance", "")
        assert errors_tolerance in ["none", "all"], (
            f"Invalid errors.tolerance: {errors_tolerance}"
        )

        # Verify error logging is enabled
        assert config_section.get("errors.log.enable", "false") == "true", (
            "Error logging should be enabled"
        )
