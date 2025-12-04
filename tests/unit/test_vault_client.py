"""
Unit tests for src/utils/vault_client.py

This module provides comprehensive tests for HashiCorp Vault client functionality,
including initialization, secret retrieval, database credential fetching, and health checks.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
from src.utils.vault_client import VaultClient, get_credentials_from_vault


class TestVaultClientInit:
    """Test VaultClient initialization scenarios"""

    def test_init_with_explicit_parameters(self):
        """Test initialization with explicitly provided parameters"""
        # Arrange & Act
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token-123"
        )

        # Assert
        assert client.vault_addr == "https://vault.example.com"
        assert client.vault_token == "test-token-123"
        assert client.namespace is None
        assert client.headers == {
            "X-Vault-Token": "test-token-123",
            "Content-Type": "application/json"
        }

    @patch('src.utils.vault_client.os.getenv')
    def test_init_with_env_variables(self, mock_getenv):
        """Test initialization using environment variables"""
        # Arrange
        def getenv_side_effect(key):
            env_vars = {
                "VAULT_ADDR": "https://vault.env.com",
                "VAULT_TOKEN": "env-token-456"
            }
            return env_vars.get(key)

        mock_getenv.side_effect = getenv_side_effect

        # Act
        client = VaultClient()

        # Assert
        assert client.vault_addr == "https://vault.env.com"
        assert client.vault_token == "env-token-456"
        assert mock_getenv.call_count == 2

    @patch('src.utils.vault_client.os.getenv')
    def test_init_missing_vault_addr_raises_error(self, mock_getenv):
        """Test that missing vault_addr raises ValueError"""
        # Arrange
        mock_getenv.return_value = None

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            VaultClient(vault_token="test-token")

        assert "Vault address not provided" in str(exc_info.value)
        assert "VAULT_ADDR" in str(exc_info.value)

    @patch('src.utils.vault_client.os.getenv')
    def test_init_missing_vault_token_raises_error(self, mock_getenv):
        """Test that missing vault_token raises ValueError"""
        # Arrange
        def getenv_side_effect(key):
            if key == "VAULT_ADDR":
                return "https://vault.example.com"
            return None

        mock_getenv.side_effect = getenv_side_effect

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            VaultClient(vault_addr="https://vault.example.com")

        assert "Vault token not provided" in str(exc_info.value)
        assert "VAULT_TOKEN" in str(exc_info.value)

    def test_init_removes_trailing_slash_from_vault_addr(self):
        """Test that trailing slash is removed from vault_addr"""
        # Arrange & Act
        client = VaultClient(
            vault_addr="https://vault.example.com/",
            vault_token="test-token"
        )

        # Assert
        assert client.vault_addr == "https://vault.example.com"

    def test_init_with_namespace_adds_header(self):
        """Test that namespace parameter adds X-Vault-Namespace header"""
        # Arrange & Act
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token",
            namespace="my-namespace"
        )

        # Assert
        assert client.namespace == "my-namespace"
        assert "X-Vault-Namespace" in client.headers
        assert client.headers["X-Vault-Namespace"] == "my-namespace"

    def test_init_without_namespace_no_namespace_header(self):
        """Test that no namespace header is added when namespace is not provided"""
        # Arrange & Act
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        # Assert
        assert "X-Vault-Namespace" not in client.headers

    @patch('src.utils.vault_client.logger')
    def test_init_logs_initialization(self, mock_logger):
        """Test that initialization is logged"""
        # Arrange & Act
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        # Assert
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0][0]
        assert "Initialized Vault client" in call_args
        assert "https://vault.example.com" in call_args


class TestGetSecret:
    """Test VaultClient.get_secret() method"""

    @patch('src.utils.vault_client.requests.get')
    def test_get_secret_success_with_kv_v2_path(self, mock_get):
        """Test successful secret retrieval with KV v2 path format"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {
                    "username": "admin",
                    "password": "secret123"
                }
            }
        }
        mock_get.return_value = mock_response

        # Act
        result = client.get_secret("secret/data/myapp/creds")

        # Assert
        assert result == {"username": "admin", "password": "secret123"}
        mock_get.assert_called_once_with(
            "https://vault.example.com/v1/secret/data/myapp/creds",
            headers=client.headers,
            timeout=10
        )

    @patch('src.utils.vault_client.requests.get')
    def test_get_secret_adds_data_to_path(self, mock_get):
        """Test that /data/ is added to path for KV v2 format"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {"key": "value"}
            }
        }
        mock_get.return_value = mock_response

        # Act
        result = client.get_secret("secret/myapp/creds")

        # Assert
        assert result == {"key": "value"}
        mock_get.assert_called_once_with(
            "https://vault.example.com/v1/secret/data/myapp/creds",
            headers=client.headers,
            timeout=10
        )

    @patch('src.utils.vault_client.requests.get')
    def test_get_secret_single_part_path_adds_data(self, mock_get):
        """Test that /data is added to single-part paths"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {"key": "value"}
            }
        }
        mock_get.return_value = mock_response

        # Act
        result = client.get_secret("secret")

        # Assert
        mock_get.assert_called_once_with(
            "https://vault.example.com/v1/secret/data",
            headers=client.headers,
            timeout=10
        )

    @patch('src.utils.vault_client.requests.get')
    def test_get_secret_404_raises_value_error(self, mock_get):
        """Test that 404 response raises ValueError"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            client.get_secret("secret/notfound")

        assert "Secret not found at path" in str(exc_info.value)
        assert "secret/data/notfound" in str(exc_info.value)

    @patch('src.utils.vault_client.requests.get')
    def test_get_secret_http_error_raises_exception(self, mock_get):
        """Test that HTTP errors are raised"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.raise_for_status.side_effect = requests.HTTPError("Forbidden")
        mock_get.return_value = mock_response

        # Act & Assert
        with pytest.raises(requests.HTTPError):
            client.get_secret("secret/forbidden")

    @patch('src.utils.vault_client.requests.get')
    def test_get_secret_empty_data_raises_value_error(self, mock_get):
        """Test that empty secret data raises ValueError"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {}
            }
        }
        mock_get.return_value = mock_response

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            client.get_secret("secret/empty")

        assert "No data found in secret" in str(exc_info.value)

    @patch('src.utils.vault_client.requests.get')
    def test_get_secret_missing_data_field_raises_value_error(self, mock_get):
        """Test that missing data field raises ValueError"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {}}
        mock_get.return_value = mock_response

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            client.get_secret("secret/invalid")

        assert "No data found in secret" in str(exc_info.value)

    @patch('src.utils.vault_client.requests.get')
    def test_get_secret_timeout(self, mock_get):
        """Test that request timeout is properly configured"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {"key": "value"}
            }
        }
        mock_get.return_value = mock_response

        # Act
        client.get_secret("secret/test")

        # Assert
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs['timeout'] == 10

    @patch('src.utils.vault_client.logger')
    @patch('src.utils.vault_client.requests.get')
    def test_get_secret_logs_debug_messages(self, mock_get, mock_logger):
        """Test that debug logging occurs"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {"key": "value"}
            }
        }
        mock_get.return_value = mock_response

        # Act
        client.get_secret("secret/test")

        # Assert
        assert mock_logger.debug.call_count == 2
        first_call = mock_logger.debug.call_args_list[0][0][0]
        second_call = mock_logger.debug.call_args_list[1][0][0]
        assert "Fetching secret from" in first_call
        assert "Successfully fetched secret" in second_call


class TestGetDatabaseCredentials:
    """Test VaultClient.get_database_credentials() method"""

    @patch('src.utils.vault_client.requests.get')
    def test_get_sqlserver_credentials_success(self, mock_get):
        """Test successful retrieval of SQL Server credentials"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {
                    "server": "sqlserver.example.com",
                    "database": "mydb",
                    "username": "sa",
                    "password": "Password123!"
                }
            }
        }
        mock_get.return_value = mock_response

        # Act
        result = client.get_database_credentials("sqlserver")

        # Assert
        assert result["server"] == "sqlserver.example.com"
        assert result["database"] == "mydb"
        assert result["username"] == "sa"
        assert result["password"] == "Password123!"
        mock_get.assert_called_once()

    @patch('src.utils.vault_client.requests.get')
    def test_get_postgresql_credentials_success(self, mock_get):
        """Test successful retrieval of PostgreSQL credentials"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {
                    "host": "postgres.example.com",
                    "database": "mydb",
                    "username": "postgres",
                    "password": "pgpass123"
                }
            }
        }
        mock_get.return_value = mock_response

        # Act
        result = client.get_database_credentials("postgresql")

        # Assert
        assert result["host"] == "postgres.example.com"
        assert result["database"] == "mydb"
        assert result["username"] == "postgres"
        assert result["password"] == "pgpass123"

    @patch('src.utils.vault_client.requests.get')
    def test_get_postgresql_credentials_adds_default_port(self, mock_get):
        """Test that default port 5432 is added to PostgreSQL credentials"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {
                    "host": "postgres.example.com",
                    "database": "mydb",
                    "username": "postgres",
                    "password": "pgpass123"
                }
            }
        }
        mock_get.return_value = mock_response

        # Act
        result = client.get_database_credentials("postgresql")

        # Assert
        assert result["port"] == 5432

    @patch('src.utils.vault_client.requests.get')
    def test_get_postgresql_credentials_preserves_custom_port(self, mock_get):
        """Test that custom port is preserved for PostgreSQL credentials"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {
                    "host": "postgres.example.com",
                    "port": 5433,
                    "database": "mydb",
                    "username": "postgres",
                    "password": "pgpass123"
                }
            }
        }
        mock_get.return_value = mock_response

        # Act
        result = client.get_database_credentials("postgresql")

        # Assert
        assert result["port"] == 5433

    def test_get_database_credentials_invalid_type_raises_error(self):
        """Test that invalid database type raises ValueError"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            client.get_database_credentials("mongodb")

        assert "Invalid database_type: mongodb" in str(exc_info.value)
        assert "Must be 'sqlserver' or 'postgresql'" in str(exc_info.value)

    @patch('src.utils.vault_client.requests.get')
    def test_get_sqlserver_credentials_missing_fields_raises_error(self, mock_get):
        """Test that missing required SQL Server fields raises ValueError"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {
                    "server": "sqlserver.example.com",
                    "username": "sa"
                    # Missing: database, password
                }
            }
        }
        mock_get.return_value = mock_response

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            client.get_database_credentials("sqlserver")

        assert "Missing required fields" in str(exc_info.value)
        assert "database" in str(exc_info.value)
        assert "password" in str(exc_info.value)

    @patch('src.utils.vault_client.requests.get')
    def test_get_postgresql_credentials_missing_fields_raises_error(self, mock_get):
        """Test that missing required PostgreSQL fields raises ValueError"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {
                    "host": "postgres.example.com",
                    "username": "postgres"
                    # Missing: database, password
                }
            }
        }
        mock_get.return_value = mock_response

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            client.get_database_credentials("postgresql")

        assert "Missing required fields" in str(exc_info.value)

    @patch('src.utils.vault_client.requests.get')
    def test_get_database_credentials_constructs_correct_path(self, mock_get):
        """Test that correct secret path is constructed"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {
                    "server": "sqlserver.example.com",
                    "database": "mydb",
                    "username": "sa",
                    "password": "pass"
                }
            }
        }
        mock_get.return_value = mock_response

        # Act
        client.get_database_credentials("sqlserver")

        # Assert
        mock_get.assert_called_once_with(
            "https://vault.example.com/v1/secret/data/database/sqlserver",
            headers=client.headers,
            timeout=10
        )

    @patch('src.utils.vault_client.logger')
    @patch('src.utils.vault_client.requests.get')
    def test_get_database_credentials_logs_success(self, mock_get, mock_logger):
        """Test that successful credential retrieval is logged"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "data": {
                    "server": "sqlserver.example.com",
                    "database": "mydb",
                    "username": "sa",
                    "password": "pass"
                }
            }
        }
        mock_get.return_value = mock_response

        # Act
        client.get_database_credentials("sqlserver")

        # Assert
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Successfully fetched sqlserver credentials" in call for call in info_calls)


class TestHealthCheck:
    """Test VaultClient.health_check() method"""

    @patch('src.utils.vault_client.requests.get')
    def test_health_check_status_200_returns_true(self, mock_get):
        """Test that status 200 (active) returns True"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        # Act
        result = client.health_check()

        # Assert
        assert result is True
        mock_get.assert_called_once_with(
            "https://vault.example.com/v1/sys/health",
            timeout=5
        )

    @patch('src.utils.vault_client.requests.get')
    def test_health_check_status_429_returns_true(self, mock_get):
        """Test that status 429 (standby) returns True"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 429
        mock_get.return_value = mock_response

        # Act
        result = client.health_check()

        # Assert
        assert result is True

    @patch('src.utils.vault_client.requests.get')
    def test_health_check_status_472_returns_true(self, mock_get):
        """Test that status 472 (data recovery mode) returns True"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 472
        mock_get.return_value = mock_response

        # Act
        result = client.health_check()

        # Assert
        assert result is True

    @patch('src.utils.vault_client.requests.get')
    def test_health_check_status_473_returns_true(self, mock_get):
        """Test that status 473 (performance standby) returns True"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 473
        mock_get.return_value = mock_response

        # Act
        result = client.health_check()

        # Assert
        assert result is True

    @patch('src.utils.vault_client.requests.get')
    def test_health_check_status_501_returns_false(self, mock_get):
        """Test that status 501 (not initialized) returns False"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 501
        mock_get.return_value = mock_response

        # Act
        result = client.health_check()

        # Assert
        assert result is False

    @patch('src.utils.vault_client.requests.get')
    def test_health_check_status_503_returns_false(self, mock_get):
        """Test that status 503 (sealed) returns False"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 503
        mock_get.return_value = mock_response

        # Act
        result = client.health_check()

        # Assert
        assert result is False

    @patch('src.utils.vault_client.requests.get')
    def test_health_check_request_exception_returns_false(self, mock_get):
        """Test that RequestException returns False"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_get.side_effect = requests.RequestException("Connection error")

        # Act
        result = client.health_check()

        # Assert
        assert result is False

    @patch('src.utils.vault_client.logger')
    @patch('src.utils.vault_client.requests.get')
    def test_health_check_exception_is_logged(self, mock_get, mock_logger):
        """Test that health check exception is logged"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_get.side_effect = requests.RequestException("Connection error")

        # Act
        result = client.health_check()

        # Assert
        assert result is False
        mock_logger.error.assert_called_once()
        error_call = mock_logger.error.call_args[0][0]
        assert "Vault health check failed" in error_call

    @patch('src.utils.vault_client.requests.get')
    def test_health_check_timeout_is_5_seconds(self, mock_get):
        """Test that health check uses 5 second timeout"""
        # Arrange
        client = VaultClient(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        # Act
        client.health_check()

        # Assert
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs['timeout'] == 5


class TestGetCredentialsFromVault:
    """Test get_credentials_from_vault() convenience function"""

    @patch('src.utils.vault_client.VaultClient')
    def test_get_credentials_from_vault_creates_client(self, mock_vault_client_class):
        """Test that convenience function creates VaultClient instance"""
        # Arrange
        mock_client = Mock()
        mock_client.get_database_credentials.return_value = {
            "server": "test.com",
            "database": "db",
            "username": "user",
            "password": "pass"
        }
        mock_vault_client_class.return_value = mock_client

        # Act
        result = get_credentials_from_vault(
            database_type="sqlserver",
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )

        # Assert
        mock_vault_client_class.assert_called_once_with(
            vault_addr="https://vault.example.com",
            vault_token="test-token"
        )
        mock_client.get_database_credentials.assert_called_once_with("sqlserver")
        assert result == {
            "server": "test.com",
            "database": "db",
            "username": "user",
            "password": "pass"
        }

    @patch('src.utils.vault_client.VaultClient')
    def test_get_credentials_from_vault_uses_env_vars(self, mock_vault_client_class):
        """Test that convenience function uses env vars when not provided"""
        # Arrange
        mock_client = Mock()
        mock_client.get_database_credentials.return_value = {
            "host": "pg.example.com",
            "database": "db",
            "username": "postgres",
            "password": "pass"
        }
        mock_vault_client_class.return_value = mock_client

        # Act
        result = get_credentials_from_vault(database_type="postgresql")

        # Assert
        mock_vault_client_class.assert_called_once_with(
            vault_addr=None,
            vault_token=None
        )
        mock_client.get_database_credentials.assert_called_once_with("postgresql")

    @patch('src.utils.vault_client.VaultClient')
    def test_get_credentials_from_vault_propagates_errors(self, mock_vault_client_class):
        """Test that convenience function propagates ValueErrors"""
        # Arrange
        mock_vault_client_class.side_effect = ValueError("Vault address not provided")

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            get_credentials_from_vault(database_type="sqlserver")

        assert "Vault address not provided" in str(exc_info.value)
