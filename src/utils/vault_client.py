"""
HashiCorp Vault client for fetching database credentials

This module provides a simple interface to fetch database credentials
from HashiCorp Vault KV v2 secrets engine.
"""

import os
import requests
from typing import Dict, Any, Optional
import logging
import re


logger = logging.getLogger(__name__)


class VaultClient:
    """
    HashiCorp Vault client for secrets management

    This client uses the KV v2 secrets engine to fetch database credentials.
    """

    def __init__(
        self,
        vault_addr: Optional[str] = None,
        vault_token: Optional[str] = None,
        namespace: Optional[str] = None
    ):
        """
        Initialize Vault client

        Args:
            vault_addr: Vault server address (default: from VAULT_ADDR env var)
            vault_token: Vault authentication token (default: from VAULT_TOKEN env var)
            namespace: Vault namespace (optional, for Vault Enterprise)

        Raises:
            ValueError: If vault_addr or vault_token are not provided
        """
        self.vault_addr = vault_addr or os.getenv("VAULT_ADDR")
        self.vault_token = vault_token or os.getenv("VAULT_TOKEN")
        self.namespace = namespace

        if not self.vault_addr:
            raise ValueError(
                "Vault address not provided. Set VAULT_ADDR environment variable "
                "or pass vault_addr parameter."
            )

        if not self.vault_token:
            raise ValueError(
                "Vault token not provided. Set VAULT_TOKEN environment variable "
                "or pass vault_token parameter."
            )

        # Remove trailing slash from vault_addr
        self.vault_addr = self.vault_addr.rstrip("/")

        # Setup headers
        self.headers = {
            "X-Vault-Token": self.vault_token,
            "Content-Type": "application/json"
        }

        if self.namespace:
            self.headers["X-Vault-Namespace"] = self.namespace

        logger.info(f"Initialized Vault client for {self.vault_addr}")

    def get_secret(self, secret_path: str) -> Dict[str, Any]:
        """
        Fetch secret from Vault KV v2 secrets engine

        Args:
            secret_path: Path to secret (e.g., "secret/database/sqlserver")

        Returns:
            Dictionary containing secret data

        Raises:
            ValueError: If secret_path is invalid
            requests.RequestException: If Vault request fails
            KeyError: If secret data is not in expected format
        """
        # Validate secret_path to prevent path traversal attacks
        if not secret_path or not isinstance(secret_path, str):
            raise ValueError("secret_path must be a non-empty string")

        # Disallow path traversal attempts
        if '..' in secret_path or secret_path.startswith('//'):
            raise ValueError(
                f"Invalid secret_path: {secret_path}. "
                "Path traversal attempts are not allowed."
            )

        # Allow only safe characters: alphanumeric, slash, underscore, hyphen
        if not re.match(r'^[a-zA-Z0-9/_-]+$', secret_path):
            raise ValueError(
                f"Invalid secret_path: {secret_path}. "
                "Only alphanumeric characters, slashes, underscores, and hyphens are allowed."
            )

        # KV v2 requires /data/ in the path
        if "/data/" not in secret_path:
            # Insert /data/ after the mount point
            parts = secret_path.split("/", 1)
            if len(parts) == 2:
                secret_path = f"{parts[0]}/data/{parts[1]}"
            else:
                secret_path = f"{secret_path}/data"

        url = f"{self.vault_addr}/v1/{secret_path}"

        logger.debug(f"Fetching secret from: {url}")

        response = requests.get(url, headers=self.headers, timeout=10)

        if response.status_code == 404:
            raise ValueError(f"Secret not found at path: {secret_path}")

        response.raise_for_status()

        data = response.json()

        # Extract secret data from KV v2 response
        secret_data = data.get("data", {}).get("data", {})

        if not secret_data:
            raise ValueError(f"No data found in secret at path: {secret_path}")

        logger.debug(f"Successfully fetched secret from {secret_path}")

        return secret_data

    def get_database_credentials(
        self,
        database_type: str,
        database_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch database credentials from Vault

        Args:
            database_type: Type of database ("sqlserver" or "postgresql")
            database_name: Optional database name (default: uses database_type)

        Returns:
            Dictionary containing database credentials:
            - For SQL Server: server, database, username, password
            - For PostgreSQL: host, port, database, username, password

        Raises:
            ValueError: If database_type is invalid or credentials not found
        """
        # Validate database_type input to prevent path traversal
        if not database_type or not isinstance(database_type, str):
            raise ValueError("database_type must be a non-empty string")

        # Allow only alphanumeric characters and underscores
        if not re.match(r'^[a-zA-Z0-9_]+$', database_type):
            raise ValueError(
                f"Invalid database_type: {database_type}. "
                "Only alphanumeric characters and underscores are allowed."
            )

        if database_type not in ["sqlserver", "postgresql"]:
            raise ValueError(
                f"Unsupported database_type: {database_type}. "
                "Must be 'sqlserver' or 'postgresql'."
            )

        secret_path = f"secret/database/{database_type}"

        secret_data = self.get_secret(secret_path)

        # Validate required fields
        if database_type == "sqlserver":
            required_fields = ["server", "database", "username", "password"]
        else:  # postgresql
            required_fields = ["host", "database", "username", "password"]

        missing_fields = [
            field for field in required_fields if field not in secret_data
        ]

        if missing_fields:
            raise ValueError(
                f"Missing required fields in secret: {', '.join(missing_fields)}"
            )

        # Add default port if not specified
        if database_type == "postgresql" and "port" not in secret_data:
            secret_data["port"] = 5432

        logger.info(f"Successfully fetched {database_type} credentials from Vault")

        return secret_data

    def health_check(self) -> bool:
        """
        Check if Vault is accessible and healthy

        Returns:
            True if Vault is healthy, False otherwise
        """
        url = f"{self.vault_addr}/v1/sys/health"

        try:
            response = requests.get(url, timeout=5)
            # Vault health endpoint returns different status codes based on state
            # 200 = initialized, unsealed, and active
            # 429 = unsealed and standby
            # 472 = data recovery mode replication secondary and active
            # 473 = performance standby
            # 501 = not initialized
            # 503 = sealed
            return response.status_code in [200, 429, 472, 473]
        except requests.RequestException as e:
            logger.error(f"Vault health check failed: {e}")
            return False


def get_credentials_from_vault(
    database_type: str,
    vault_addr: Optional[str] = None,
    vault_token: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convenience function to fetch database credentials from Vault

    Args:
        database_type: Type of database ("sqlserver" or "postgresql")
        vault_addr: Vault server address (optional, uses env var if not provided)
        vault_token: Vault token (optional, uses env var if not provided)

    Returns:
        Dictionary containing database credentials

    Raises:
        ValueError: If credentials cannot be fetched
    """
    client = VaultClient(vault_addr=vault_addr, vault_token=vault_token)
    return client.get_database_credentials(database_type)
