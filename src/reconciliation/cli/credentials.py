"""
Credential management and logging setup for CLI.

This module handles fetching database credentials from Vault or environment
variables, and configures logging for the CLI application.
"""

import argparse
import logging
import os
import sys

from src.utils.vault_client import VaultClient

logger = logging.getLogger(__name__)


def setup_logging(log_level: str = "INFO") -> None:
    """
    Setup logging configuration

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def get_credentials_from_vault_or_env(args: argparse.Namespace) -> tuple:
    """
    Get database credentials from Vault or environment/args

    Args:
        args: Parsed command-line arguments

    Returns:
        Tuple of (source_config, target_config)
    """
    if args.use_vault:
        try:
            vault_client = VaultClient()
            source_creds = vault_client.get_database_credentials("sqlserver")
            target_creds = vault_client.get_database_credentials("postgresql")

            source_config = {
                "server": source_creds["server"],
                "database": source_creds["database"],
                "username": source_creds["username"],
                "password": source_creds["password"]
            }

            target_config = {
                "host": target_creds["host"],
                "port": target_creds.get("port", 5432),
                "database": target_creds["database"],
                "username": target_creds["username"],
                "password": target_creds["password"]
            }

            logger.info("Successfully fetched credentials from Vault")
        except Exception as e:
            logger.error(f"Failed to fetch credentials from Vault: {e}")
            sys.exit(1)
    else:
        # Get from command-line args or environment variables
        source_config = {
            "server": args.source_server or os.getenv("SQLSERVER_HOST", "localhost"),
            "database": args.source_database or os.getenv("SQLSERVER_DATABASE", "warehouse_source"),
            "username": args.source_user or os.getenv("SQLSERVER_USER", "sa"),
            "password": args.source_password or os.getenv("SQLSERVER_PASSWORD")
        }

        target_config = {
            "host": args.target_host or os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(args.target_port or os.getenv("POSTGRES_PORT", "5432")),
            "database": args.target_database or os.getenv("POSTGRES_DB", "warehouse_target"),
            "username": args.target_user or os.getenv("POSTGRES_USER", "postgres"),
            "password": args.target_password or os.getenv("POSTGRES_PASSWORD")
        }

        # Validate passwords
        if not source_config["password"]:
            logger.error("Source database password not provided")
            sys.exit(1)
        if not target_config["password"]:
            logger.error("Target database password not provided")
            sys.exit(1)

    return source_config, target_config
