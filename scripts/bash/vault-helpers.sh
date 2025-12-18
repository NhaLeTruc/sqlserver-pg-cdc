#!/bin/bash
# Vault Helper Functions
# Source this file to get helper functions for fetching secrets from Vault

# Auto-source .env from project root
# This ensures environment variables are available when vault-helpers is loaded
source_project_env() {
    local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local project_root="$(cd "$script_dir/../.." && pwd)"
    local env_file="$project_root/.env"

    if [ -f "$env_file" ]; then
        # Silently load environment variables
        set -a  # automatically export all variables
        # shellcheck source=/dev/null
        source "$env_file" || true  # Don't fail if .env has issues
        set +a
    fi
}

# Auto-source when this helper is loaded (but don't fail)
source_project_env || true

# Set error handling after sourcing
set -euo pipefail

# Vault configuration
VAULT_ADDR="${VAULT_ADDR:-http://localhost:8200}"
VAULT_TOKEN="${VAULT_TOKEN:-dev-root-token}"

# Function to check if Vault is accessible
vault_is_ready() {
    docker exec cdc-vault vault status > /dev/null 2>&1
    return $?
}

# Function to get a specific secret value from Vault
# Usage: vault_get_secret <path> <key>
# Example: vault_get_secret secret/database sqlserver_password
vault_get_secret() {
    local secret_path="$1"
    local secret_key="$2"

    if ! vault_is_ready; then
        echo "ERROR: Vault is not accessible" >&2
        return 1
    fi

    local value=$(docker exec -e VAULT_TOKEN="$VAULT_TOKEN" cdc-vault \
        vault kv get -format=json "$secret_path" | \
        jq -r ".data.data.${secret_key}")

    if [ "$value" == "null" ] || [ -z "$value" ]; then
        echo "ERROR: Secret ${secret_path}#${secret_key} not found" >&2
        return 1
    fi

    echo "$value"
}

# Function to load all database secrets into environment variables
# Usage: load_database_secrets
load_database_secrets() {
    if ! vault_is_ready; then
        echo "ERROR: Vault is not accessible. Using default values from environment." >&2
        return 1
    fi

    echo "Loading database secrets from Vault..." >&2

    # SQL Server credentials
    export SQLSERVER_HOST=$(vault_get_secret secret/database sqlserver_host)
    export SQLSERVER_USER=$(vault_get_secret secret/database sqlserver_user)
    export SQLSERVER_PASSWORD=$(vault_get_secret secret/database sqlserver_password)

    # PostgreSQL credentials
    export POSTGRES_HOST=$(vault_get_secret secret/database postgres_host)
    export POSTGRES_USER=$(vault_get_secret secret/database postgres_user)
    export POSTGRES_PASSWORD=$(vault_get_secret secret/database postgres_password)

    echo "✓ Database secrets loaded from Vault" >&2
}

# Function to export secrets for use in other scripts
# Usage: export_database_secrets
export_database_secrets() {
    load_database_secrets

    # Also export common database names
    export SQLSERVER_DB="${SQLSERVER_DB:-warehouse_source}"
    export POSTGRES_DB="${POSTGRES_DB:-warehouse_target}"
    export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
    export SQLSERVER_PORT="${SQLSERVER_PORT:-1433}"
}

# Function to validate required secrets are available
validate_secrets() {
    local required_vars=(
        "SQLSERVER_HOST"
        "SQLSERVER_USER"
        "SQLSERVER_PASSWORD"
        "POSTGRES_HOST"
        "POSTGRES_USER"
        "POSTGRES_PASSWORD"
    )

    local missing=0
    for var in "${required_vars[@]}"; do
        if [ -z "${!var:-}" ]; then
            echo "ERROR: Required variable $var is not set" >&2
            missing=1
        fi
    done

    return $missing
}
