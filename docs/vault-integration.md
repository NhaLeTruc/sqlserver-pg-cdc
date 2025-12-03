# Vault Integration Guide

This guide explains how to use HashiCorp Vault to securely manage credentials for the CDC pipeline instead of hardcoding them in configuration files.

## Overview

The CDC pipeline can fetch credentials from Vault in two ways:

1. **Runtime Substitution** (Recommended): Scripts fetch secrets from Vault at runtime and inject them into configs
2. **Vault Config Provider** (Advanced): Kafka Connect natively reads from Vault using config provider

This guide focuses on Runtime Substitution as it's more reliable and doesn't require additional Kafka Connect plugins.

## Architecture

```
Vault (Dev Mode)
    ↓
Scripts fetch secrets
    ↓
Substitute into configs/commands
    ↓
Deploy to Kafka Connect / Execute commands
```

## Prerequisites

- Vault service running in Docker (`cdc-vault` container)
- Vault initialized with secrets (run `./scripts/bash/vault-init.sh`)
- `jq` installed for JSON parsing

## Stored Secrets

The following secrets are stored in Vault at path `secret/database`:

| Key | Description | Example Value |
|-----|-------------|---------------|
| `sqlserver_host` | SQL Server hostname | `sqlserver` |
| `sqlserver_user` | SQL Server username | `sa` |
| `sqlserver_password` | SQL Server password | `YourStrong!Passw0rd` |
| `postgres_host` | PostgreSQL hostname | `postgres` |
| `postgres_user` | PostgreSQL username | `postgres` |
| `postgres_password` | PostgreSQL password | `postgres_secure_password` |

## Vault Helper Functions

The `scripts/bash/vault-helpers.sh` script provides reusable functions:

### `vault_is_ready()`
Check if Vault is accessible.

```bash
if vault_is_ready; then
    echo "Vault is ready"
fi
```

### `vault_get_secret <path> <key>`
Fetch a specific secret value.

```bash
PASSWORD=$(vault_get_secret secret/database sqlserver_password)
```

### `export_database_secrets()`
Load all database secrets into environment variables.

```bash
source scripts/bash/vault-helpers.sh
export_database_secrets

echo $SQLSERVER_PASSWORD  # Loaded from Vault
```

## Using Vault with Scripts

### 1. Initialize Databases with Vault Credentials

The initialization scripts (`init-sqlserver.sh` and `init-postgres.sh`) automatically attempt to load credentials from Vault:

```bash
# This will fetch credentials from Vault if available
./scripts/bash/init-sqlserver.sh

# Output:
# Loading credentials from Vault...
# Loading database secrets from Vault...
# ✓ Database secrets loaded from Vault
# Initializing SQL Server database: warehouse_source
# ...
```

**Fallback Behavior**: If Vault is unavailable, scripts fall back to environment variables or defaults.

### 2. Deploy Connectors with Vault Credentials

Use the `deploy-with-vault.sh` script to deploy connectors with credentials fetched from Vault:

```bash
./scripts/bash/deploy-with-vault.sh
```

This script:
1. Loads secrets from Vault
2. Generates temporary connector configs with substituted values
3. Deploys both Debezium and PostgreSQL sink connectors
4. Cleans up temporary files

**Output**:
```
Loading credentials from Vault...
Loading database secrets from Vault...
✓ Database secrets loaded from Vault
✓ All required secrets loaded from Vault

Generating Debezium connector config...
✓ Debezium config generated with Vault credentials

Generating PostgreSQL sink connector config...
✓ PostgreSQL sink config generated with Vault credentials

Deploying connectors...
...
```

### 3. Using Vault Helpers in Custom Scripts

Source the helpers in your own scripts:

```bash
#!/bin/bash
source "$(dirname "$0")/vault-helpers.sh"

# Load secrets
export_database_secrets

# Use secrets
docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
    -Q "SELECT @@VERSION"
```

## Manual Vault Operations

### View All Secrets

```bash
docker exec cdc-vault vault kv get -format=json secret/database | jq
```

### Get Specific Secret

```bash
docker exec cdc-vault vault kv get -field=sqlserver_password secret/database
```

### Update a Secret

```bash
docker exec cdc-vault vault kv put secret/database \
    sqlserver_password="NewPassword123!"
```

### Add New Secret

```bash
docker exec cdc-vault vault kv patch secret/database \
    new_key="new_value"
```

## Environment Variables

Scripts use these environment variables (in priority order):

1. **Loaded from Vault** (if available)
2. **Environment variables** (if set)
3. **Default values** (hardcoded fallback)

### Vault Configuration

```bash
export VAULT_ADDR="http://localhost:8200"
export VAULT_TOKEN="dev-root-token"
```

### Database Configuration

```bash
# SQL Server
export SQLSERVER_HOST="sqlserver"
export SQLSERVER_USER="sa"
export SQLSERVER_PASSWORD="YourStrong!Passw0rd"
export SQLSERVER_DB="warehouse_source"
export SQLSERVER_PORT="1433"

# PostgreSQL
export POSTGRES_HOST="postgres"
export POSTGRES_USER="postgres"
export POSTGRES_PASSWORD="postgres_secure_password"
export POSTGRES_DB="warehouse_target"
export POSTGRES_PORT="5432"
```

## Vault Config Provider (Advanced)

For production use, configure Kafka Connect to use the Vault Config Provider plugin:

### 1. Install Vault Config Provider Plugin

Add to `docker-compose.yml`:

```yaml
kafka-connect:
  command:
    - bash
    - -c
    - |
      # Install Vault config provider
      confluent-hub install --no-prompt jcustenborder/kafka-config-provider-vault:0.1.2

      /etc/confluent/docker/run
  environment:
    CONNECT_CONFIG_PROVIDERS: 'vault'
    CONNECT_CONFIG_PROVIDERS_VAULT_CLASS: 'com.github.jcustenborder.kafka.config.vault.VaultConfigProvider'
    CONNECT_CONFIG_PROVIDERS_VAULT_PARAM_VAULT_ADDR: 'http://vault:8200'
    CONNECT_CONFIG_PROVIDERS_VAULT_PARAM_VAULT_TOKEN: 'dev-root-token'
```

### 2. Use Vault Placeholders in Configs

```json
{
  "database.password": "${vault:secret/database:sqlserver_password}"
}
```

### 3. Generate Vault-Enabled Configs

```bash
./scripts/bash/generate-connector-configs.sh
```

This creates `*-vault.json` files with Vault placeholders.

## Security Best Practices

### Development

- ✅ Use Vault dev mode for local development
- ✅ Commit Vault placeholder configs (e.g., `*-vault.json`)
- ❌ Never commit files with actual credentials
- ❌ Don't use dev root token in production

### Production

- ✅ Use Vault in production mode with TLS
- ✅ Use AppRole or Kubernetes auth for Kafka Connect
- ✅ Rotate credentials regularly
- ✅ Enable Vault audit logging
- ✅ Use least-privilege policies
- ❌ Never use dev mode in production
- ❌ Don't expose Vault directly to the internet

### Vault Production Setup

```bash
# Initialize Vault in production mode
vault operator init

# Unseal Vault (requires 3 of 5 keys by default)
vault operator unseal <key1>
vault operator unseal <key2>
vault operator unseal <key3>

# Create AppRole for Kafka Connect
vault auth enable approle

vault write auth/approle/role/kafka-connect \
    token_policies="kafka-connect" \
    token_ttl=1h \
    token_max_ttl=4h

# Get RoleID and SecretID
vault read auth/approle/role/kafka-connect/role-id
vault write -f auth/approle/role/kafka-connect/secret-id
```

## Troubleshooting

### Vault Not Accessible

**Error**: `ERROR: Vault is not accessible`

**Solution**:
```bash
# Check Vault status
docker ps | grep cdc-vault

# Check Vault health
docker exec cdc-vault vault status

# Restart Vault
docker restart cdc-vault
```

### Secret Not Found

**Error**: `ERROR: Secret secret/database#password not found`

**Solution**:
```bash
# Verify secret exists
docker exec cdc-vault vault kv get secret/database

# Re-initialize secrets
./scripts/bash/vault-init.sh
```

### Permission Denied

**Error**: `permission denied`

**Solution**:
```bash
# Set Vault token
export VAULT_TOKEN="dev-root-token"

# Or pass it to the container
docker exec -e VAULT_TOKEN="dev-root-token" cdc-vault vault kv get secret/database
```

### Scripts Still Use Hardcoded Credentials

**Cause**: Vault helpers not sourced, or Vault unavailable.

**Solution**:
```bash
# Verify Vault is running
docker ps | grep cdc-vault

# Check if script sources vault-helpers.sh
grep "vault-helpers" scripts/bash/init-sqlserver.sh

# Run with verbose output
bash -x ./scripts/bash/init-sqlserver.sh
```

## Migration from Hardcoded Credentials

### Step 1: Backup Current Configs

```bash
cp docker/configs/debezium/sqlserver-source.json{,.bak}
cp docker/configs/kafka-connect/postgresql-sink.json{,.bak}
```

### Step 2: Verify Secrets in Vault

```bash
./scripts/bash/vault-init.sh
docker exec cdc-vault vault kv get secret/database
```

### Step 3: Test with One Script

```bash
# Test init script with Vault
./scripts/bash/init-sqlserver.sh
# Should show: "Loading credentials from Vault..."
```

### Step 4: Deploy with Vault

```bash
# Deploy connectors using Vault
./scripts/bash/deploy-with-vault.sh
```

### Step 5: Verify Everything Works

```bash
# Check connector status
curl -s http://localhost:8083/connectors/sqlserver-cdc-source/status | jq

# Test data flow
docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P YourStrong!Passw0rd -C \
    -d warehouse_source \
    -Q "INSERT INTO dbo.customers (name, email) VALUES ('Vault Test', 'vault@test.com')"

# Verify in PostgreSQL
docker exec cdc-postgres psql -U postgres -d warehouse_target \
    -c "SELECT * FROM customers WHERE name = 'Vault Test'"
```

## Files Reference

### Scripts

| File | Purpose |
|------|---------|
| `scripts/bash/vault-helpers.sh` | Reusable Vault functions |
| `scripts/bash/vault-init.sh` | Initialize Vault with secrets |
| `scripts/bash/deploy-with-vault.sh` | Deploy connectors with Vault |
| `scripts/bash/generate-connector-configs.sh` | Generate Vault-enabled configs |
| `scripts/bash/init-sqlserver.sh` | Initialize SQL Server (Vault-aware) |
| `scripts/bash/init-postgres.sh` | Initialize PostgreSQL (Vault-aware) |

### Configuration Files

| File | Type | Credentials |
|------|------|-------------|
| `docker/configs/debezium/sqlserver-source.json` | Runtime | Hardcoded (fallback) |
| `docker/configs/debezium/sqlserver-source-vault.json` | Template | Vault placeholders |
| `docker/configs/kafka-connect/postgresql-sink.json` | Runtime | Hardcoded (fallback) |
| `docker/configs/kafka-connect/postgresql-sink-vault.json` | Template | Vault placeholders |

## Next Steps

1. **Test Locally**: Use `deploy-with-vault.sh` to verify Vault integration works
2. **Update Documentation**: Document your specific Vault setup for your team
3. **Production Planning**: Plan Vault production deployment with proper auth and policies
4. **CI/CD Integration**: Integrate Vault secrets into your deployment pipeline
5. **Monitoring**: Set up alerts for Vault accessibility and secret expiration

## Additional Resources

- [HashiCorp Vault Documentation](https://www.vaultproject.io/docs)
- [Vault KV Secrets Engine](https://www.vaultproject.io/docs/secrets/kv)
- [Kafka Connect Config Providers](https://docs.confluent.io/platform/current/connect/security.html#externalizing-secrets)
- [Vault AppRole Auth](https://www.vaultproject.io/docs/auth/approle)
