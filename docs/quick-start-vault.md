# Quick Start: Using Vault for Credentials

This guide shows you how to quickly start using Vault instead of hardcoded credentials.

## TL;DR

```bash
# 1. Initialize Vault with secrets
./scripts/bash/vault-init.sh

# 2. Initialize databases (automatically uses Vault)
./scripts/bash/init-sqlserver.sh
./scripts/bash/init-postgres.sh

# 3. Deploy connectors with Vault credentials
./scripts/bash/deploy-with-vault.sh

# 4. Test data replication
docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P YourStrong!Passw0rd -C \
    -d warehouse_source \
    -Q "INSERT INTO dbo.customers (name, email) VALUES ('Vault User', 'vault@example.com')"

# 5. Verify in PostgreSQL
docker exec cdc-postgres psql -U postgres -d warehouse_target \
    -c "SELECT * FROM customers WHERE name = 'Vault User'"
```

## Comparison: Before vs After

### Before (Hardcoded)

```bash
# Credentials exposed in scripts
SQLSERVER_PASSWORD="YourStrong!Passw0rd"
POSTGRES_PASSWORD="postgres_secure_password"

# Credentials exposed in connector configs
{
  "database.password": "YourStrong!Passw0rd",
  "connection.password": "postgres_secure_password"
}
```

### After (Vault)

```bash
# Scripts fetch from Vault
source vault-helpers.sh
export_database_secrets
# Now $SQLSERVER_PASSWORD is loaded from Vault

# Connector configs generated at runtime
./scripts/bash/deploy-with-vault.sh
# Credentials never stored in files
```

## When to Use What

### Use Vault Runtime Substitution (Current Setup)

✅ **Best for**:
- Development and testing
- Simple deployments
- When you want quick setup
- When you don't want Kafka Connect plugins

**How it works**:
1. Scripts fetch secrets from Vault
2. Generate configs with actual values
3. Deploy to Kafka Connect

### Use Vault Config Provider (Advanced)

✅ **Best for**:
- Production deployments
- When you want Kafka Connect to directly read Vault
- When credentials rotate frequently
- Maximum security (credentials never written to disk)

**How it works**:
1. Kafka Connect has Vault plugin installed
2. Configs use placeholders: `${vault:secret/database:password}`
3. Kafka Connect fetches secrets directly from Vault

**Note**: Currently disabled due to plugin not being installed. See [docs/vault-integration.md](vault-integration.md) for setup.

## Common Commands

### Check Vault Status

```bash
docker exec cdc-vault vault status
```

### View Secrets

```bash
# All secrets
docker exec cdc-vault vault kv get secret/database

# Specific secret
docker exec cdc-vault vault kv get -field=sqlserver_password secret/database
```

### Update Password

```bash
# Update SQL Server password
docker exec cdc-vault vault kv patch secret/database \
    sqlserver_password="NewPassword123!"

# Redeploy connectors with new password
./scripts/bash/deploy-with-vault.sh
```

### Manual Query with Vault Credentials

```bash
# Load secrets
source scripts/bash/vault-helpers.sh
export_database_secrets

# Query SQL Server
docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
    -d warehouse_source \
    -Q "SELECT * FROM dbo.customers"

# Query PostgreSQL
docker exec cdc-postgres psql \
    -U "$POSTGRES_USER" \
    -d warehouse_target \
    -c "SELECT * FROM customers"
```

## Troubleshooting

### "Vault is not accessible"

```bash
# Start Vault
docker-compose -f docker/docker-compose.yml up -d cdc-vault

# Wait for it to be ready
sleep 5

# Verify
docker exec cdc-vault vault status
```

### "Secret not found"

```bash
# Initialize Vault with secrets
./scripts/bash/vault-init.sh

# Verify secrets exist
docker exec cdc-vault vault kv get secret/database
```

### Scripts still use defaults

```bash
# Check if vault-helpers.sh exists
ls -la scripts/bash/vault-helpers.sh

# Run with debug
bash -x ./scripts/bash/init-sqlserver.sh 2>&1 | grep -i vault
```

## Benefits of Using Vault

| Benefit | Without Vault | With Vault |
|---------|---------------|------------|
| **Security** | Credentials in files | Credentials in Vault |
| **Rotation** | Update all files manually | Update once in Vault |
| **Audit** | No audit trail | Full audit log |
| **Access Control** | File permissions | Vault policies |
| **Git Safety** | Risk of committing secrets | Never in git |

## Next Steps

1. ✅ Verify current setup works with Vault
2. ✅ Review [docs/vault-integration.md](vault-integration.md) for details
3. ⬜ Plan Vault production deployment
4. ⬜ Set up credential rotation policy
5. ⬜ Enable Vault audit logging
6. ⬜ Configure proper Vault policies
7. ⬜ (Optional) Install Vault Config Provider plugin

## See Also

- [Vault Integration Guide](vault-integration.md) - Complete documentation
- [CDC Pipeline Setup Learnings](cdc-pipeline-setup-learnings.md) - Troubleshooting guide
- [Vault Init Script](../scripts/bash/vault-init.sh) - Initialization details
