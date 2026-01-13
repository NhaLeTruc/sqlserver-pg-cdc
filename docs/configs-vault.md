# IMPORTANT: Vault Configuration in Connector Files

## Current State

The connector configuration files now use **Vault placeholders**:

- `docker/configs/debezium/sqlserver-source.json`
- `docker/configs/kafka-connect/postgresql-sink.json`

These files contain placeholders like:
```json
"database.password": "${vault:secret/database:sqlserver_password}"
```

## ⚠️ Important: These Files Cannot Be Used Directly

**The connector configs with Vault placeholders will NOT work** with the current Kafka Connect setup because:

1. The Vault Config Provider plugin is **not installed** in Kafka Connect
2. Kafka Connect cannot resolve `${vault:...}` placeholders without the plugin

## How to Deploy Connectors

### ✅ Recommended: Use deploy-with-vault.sh

This script reads the Vault placeholders, fetches actual values from Vault, and deploys:

```bash
./scripts/bash/deploy-with-vault.sh
```

This script:
1. Reads the connector config files with Vault placeholders
2. Fetches the actual secrets from Vault
3. Substitutes the real values
4. Deploys the connectors with actual credentials

### ❌ Do NOT Use deploy-connector.sh Directly

```bash
# This will FAIL because Kafka Connect can't resolve ${vault:...}
./scripts/bash/deploy-connector.sh docker/configs/debezium/sqlserver-source.json
```

**Error you'll see:**
```
Connector validation error: database.password has invalid format
```

## Solution Options

### Option 1: Use deploy-with-vault.sh (Current)

**Status**: ✅ Working now

**Pros**:
- No Kafka Connect plugin needed
- Works immediately
- Secrets fetched at deploy time

**Cons**:
- Must use special deployment script
- Credentials exist in memory during deployment

**Usage**:
```bash
./scripts/bash/deploy-with-vault.sh
```

### Option 2: Install Vault Config Provider Plugin (Future)

**Status**: ❌ Not implemented (plugin not installed)

**Pros**:
- Kafka Connect directly reads from Vault
- Can use config files as-is
- Credentials never in memory/disk
- Auto-refresh on rotation

**Cons**:
- Requires plugin installation
- More complex setup
- Need to configure Kafka Connect environment

**Steps to Enable** (when ready):

1. Update `docker-compose.yml` to install the plugin:
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
    CONNECT_CONFIG_PROVIDERS_VAULT_PARAM_VAULT_TOKEN: '${VAULT_DEV_ROOT_TOKEN_ID:-dev-root-token}'
```

2. Restart Kafka Connect:
```bash
docker-compose -f docker/docker-compose.yml restart kafka-connect
```

3. Then you can use `deploy-connector.sh` directly:
```bash
./scripts/bash/deploy-connector.sh docker/configs/debezium/sqlserver-source.json
```

## File Compatibility Matrix

| File | Vault Placeholders | Works with deploy-connector.sh | Works with deploy-with-vault.sh |
|------|-------------------|--------------------------------|--------------------------------|
| `sqlserver-source.json` | Yes | ❌ No (needs plugin) | ✅ Yes |
| `postgresql-sink.json` | Yes | ❌ No (needs plugin) | ✅ Yes |

## Migration Path

### Phase 1: Current (Vault Placeholders + Runtime Substitution)
- ✅ Config files have Vault placeholders (secure in git)
- ✅ Use `deploy-with-vault.sh` for deployments
- ✅ Credentials never hardcoded in git

### Phase 2: Future (Native Vault Integration)
- ⬜ Install Vault Config Provider plugin
- ⬜ Enable in docker-compose.yml
- ⬜ Use `deploy-connector.sh` directly
- ⬜ Kafka Connect reads directly from Vault

## Quick Reference

### Deploy Connectors (Current Method)
```bash
# This is the ONLY way to deploy with current setup
./scripts/bash/deploy-with-vault.sh
```

### Initialize Databases (Works with or without Vault)
```bash
# These scripts auto-detect Vault and use it if available
./scripts/bash/init-sqlserver.sh
./scripts/bash/init-postgres.sh
```

### Verify Current Setup
```bash
# Check if Vault Config Provider is installed (currently: NO)
docker exec cdc-kafka-connect ls -la /usr/share/confluent-hub-components/ | grep vault

# Check connector configs have Vault placeholders (currently: YES)
grep 'vault:' docker/configs/debezium/sqlserver-source.json
```

## Troubleshooting

### Error: "has invalid format"
**Cause**: Trying to deploy config with `${vault:...}` placeholders but plugin not installed

**Solution**: Use `deploy-with-vault.sh` instead of `deploy-connector.sh`

### Error: "Vault is not accessible"
**Cause**: Vault service not running

**Solution**:
```bash
docker-compose -f docker/docker-compose.yml up -d cdc-vault
sleep 5
docker exec cdc-vault vault status
```

### Error: "Secret not found"
**Cause**: Secrets not initialized in Vault

**Solution**:
```bash
./scripts/bash/vault-init.sh
docker exec cdc-vault vault kv get secret/database
```

## Summary

✅ **Config files now use Vault placeholders** (safe to commit to git)
✅ **Use `deploy-with-vault.sh` to deploy** connectors
❌ **Do NOT use `deploy-connector.sh` directly** (will fail without plugin)
⚠️ **Vault Config Provider plugin is NOT installed** (future enhancement)

The current setup is secure and functional, but requires using the special deployment script. To use the config files directly, you'll need to install and configure the Vault Config Provider plugin in Kafka Connect.
