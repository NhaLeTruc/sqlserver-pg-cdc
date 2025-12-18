# Connector Configuration Guide

## Overview

This guide explains the parameterized configuration system for Kafka Connect connectors in the SQL Server to PostgreSQL CDC pipeline. The system uses a template-based approach that supports both environment variable substitution and HashiCorp Vault secret placeholders.

### Key Features

- **Template-Based Configuration**: All connector configs are generated from templates
- **Environment Variable Substitution**: Data flow parameters (batch sizes, parallelism, etc.) from `.env` file
- **Vault Integration**: Database credentials securely fetched from HashiCorp Vault
- **Two-Phase Substitution**: Environment variables substituted first, Vault secrets during deployment
- **No Secrets in Git**: Runtime configs are gitignored, only templates are version-controlled
- **Backward Compatible**: Falls back to legacy configs if generation fails

### Architecture

```
┌─────────────────┐
│  .env (root)    │  ← User-configurable data flow parameters
└────────┬────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────┐
│  Template Files (docker/configs/templates/)              │
│  - Contains ${ENV_VAR} and ${vault:secret/path:key}     │
└────────┬─────────────────────────────────────────────────┘
         │
         │  generate-connector-configs.sh
         │  (Phase 1: Substitute ENV_VARs, preserve Vault)
         ▼
┌──────────────────────────────────────────────────────────┐
│  Runtime Configs (docker/configs/runtime/)               │
│  - ENV_VARs replaced, ${vault:...} placeholders intact  │
└────────┬─────────────────────────────────────────────────┘
         │
         │  deploy-with-vault.sh
         │  (Phase 2: Fetch secrets, substitute Vault)
         ▼
┌──────────────────────────────────────────────────────────┐
│  Deployment Configs (/tmp/)                              │
│  - All values substituted (credentials + data flow)      │
└────────┬─────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────┐
│  Kafka Connect                                           │
└──────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Copy and Customize Configuration

```bash
# Copy example to .env at project root
cp .env.example .env

# Edit parameters as needed
vi .env
```

### 2. Generate Connector Configurations

```bash
# Generate runtime configs from templates
make generate-configs

# Verify generated configs
make validate-configs

# View generated configs (optional)
make show-config
```

### 3. Deploy Connectors

```bash
# Deploy both connectors (auto-generates configs if needed)
make deploy

# Verify connector status
make verify-connectors
```

## Configuration Reference

### Debezium Source Connector Parameters

#### Task Parallelism

| Parameter | Default | Valid Range | Description |
|-----------|---------|-------------|-------------|
| `DEBEZIUM_TASKS_MAX` | `1` | `1` only | **MUST be 1** for SQL Server CDC. Multiple tasks cause data inconsistency due to sequential transaction log reading. |

**Why tasks.max=1 is Mandatory:**
- SQL Server CDC reads the database transaction log sequentially
- Multiple tasks would cause out-of-order event processing
- Transaction boundaries would be violated
- Data inconsistency and duplicates would occur
- This is a Debezium/SQL Server CDC limitation, not a configuration choice

#### Batch Processing

| Parameter | Default | Valid Range | Description |
|-----------|---------|-------------|-------------|
| `DEBEZIUM_MAX_BATCH_SIZE` | `2048` | 100-8192 | Number of records batched together per processing cycle. Higher = better throughput, more memory. |
| `DEBEZIUM_MAX_QUEUE_SIZE` | `8192` | 1024-16384 | Internal queue size for buffering change events. Higher = better burst handling, more memory. |
| `DEBEZIUM_POLL_INTERVAL_MS` | `500` | 100-5000 | How often (ms) to poll transaction log. Lower = lower latency, higher CPU usage. |

#### Database Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `DEBEZIUM_DATABASE_NAMES` | `warehouse_source` | Comma-separated list of databases to capture |
| `DEBEZIUM_TABLE_INCLUDE_LIST` | `dbo.customers,dbo.orders,...` | Comma-separated list of tables to capture (schema.table) |

#### Snapshot Configuration

| Parameter | Default | Options | Description |
|-----------|---------|---------|-------------|
| `DEBEZIUM_SNAPSHOT_MODE` | `schema_only` | `initial`, `schema_only`, `initial_only`, `never` | Snapshot behavior on connector start |
| `DEBEZIUM_SNAPSHOT_LOCKING_MODE` | `none` | `minimal`, `exclusive`, `none` | Table locking during snapshot |

**Snapshot Mode Options:**
- `initial`: Full snapshot then CDC (slowest startup, complete data)
- `schema_only`: Skip snapshot, CDC only (fastest, requires pre-existing CDC)
- `initial_only`: Snapshot only, no CDC (one-time data migration)
- `never`: No snapshot (use for active CDC tables)

#### Topic Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `DEBEZIUM_TOPIC_PREFIX` | `sqlserver` | Prefix for Kafka topics |
| `DEBEZIUM_SCHEMA_HISTORY_TOPIC` | `schema-changes.warehouse_source` | Topic for schema change history |

### PostgreSQL Sink Connector Parameters

#### Task Parallelism

| Parameter | Default | Valid Range | Description |
|-----------|---------|-------------|-------------|
| `SINK_TASKS_MAX` | `3` | 1-10 | Number of parallel tasks. Higher = better multi-table throughput. |

**Tuning Guidelines:**
- Low volume (1-2 tables): `1`
- Balanced (3-5 tables): `3`
- High volume (5+ tables): `5-10`
- Should be ≤ number of topics being consumed

#### Batch Processing

| Parameter | Default | Valid Range | Description |
|-----------|---------|-------------|-------------|
| `SINK_BATCH_SIZE` | `3000` | 100-5000 | Records batched before writing to PostgreSQL. Higher = better throughput, higher latency. |

#### Connection Management

| Parameter | Default | Valid Range | Description |
|-----------|---------|-------------|-------------|
| `SINK_CONNECTION_POOL_SIZE` | `10` | 1-20 | JDBC connection pool size. Should be ≥ SINK_TASKS_MAX. |
| `SINK_CONNECTION_ATTEMPTS` | `10` | 1-100 | Number of retry attempts for failed connections. |
| `SINK_CONNECTION_BACKOFF_MS` | `5000` | 1000-30000 | Backoff delay (ms) between connection retry attempts. |

**Connection Pool Sizing:**
- Recommended: `SINK_TASKS_MAX + buffer`
- Example: 3 tasks → 10 pool size (3 active + 7 buffer)

#### Error Handling

| Parameter | Default | Valid Range | Description |
|-----------|---------|-------------|-------------|
| `SINK_ERRORS_RETRY_TIMEOUT` | `300000` | 60000-3600000 | Total retry timeout (ms) for failed operations. Default = 5 minutes. |
| `SINK_ERRORS_RETRY_DELAY_MAX_MS` | `60000` | 10000-300000 | Maximum delay (ms) between retries. Uses exponential backoff up to this max. |

#### Topic Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SINK_TOPICS` | `sqlserver.warehouse_source.dbo.customers,...` | Comma-separated list of Kafka topics to consume |
| `SINK_DLQ_TOPIC_NAME` | `dlq-postgresql-sink` | Dead letter queue topic for failed messages |
| `SINK_DLQ_REPLICATION_FACTOR` | `1` | DLQ topic replication factor (set to 3 in production) |

### Infrastructure Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SQLSERVER_PORT` | `1433` | SQL Server port |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `warehouse_target` | PostgreSQL database name |
| `KAFKA_BROKER` | `kafka:9092` | Kafka broker address |
| `SCHEMA_REGISTRY_HOST` | `schema-registry` | Schema Registry host |
| `SCHEMA_REGISTRY_PORT` | `8081` | Schema Registry port |

## Performance Tuning

### Tuning Matrices by Scenario

#### Low Latency, Low Volume
**Use Case:** Real-time updates, few records per second

```bash
DEBEZIUM_MAX_BATCH_SIZE=512
DEBEZIUM_MAX_QUEUE_SIZE=2048
DEBEZIUM_POLL_INTERVAL_MS=100
SINK_BATCH_SIZE=1000
SINK_TASKS_MAX=1
```

**Characteristics:**
- Minimal latency (sub-second)
- Low memory footprint
- Higher CPU usage due to frequent polling

#### Balanced (Default)
**Use Case:** General purpose, moderate throughput

```bash
DEBEZIUM_MAX_BATCH_SIZE=2048
DEBEZIUM_MAX_QUEUE_SIZE=8192
DEBEZIUM_POLL_INTERVAL_MS=500
SINK_BATCH_SIZE=3000
SINK_TASKS_MAX=3
```

**Characteristics:**
- Good balance of latency and throughput
- Moderate memory usage
- Suitable for most production workloads

#### High Throughput
**Use Case:** Bulk data migrations, high-volume changes

```bash
DEBEZIUM_MAX_BATCH_SIZE=4096
DEBEZIUM_MAX_QUEUE_SIZE=16384
DEBEZIUM_POLL_INTERVAL_MS=1000
SINK_BATCH_SIZE=5000
SINK_TASKS_MAX=5
```

**Characteristics:**
- Maximum throughput (10k+ records/sec)
- Higher memory usage
- Acceptable latency (1-2 seconds)

#### Bursty Workload
**Use Case:** Intermittent high-volume bursts

```bash
DEBEZIUM_MAX_BATCH_SIZE=2048
DEBEZIUM_MAX_QUEUE_SIZE=16384  # Larger queue for burst absorption
DEBEZIUM_POLL_INTERVAL_MS=500
SINK_BATCH_SIZE=3000
SINK_TASKS_MAX=3
```

**Characteristics:**
- Large queue absorbs bursts without dropping events
- Balanced steady-state performance
- Good for overnight batch jobs + real-time CDC

### Tuning Workflow

1. **Start with defaults** (Balanced scenario)
2. **Monitor metrics**:
   - Kafka consumer lag (`make kafka-lag`)
   - Connector task throughput
   - Database query performance
3. **Adjust based on bottleneck**:
   - High lag → Increase batch sizes, add sink tasks
   - High latency → Decrease poll interval, reduce batch sizes
   - High memory → Decrease queue sizes
   - High CPU → Increase poll interval

## Advanced Usage

### Multiple Environments

Use different `.env` files for different environments:

```bash
# Development
cp .env.example .env.dev
# Edit .env.dev with dev settings

# Production
cp .env.example .env.prod
# Edit .env.prod with production settings

# Generate configs for specific environment
cp .env.dev .env
make generate-configs
```

### CI/CD Integration

```yaml
# Example GitHub Actions workflow
- name: Generate connector configs
  run: |
    cp .env.example .env
    # Inject environment-specific values
    sed -i 's/DEBEZIUM_MAX_BATCH_SIZE=.*/DEBEZIUM_MAX_BATCH_SIZE=4096/' .env
    make generate-configs

- name: Validate configs
  run: make validate-configs

- name: Deploy connectors
  run: make deploy
```

### Custom Transformations

To add custom transformations, edit the template files directly:

```bash
# Edit template
vi docker/configs/templates/kafka-connect/postgresql-sink.json.template

# Add your transformation
# Example: Add timestamp conversion
"transforms": "unwrap,route,convertTimestamp",
"transforms.convertTimestamp.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
...

# Regenerate configs
make generate-configs
make deploy
```

## Troubleshooting

### Config Generation Fails

**Problem:** `make generate-configs` fails with validation errors

**Solutions:**

1. **Check for missing environment variables:**
   ```bash
   # Review .env file
   cat .env

   # Check which vars are missing
   ./scripts/bash/generate-connector-configs.sh
   ```

2. **Validate .env syntax:**
   ```bash
   # Ensure no syntax errors (trailing spaces, quotes, etc.)
   cat -A .env | grep -n '\$'
   ```

3. **Check template syntax:**
   ```bash
   # Validate template JSON (with placeholders intact)
   jq empty docker/configs/templates/debezium/sqlserver-source.json.template || echo "Invalid JSON"
   ```

### Vault Placeholders Not Preserved

**Problem:** Generated configs don't contain `${vault:...}` placeholders

**Solution:**

Check the generate-connector-configs.sh script is properly handling Vault placeholders:

```bash
# Verify Vault placeholders in generated config
grep 'vault:' docker/configs/runtime/debezium/sqlserver-source.json

# If missing, regenerate
rm -rf docker/configs/runtime/
make generate-configs
```

### Deployment Uses Legacy Configs

**Problem:** Deploy script falls back to legacy configs

**Solution:**

1. **Ensure runtime configs exist:**
   ```bash
   ls -la docker/configs/runtime/debezium/
   ls -la docker/configs/runtime/kafka-connect/
   ```

2. **Manually generate if missing:**
   ```bash
   make generate-configs
   make deploy
   ```

3. **Check for generation errors:**
   ```bash
   ./scripts/bash/generate-connector-configs.sh 2>&1 | tee config-gen.log
   ```

### Connector Fails to Start

**Problem:** Connector deployed but fails to start

**Solutions:**

1. **Check connector status:**
   ```bash
   make connector-status
   ```

2. **View connector logs:**
   ```bash
   curl -s http://localhost:8083/connectors/sqlserver-cdc-source/status | jq '.tasks[].trace'
   ```

3. **Common issues:**
   - Invalid batch size (too large): Reduce `DEBEZIUM_MAX_BATCH_SIZE`
   - Database connection fails: Check Vault secrets
   - Topic not found: Verify Kafka is running

### High Replication Lag

**Problem:** Consumer lag increases over time

**Solutions:**

1. **Check current lag:**
   ```bash
   make kafka-lag
   ```

2. **Increase sink throughput:**
   ```bash
   # Edit .env
   SINK_BATCH_SIZE=5000
   SINK_TASKS_MAX=5

   # Regenerate and redeploy
   make generate-configs
   make connector-restart
   ```

3. **Monitor database performance:**
   ```bash
   # Check PostgreSQL query times
   make db-postgres
   SELECT * FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 10;
   ```

### Memory Issues

**Problem:** Kafka Connect container OOM errors

**Solutions:**

1. **Reduce queue sizes:**
   ```bash
   DEBEZIUM_MAX_QUEUE_SIZE=4096  # Reduce from 8192
   DEBEZIUM_MAX_BATCH_SIZE=1024  # Reduce from 2048
   ```

2. **Increase container memory:**
   ```yaml
   # In docker-compose.yml
   kafka-connect:
     deploy:
       resources:
         limits:
           memory: 4G  # Increase from default
   ```

## Migration Guide

### Migrating from Legacy Configs

If you're using the old hardcoded config files, follow these steps:

1. **Review current configuration:**
   ```bash
   jq . docker/configs/debezium/sqlserver-source.json
   jq . docker/configs/kafka-connect/postgresql-sink.json
   ```

2. **Create .env with current values:**
   ```bash
   cp .env.example .env

   # Copy hardcoded values from legacy configs to .env
   # Example: If old config has "max.batch.size": "4096"
   # Set in .env: DEBEZIUM_MAX_BATCH_SIZE=4096
   ```

3. **Generate and validate:**
   ```bash
   make generate-configs
   make validate-configs

   # Compare generated vs legacy
   diff <(jq -S . docker/configs/debezium/sqlserver-source.json) \
        <(jq -S . docker/configs/runtime/debezium/sqlserver-source.json)
   ```

4. **Test in development first:**
   ```bash
   # Deploy to dev environment
   make deploy
   make verify-connectors

   # Test data flow
   make test
   ```

5. **Rollback if needed:**
   ```bash
   # Legacy configs still work as fallback
   rm -rf docker/configs/runtime/
   make deploy  # Will use legacy configs
   ```

## Best Practices

### Configuration Management

1. **Never commit .env files** - They may contain sensitive data
2. **Use .env.example as template** - Keep it updated with all parameters
3. **Document custom values** - Add comments to .env explaining non-obvious settings
4. **Version control templates** - Templates should be in Git, runtime configs should not
5. **Validate before deploying** - Always run `make validate-configs`

### Performance Optimization

1. **Start conservative** - Use default values initially
2. **Monitor before tuning** - Establish baseline metrics
3. **Change one parameter at a time** - Easier to identify impact
4. **Test under load** - Performance tuning should use realistic workloads
5. **Document changes** - Keep notes on what works for your use case

### Security

1. **Use Vault for credentials** - Never hardcode passwords
2. **Rotate secrets regularly** - Update Vault secrets, redeploy
3. **Limit access to .env** - Only authorized users should edit
4. **Review configs before deploy** - Check for accidentally exposed secrets

### Disaster Recovery

1. **Backup .env file** - Store encrypted in secure location
2. **Document custom templates** - If you modify templates, document why
3. **Test recovery process** - Practice recreating configs from .env.example
4. **Monitor connector health** - Set up alerts for connector failures

## Reference

### File Locations

```
sqlserver-pg-cdc/
├── .env                                      # User configuration (gitignored)
├── .env.example                              # Configuration template
├── docker/
│   └── configs/
│       ├── templates/                        # Config templates (in Git)
│       │   ├── debezium/
│       │   │   └── sqlserver-source.json.template
│       │   └── kafka-connect/
│       │       └── postgresql-sink.json.template
│       ├── runtime/                          # Generated configs (gitignored)
│       │   ├── debezium/
│       │   │   └── sqlserver-source.json
│       │   └── kafka-connect/
│       │       └── postgresql-sink.json
│       ├── debezium/                         # Legacy (deprecated)
│       │   └── sqlserver-source.json
│       └── kafka-connect/                    # Legacy (deprecated)
│           └── postgresql-sink.json
└── scripts/bash/
    ├── generate-connector-configs.sh         # Config generation script
    └── deploy-with-vault.sh                  # Deployment script
```

### Make Targets

| Target | Description |
|--------|-------------|
| `make generate-configs` | Generate runtime configs from templates |
| `make validate-configs` | Validate generated JSON syntax |
| `make show-config` | Display generated configurations |
| `make clean-configs` | Remove generated runtime configs |
| `make deploy` | Generate configs and deploy connectors |
| `make connector-status` | Show connector status |
| `make connector-restart` | Restart all connectors |

### Related Documentation

- [Vault Integration Guide](vault-integration.md)
- [Operations Guide](operations.md)
- [Troubleshooting Guide](troubleshooting.md)
- [Architecture Overview](architecture.md)
- [Debezium SQL Server Connector Docs](https://debezium.io/documentation/reference/connectors/sqlserver.html)
- [Confluent JDBC Sink Connector Docs](https://docs.confluent.io/kafka-connect-jdbc/current/)

## FAQ

### Q: Why can't I increase Debezium tasks.max?

**A:** SQL Server CDC reads the transaction log sequentially. Multiple tasks would cause out-of-order events, duplicates, and broken transaction boundaries. This is a fundamental limitation of SQL Server CDC, not this system.

### Q: What's the difference between templates and runtime configs?

**A:** Templates contain placeholders (`${ENV_VAR}` and `${vault:...}`). Runtime configs have environment variables substituted but Vault placeholders intact. Deployment configs have all placeholders substituted.

### Q: Can I use environment variables for credentials instead of Vault?

**A:** Not recommended. The system is designed for Vault credentials. If you absolutely must, you can modify the templates to use `${SQLSERVER_PASSWORD}` instead of `${vault:...}`, but this is insecure.

### Q: How do I add a new table to CDC?

**A:** Edit `.env` and add the table to `DEBEZIUM_TABLE_INCLUDE_LIST` and `SINK_TOPICS`, then run `make generate-configs && make deploy`.

### Q: Can I use different configurations per connector?

**A:** Yes. You can create separate template files and reference different env vars. Or use environment-specific .env files.

### Q: What happens if config generation fails during deployment?

**A:** The deploy script automatically falls back to legacy configs in `docker/configs/debezium/` and `docker/configs/kafka-connect/`.

### Q: How do I test config changes without deploying?

**A:** Run `make generate-configs && make show-config` to see the generated configs without deploying them.
