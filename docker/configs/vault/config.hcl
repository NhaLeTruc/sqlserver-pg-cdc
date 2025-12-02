# HashiCorp Vault Configuration for CDC Pipeline
# Development mode configuration (NOT for production use)

storage "file" {
  path = "/vault/data"
}

listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = 1
}

# UI configuration
ui = true

# API address
api_addr = "http://0.0.0.0:8200"

# Cluster address (for HA setup)
cluster_addr = "http://0.0.0.0:8201"

# Logging
log_level = "info"
log_format = "json"

# Disable mlock for development
disable_mlock = true

# Telemetry (optional, for monitoring Vault itself)
telemetry {
  prometheus_retention_time = "30s"
  disable_hostname = true
}

# Default lease duration
default_lease_ttl = "168h"  # 7 days
max_lease_ttl = "720h"      # 30 days
