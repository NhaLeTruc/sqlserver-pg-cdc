# Vault Policy for Kafka Connect
# Grants read-only access to database credentials

# Allow reading database credentials from KV v2 secrets engine
path "secret/data/database/*" {
  capabilities = ["read"]
}

# Allow listing secrets (for discovery)
path "secret/metadata/database/*" {
  capabilities = ["list"]
}

# Deny all other operations
path "secret/*" {
  capabilities = ["deny"]
}

# Allow token self-renewal (for long-running connectors)
path "auth/token/renew-self" {
  capabilities = ["update"]
}

# Allow token lookup (for validation)
path "auth/token/lookup-self" {
  capabilities = ["read"]
}
