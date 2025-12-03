#!/bin/bash
# Initialize PostgreSQL with target tables matching Debezium CDC schema
# Tables are created with BIGINT for timestamps (Debezium sends microseconds since epoch)
# and __deleted column for soft delete tracking

set -euo pipefail

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source vault helpers to load secrets
if [ -f "$SCRIPT_DIR/vault-helpers.sh" ]; then
    source "$SCRIPT_DIR/vault-helpers.sh"

    # Try to load secrets from Vault, fall back to environment variables
    if vault_is_ready; then
        echo "Loading credentials from Vault..."
        export_database_secrets || {
            echo "WARNING: Failed to load from Vault, using environment variables"
        }
    else
        echo "WARNING: Vault not available, using environment variables"
    fi
fi

# Set defaults if not loaded from Vault
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres_secure_password}"
POSTGRES_DB="${POSTGRES_DB:-warehouse_target}"

echo "Initializing PostgreSQL database: $POSTGRES_DB"
echo ""

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if docker exec cdc-postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" > /dev/null 2>&1; then
        echo "PostgreSQL is ready!"
        break
    fi
    echo "Waiting for PostgreSQL... (attempt $i/30)"
    sleep 2
done

# Create customers table
echo ""
echo "Creating customers table..."
docker exec cdc-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY,
    name VARCHAR(200),
    email VARCHAR(200),
    created_at BIGINT,
    updated_at BIGINT,
    __deleted VARCHAR(10)
);

CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_created_at ON customers(created_at);
"

# Create orders table
echo "Creating orders table..."
docker exec cdc-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date BIGINT,
    total_amount NUMERIC(10,2),
    status VARCHAR(50),
    __deleted VARCHAR(10)
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
"

# Create line_items table
echo "Creating line_items table..."
docker exec cdc-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
CREATE TABLE IF NOT EXISTS line_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    __deleted VARCHAR(10)
);

CREATE INDEX IF NOT EXISTS idx_line_items_order_id ON line_items(order_id);
CREATE INDEX IF NOT EXISTS idx_line_items_product_id ON line_items(product_id);
"

# Verify tables were created
echo ""
echo "Verifying tables..."
TABLES=$(docker exec cdc-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('customers', 'orders', 'line_items')")

if [ "$TABLES" -eq 3 ]; then
    echo "✓ All 3 tables created successfully"
else
    echo "⚠ Warning: Expected 3 tables but found $TABLES"
fi

echo ""
echo "PostgreSQL initialization complete!"
echo ""
echo "Tables created with Debezium CDC schema:"
echo "  - customers (id, name, email, created_at, updated_at, __deleted)"
echo "  - orders (id, customer_id, order_date, total_amount, status, __deleted)"
echo "  - line_items (id, order_id, product_id, quantity, unit_price, __deleted)"
echo ""
echo "Note: Timestamp columns use BIGINT (microseconds since epoch)"
echo "      __deleted column tracks soft deletes from Debezium"
echo ""
echo "Verify tables:"
echo "  docker exec cdc-postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"\\dt\""
echo ""
echo "Check table structure:"
echo "  docker exec cdc-postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"\\d+ customers\""
