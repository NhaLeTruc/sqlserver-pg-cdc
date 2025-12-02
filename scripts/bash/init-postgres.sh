#!/bin/bash
# Initialize PostgreSQL with target tables matching SQL Server schema

set -euo pipefail

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
docker exec cdc-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<EOF
CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_created_at ON customers(created_at);
EOF

# Create orders table
echo "Creating orders table..."
docker exec cdc-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<EOF
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP,
    total_amount NUMERIC(10,2),
    status VARCHAR(50),
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
EOF

# Create line_items table
echo "Creating line_items table..."
docker exec cdc-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<EOF
CREATE TABLE IF NOT EXISTS line_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE INDEX IF NOT EXISTS idx_line_items_order_id ON line_items(order_id);
CREATE INDEX IF NOT EXISTS idx_line_items_product_id ON line_items(product_id);
EOF

echo ""
echo "PostgreSQL initialization complete!"
echo ""
echo "Tables created:"
echo "  - customers (with indexes on email, created_at)"
echo "  - orders (with indexes on customer_id, order_date, status)"
echo "  - line_items (with indexes on order_id, product_id)"
echo ""
echo "Verify tables:"
echo "  docker exec cdc-postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"\\dt\""
echo ""
echo "Check table structure:"
echo "  docker exec cdc-postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"\\d+ customers\""
