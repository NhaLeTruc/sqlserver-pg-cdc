#!/bin/bash
# Initialize SQL Server with sample tables and enable CDC

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
SQLSERVER_HOST="${SQLSERVER_HOST:-localhost}"
SQLSERVER_USER="${SQLSERVER_USER:-sa}"
SQLSERVER_PASSWORD="${SQLSERVER_PASSWORD:-YourStrong!Passw0rd}"
SQLSERVER_DATABASE="${SQLSERVER_DATABASE:-warehouse_source}"

echo "Initializing SQL Server database: $SQLSERVER_DATABASE"
echo ""

# Wait for SQL Server to be ready
echo "Waiting for SQL Server to be ready..."
for i in {1..30}; do
    if docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
        -Q "SELECT 1" > /dev/null 2>&1; then
        echo "SQL Server is ready!"
        break
    fi
    echo "Waiting for SQL Server... (attempt $i/30)"
    sleep 2
done

# Create database if it doesn't exist
echo ""
echo "Creating database $SQLSERVER_DATABASE..."
docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
    -Q "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '$SQLSERVER_DATABASE') CREATE DATABASE $SQLSERVER_DATABASE"

# Enable CDC on database
echo "Enabling CDC on database..."
docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
    -d "$SQLSERVER_DATABASE" \
    -Q "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = '$SQLSERVER_DATABASE' AND is_cdc_enabled = 1) EXEC sys.sp_cdc_enable_db"

# Create customers table
echo ""
echo "Creating customers table..."
docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
    -d "$SQLSERVER_DATABASE" \
    -Q "
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customers' AND schema_id = SCHEMA_ID('dbo'))
    BEGIN
        CREATE TABLE dbo.customers (
            id INT PRIMARY KEY IDENTITY(1,1),
            name NVARCHAR(100) NOT NULL,
            email NVARCHAR(100),
            created_at DATETIME2 DEFAULT GETDATE(),
            updated_at DATETIME2
        );
    END
    "

# Enable CDC on customers table
echo "Enabling CDC on customers table..."
docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
    -d "$SQLSERVER_DATABASE" \
    -Q "
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN cdc.change_tables ct ON t.object_id = ct.source_object_id
        WHERE t.name = 'customers' AND SCHEMA_NAME(t.schema_id) = 'dbo'
    )
    BEGIN
        EXEC sys.sp_cdc_enable_table
            @source_schema = N'dbo',
            @source_name = N'customers',
            @role_name = NULL,
            @supports_net_changes = 1;
    END
    "

# Create orders table
echo ""
echo "Creating orders table..."
docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
    -d "$SQLSERVER_DATABASE" \
    -Q "
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'orders' AND schema_id = SCHEMA_ID('dbo'))
    BEGIN
        CREATE TABLE dbo.orders (
            id INT PRIMARY KEY IDENTITY(1,1),
            customer_id INT NOT NULL,
            order_date DATETIME2 DEFAULT GETDATE(),
            total_amount DECIMAL(10,2),
            status NVARCHAR(50),
            FOREIGN KEY (customer_id) REFERENCES dbo.customers(id)
        );
    END
    "

# Enable CDC on orders table
echo "Enabling CDC on orders table..."
docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
    -d "$SQLSERVER_DATABASE" \
    -Q "
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN cdc.change_tables ct ON t.object_id = ct.source_object_id
        WHERE t.name = 'orders' AND SCHEMA_NAME(t.schema_id) = 'dbo'
    )
    BEGIN
        EXEC sys.sp_cdc_enable_table
            @source_schema = N'dbo',
            @source_name = N'orders',
            @role_name = NULL,
            @supports_net_changes = 1;
    END
    "

# Create line_items table
echo ""
echo "Creating line_items table..."
docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
    -d "$SQLSERVER_DATABASE" \
    -Q "
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'line_items' AND schema_id = SCHEMA_ID('dbo'))
    BEGIN
        CREATE TABLE dbo.line_items (
            id INT PRIMARY KEY IDENTITY(1,1),
            order_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            unit_price DECIMAL(10,2) NOT NULL,
            FOREIGN KEY (order_id) REFERENCES dbo.orders(id)
        );
    END
    "

# Enable CDC on line_items table
echo "Enabling CDC on line_items table..."
docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U "$SQLSERVER_USER" -P "$SQLSERVER_PASSWORD" -C \
    -d "$SQLSERVER_DATABASE" \
    -Q "
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN cdc.change_tables ct ON t.object_id = ct.source_object_id
        WHERE t.name = 'line_items' AND SCHEMA_NAME(t.schema_id) = 'dbo'
    )
    BEGIN
        EXEC sys.sp_cdc_enable_table
            @source_schema = N'dbo',
            @source_name = N'line_items',
            @role_name = NULL,
            @supports_net_changes = 1;
    END
    "

echo ""
echo "SQL Server initialization complete!"
echo ""
echo "Tables created:"
echo "  - dbo.customers (CDC enabled)"
echo "  - dbo.orders (CDC enabled)"
echo "  - dbo.line_items (CDC enabled)"
echo ""
echo "Verify CDC status:"
echo "  docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U $SQLSERVER_USER -P $SQLSERVER_PASSWORD -C -d $SQLSERVER_DATABASE -Q \"SELECT name, is_tracked_by_cdc FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo')\""
