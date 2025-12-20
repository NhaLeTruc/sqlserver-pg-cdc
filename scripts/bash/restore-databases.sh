#!/bin/bash
################################################################################
# Automated Database Restore Script
#
# Restores SQL Server and PostgreSQL databases from backups with:
# - Backup verification
# - Restore validation
# - Point-in-time restore support
#
# Usage:
#   ./restore-databases.sh --timestamp TIMESTAMP [--database DB_NAME] [--type sqlserver|postgresql]
#
# Examples:
#   # Restore all databases from specific backup
#   ./restore-databases.sh --timestamp 20251220_143000
#
#   # Restore specific SQL Server database
#   ./restore-databases.sh --timestamp 20251220_143000 --database warehouse_source --type sqlserver
#
#   # Restore specific PostgreSQL database
#   ./restore-databases.sh --timestamp 20251220_143000 --database warehouse_target --type postgresql
################################################################################

set -euo pipefail

# Configuration
BACKUP_DIR="${BACKUP_DIR:-/var/backups/cdc}"
TIMESTAMP=""
DATABASE=""
DB_TYPE=""
LOG_FILE="${BACKUP_DIR}/restore_$(date +%Y%m%d_%H%M%S).log"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --timestamp)
            TIMESTAMP="$2"
            shift 2
            ;;
        --database)
            DATABASE="$2"
            shift 2
            ;;
        --type)
            DB_TYPE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 --timestamp TIMESTAMP [--database DB_NAME] [--type sqlserver|postgresql]"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [ -z "$TIMESTAMP" ]; then
    echo "Error: --timestamp is required"
    echo "Usage: $0 --timestamp TIMESTAMP [--database DB_NAME] [--type sqlserver|postgresql]"
    exit 1
fi

# Logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

error() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "$LOG_FILE" >&2
}

log "Starting restore process"
log "Timestamp: $TIMESTAMP"
log "Backup directory: $BACKUP_DIR"

if [ -n "$DATABASE" ]; then
    log "Target database: $DATABASE"
fi

if [ -n "$DB_TYPE" ]; then
    log "Database type: $DB_TYPE"
fi

# Initialize status
RESTORE_SUCCESS=true

################################################################################
# Restore SQL Server
################################################################################
if [ -z "$DB_TYPE" ] || [ "$DB_TYPE" = "sqlserver" ]; then
    log "=== Restoring SQL Server databases ==="

    SQLSERVER_BACKUP_DIR="${BACKUP_DIR}/sqlserver"

    if [ ! -d "$SQLSERVER_BACKUP_DIR" ]; then
        log "SQL Server backup directory not found, skipping"
    else
        # Check if SQL Server container is running
        if ! docker ps --format '{{.Names}}' | grep -q "sqlserver"; then
            error "SQL Server container not running"
            RESTORE_SUCCESS=false
        else
            # Find backup files
            if [ -n "$DATABASE" ]; then
                # Restore specific database
                BACKUP_FILES=$(find "$SQLSERVER_BACKUP_DIR" -name "${DATABASE}_${TIMESTAMP}.bak" 2>/dev/null)
            else
                # Restore all databases from timestamp
                BACKUP_FILES=$(find "$SQLSERVER_BACKUP_DIR" -name "*_${TIMESTAMP}.bak" 2>/dev/null)
            fi

            if [ -z "$BACKUP_FILES" ]; then
                error "No SQL Server backup files found for timestamp: $TIMESTAMP"
                RESTORE_SUCCESS=false
            else
                for BACKUP_FILE in $BACKUP_FILES; do
                    DB_NAME=$(basename "$BACKUP_FILE" | sed "s/_${TIMESTAMP}.bak//")
                    log "Restoring SQL Server database: $DB_NAME from $BACKUP_FILE"

                    # Copy backup file to container
                    if ! docker cp "$BACKUP_FILE" "sqlserver:/tmp/restore.bak" >> "$LOG_FILE" 2>&1; then
                        error "  ✗ Failed to copy backup file to container"
                        RESTORE_SUCCESS=false
                        continue
                    fi

                    # Restore database
                    if docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
                        -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
                        -Q "RESTORE DATABASE [$DB_NAME] FROM DISK = N'/tmp/restore.bak' WITH REPLACE, CHECKSUM" \
                        >> "$LOG_FILE" 2>&1; then

                        # Clean up temp file
                        docker exec sqlserver rm -f /tmp/restore.bak

                        # Verify restore
                        ROW_COUNT=$(docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
                            -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
                            -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.tables WHERE database_id = DB_ID('$DB_NAME')" \
                            -h -1 2>/dev/null | tr -d ' \r')

                        log "  ✓ Database restored successfully ($ROW_COUNT tables)"
                    else
                        error "  ✗ Failed to restore database: $DB_NAME"
                        RESTORE_SUCCESS=false
                    fi
                done
            fi
        fi
    fi
fi

################################################################################
# Restore PostgreSQL
################################################################################
if [ -z "$DB_TYPE" ] || [ "$DB_TYPE" = "postgresql" ]; then
    log "=== Restoring PostgreSQL databases ==="

    POSTGRES_BACKUP_DIR="${BACKUP_DIR}/postgresql"

    if [ ! -d "$POSTGRES_BACKUP_DIR" ]; then
        log "PostgreSQL backup directory not found, skipping"
    else
        # Check if PostgreSQL container is running
        if ! docker ps --format '{{.Names}}' | grep -q "postgres"; then
            error "PostgreSQL container not running"
            RESTORE_SUCCESS=false
        else
            # Find backup files
            if [ -n "$DATABASE" ]; then
                # Restore specific database
                BACKUP_FILES=$(find "$POSTGRES_BACKUP_DIR" -name "${DATABASE}_${TIMESTAMP}.sql.gz" 2>/dev/null)
            else
                # Restore all databases from timestamp
                BACKUP_FILES=$(find "$POSTGRES_BACKUP_DIR" -name "*_${TIMESTAMP}.sql.gz" 2>/dev/null)
            fi

            if [ -z "$BACKUP_FILES" ]; then
                error "No PostgreSQL backup files found for timestamp: $TIMESTAMP"
                RESTORE_SUCCESS=false
            else
                for BACKUP_FILE in $BACKUP_FILES; then
                    DB_NAME=$(basename "$BACKUP_FILE" | sed "s/_${TIMESTAMP}.sql.gz//")
                    log "Restoring PostgreSQL database: $DB_NAME from $BACKUP_FILE"

                    # Decompress backup
                    TEMP_BACKUP="/tmp/restore_${DB_NAME}.backup"
                    if ! gunzip -c "$BACKUP_FILE" > "$TEMP_BACKUP" 2>> "$LOG_FILE"; then
                        error "  ✗ Failed to decompress backup file"
                        RESTORE_SUCCESS=false
                        continue
                    fi

                    # Drop existing database if it exists
                    docker exec postgres psql -U postgres -c "DROP DATABASE IF EXISTS \"$DB_NAME\"" >> "$LOG_FILE" 2>&1 || true

                    # Create new database
                    if ! docker exec postgres psql -U postgres -c "CREATE DATABASE \"$DB_NAME\"" >> "$LOG_FILE" 2>&1; then
                        error "  ✗ Failed to create database: $DB_NAME"
                        rm -f "$TEMP_BACKUP"
                        RESTORE_SUCCESS=false
                        continue
                    fi

                    # Copy backup to container
                    if ! docker cp "$TEMP_BACKUP" "postgres:/tmp/restore.backup" >> "$LOG_FILE" 2>&1; then
                        error "  ✗ Failed to copy backup to container"
                        rm -f "$TEMP_BACKUP"
                        RESTORE_SUCCESS=false
                        continue
                    fi

                    # Restore database
                    if docker exec postgres pg_restore -U postgres -d "$DB_NAME" \
                        --verbose --clean --if-exists \
                        /tmp/restore.backup >> "$LOG_FILE" 2>&1; then

                        # Clean up temp files
                        docker exec postgres rm -f /tmp/restore.backup
                        rm -f "$TEMP_BACKUP"

                        # Verify restore
                        TABLE_COUNT=$(docker exec postgres psql -U postgres -d "$DB_NAME" -t -c \
                            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'" \
                            2>/dev/null | tr -d ' \r')

                        log "  ✓ Database restored successfully ($TABLE_COUNT tables)"
                    else
                        error "  ✗ Failed to restore database: $DB_NAME"
                        rm -f "$TEMP_BACKUP"
                        RESTORE_SUCCESS=false
                    fi
                done
            fi
        fi
    fi
fi

################################################################################
# Summary
################################################################################
log "=== Restore Summary ==="

if [ "$RESTORE_SUCCESS" = true ]; then
    log "✓ Restore completed successfully"
    log "Next steps:"
    log "  1. Verify data integrity with reconciliation tool"
    log "  2. Check application connectivity"
    log "  3. Review restore log: $LOG_FILE"
    exit 0
else
    error "✗ Restore completed with errors"
    error "Review restore log for details: $LOG_FILE"
    exit 1
fi