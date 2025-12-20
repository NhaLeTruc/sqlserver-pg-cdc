#!/bin/bash
################################################################################
# Automated Database Backup Script
#
# Backs up SQL Server and PostgreSQL databases with:
# - Compression
# - Retention policy (7 days)
# - Optional S3 upload
# - Error handling and logging
#
# Usage:
#   ./backup-databases.sh [--s3-bucket BUCKET_NAME]
#
# Environment Variables:
#   BACKUP_DIR - Backup directory (default: /var/backups/cdc)
#   RETENTION_DAYS - Days to keep backups (default: 7)
#   S3_BUCKET - S3 bucket for off-site backups (optional)
################################################################################

set -euo pipefail

# Configuration
BACKUP_DIR="${BACKUP_DIR:-/var/backups/cdc}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${BACKUP_DIR}/backup_${TIMESTAMP}.log"
S3_BUCKET="${S3_BUCKET:-}"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --s3-bucket)
            S3_BUCKET="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--s3-bucket BUCKET_NAME]"
            exit 1
            ;;
    esac
done

# Logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

error() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "$LOG_FILE" >&2
}

# Create backup directory
mkdir -p "$BACKUP_DIR"
log "Starting backup process"
log "Backup directory: $BACKUP_DIR"
log "Retention period: $RETENTION_DAYS days"

# Initialize status
BACKUP_SUCCESS=true

################################################################################
# Backup SQL Server
################################################################################
log "=== Backing up SQL Server databases ==="

SQLSERVER_BACKUP_DIR="${BACKUP_DIR}/sqlserver"
mkdir -p "$SQLSERVER_BACKUP_DIR"

# Check if SQL Server container is running
if docker ps --format '{{.Names}}' | grep -q "sqlserver"; then
    log "SQL Server container found"

    # Get database list
    DATABASES=$(docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
        -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
        -Q "SET NOCOUNT ON; SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')" \
        -h -1 2>/dev/null | tr -d ' \r')

    if [ -z "$DATABASES" ]; then
        log "No user databases found in SQL Server"
    else
        for DB in $DATABASES; do
            log "Backing up SQL Server database: $DB"

            BACKUP_FILE="${SQLSERVER_BACKUP_DIR}/${DB}_${TIMESTAMP}.bak"

            # Create compressed backup
            if docker exec sqlserver /opt/mssql-tools/bin/sqlcmd \
                -S localhost -U sa -P "${SQLSERVER_PASSWORD}" \
                -Q "BACKUP DATABASE [$DB] TO DISK = N'/tmp/${DB}_${TIMESTAMP}.bak' WITH COMPRESSION, CHECKSUM" \
                >> "$LOG_FILE" 2>&1; then

                # Copy backup file from container
                if docker cp "sqlserver:/tmp/${DB}_${TIMESTAMP}.bak" "$BACKUP_FILE" >> "$LOG_FILE" 2>&1; then
                    # Clean up temp file in container
                    docker exec sqlserver rm -f "/tmp/${DB}_${TIMESTAMP}.bak"

                    BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
                    log "  ✓ Backup completed: $BACKUP_FILE ($BACKUP_SIZE)"
                else
                    error "  ✗ Failed to copy backup file for $DB"
                    BACKUP_SUCCESS=false
                fi
            else
                error "  ✗ Failed to backup SQL Server database: $DB"
                BACKUP_SUCCESS=false
            fi
        done
    fi
else
    log "SQL Server container not running, skipping SQL Server backup"
fi

################################################################################
# Backup PostgreSQL
################################################################################
log "=== Backing up PostgreSQL databases ==="

POSTGRES_BACKUP_DIR="${BACKUP_DIR}/postgresql"
mkdir -p "$POSTGRES_BACKUP_DIR"

# Check if PostgreSQL container is running
if docker ps --format '{{.Names}}' | grep -q "postgres"; then
    log "PostgreSQL container found"

    # Get database list
    DATABASES=$(docker exec postgres psql -U postgres -t -c \
        "SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('postgres')" \
        2>/dev/null | tr -d ' \r')

    if [ -z "$DATABASES" ]; then
        log "No user databases found in PostgreSQL"
    else
        for DB in $DATABASES; do
            log "Backing up PostgreSQL database: $DB"

            BACKUP_FILE="${POSTGRES_BACKUP_DIR}/${DB}_${TIMESTAMP}.sql.gz"

            # Create compressed backup using pg_dump
            if docker exec postgres pg_dump -U postgres -d "$DB" \
                --format=custom --compress=9 --verbose \
                > "${POSTGRES_BACKUP_DIR}/${DB}_${TIMESTAMP}.backup" 2>> "$LOG_FILE"; then

                # Compress with gzip for additional space savings
                if gzip -9 < "${POSTGRES_BACKUP_DIR}/${DB}_${TIMESTAMP}.backup" > "$BACKUP_FILE" 2>> "$LOG_FILE"; then
                    rm -f "${POSTGRES_BACKUP_DIR}/${DB}_${TIMESTAMP}.backup"

                    BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
                    log "  ✓ Backup completed: $BACKUP_FILE ($BACKUP_SIZE)"
                else
                    error "  ✗ Failed to compress backup for $DB"
                    BACKUP_SUCCESS=false
                fi
            else
                error "  ✗ Failed to backup PostgreSQL database: $DB"
                BACKUP_SUCCESS=false
            fi
        done
    fi
else
    log "PostgreSQL container not running, skipping PostgreSQL backup"
fi

################################################################################
# Backup Kafka Connect Configuration
################################################################################
log "=== Backing up Kafka Connect configuration ==="

KAFKA_BACKUP_DIR="${BACKUP_DIR}/kafka-connect"
mkdir -p "$KAFKA_BACKUP_DIR"

# Check if Kafka Connect is running
if docker ps --format '{{.Names}}' | grep -q "kafka-connect"; then
    log "Kafka Connect container found"

    KAFKA_CONFIG_FILE="${KAFKA_BACKUP_DIR}/connectors_${TIMESTAMP}.json"

    # Get list of connectors
    if curl -s http://localhost:8083/connectors > "$KAFKA_CONFIG_FILE" 2>> "$LOG_FILE"; then
        BACKUP_SIZE=$(du -h "$KAFKA_CONFIG_FILE" | cut -f1)
        log "  ✓ Kafka Connect configuration saved: $KAFKA_CONFIG_FILE ($BACKUP_SIZE)"
    else
        error "  ✗ Failed to backup Kafka Connect configuration"
        BACKUP_SUCCESS=false
    fi
else
    log "Kafka Connect container not running, skipping Kafka Connect backup"
fi

################################################################################
# Apply Retention Policy
################################################################################
log "=== Applying retention policy (${RETENTION_DAYS} days) ==="

# Find and delete old backups
DELETED_COUNT=0
for BACKUP_SUBDIR in sqlserver postgresql kafka-connect; do
    if [ -d "${BACKUP_DIR}/${BACKUP_SUBDIR}" ]; then
        while IFS= read -r -d '' OLD_FILE; do
            log "Deleting old backup: $OLD_FILE"
            rm -f "$OLD_FILE"
            ((DELETED_COUNT++))
        done < <(find "${BACKUP_DIR}/${BACKUP_SUBDIR}" -type f -mtime "+${RETENTION_DAYS}" -print0)
    fi
done

log "Deleted $DELETED_COUNT old backup file(s)"

################################################################################
# Upload to S3 (optional)
################################################################################
if [ -n "$S3_BUCKET" ]; then
    log "=== Uploading backups to S3: s3://${S3_BUCKET}/cdc-backups/ ==="

    if command -v aws &> /dev/null; then
        S3_PATH="s3://${S3_BUCKET}/cdc-backups/$(date +%Y/%m/%d)"

        if aws s3 sync "$BACKUP_DIR" "$S3_PATH" \
            --exclude "*.log" \
            --storage-class STANDARD_IA \
            >> "$LOG_FILE" 2>&1; then
            log "  ✓ Backups uploaded to $S3_PATH"
        else
            error "  ✗ Failed to upload backups to S3"
            BACKUP_SUCCESS=false
        fi
    else
        error "AWS CLI not found, skipping S3 upload"
        BACKUP_SUCCESS=false
    fi
fi

################################################################################
# Summary
################################################################################
log "=== Backup Summary ==="

TOTAL_SIZE=$(du -sh "$BACKUP_DIR" | cut -f1)
log "Total backup size: $TOTAL_SIZE"

# Count backup files by type
SQLSERVER_COUNT=$(find "$SQLSERVER_BACKUP_DIR" -type f -name "*_${TIMESTAMP}.bak" 2>/dev/null | wc -l)
POSTGRES_COUNT=$(find "$POSTGRES_BACKUP_DIR" -type f -name "*_${TIMESTAMP}.sql.gz" 2>/dev/null | wc -l)

log "SQL Server backups created: $SQLSERVER_COUNT"
log "PostgreSQL backups created: $POSTGRES_COUNT"

if [ "$BACKUP_SUCCESS" = true ]; then
    log "✓ Backup completed successfully"
    exit 0
else
    error "✗ Backup completed with errors"
    exit 1
fi