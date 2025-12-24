-- ============================================================================
-- Reconciliation Performance Indexes
-- ============================================================================
-- This script creates indexes to optimize reconciliation queries.
-- Run this on both source (SQL Server) and target (PostgreSQL) databases.
--
-- IMPORTANT: Adjust table names and columns based on your schema.
-- Review existing indexes before running to avoid duplicates.
-- ============================================================================

-- ============================================================================
-- SQL Server Indexes
-- ============================================================================

-- Example: Create index on primary key columns for faster lookups
-- Uncomment and adjust for your tables
/*
CREATE NONCLUSTERED INDEX IX_TableName_PK
ON dbo.TableName (id)
INCLUDE (updated_at, checksum_column)
WITH (ONLINE = ON, FILLFACTOR = 90);

-- Index for change tracking timestamp queries
CREATE NONCLUSTERED INDEX IX_TableName_UpdatedAt
ON dbo.TableName (updated_at DESC)
INCLUDE (id)
WITH (ONLINE = ON, FILLFACTOR = 90);

-- Composite index for common reconciliation filters
CREATE NONCLUSTERED INDEX IX_TableName_Status_UpdatedAt
ON dbo.TableName (status, updated_at DESC)
INCLUDE (id, checksum_column)
WITH (ONLINE = ON, FILLFACTOR = 90);

-- Index for checksum validation queries
CREATE NONCLUSTERED INDEX IX_TableName_Checksum
ON dbo.TableName (checksum_column)
INCLUDE (id, updated_at)
WITH (ONLINE = ON, FILLFACTOR = 90);
*/

-- ============================================================================
-- Performance Statistics Update
-- ============================================================================

-- Update statistics for better query planning
/*
UPDATE STATISTICS dbo.TableName WITH FULLSCAN;
*/

-- ============================================================================
-- Index Maintenance Recommendations
-- ============================================================================

/*
-- Check index fragmentation
SELECT
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    s.avg_fragmentation_in_percent,
    s.page_count
FROM sys.dm_db_index_physical_stats(
    DB_ID(), NULL, NULL, NULL, 'LIMITED'
) s
JOIN sys.indexes i ON s.object_id = i.object_id
    AND s.index_id = i.index_id
WHERE s.avg_fragmentation_in_percent > 10
    AND s.page_count > 1000
ORDER BY s.avg_fragmentation_in_percent DESC;

-- Rebuild fragmented indexes
ALTER INDEX IX_TableName_PK ON dbo.TableName REBUILD
WITH (ONLINE = ON, FILLFACTOR = 90);

-- Update statistics
UPDATE STATISTICS dbo.TableName WITH FULLSCAN;
*/

-- ============================================================================
-- Common Reconciliation Patterns
-- ============================================================================

-- Pattern 1: Row count queries
-- Ensure tables have updated statistics for accurate COUNT(*) estimates
/*
CREATE STATISTICS STAT_TableName_RowCount
ON dbo.TableName (id)
WITH FULLSCAN;
*/

-- Pattern 2: Checksum aggregation
-- Create computed column for stable checksums (optional)
/*
ALTER TABLE dbo.TableName
ADD checksum_computed AS
    CHECKSUM(column1, column2, column3) PERSISTED;

CREATE NONCLUSTERED INDEX IX_TableName_ChecksumComputed
ON dbo.TableName (checksum_computed)
INCLUDE (id);
*/

-- Pattern 3: Date range queries for incremental reconciliation
/*
CREATE NONCLUSTERED INDEX IX_TableName_DateRange
ON dbo.TableName (created_at, updated_at)
INCLUDE (id, status)
WITH (ONLINE = ON, FILLFACTOR = 90);
*/

-- ============================================================================
-- PostgreSQL Indexes
-- ============================================================================

-- Example: Create B-tree indexes for primary key lookups
/*
CREATE INDEX CONCURRENTLY ix_tablename_pk
ON tablename (id);

-- Index for timestamp queries
CREATE INDEX CONCURRENTLY ix_tablename_updated_at
ON tablename (updated_at DESC)
INCLUDE (id);

-- Partial index for active records only
CREATE INDEX CONCURRENTLY ix_tablename_active_records
ON tablename (id, updated_at)
WHERE status = 'active';

-- Hash index for exact match checksum queries (PostgreSQL-specific)
CREATE INDEX CONCURRENTLY ix_tablename_checksum_hash
ON tablename USING hash (checksum_column);

-- GIN index for JSONB columns (if applicable)
CREATE INDEX CONCURRENTLY ix_tablename_metadata_gin
ON tablename USING gin (metadata_column);

-- Composite index for common filters
CREATE INDEX CONCURRENTLY ix_tablename_composite
ON tablename (status, updated_at DESC)
INCLUDE (id, checksum_column);
*/

-- ============================================================================
-- PostgreSQL Statistics
-- ============================================================================

-- Update table statistics
/*
ANALYZE tablename;

-- Set statistics target for better cardinality estimates
ALTER TABLE tablename ALTER COLUMN id SET STATISTICS 1000;
ALTER TABLE tablename ALTER COLUMN updated_at SET STATISTICS 1000;

ANALYZE tablename;
*/

-- ============================================================================
-- PostgreSQL Index Maintenance
-- ============================================================================

/*
-- Check index bloat
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    idx_scan AS index_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY pg_relation_size(indexrelid) DESC;

-- Reindex if needed
REINDEX INDEX CONCURRENTLY ix_tablename_pk;

-- Or reindex entire table
REINDEX TABLE CONCURRENTLY tablename;
*/

-- ============================================================================
-- Monitoring Queries
-- ============================================================================

-- SQL Server: Check index usage
/*
SELECT
    OBJECT_NAME(s.object_id) AS TableName,
    i.name AS IndexName,
    s.user_seeks,
    s.user_scans,
    s.user_lookups,
    s.user_updates,
    s.last_user_seek,
    s.last_user_scan
FROM sys.dm_db_index_usage_stats s
JOIN sys.indexes i ON s.object_id = i.object_id
    AND s.index_id = i.index_id
WHERE database_id = DB_ID()
    AND OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
ORDER BY s.user_seeks + s.user_scans + s.user_lookups DESC;
*/

-- PostgreSQL: Check index usage
/*
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;
*/

-- ============================================================================
-- Query Performance Tips
-- ============================================================================

/*
TIPS FOR RECONCILIATION QUERIES:

1. Row Count Optimization:
   - Use COUNT(1) instead of COUNT(*) on SQL Server
   - Use approximate counts for large tables (e.g., from statistics)
   - Consider table partitioning for very large tables

2. Checksum Queries:
   - Create indexed computed columns for stable checksums
   - Use parallel query execution for aggregations
   - Consider batching large checksum calculations

3. Change Detection:
   - Index timestamp columns (created_at, updated_at)
   - Use Change Tracking (SQL Server) or triggers (PostgreSQL)
   - Implement incremental reconciliation using watermarks

4. Join Optimization:
   - Ensure join columns are indexed
   - Use appropriate join types (INNER, LEFT, etc.)
   - Consider hash joins for large datasets

5. General Performance:
   - Update statistics regularly
   - Monitor query execution plans
   - Use connection pooling
   - Implement query timeouts
   - Consider read replicas for reconciliation queries

6. PostgreSQL-Specific:
   - Increase work_mem for large sorts/aggregations
   - Use VACUUM ANALYZE regularly
   - Configure autovacuum appropriately
   - Consider table partitioning for time-series data

7. SQL Server-Specific:
   - Update statistics with FULLSCAN
   - Use query hints sparingly (OPTION clause)
   - Monitor wait statistics
   - Consider In-Memory OLTP for hot tables
*/
