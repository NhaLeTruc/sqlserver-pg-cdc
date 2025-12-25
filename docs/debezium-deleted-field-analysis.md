# Debezium `__deleted` Field Analysis

## Question
When filtering out soft-deleted rows, should we use:
- `__deleted != 'true'` (current implementation)
- `__deleted = 'false'` (alternative)
- Something else?

## Investigation

### Connector Configuration

From [docker/configs/kafka-connect/postgresql-sink.json](../docker/configs/kafka-connect/postgresql-sink.json):

```json
{
  "transforms.unwrap.delete.handling.mode": "rewrite",
  "transforms.unwrap.add.fields": "op,deleted"
}
```

This configuration tells Debezium's `ExtractNewRecordState` transform to:
1. Rewrite DELETE events as UPDATE events with a `__deleted` field
2. Add additional fields: `__op` (operation type) and `__deleted` (deletion status)

### Database Schema

```sql
Column: __deleted
Type: character varying(10)
```

### Actual Values in Database

Query: `SELECT DISTINCT __deleted, COUNT(*) FROM customers GROUP BY __deleted;`

Result:
```
__deleted | count
-----------+-------
 false     | 10000  (active rows)
 true      | 17035  (soft-deleted rows/tombstones)
```

### Possible Values

The `__deleted` field has **three possible values**:

1. **`'false'`** - Active rows (INSERT, UPDATE operations)
2. **`'true'`** - Soft-deleted rows (DELETE operations with rewrite mode)
3. **`NULL`** - Possible for:
   - Rows inserted before `__deleted` field was added
   - Rows from snapshots before the transform was configured
   - Corner cases with schema evolution

## Answer: Current Implementation is Correct ✓

The current filter is **optimal**:

```sql
WHERE __deleted IS NULL OR __deleted != 'true'
```

### Why This is Better Than `__deleted = 'false'`

| Filter | Includes NULL? | Includes 'false'? | Includes 'true'? | Best Use Case |
|--------|---------------|-------------------|------------------|---------------|
| `__deleted = 'false'` | ❌ No | ✅ Yes | ❌ No | When you're sure all rows have the field |
| `__deleted != 'true'` | ✅ Yes | ✅ Yes | ❌ No | When NULL values might exist |
| `(__deleted IS NULL OR __deleted != 'true')` | ✅ Yes | ✅ Yes | ❌ No | **Most robust** (explicit NULL handling) |

### Why We Need NULL Handling

1. **Schema Evolution**: If CDC was enabled on existing data, older rows might not have `__deleted` set
2. **Snapshots**: Initial snapshot data might not have the field
3. **Migration**: Tables migrated from other systems
4. **Field Addition**: The `__deleted` field is added by the SMT (Single Message Transform), and older messages might not have it

### Performance Consideration

In PostgreSQL:
- `__deleted != 'true'` automatically handles NULL (NULL != 'true' is NULL, which is FALSE in WHERE clauses)
- But explicit `IS NULL` is clearer and may help query optimizer

## Recommendation

### ✅ BEST OPTION (Current Implementation)

```sql
WHERE __deleted IS NULL OR __deleted = 'false'
```

**Pros:**
- ✅ **Most precise** - only includes exactly what we want (active rows + legacy NULL rows)
- ✅ **Excludes unexpected values** - won't accidentally include `__deleted = 'pending'` or other future states
- ✅ **Explicit NULL handling** - crystal clear intent
- ✅ **Future-proof** - handles schema evolution gracefully
- ✅ **Self-documenting** - readers immediately understand the logic

**Cons:**
- None! This is the superior choice.

### ⚠️ Previously Used (Less Precise)

```sql
WHERE __deleted IS NULL OR __deleted != 'true'
```

**Pros:**
- Handles NULL values
- Excludes deleted rows

**Cons:**
- ⚠️ **Imprecise** - includes ANY value that's not 'true' (e.g., 'pending', 'error', etc.)
- ⚠️ **Could hide bugs** - if Debezium emits unexpected values, they'd be silently included
- ⚠️ **Less clear** - negative logic is harder to reason about

### Alternative: Simplified (Too Risky)

```sql
WHERE __deleted != 'true'
```

**Cons:**
- ❌ **Excludes NULL values** - `NULL != 'true'` → NULL → FALSE in WHERE clause
- ❌ **Will miss legacy rows** without the field
- ❌ **Same imprecision issue** as above

### NOT Recommended

```sql
WHERE __deleted = 'false'
```

**Cons:**
- ❌ **Excludes NULL values** - will miss rows without the field!
- ❌ **Breaks if field is missing** - not backward compatible
- ❌ **Fragile** - assumes all rows have the field set

## Testing

To verify which rows we're getting:

```sql
-- Check distribution
SELECT __deleted, COUNT(*) FROM customers GROUP BY __deleted;

-- Current filter (with explicit NULL)
SELECT COUNT(*) FROM customers
WHERE name LIKE 'Perf Test %'
AND (__deleted IS NULL OR __deleted != 'true');

-- Alternative filter (implicit NULL)
SELECT COUNT(*) FROM customers
WHERE name LIKE 'Perf Test %'
AND __deleted != 'true';

-- Bad filter (excludes NULL)
SELECT COUNT(*) FROM customers
WHERE name LIKE 'Perf Test %'
AND __deleted = 'false';
```

## Conclusion

✅ **Keep the current implementation**: `__deleted IS NULL OR __deleted != 'true'`

This is the most robust approach because:
1. Handles all three possible states (NULL, 'false', 'true')
2. Explicit about NULL handling (better for code readability)
3. Future-proof for schema evolution
4. Documents the intent clearly

The explicit NULL check makes the code self-documenting and prevents subtle bugs if the schema changes or older data is encountered.

## Update: Simplification Possible

After checking the actual database state, if you're **certain** that:
1. All rows will always have `__deleted` set (no NULLs)
2. The field is always added by the SMT
3. No schema migration scenarios exist

Then you could simplify to:

```sql
WHERE __deleted = 'false'
```

However, for **maximum robustness**, the current implementation is recommended.

## Implementation Location

Current implementation in:
- [tests/performance/test_performance.py:174-178](../tests/performance/test_performance.py#L174-L178) - `wait_for_replication()`
- [tests/performance/test_performance.py:197-201](../tests/performance/test_performance.py#L197-L201) - Timeout handling
- [tests/performance/test_performance.py:284-288](../tests/performance/test_performance.py#L284-L288) - Final count verification