# Known Issues & Side Effects

**Last Updated:** January 2026  
**Purpose:** Document issues encountered during implementation and OSS Delta Lake limitations

---

## Issue 1: BigQuery Schema Drift (Nullable → Non-Nullable)

### Symptom
```
Error: Invalid schema update. 
Field transaction_id has changed mode from REQUIRED to NULLABLE.
```

**When it occurs:** After running `bronze_full_refresh.py` with implicit schema inference

---

### Root Cause

**The Problem:** `CREATE OR REPLACE TABLE ... AS SELECT` without explicit schema

**What happened:**
```python
# WRONG - Infers schema from data
spark.sql("""
    CREATE OR REPLACE TABLE bronze.transactions 
    USING DELTA
    AS SELECT * FROM bronze_staging
""")
```

**Result:**
1. `CAST(transaction_id AS STRING)` creates nullable column
2. CREATE TABLE infers nullable schema from SELECT
3. Original schema had `transaction_id STRING NOT NULL`
4. BigQuery external table sees: REQUIRED → NULLABLE (not allowed!)

---

### Why BigQuery Rejects This

**BigQuery External Table Policy:**
- ✅ Can add new columns
- ✅ Can widen types (INT → BIGINT)
- ❌ **Cannot change REQUIRED → NULLABLE** (breaks downstream queries)

**Example of broken query:**
```sql
-- This query relied on NOT NULL guarantee
SELECT transaction_id, COUNT(*) 
FROM bronze_dataset.transactions
GROUP BY transaction_id
-- Now fails: "Cannot group by nullable column without COALESCE"
```

---

### Fix

**Solution 1: Explicit Schema in CREATE TABLE**
```python
spark.sql("""
    CREATE OR REPLACE TABLE bronze.transactions (
        transaction_id STRING NOT NULL,
        customer_id STRING,
        -- ... all 25 columns with correct nullability
    )
    USING DELTA
    AS SELECT 
        transaction_id, customer_id, ...
    FROM bronze_staging
""")
```

**Solution 2: Use INSERT OVERWRITE (Preserves Existing Schema)**
```python
# Assumes table already exists with correct schema
spark.sql("""
    INSERT OVERWRITE bronze.transactions
    SELECT 
        transaction_id, customer_id, ...
    FROM bronze_staging
""")
```

**We chose Solution 2** because:
- Tables created once in Blog 3 setup with correct schema
- INSERT OVERWRITE preserves schema
- Simpler code (no 25-line schema definition)

---

### Prevention

**Best Practice:**
1. Create tables with explicit schema ONCE (DDL scripts)
2. Use INSERT/MERGE for data operations (never CREATE OR REPLACE)
3. If you MUST recreate, use explicit schema

**Code Review Checklist:**
- ❌ `CREATE OR REPLACE TABLE ... AS SELECT` (dangerous!)
- ✅ `INSERT OVERWRITE` (safe)
- ✅ `MERGE INTO` (safe)
- ✅ `CREATE TABLE ... AS SELECT` (safe, only if table doesn't exist)

---

### How to Fix BigQuery After Schema Drift

```sql
-- 1. Drop broken external table
DROP EXTERNAL TABLE `bronze_dataset.transactions`;

-- 2. Recreate (reads fresh schema from Delta)
CREATE EXTERNAL TABLE `bronze_dataset.transactions`
WITH CONNECTION `us-central1.delta_biglake_connection`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://delta-lake-payment-gateway-476820/bronze/transactions/']
);

-- 3. Verify schema
DESCRIBE `bronze_dataset.transactions`;
-- transaction_id should show "REQUIRED" again
```

---

## Issue 2: Open Source Delta Lake Limitations

### Context

This project uses **open source Delta Lake** (not Databricks). Several convenient features available in Databricks are NOT supported in OSS.

---

### Limitation 1: `SELECT * EXCEPT(column)`

**What we wanted:**
```python
# Databricks only!
spark.sql("""
    CREATE TABLE bronze.transactions 
    AS SELECT * EXCEPT(row_num) FROM bronze_staging
""")
```

**Error in OSS Delta:**
```
ParseException: Syntax error at or near 'EXCEPT'
```

**Workaround:** Explicit column list
```python
spark.sql("""
    CREATE TABLE bronze.transactions 
    AS SELECT 
        transaction_id, customer_id, transaction_timestamp,
        merchant_id, merchant_name, product_category, product_name,
        amount, fee_amount, cashback_amount, loyalty_points,
        payment_method, transaction_status, device_type, location_type,
        currency, updated_at, delta_change_type, delta_version,
        is_deleted, deleted_at, is_late_arrival, arrival_delay_hours,
        data_quality_flag, validation_errors
    FROM bronze_staging
""")
```

**Why it matters:** More verbose, but ensures `row_num` helper column is excluded

---

### Limitation 2: `ALTER COLUMN ... SET NOT NULL`

**What we wanted:**
```sql
-- Fix nullable column after schema drift
ALTER TABLE bronze.transactions 
ALTER COLUMN transaction_id SET NOT NULL;
```

**Error in OSS Delta:**
```
AnalysisException: ALTER COLUMN ... SET NOT NULL is not supported
```

**OSS Delta Policy:**
- ✅ Can change nullable → NOT NULL at **CREATE TIME**
- ❌ Cannot change existing nullable column to NOT NULL

**References:**
- [GitHub Issue #3360](https://github.com/delta-io/delta/issues/3360): "Only way is at create time"
- [GitHub Issue #831](https://github.com/delta-io/delta/issues/831): ALTER COLUMN limitations

**Workaround:**
1. Recreate table with correct schema (loses data!)
2. OR: Use INSERT OVERWRITE to preserve existing schema

**Prevention:** Always set NOT NULL constraints in original CREATE TABLE

---

### Limitation 3: Databricks Unity Catalog Features

**Not available in OSS:**
- Feature tables with automatic primary key management
- `ALTER TABLE ... ADD CONSTRAINT` for primary keys
- Automatic schema validation on write

**Example (Databricks only):**
```sql
-- Unity Catalog
ALTER TABLE bronze.transactions 
ADD CONSTRAINT pk_transaction PRIMARY KEY (transaction_id);
```

**OSS Equivalent:**
```python
# Manual validation in Spark job
if df.filter("transaction_id IS NULL").count() > 0:
    raise Exception("Primary key violation: NULL transaction_id found")
```

---

### Limitation 4: `QUALIFY` Clause

**What we wanted:**
```sql
-- Cleaner deduplication (Databricks only)
SELECT *
FROM bronze.transactions
QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY updated_at DESC) = 1
```

**Error in OSS Delta:**
```
ParseException: Syntax error at or near 'QUALIFY'
```

**Workaround:** Subquery with WHERE
```sql
SELECT * FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY updated_at DESC) AS row_num
    FROM bronze.transactions
) WHERE row_num = 1
```

---

### Summary: Databricks vs OSS Delta Lake

| Feature | Databricks | OSS Delta Lake |
|---------|------------|----------------|
| Core Delta (ACID, Time Travel, MERGE) | ✅ | ✅ |
| `SELECT * EXCEPT` | ✅ | ❌ Use explicit columns |
| `ALTER COLUMN SET NOT NULL` | ✅ | ❌ Set at CREATE time |
| `QUALIFY` clause | ✅ | ❌ Use subquery |
| Unity Catalog features | ✅ | ❌ Manual validation |
| BigQuery integration | ✅ (via connector) | ✅ (via BigLake) |

**Recommendation:** If using OSS Delta Lake:
- Explicit column lists everywhere
- Set constraints at table creation
- Manual primary key validation
- Test schema changes thoroughly

---

## Issue 3: Intra-Batch Deduplication

### Problem

**Discovered During Testing:**  
Same CSV file contained exact duplicate rows:

```csv
transaction_id,customer_id,amount,updated_at
TXN_001,USER_123,1500.00,2025-12-01 10:00:00
TXN_001,USER_123,1500.00,2025-12-01 10:00:00  ← Duplicate!
TXN_001,USER_123,1500.00,2025-12-01 10:00:00  ← Duplicate!
```

**Not the intended behavior:**  
We want multiple versions with DIFFERENT `updated_at` (status changes), not exact duplicates.

---

### Root Cause

**Data Generator Bug:**  
When creating status updates, accidentally duplicated rows instead of modifying existing ones.

**Expected:**
```python
# Update status (different updated_at)
TXN_001 | Pending    | 2025-11-29 10:00
TXN_001 | Successful | 2025-11-30 15:30  ← Different timestamp
```

**Actual (bug):**
```python
# Duplicate row (same updated_at)
TXN_001 | Pending | 2025-11-29 10:00
TXN_001 | Pending | 2025-11-29 10:00  ← Exact duplicate!
```

---

### Fix

**Added ROW_NUMBER deduplication in Bronze jobs:**

```python
# validate_bronze.py, bronze_backfill.py, bronze_full_refresh.py
CREATE OR REPLACE TEMP VIEW bronze_staging AS
SELECT * FROM (
    SELECT 
        transaction_id, customer_id, ...,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id, updated_at 
            ORDER BY transaction_id
        ) AS row_num
    FROM filtered_data
    WHERE [validation filters]
) WHERE row_num = 1
```

**What it does:**
- Group by `(transaction_id, updated_at)` (composite key)
- Keep first occurrence (ORDER BY is arbitrary since rows are identical)
- Drop `row_num` helper column before write

---

### Why Not Fix Data Generator Instead?

**We did both:**
1. Fixed data generator (stop creating duplicates)
2. Added deduplication (defensive coding)

**Reason for #2:** Real-world data is messy  
Upstream systems might send duplicates due to:
- Retry logic (network failure → resend)
- Batch job errors (process same file twice)
- CDC tools (duplicate change events)

**Better to handle gracefully than crash the pipeline.**

---

### Performance Impact

**Overhead:**
- ROW_NUMBER window function: ~5-10% slower
- Worth it: Prevents duplicate key errors in MERGE

**Tested on 1.4M records:**
- Before dedup: 8 min
- After dedup: 9 min
- Cost: $0.01 (1 extra minute at $0.40/hour)

---

## Issue 4: Watermark Confusion (Bronze vs Silver)

### Problem

**Initial Implementation:**  
Silver read Bronze's watermark → processed old data repeatedly

```python
# WRONG
bronze_watermark = get_bronze_watermark()
new_silver_data = filter(bronze.transactions, updated_at > bronze_watermark)
# Problem: Bronze watermark advances, Silver reprocesses same data
```

---

### Root Cause

**Each layer needs its own watermark:**
- Bronze watermark: "What raw CSV data have I processed?"
- Silver watermark: "What Bronze data have I transformed?"

**If Silver uses Bronze's watermark:**
1. Day 1: Bronze watermark = 2025-12-01
2. Day 2: Bronze loads new data, watermark = 2025-12-02
3. Silver runs: "Process Bronze > 2025-12-01" → reprocesses Day 1 again!

---

### Fix

**Separate watermark tables:**
```python
# validate_silver.py - CORRECT
silver_watermark = """
    SELECT last_processed_timestamp 
    FROM silver.job_control
    WHERE job_name = 'silver_incremental_load'
    ORDER BY completed_at DESC LIMIT 1
"""

new_bronze_data = """
    SELECT * FROM bronze.transactions
    WHERE updated_at > :silver_watermark
"""
```

**Result:**
- Silver tracks what IT has processed
- Bronze tracks what IT has ingested
- No duplicate processing

---

### Lesson Learned

**Watermark = "Progress Marker for THIS Layer"**

Each layer must track its own progress:
- Bronze → Raw CSV files
- Silver → Bronze Delta table
- Gold → Silver Delta table

**Never share watermarks across layers** (except for initial value after full refresh)

---

## Issue 5: Airflow ExternalTaskSensor Timing

### Problem

**Silver DAG kept failing with:**
```
ERROR: ExternalTaskSensor timed out waiting for Bronze
```

---

### Root Cause

**Incorrect execution_delta:**
```python
# WRONG
wait_for_bronze = ExternalTaskSensor(
    external_dag_id="bronze_incremental_load",
    external_task_id="load_bronze",
    execution_delta=timedelta(minutes=30)  # Too short!
)
```

**Bronze takes 8-10 minutes** → Sensor checks after 30 min → Bronze not done yet

---

### Fix

**Match schedule difference:**
```python
# Bronze runs: 2 AM
# Silver runs: 3 AM
# execution_delta = 1 hour (matches schedule gap)

wait_for_bronze = ExternalTaskSensor(
    external_dag_id="bronze_incremental_load-14",  # Include version!
    external_task_id="load_bronze",
    execution_delta=timedelta(hours=1),  # CORRECT
    timeout=600,  # 10 min timeout
    mode="reschedule"  # Don't block worker
)
```

---

### Best Practices

1. **execution_delta = schedule gap** between DAGs
2. **timeout > expected duration** of upstream task
3. **mode="reschedule"** to free up worker slots
4. **Include DAG version** in external_dag_id if using versioning

---

## Preventing These Issues

### Code Review Checklist

**Schema Changes:**
- [ ] Using INSERT/MERGE (not CREATE OR REPLACE)?
- [ ] Explicit column lists (no SELECT *)?
- [ ] Testing on dev cluster first?

**Watermarks:**
- [ ] Each layer reads ITS OWN watermark?
- [ ] Watermark updated AFTER successful write?
- [ ] Idempotent (safe to re-run)?

**Data Quality:**
- [ ] Deduplication logic present?
- [ ] NULL handling for all columns?
- [ ] Test with bad data?

**Airflow:**
- [ ] execution_delta matches schedule gap?
- [ ] Timeout > expected duration?
- [ ] DAG IDs match exactly?

---

## Getting Help

**If you encounter these issues:**
1. Check this document first
2. Review job logs in Dataproc
3. Query watermark tables in BigQuery
4. Test with small data sample first

**Contributing:**  
Found a new issue? Add it to this doc via PR.

---

**Last Updated:** January 2026 | **Maintainer:** Mohamed Kashifuddin