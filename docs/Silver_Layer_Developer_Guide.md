# Silver Layer Developer Guide

**Purpose:** Guide for extending Silver layer functionality without breaking existing code  
**Audience:** Data engineers adding new features

---

## 📐 Architecture Decisions

### Why Deduplication in Silver (Not Bronze)?

**Question developers ask:**  
"Why not deduplicate in Bronze and skip this complexity in Silver?"

**Answer:**

**Bronze = Audit Trail (Must Keep All Versions)**
```
Required by:
- Financial regulations (prove transaction status at any point)
- Compliance audits (show complete change history)
- Dispute resolution (customer claims: "I was charged twice!")
```

**Example audit query:**
```sql
-- "Prove this transaction was Pending on Dec 2"
SELECT transaction_status 
FROM bronze.transactions
WHERE transaction_id = 'TXN_001'
  AND DATE(updated_at) = '2025-12-02'
ORDER BY updated_at DESC LIMIT 1;
```

**If Bronze deduped → audit trail lost → legal risk**

---

**Silver = Analytics (Need Current State Only)**
```
Required by:
- Dashboards (show today's revenue)
- Reports (aggregate by merchant/status)
- ML models (train on current patterns)
```

**Example analytics query:**
```sql
-- "What's today's revenue?"
SELECT SUM(amount)
FROM silver.transactions
WHERE transaction_status = 'Successful'
  AND DATE(transaction_timestamp) = CURRENT_DATE();
```

**If Silver had duplicates → wrong totals → bad business decisions**

---

### Why Hard Delete in Silver (Not Soft Delete)?

**Bronze:**
```sql
-- Soft delete (keeps data, marks deleted)
UPDATE bronze.transactions
SET is_deleted = true, deleted_at = NOW()
WHERE customer_id = 'USER_0331';
```

**Silver:**
```sql
-- Hard delete (removes data completely)
DELETE FROM silver.transactions
WHERE customer_id = 'USER_0331';
```

**Reasoning:**

| Requirement | Bronze (Soft) | Silver (Hard) |
|-------------|---------------|---------------|
| GDPR "Right to be Forgotten" | ❌ Data still exists | ✅ Data removed |
| Compliance audit trail | ✅ Track deletions | ❌ Not needed here |
| Analytics accuracy | ❌ Must filter is_deleted | ✅ Auto-excluded |
| Storage cost | Higher (keeps data) | Lower (removes data) |

**Legal standpoint:**
- Audit logs CAN keep deleted data (with access controls)
- Analytics systems MUST NOT show deleted data to business users
- Bronze = audit system, Silver = analytics system

---

### Why Separate Staging Table?

**Alternative approach (no staging):**
```python
# Read Bronze → Dedupe → Write directly to Silver
spark.sql("INSERT INTO silver.transactions SELECT ...")
```

**Problem:** If MERGE fails halfway, Silver has incomplete data

**Our approach (staging):**
```python
# Read Bronze → Dedupe → Write to STAGING → MERGE staging to Silver
spark.sql("INSERT OVERWRITE silver.transactions_staging SELECT ...")
spark.sql("MERGE INTO silver.transactions USING silver.transactions_staging ...")
```

**Benefits:**
1. **Atomic writes:** MERGE is all-or-nothing (Delta ACID guarantee)
2. **Debuggability:** Can inspect staging before MERGE
3. **Retryability:** If MERGE fails, re-run load_silver.py (staging still has data)
4. **Testing:** Can validate staging without touching production Silver

---

## 🔧 Adding New Transformations

### Use Case: Add Derived Column

**Scenario:** Add `net_amount = amount - fee_amount` to Silver

---

### Step 1: Update Schema

**File:** `/docs/SCHEMA_REGISTRY.md`

```markdown
silver.transactions (22 columns)  ← Was 21
  ... existing columns ...
  net_amount DOUBLE  ← NEW
```

---

### Step 2: Update Delta Table

**Run ALTER TABLE (one-time):**
```sql
ALTER TABLE silver.transactions 
ADD COLUMN net_amount DOUBLE;

ALTER TABLE silver.transactions_staging 
ADD COLUMN net_amount DOUBLE;
```

**Or:** Use Spark schema evolution:
```python
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# New column added automatically on write
```

---

### Step 3: Update validate_silver.py

**Add transformation after deduplication:**
```python
# After ROW_NUMBER dedup
deduped_df = ... (existing code)

# Add derived column
from pyspark.sql.functions import col

enriched_df = deduped_df.withColumn(
    "net_amount",
    col("amount") - col("fee_amount")
)

# Write to staging (now includes net_amount)
enriched_df.createOrReplaceTempView("silver_staging_temp")
spark.sql("INSERT OVERWRITE silver.transactions_staging SELECT * FROM silver_staging_temp")
```

---

### Step 4: Update load_silver.py

**Add to MERGE statement:**
```sql
MERGE INTO silver.transactions AS target
USING silver.transactions_staging AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED THEN
    UPDATE SET
        ... existing columns ...,
        target.net_amount = source.net_amount  ← NEW

WHEN NOT MATCHED THEN
    INSERT (
        ... existing columns ...,
        net_amount  ← NEW
    )
    VALUES (
        ... existing values ...,
        source.net_amount  ← NEW
    )
```

---

### Step 5: Test

**Test with small dataset:**
```bash
# 1. Create test data (1 day, 100 records)
# 2. Run validate_silver.py
# 3. Check staging has net_amount
bq query "SELECT net_amount FROM silver_dataset.transactions_staging LIMIT 5"

# 4. Run load_silver.py
# 5. Check Silver has net_amount
bq query "SELECT net_amount FROM silver_dataset.transactions LIMIT 5"

# 6. Verify calculation
bq query "SELECT amount, fee_amount, net_amount, 
          (amount - fee_amount) as calculated,
          ABS(net_amount - (amount - fee_amount)) as diff
          FROM silver_dataset.transactions 
          WHERE diff > 0.01"
# Should return 0 rows (no calculation errors)
```

---

## 🆕 Adding New Jobs

### Use Case: Silver Backfill (Date Range)

**Why it wasn't built:** Incremental load already pulls all Bronze records newer than Silver watermark, so backfill is redundant for current design.

**When you'd need it:** If you change deduplication logic and want to reprocess specific dates without full refresh.

---

### Template: silver_backfill.py

```python
#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import sys

spark = SparkSession.builder \
    .appName("Silver_Backfill") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

# Parse arguments
START_DATE = sys.argv[1]  # YYYY-MM-DD
END_DATE = sys.argv[2]    # YYYY-MM-DD

# Read Bronze for date range
bronze_df = spark.sql(f"""
    SELECT * 
    FROM bronze.transactions
    WHERE DATE(transaction_timestamp) BETWEEN '{START_DATE}' AND '{END_DATE}'
      AND (is_deleted = false OR is_deleted IS NULL)
      AND (data_quality_flag != 'FAILED_VALIDATION' OR data_quality_flag IS NULL)
""")

# Deduplicate (same logic as validate_silver.py)
window_spec = Window.partitionBy("transaction_id").orderBy(
    col("updated_at").desc(),
    col("transaction_id").asc()
)

deduped_df = bronze_df \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# Write to staging
deduped_df.createOrReplaceTempView("backfill_staging")

spark.sql(f"""
    MERGE INTO silver.transactions AS target
    USING backfill_staging AS source
    ON target.transaction_id = source.transaction_id
       AND DATE(target.transaction_timestamp) BETWEEN '{START_DATE}' AND '{END_DATE}'
    
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"Backfilled {deduped_df.count()} records for {START_DATE} to {END_DATE}")
spark.stop()
```

**Key differences from incremental:**
- No watermark filter (uses date range instead)
- MERGE condition includes date filter (updates only backfill range)
- Doesn't update watermark (backfill shouldn't affect incremental)

---

## 🧪 Testing New Features

### Best Practices

**1. Use Separate Test Tables**
```sql
-- Don't test on production tables!
CREATE TABLE silver.transactions_test LIKE silver.transactions;
```

**2. Small Dataset First**
```python
# Test with 100 rows before 1M rows
test_df = bronze_df.limit(100)
```

**3. Validate Before MERGE**
```python
# Check staging before writing to Silver
staging_df = spark.table("silver.transactions_staging")
print(f"Records: {staging_df.count()}")
staging_df.show(5)
staging_df.printSchema()

# Manual inspection
staging_df.createOrReplaceTempView("inspect")
spark.sql("SELECT * FROM inspect WHERE [condition]").show()
```

**4. Idempotency Test**
```bash
# Run job twice with same input
./silver_incremental_load.py
./silver_incremental_load.py  # Should produce same result

# Check duplicates
bq query "SELECT transaction_id, COUNT(*) FROM silver_dataset.transactions 
          GROUP BY transaction_id HAVING COUNT(*) > 1"
# Should return 0 rows
```

---

### Testing Checklist

**Before deploying new feature:**
- [ ] Tested with 100-row sample
- [ ] Tested with 10K-row sample
- [ ] Tested with duplicate data
- [ ] Tested with NULL values
- [ ] Tested idempotency (run twice)
- [ ] Verified record counts match expected
- [ ] Checked for duplicate transaction_ids
- [ ] Validated watermark updates correctly
- [ ] Reviewed Spark UI for performance
- [ ] Checked staging table cleaned up

---

## 🏗️ Code Conventions

### Naming Conventions

**Spark Jobs:**
- `validate_*.py` - Read source, transform, write to staging
- `load_*.py` - MERGE staging to target
- `*_full_refresh.py` - Complete reload (INSERT OVERWRITE)
- `*_backfill.py` - Date range reload

**Temp Views:**
- `*_staging` - For MERGE operations
- `*_temp` - Intermediate transformations
- `*_deduped` - After ROW_NUMBER dedup

**Job Control:**
- `job_name` - Always `{layer}_{operation}` (e.g., `silver_incremental_load`)
- `run_mode` - One of: `incremental`, `backfill`, `full_refresh`, `manual`

---

### Watermark Pattern

**Every layer reads its OWN watermark:**

```python
# ❌ WRONG (reads Bronze watermark)
watermark = spark.sql("""
    SELECT MAX(last_processed_timestamp) 
    FROM bronze.job_control
    WHERE job_name = 'bronze_incremental_load'
""")

# ✅ CORRECT (reads Silver watermark)
watermark = spark.sql("""
    SELECT MAX(last_processed_timestamp) 
    FROM silver.job_control
    WHERE job_name = 'silver_incremental_load'  ← Same layer!
      AND status = 'SUCCESS'
""")
```

**Exception:** Initial load (Silver watermark doesn't exist yet)
```python
# First run after full_refresh
watermark = spark.sql("""
    SELECT MAX(last_processed_timestamp) 
    FROM silver.job_control
    WHERE job_name IN ('silver_full_refresh', 'silver_incremental_load')
    ORDER BY completed_at DESC LIMIT 1
""")
```

---

### Error Handling Pattern

**Always write job_control metadata (even on failure):**

```python
try:
    # Main job logic
    result = process_data()
    status = "SUCCESS"
    error_msg = None
    
except Exception as e:
    result = {"records_written": 0}
    status = "FAILED"
    error_msg = str(e)[:500]  # Truncate to fit column
    
finally:
    # Always write metadata
    spark.sql(f"""
        INSERT INTO silver.job_control VALUES (
            'silver_incremental_load',
            'silver',
            '{batch_id}',
            'incremental',
            '{status}',
            ... other fields ...,
            '{error_msg if error_msg else 'NULL'}'
        )
    """)
```

**Why?** Operations team needs to see failed runs in monitoring dashboards.

---

## 🔍 Debugging Tips

### View Spark Execution Plan

```python
# Before running expensive operation
df.explain(mode="extended")
# Check for: full table scans, missing filters, shuffle operations
```

---

### Check Delta Table History

```sql
-- See what changed in last 10 commits
DESCRIBE HISTORY silver.transactions LIMIT 10;

-- Time travel to previous version
SELECT COUNT(*) FROM silver.transactions VERSION AS OF 5;
```

---

### Inspect Staging Table

```sql
-- Check what's about to be merged
SELECT 
    COUNT(*) as total,
    COUNT(DISTINCT transaction_id) as unique_ids,
    MIN(updated_at) as earliest,
    MAX(updated_at) as latest
FROM silver.transactions_staging;

-- Find suspicious patterns
SELECT transaction_id, COUNT(*) as versions
FROM silver.transactions_staging
GROUP BY transaction_id
HAVING COUNT(*) > 1;  -- Should be 0 (already deduped)
```

---

### Compare Before/After MERGE

```python
# Before MERGE
before_count = spark.sql("SELECT COUNT(*) FROM silver.transactions").collect()[0][0]

# Run MERGE
spark.sql("MERGE INTO silver.transactions ...")

# After MERGE
after_count = spark.sql("SELECT COUNT(*) FROM silver.transactions").collect()[0][0]

print(f"Records added: {after_count - before_count}")
# Compare to staging count (should match inserts)
```

---

## 📊 Performance Optimization

### Window Function Optimization

**Slow (Spark reads all columns):**
```python
window_spec = Window.partitionBy("transaction_id").orderBy(col("updated_at").desc())
deduped_df = df.withColumn("row_num", row_number().over(window_spec))
# Shuffles ALL 21 columns across network
```

**Fast (Select only needed columns first):**
```python
# Select dedup columns + primary key first
light_df = df.select("transaction_id", "updated_at", "customer_id")
window_spec = Window.partitionBy("transaction_id").orderBy(col("updated_at").desc())
deduped_ids = light_df.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .select("transaction_id", "updated_at")

# Join back to full dataset
final_df = df.join(deduped_ids, ["transaction_id", "updated_at"])
# Shuffles 3 columns instead of 21 (7x less network traffic)
```

---

### Partition Pruning

```python
# Slow (scans all Bronze data)
df = spark.sql("SELECT * FROM bronze.transactions WHERE updated_at > ...")

# Fast (if partitioned by date)
df = spark.sql("""
    SELECT * FROM bronze.transactions 
    WHERE DATE(transaction_timestamp) >= '2025-12-01'  ← Partition filter
      AND updated_at > ...
""")
```

---

### Broadcast Join (For Small Dimensions)

```python
from pyspark.sql.functions import broadcast

# If merchant dimension is small (<10MB)
silver_df = bronze_df.join(
    broadcast(merchant_dim_df),  ← Tell Spark to broadcast
    "merchant_id"
)
# Avoids shuffle, much faster
```

---

## 🚀 Deployment Checklist

**Before deploying to production:**

- [ ] Code reviewed (at least 1 reviewer)
- [ ] Tested on dev cluster with full data
- [ ] Performance validated (check Spark UI)
- [ ] Schema changes documented (`SCHEMA_REGISTRY.md`)
- [ ] Runbook updated (`RUNBOOK.md`)
- [ ] Monitoring queries added (alerts for failures)
- [ ] Rollback plan documented (how to revert)
- [ ] Job uploaded to GCS (`gs://bucket/airflow/jobs/`)
- [ ] DAG tested in Airflow dev environment
- [ ] On-call team notified (new job alert thresholds)

---

## 📚 Additional Resources

- **Architecture docs:** `/docs/MIGRATION_DOC_COMPLETE.md`
- **Schema reference:** `/docs/SCHEMA_REGISTRY.md`
- **Operations guide:** `/silver/RUNBOOK.md`
- **Known issues:** `/docs/KNOWN_ISSUES.md`
- **Delta Lake docs:** https://docs.delta.io/latest/index.html
- **Spark SQL guide:** https://spark.apache.org/docs/latest/sql-programming-guide.html

---

**Questions?** Add to this guide via PR or contact data engineering team.

---

**Last Updated:** January 2026 | **Maintainer:** Mohamed Kashifuddin