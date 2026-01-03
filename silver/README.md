# Silver Layer - Clean & Current State

**Status:** ✅ Complete | **Blog:** 3b  
**Purpose:** Deduplicate Bronze audit trail to current state, apply business rules, filter bad data

---

## 📋 Overview

### What Is Silver Layer?

The Silver layer transforms Bronze's multi-version audit trail into a **single current state per transaction**. It's the "clean room" where raw data becomes analytics-ready.

**Input:** `bronze.transactions` (89,800 records with duplicate transaction_ids for audit trail)  
**Output:** `silver.transactions` (89,650 unique records - current state only)

**Key Transformation:**
```
Bronze (Audit Trail):
TXN_001 | Pending    | 2025-11-29 10:00  ← Version 1
TXN_001 | Successful | 2025-11-30 15:30  ← Version 2
TXN_001 | Refunded   | 2025-12-01 09:00  ← Version 3 (CURRENT)

Silver (Current State):
TXN_001 | Refunded   | 2025-12-01 09:00  ← ONLY latest version
```

---

## 🎯 Why Silver Layer Exists

**Problem Bronze Solves:** Compliance (prove transaction status at any point in time)  
**Problem Silver Solves:** Analytics (fast queries on current state without scanning history)

**Without Silver:**
```sql
-- Query current revenue (scanning 3 versions per transaction!)
SELECT SUM(amount) 
FROM bronze.transactions
WHERE transaction_status = 'Successful'
  AND is_deleted = false
  AND updated_at = (SELECT MAX(updated_at) 
                    FROM bronze.transactions b2 
                    WHERE b2.transaction_id = bronze.transactions.transaction_id)
-- Slow: 3x data to scan, complex subquery
```

**With Silver:**
```sql
-- Query current revenue (single version per transaction)
SELECT SUM(amount) 
FROM silver.transactions
WHERE transaction_status = 'Successful'
-- Fast: No subquery, no duplicate scanning
```

---

## 🏗️ Architecture

### Schema (21 Columns)

**Removed from Bronze (4 columns):**
- `is_late_arrival` (Bronze-specific tracking)
- `arrival_delay_hours` (Bronze-specific tracking)
- `data_quality_flag` (filtered out in Silver)
- `validation_errors` (filtered out in Silver)

**Bronze: 25 columns** → **Silver: 21 columns**

**Full Schema:**
```sql
CREATE TABLE silver.transactions (
    -- Original 17 columns
    transaction_id STRING NOT NULL,
    customer_id STRING,
    transaction_timestamp TIMESTAMP,
    merchant_id STRING,
    merchant_name STRING,
    product_category STRING,
    product_name STRING,
    amount DOUBLE,
    fee_amount DOUBLE,
    cashback_amount DOUBLE,
    loyalty_points INT,
    payment_method STRING,
    transaction_status STRING,
    device_type STRING,
    location_type STRING,
    currency STRING,
    updated_at TIMESTAMP,
    
    -- Delta/CDC columns (2)
    delta_change_type STRING,
    delta_version INT,
    
    -- Soft delete tracking (2)
    is_deleted BOOLEAN,
    deleted_at TIMESTAMP
) USING DELTA
LOCATION 'gs://delta-lake-payment-gateway-476820/silver/transactions';
```

**Key Differences:**
| Feature | Bronze | Silver |
|---------|--------|--------|
| **Unique Key** | `(transaction_id, updated_at)` | `transaction_id` |
| **Versions per TXN** | Multiple (audit trail) | Single (current state) |
| **Late Arrival Tracking** | Yes (`is_late_arrival`) | No (removed) |
| **Data Quality Columns** | Yes (2 columns) | No (bad data filtered) |
| **Soft Deletes** | Yes (keep for audit) | No (hard deleted) |

---

## 🔧 Jobs (5 Total)

### 1. validate_silver.py

**Purpose:** Read new Bronze records, deduplicate, write to staging

**Logic:**
```python
# STEP 1: Read Silver watermark (not Bronze's!)
silver_watermark = get_silver_watermark()

# STEP 2: Filter Bronze for new records
bronze_new = """
    SELECT * FROM bronze.transactions
    WHERE updated_at > :silver_watermark
      AND (is_deleted = false OR is_deleted IS NULL)
      AND (data_quality_flag != 'FAILED_VALIDATION' OR data_quality_flag IS NULL)
"""

# STEP 3: Deduplicate (ROW_NUMBER)
deduped = """
    SELECT * FROM (
        SELECT *, 
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id 
                ORDER BY updated_at DESC
            ) AS row_num
        FROM bronze_new
    ) WHERE row_num = 1
"""

# STEP 4: Write to staging
INSERT OVERWRITE silver.transactions_staging SELECT * FROM deduped
```

**Key Features:**
- Reads **Silver's watermark** (not Bronze's) to avoid reprocessing
- Filters out soft deletes (`is_deleted = true`)
- Filters out bad data (`data_quality_flag = 'FAILED_VALIDATION'`)
- Window function deduplication (keeps latest `updated_at` per `transaction_id`)

**Output:**
```json
{
  "records_read": 5258,
  "records_filtered": 150,
  "records_deduped": 5253,
  "duplicates_removed": 5,
  "late_arrivals": 810
}
```

---

### 2. load_silver.py

**Purpose:** MERGE staging → silver.transactions

**Logic:**
```sql
MERGE INTO silver.transactions AS target
USING silver.transactions_staging AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED THEN
    UPDATE SET
        target.updated_at = source.updated_at,
        target.transaction_status = source.transaction_status,
        target.delta_change_type = 'UPDATE',
        -- ... other columns

WHEN NOT MATCHED THEN
    INSERT (transaction_id, ...) 
    VALUES (source.transaction_id, ...)
```

**Key Difference from Bronze:**
- Single key: `transaction_id` (not composite)
- Simpler MERGE (no duplicate handling needed, already deduped)

**Output:**
```json
{
  "records_merged": 5253,
  "inserts": 5248,
  "updates": 5,
  "new_watermark": "2025-12-30 22:52:54"
}
```

---

### 3. silver_full_refresh.py

**Purpose:** Rebuild Silver from ALL Bronze data

**When to Use:**
- Initial load (Silver is empty)
- After Bronze full refresh
- Testing/validation

**Logic:**
```python
# Read ALL Bronze (not just new)
bronze_all = """
    SELECT * FROM bronze.transactions
    WHERE (is_deleted = false OR is_deleted IS NULL)
      AND (data_quality_flag != 'FAILED_VALIDATION' OR data_quality_flag IS NULL)
"""

# Deduplicate (same as incremental)
deduped = ROW_NUMBER() deduplication

# INSERT OVERWRITE (replaces all Silver data)
INSERT OVERWRITE silver.transactions SELECT * FROM deduped
```

**Output:**
```json
{
  "records_read": 89800,
  "records_written": 89650,
  "duplicates_removed": 150,
  "new_watermark": "2025-12-30 22:52:54"
}
```

---

### 4. bronze_mark_deleted_by_customer.py (GDPR)

**Purpose:** Mark customer's transactions as deleted in Bronze (soft delete)

**Usage:**
```bash
gcloud dataproc jobs submit pyspark \
  gs://bucket/jobs/bronze_mark_deleted_by_customer.py \
  --cluster=dataproc-cluster \
  --region=us-central1 \
  -- \
  --customer_id=USER_0331
```

**Logic:**
```sql
UPDATE bronze.transactions
SET 
    is_deleted = true,
    deleted_at = CURRENT_TIMESTAMP(),
    delta_change_type = 'DELETE'
WHERE customer_id = :customer_id
  AND (is_deleted = false OR is_deleted IS NULL)
```

**Output:**
```json
{
  "customer_id": "USER_0331",
  "newly_deleted": 1240,
  "next_steps": "Run silver_propagate_deletes to sync"
}
```

---

### 5. silver_propagate_deletes.py

**Purpose:** Hard delete from Silver (GDPR compliance)

**Why Hard Delete?**
- Bronze: Soft delete (audit trail for compliance)
- Silver: Hard delete (analytics shouldn't see deleted data)

**Logic:**
```sql
-- Find deleted customers in Bronze
deleted_ids = SELECT DISTINCT transaction_id 
              FROM bronze.transactions 
              WHERE is_deleted = true

-- Delete from Silver (MERGE with DELETE)
MERGE INTO silver.transactions AS target
USING deleted_ids AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN DELETE
```

**Output:**
```json
{
  "customer_id": "USER_0331",
  "bronze_deleted": 1309,
  "silver_before": 1240,
  "silver_after": 0,
  "records_deleted": 1240
}
```

---

## 🚀 How to Run

### Prerequisites
- Bronze layer loaded with data
- Dataproc cluster running
- Silver tables created (from Blog 3 setup)

### Manual Execution (No Airflow)

**1. Silver Incremental Load:**
```bash
# Step 1: Validate (read Bronze, dedupe, write staging)
gcloud dataproc jobs submit pyspark \
  gs://delta-lake-payment-gateway-476820/airflow/jobs/validate_silver.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1

# Step 2: Load (MERGE staging → silver)
gcloud dataproc jobs submit pyspark \
  gs://delta-lake-payment-gateway-476820/airflow/jobs/load_silver.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1
```

**2. Silver Full Refresh:**
```bash
gcloud dataproc jobs submit pyspark \
  gs://delta-lake-payment-gateway-476820/airflow/jobs/silver_full_refresh.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1
```

**3. GDPR Deletion:**
```bash
# Step 1: Mark deleted in Bronze
gcloud dataproc jobs submit pyspark \
  gs://delta-lake-payment-gateway-476820/airflow/jobs/bronze_mark_deleted_by_customer.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1 \
  -- \
  --customer_id=USER_0331

# Step 2: Propagate to Silver (hard delete)
gcloud dataproc jobs submit pyspark \
  gs://delta-lake-payment-gateway-476820/airflow/jobs/silver_propagate_deletes.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1 \
  -- \
  USER_0331
```

**Full command reference:** See `/docs/MANUAL_COMMANDS.md`

---

## 📊 Results & Metrics

**Test Dataset:**
- Bronze: 1,462,765 records (with audit trail)
- Silver: 1,379,914 records (deduplicated)
- Duplicates removed: 82,851 (5.66%)

**Performance:**
- Incremental (0 records): 30 sec
- Incremental (5,258 records): 45 sec
- Full refresh (1.4M records): 69 sec
- GDPR deletion: 35 sec

**Data Quality:**
- Tier 1 filtered (quarantined): 0% (already blocked in Bronze)
- Tier 2 filtered (bad data): 2.67% (filtered out)
- Clean data in Silver: 97.33%

---

## 🧪 Testing

### Validation Queries (BigQuery)

**1. Check Deduplication:**
```sql
-- Should return 0 rows
SELECT transaction_id, COUNT(*) as versions
FROM `silver_dataset.transactions`
GROUP BY transaction_id
HAVING COUNT(*) > 1;
```

**2. Verify Data Quality Filtering:**
```sql
-- Should return 0 rows (bad data excluded)
SELECT COUNT(*) 
FROM `silver_dataset.transactions` s
WHERE EXISTS (
    SELECT 1 FROM `bronze_dataset.transactions` b
    WHERE b.transaction_id = s.transaction_id
      AND b.data_quality_flag = 'FAILED_VALIDATION'
);
```

**3. Verify GDPR Hard Delete:**
```sql
-- Should show 0 in Silver, >0 in Bronze
SELECT 
    (SELECT COUNT(*) FROM `bronze_dataset.transactions` 
     WHERE customer_id = 'USER_0331' AND is_deleted = true) as bronze_deleted,
    (SELECT COUNT(*) FROM `silver_dataset.transactions` 
     WHERE customer_id = 'USER_0331') as silver_exists;
```

**Full test suite:** See `/silver/TESTING_QUERIES.sql` (10 validation queries)

---

## 🔍 Common Issues

### Issue 1: "records_read: 0 on first incremental run"

**Symptom:** First Silver incremental shows 0 records even though Bronze has data.

**Cause:** Bronze watermark = max(Bronze timestamp). Silver incremental only processes NEWER data.

**Fix:** Run `silver_full_refresh.py` for initial load.

---

### Issue 2: "Deduplication not working"

**Symptom:** Silver still has duplicate transaction_ids.

**Cause:** Window function not excluding `row_num` column.

**Fix:** Explicit column selection (already implemented):
```python
deduped_df.drop("row_num").createOrReplaceTempView("silver_staging")
```

---

### Issue 3: "GDPR deletion not propagating"

**Symptom:** Customer deleted in Bronze, still exists in Silver.

**Cause:** Forgot to run `silver_propagate_deletes.py`.

**Fix:** Always run both jobs:
1. `bronze_mark_deleted_by_customer.py` (soft delete)
2. `silver_propagate_deletes.py` (hard delete)

**More troubleshooting:** See `/silver/RUNBOOK.md`

---

## 🛠️ Data Generator Configuration

**Location:** `/data_generator/generate_payment_data.py`

**Silver-Specific Test Data:**
```python
# Data quality issues (for filtering tests)
TIER1_ISSUES_PCT = 0.67   # Quarantined (not in Bronze/Silver)
TIER2_ISSUES_PCT = 60.67  # Flagged (Bronze only, filtered in Silver)
TIER3_ISSUES_PCT = 1.33   # Auto-fixed (in Bronze/Silver)

# Silver layer tests
SOFT_DELETE_COUNT = 50        # Per day (GDPR test data)
LATE_ARRIVAL_COUNT = 50       # Per day (late arrival handling)
STATUS_UPDATE_COUNT = 100     # Day 4+ (status change tracking)
EXTRA_DUPLICATES_COUNT = 50   # Per day (deduplication test)

# Time-aware incremental (Day 4+)
FRESH_DATA_PCT = 0.30   # 30% have updated_at between watermark and NOW
HISTORICAL_DATA_PCT = 0.70  # 70% random historical timestamps
```

**Usage:**
```bash
cd data_generator
python generate_payment_data.py
# Output: generated_data/day1.csv, day2.csv, ..., day100.csv
```

**What It Generates:**
- 15,000 transactions per day
- 0.67% Tier 1 failures (quarantine)
- 60.67% Tier 2 violations (flagged)
- 50 soft deletes per day (GDPR testing)
- 50 late arrivals per day (timestamp < watermark)
- 100 status updates per day (Pending → Successful)

---

## 📝 Key Learnings

### 1. Why Dedup in Silver, Not Bronze?

**Bronze = Audit Trail**  
Keeping all versions is the POINT. If we deduped, we'd lose compliance history.

**Silver = Analytics**  
Queries need current state. Scanning 3 versions per transaction wastes resources.

---

### 2. Why Hard Delete in Silver?

**Legal Requirement:**  
GDPR says "delete all personal data." Soft delete = data still exists.

**Bronze Exception:**  
Audit logs can keep deleted data for compliance (with proper access controls).

---

### 3. Why Separate Staging Table?

**Alternative:** Write directly to Silver  
**Problem:** If MERGE fails halfway, Silver has incomplete data

**Solution:** Staging table  
1. Validate → staging (safe, can retry)
2. MERGE staging → silver (atomic operation)

---

## 🎯 Next Steps

### For This Layer:
- ✅ Incremental load working
- ✅ Full refresh working
- ✅ GDPR deletion working
- ⏸️ Backfill (skipped - incremental pulls all Bronze records)

### For Gold Layer (Blog 3c):
- Star schema design
- SCD Type 2 for customers/merchants
- Fact table with surrogate keys
- BI-optimized queries

---

## 📚 Additional Resources

- **Runbook:** `/silver/RUNBOOK.md` (operations guide)
- **Developer Guide:** `/silver/HELPER.md` (extending the code)
- **Known Issues:** `/docs/KNOWN_ISSUES.md` (side effects & limitations)
- **Manual Commands:** `/docs/MANUAL_COMMANDS.md` (all 9 jobs)
- **Blog Post:** [Link to Medium article when published]

---

**Built with ❤️ using Delta Lake, Spark, and extensive testing**