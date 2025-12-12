# Bronze Layer - Testing Guide

**Last Updated:** 2025-12-07  
**Purpose:** Validate Bronze layer functionality with test scenarios

---

## Prerequisites

✅ Dataproc cluster running (or DAGs will create ephemeral)  
✅ Test data uploaded to GCS  
✅ BigQuery external tables configured  
✅ Airflow Composer environment active  

---

## Test Data Overview

**Location:** `gs://delta-lake-payment-gateway-476820/raw/20241202/`

**Files:**
- `day1_transactions.csv` (2025-11-29) - ~15,000 rows
- `day2_transactions.csv` (2025-11-30) - ~15,000 rows
- `day3_transactions.csv` (2025-12-01) - ~15,000 rows
- `day4_transactions.csv` (2025-12-02) - ~15,000 rows
- `day5_transactions.csv` (2025-12-03) - ~15,000 rows
- `day6_transactions.csv` (2025-12-04) - ~15,000 rows

**Total:** ~90,000 rows

**Data Characteristics:**
- Tier 1 failures: ~0.67% (100 rows/day) → NULL transaction_id, NULL amount
- Tier 2 violations: ~2.67% (400 rows/day) → Negative amounts, future timestamps
- Tier 3 defaults: ~1.33% (200 rows/day) → NULL device_type, location_type
- Status updates: ~100 rows/day (Day 4+) → Same transaction_id, different updated_at
- Late arrivals: ~50 rows/day → Old transaction_timestamp, recent updated_at

---

## Test Scenario 1: Fresh Start (First Load)

**Objective:** Validate incremental load with empty bronze table

### Steps

**1. Clear bronze tables (if needed):**
```sql
-- Run in BigQuery or via Dataproc job
DELETE FROM bronze.transactions WHERE 1=1;
DELETE FROM bronze.quarantine WHERE 1=1;
DELETE FROM bronze.job_control WHERE 1=1;
```

**2. Trigger incremental DAG:**
- Go to Airflow UI → `bronze_incremental_load`
- Click "Trigger DAG"
- Wait 8-10 minutes

**3. Validate results:**

**Check job_control:**
```sql
SELECT 
  job_name,
  run_mode,
  status,
  records_read,
  records_written,
  records_quarantined,
  last_processed_timestamp
FROM bronze.job_control
ORDER BY completed_at DESC
LIMIT 1;
```

**Expected:**
```
job_name: bronze_incremental_load
run_mode: incremental
status: SUCCESS
records_read: 90000
records_written: 89800
records_quarantined: 100
last_processed_timestamp: 2025-12-30 22:52:54
```

**Check bronze.transactions:**
```sql
SELECT COUNT(*) as total_records FROM bronze.transactions;
-- Expected: 89,800 (90K - 100 tier1 - 100 late status updates not counted)
```

**Check quarantine:**
```sql
SELECT 
  error_reason,
  COUNT(*) as count
FROM bronze.quarantine
GROUP BY error_reason;
```

**Expected:**
```
NULL_TRANSACTION_ID: ~33
NULL_AMOUNT: ~33
NULL_TIMESTAMP: ~33
```

**Check for history (status updates):**
```sql
SELECT 
  transaction_id,
  COUNT(*) as version_count
FROM bronze.transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC
LIMIT 5;
```

**Expected:** ~75 transactions with 2 versions (status updates)

---

## Test Scenario 2: Incremental Load (No New Data)

**Objective:** Validate watermark filtering works

### Steps

**1. Trigger incremental DAG again** (same data, no new files)

**2. Check logs:**
```
validate_bronze output:
Records to process: 0
RESULT_JSON: {"records_to_merge": 0, ...}

load_bronze output:
No records to merge. Writing metadata only.
```

**3. Validate job_control:**
```sql
SELECT 
  batch_id,
  records_read,
  records_written,
  last_processed_timestamp
FROM bronze.job_control
WHERE job_name = 'bronze_incremental_load'
ORDER BY completed_at DESC
LIMIT 2;
```

**Expected:**
```
Row 1 (latest): records_written = 0, watermark unchanged
Row 2 (previous): records_written = 89800
```

---

## Test Scenario 3: Backfill (Specific Dates)

**Objective:** Reprocess Nov 29 - Dec 1 only

### Steps

**1. Trigger backfill DAG with params:**
```json
{
  "start_date": "2025-11-29",
  "end_date": "2025-12-01"
}
```

**2. Validate results:**
```sql
SELECT 
  job_name,
  run_mode,
  start_date,
  end_date,
  records_written
FROM bronze.job_control
WHERE job_name = 'bronze_backfill'
ORDER BY completed_at DESC
LIMIT 1;
```

**Expected:**
```
run_mode: backfill
start_date: 2025-11-29
end_date: 2025-12-01
records_written: 44804 (3 days × ~15K)
```

**3. Check delta_change_type:**
```sql
SELECT 
  delta_change_type,
  COUNT(*) as count
FROM bronze.transactions
WHERE DATE(transaction_timestamp) BETWEEN '2025-11-29' AND '2025-12-01'
GROUP BY delta_change_type;
```

**Expected:**
```
BACKFILL: 44804  (if reprocessing existing data)
INSERT: X (original load count if first time)
```

---

## Test Scenario 4: Backfill Validation Errors

**Objective:** Test parameter validation

### Test 4a: Missing Parameters
```json
{}
```

**Expected:** `validate_params` task fails with:
```
Missing required parameters. Please trigger with config:
{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}
```

### Test 4b: Invalid Date Format
```json
{
  "start_date": "11/29/2025",
  "end_date": "12/01/2025"
}
```

**Expected:** Fails with "Invalid date format. Use YYYY-MM-DD."

### Test 4c: Backwards Dates
```json
{
  "start_date": "2025-12-01",
  "end_date": "2025-11-29"
}
```

**Expected:** Fails with "start_date must be <= end_date"

### Test 4d: Range Too Large
```json
{
  "start_date": "2025-01-01",
  "end_date": "2025-12-31"
}
```

**Expected:** Fails with "Date range too large: 365 days. Maximum allowed: 90 days."

---

## Test Scenario 5: Full Refresh

**Objective:** Complete data reload

### Steps

**1. Note current row count:**
```sql
SELECT COUNT(*) FROM bronze.transactions;
-- Note the number
```

**2. Trigger full refresh WITHOUT confirmation:**
```json
{
  "confirm_full_refresh": "NO"
}
```

**Expected:** `validate_confirmation` task fails with warning message

**3. Trigger full refresh WITH confirmation:**
```json
{
  "confirm_full_refresh": "YES"
}
```

**4. Validate results:**
```sql
-- Check all data reloaded
SELECT COUNT(*) FROM bronze.transactions;
-- Expected: 89,800

-- Check change type
SELECT 
  delta_change_type,
  COUNT(*) as count
FROM bronze.transactions
GROUP BY delta_change_type;
-- Expected: FULL_REFRESH: 89,800

-- Check watermark reset
SELECT 
  last_processed_timestamp
FROM bronze.job_control
WHERE job_name = 'bronze_full_refresh'
ORDER BY completed_at DESC
LIMIT 1;
-- Expected: 2025-12-30 22:52:54 (latest timestamp from data)
```

---

## Test Scenario 6: Data Quality Validation

**Objective:** Verify 3-tier validation working

### Tier 1 Validation

**Query quarantined records:**
```sql
SELECT 
  transaction_id,
  amount,
  transaction_timestamp,
  error_reason,
  error_tier
FROM bronze.quarantine
LIMIT 10;
```

**Expected:** Records with NULL critical fields, error_tier = 'TIER_1'

### Tier 2 Validation

**Query flagged records:**
```sql
SELECT 
  transaction_id,
  amount,
  transaction_status,
  data_quality_flag
FROM bronze.transactions
WHERE data_quality_flag = 'FAILED_VALIDATION'
LIMIT 10;
```

**Expected:** Records with negative amounts, future timestamps, but still loaded to bronze

### Tier 3 Validation

**Query defaulted fields:**
```sql
SELECT 
  transaction_id,
  device_type,
  location_type,
  product_name
FROM bronze.transactions
WHERE device_type = 'UNKNOWN'
   OR location_type = 'NOT_AVAILABLE'
   OR product_name = 'NOT_AVAILABLE'
LIMIT 10;
```

**Expected:** Records with defaults applied (were NULL in source)

---

## Test Scenario 7: Status Updates (Composite Key)

**Objective:** Verify multiple versions per transaction_id

### Steps

**1. Find transactions with status updates:**
```sql
SELECT 
  transaction_id,
  updated_at,
  transaction_status,
  delta_change_type
FROM bronze.transactions
WHERE transaction_id IN (
  SELECT transaction_id
  FROM bronze.transactions
  GROUP BY transaction_id
  HAVING COUNT(*) > 1
)
ORDER BY transaction_id, updated_at
LIMIT 20;
```

**Expected:**
```
TXN001 | 2025-11-29 10:00:00 | Pending   | INSERT
TXN001 | 2025-11-30 15:30:00 | Completed | MERGE
TXN001 | 2025-12-01 09:00:00 | Refunded  | MERGE
```

**2. Verify status changes:**
```sql
WITH txn_versions AS (
  SELECT 
    transaction_id,
    updated_at,
    transaction_status,
    LAG(transaction_status) OVER (PARTITION BY transaction_id ORDER BY updated_at) as prev_status
  FROM bronze.transactions
)
SELECT 
  transaction_id,
  updated_at,
  CONCAT(prev_status, ' → ', transaction_status) as status_change
FROM txn_versions
WHERE transaction_status != prev_status
LIMIT 10;
```

**Expected:** See actual status transitions (Failed → Successful, Successful → Refunded, etc.)

---

## Test Scenario 8: Late Arrivals

**Objective:** Verify late arrival tracking

### Steps

**1. Query late arrivals:**
```sql
SELECT 
  transaction_id,
  transaction_timestamp,
  updated_at,
  is_late_arrival,
  arrival_delay_hours
FROM bronze.transactions
WHERE is_late_arrival = TRUE
ORDER BY arrival_delay_hours DESC
LIMIT 10;
```

**Expected:** Records where `updated_at` is 3+ days after `transaction_timestamp`

**2. Count late arrivals:**
```sql
SELECT COUNT(*) as late_arrival_count
FROM bronze.transactions
WHERE is_late_arrival = TRUE;
```

**Expected:** ~50-150 records (depending on data generator settings)

---

## Test Scenario 9: Manual Job Submission

**Objective:** Test jobs outside Airflow (direct gcloud)

### Validate Bronze Job

```bash
gcloud dataproc jobs submit pyspark \
  gs://delta-lake-payment-gateway-476820/airflow/jobs/validate_bronze.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1 \
  -- gs://delta-lake-payment-gateway-476820/raw/20241202/day*.csv \
     NULL \
     batch-manual-test-001
```

**Check output:**
```bash
gcloud dataproc jobs wait <JOB_ID> --region=us-central1 2>&1 | grep "RESULT_JSON"
```

**Expected:**
```
RESULT_JSON:{"records_to_merge": 89800, "records_quarantined": 100, "late_arrivals": X}
```

### Load Bronze Job

```bash
gcloud dataproc jobs submit pyspark \
  gs://delta-lake-payment-gateway-476820/airflow/jobs/load_bronze.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1 \
  -- batch-manual-test-001 \
     bronze_incremental_load \
     incremental \
     90000 \
     100 \
     2025-12-07T10:00:00
```

**Expected:**
```
RESULT_JSON:{"status": "SUCCESS", "records_written": 89800, "new_watermark": "2025-12-30 22:52:54.000"}
```

### Backfill Job

```bash
gcloud dataproc jobs submit pyspark \
  gs://delta-lake-payment-gateway-476820/airflow/jobs/bronze_backfill.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1 \
  -- gs://delta-lake-payment-gateway-476820/raw/20241202/day*.csv \
     2025-11-29 \
     2025-12-01 \
     batch-backfill-manual-001
```

**Expected:**
```
RESULT_JSON:{"status": "SUCCESS", "records_processed": 44804, "records_quarantined": 100, "date_range": "2025-11-29 to 2025-12-01"}
```

### Full Refresh Job

```bash
gcloud dataproc jobs submit pyspark \
  gs://delta-lake-payment-gateway-476820/airflow/jobs/bronze_full_refresh.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1 \
  -- gs://delta-lake-payment-gateway-476820/raw/20241202/day*.csv \
     batch-full-refresh-manual-001
```

**Expected:**
```
RESULT_JSON:{"status": "SUCCESS", "records_loaded": 89800, "records_quarantined": 100, "new_watermark": "2025-12-30 22:52:54.000", "mode": "FULL_REFRESH"}
```

---

## Performance Benchmarks

**Expected Execution Times:**

| Job | Cluster | Data Volume | Expected Time |
|-----|---------|-------------|---------------|
| Incremental (first load) | Ephemeral | 90K rows | 8-10 min |
| Incremental (no new data) | Ephemeral | 0 rows | 6-8 min |
| Backfill (3 days) | Ephemeral | 45K rows | 8-9 min |
| Full Refresh | Ephemeral | 90K rows | 8-10 min |

**Cluster Breakdown:**
- Cluster creation: 3-4 min
- Job execution: 2-3 min
- Cluster deletion: 30-60 sec

**Manual Job (persistent cluster):**
- Job execution only: 2-3 min

---

## Troubleshooting Test Failures

### Job Shows 0 Records Processed

**Possible Causes:**
1. Watermark filtering out all data (incremental)
2. Date range has no matching data (backfill)
3. Raw CSV path incorrect

**Solution:**
```sql
-- Check watermark
SELECT last_processed_timestamp 
FROM bronze.job_control 
WHERE job_name = 'bronze_incremental_load'
ORDER BY completed_at DESC LIMIT 1;

-- Check raw data timestamps
gsutil cat gs://bucket/raw/20241202/day1_transactions.csv | head -5

-- Verify path exists
gsutil ls gs://bucket/raw/20241202/
```

---

### High Quarantine Rate (>5%)

**Check quarantine breakdown:**
```sql
SELECT 
  error_reason,
  COUNT(*) as count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
FROM bronze.quarantine
GROUP BY error_reason
ORDER BY count DESC;
```

**If unexpected:** Data generator may have bugs, check source CSV quality

---

### Duplicate Key Error in MERGE

**Error Message:** `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`

**Cause:** Staging table has duplicates on `(transaction_id, updated_at)`

**Solution:**
```sql
-- Find duplicates in staging
SELECT 
  transaction_id,
  updated_at,
  COUNT(*) as dup_count
FROM bronze.transactions_staging
GROUP BY transaction_id, updated_at
HAVING COUNT(*) > 1;
```

**Fix:** Check validate_bronze.py logic, should not produce duplicates

---

## Validation Checklist

After running all tests, verify:

- [ ] Incremental load works with watermark filtering
- [ ] Backfill validates date parameters correctly
- [ ] Full refresh requires confirmation
- [ ] Tier 1 validation quarantines critical errors
- [ ] Tier 2 validation flags business rule violations
- [ ] Tier 3 validation applies defaults
- [ ] Composite key allows multiple transaction versions
- [ ] Status updates tracked correctly
- [ ] Late arrivals flagged appropriately
- [ ] Job metadata written to job_control table
- [ ] BigQuery external tables queryable
- [ ] Manual gcloud submissions work
- [ ] DAGs retry on failure (3 attempts)
- [ ] Cluster auto-deletes after completion

---


## References

- **Blog Post:** _Coming soon_

- **GitHub Repository:**  
  https://github.com/mohamedkashifuddin/payment-gateway-dw/tree/main

- **Data Generator:**  
  - [`Data_Generator_3a_till_3e/Delta_Lake_Payment_Data_Generator.py`](./Data_Generator_3a_till_3e/Delta_Lake_Payment_Data_Generator.py)  
  - Generated outputs: [`Data_Generator_3a_till_3e/generated_data/`](./Data_Generator_3a_till_3e/generated_data/)

- **Architecture Document:**  
  [`docs/MIGRATION_DOC_COMPLETE.md`](./docs/MIGRATION_DOC_COMPLETE.md)

- **Data Lineage:**  
  [`docs/DATA_LINEAGE.md`](./docs/DATA_LINEAGE.md)

- **Schema Registry:**  
  [`docs/SCHEMA_REGISTRY.md`](./docs/SCHEMA_REGISTRY.md)

- **Validation Rules:**  
  [`docs/VALIDATION_RULES.md`](./docs/VALIDATION_RULES.md)

- **Bronze Layer Testing Guide:**  
  [`bronze/TESTING_GUIDE.md`](./bronze/TESTING_GUIDE.md)

- **Bronze Airflow DAGs:**  
  - [`bronze/dags/bronze_backfill_dag.py`](./bronze/dags/bronze_backfill_dag.py)  
  - [`bronze/dags/bronze_full_refresh_dag.py`](./bronze/dags/bronze_full_refresh_dag.py)  
  - [`bronze/dags/bronze_incremental_dag.py`](./bronze/dags/bronze_incremental_dag.py)

- **Bronze Job Scripts:**  
  - [`bronze/jobs/load_bronze.py`](./bronze/jobs/load_bronze.py)  
  - [`bronze/jobs/validate_bronze.py`](./bronze/jobs/validate_bronze.py)  
  - [`bronze/jobs/bronze_full_refresh.py`](./bronze/jobs/bronze_full_refresh.py)  
  - [`bronze/jobs/bronze_backfill.py`](./bronze/jobs/bronze_backfill.py)

- **Shared Utilities:**  
  - [`shared/generate_batch_id.py`](./shared/generate_batch_id.py)  
  - [`shared/read_watermark.py`](./shared/read_watermark.py)  
  - [`shared/write_watermark.py`](./shared/write_watermark.py)

---

**Testing Guide Complete ✅**