# Silver Layer Operations Runbook

**Purpose:** Practical guide for Silver layer operations, troubleshooting, and maintenance  
**Audience:** Data engineers, on-call operators

---

## 📋 Table of Contents

1. [When to Run Each Job](#when-to-run-each-job)
2. [GDPR Deletion Workflow](#gdpr-deletion-workflow)
3. [Troubleshooting Guide](#troubleshooting-guide)
4. [Performance Expectations](#performance-expectations)
5. [Recovery Procedures](#recovery-procedures)

---

## When to Run Each Job

### silver_incremental_load (Automated - Daily 3 AM)

**Purpose:** Process new Bronze data since last Silver run

**Trigger:** Automatically via Airflow DAG after Bronze completes

**When to run manually:**
- Bronze loaded new data outside schedule
- Previous Silver run failed
- Testing new deployment

**Command:**
```bash
# Step 1: Validate
gcloud dataproc jobs submit pyspark \
  gs://bucket/jobs/validate_silver.py \
  --cluster=dataproc-cluster \
  --region=us-central1

# Step 2: Load (only if validate succeeds)
gcloud dataproc jobs submit pyspark \
  gs://bucket/jobs/load_silver.py \
  --cluster=dataproc-cluster \
  --region=us-central1
```

**Expected outcome:**
- 0-5,000 records processed (typical day)
- Duration: 30-60 seconds
- Silver watermark advances

**When it processes 0 records:**  
✅ **Normal!** Means Bronze hasn't loaded new data since Silver's last run.

---

### silver_full_refresh (Manual - On Demand)

**Purpose:** Rebuild Silver from ALL Bronze data

**When to run:**
1. **Initial setup** (Silver is empty)
2. **After Bronze full refresh** (Bronze rebuilt from scratch)
3. **After schema changes** (added/removed columns)
4. **For testing/validation** (compare Silver vs Bronze)

**Command:**
```bash
gcloud dataproc jobs submit pyspark \
  gs://bucket/jobs/silver_full_refresh.py \
  --cluster=dataproc-cluster \
  --region=us-central1
```

**Expected outcome:**
- Processes ALL Bronze records (e.g., 1.4M rows)
- Duration: 60-120 seconds
- Silver completely replaced
- Watermark reset to max(Bronze.updated_at)

**⚠️ Warning:** This DELETES all Silver data before reload. Not reversible!

---

### GDPR Deletion (Manual - Compliance Request)

**See full workflow:** [GDPR Deletion Workflow](#gdpr-deletion-workflow)

---

## GDPR Deletion Workflow

### Step-by-Step Process

**Scenario:** Customer requests deletion under GDPR/CCPA

**Timeline:** Complete within 30 days of request (legal requirement)

---

### Step 1: Verify Customer Exists

**Before deletion, confirm customer has data:**
```sql
-- BigQuery query
SELECT 
    COUNT(*) as bronze_records,
    MIN(transaction_timestamp) as first_transaction,
    MAX(transaction_timestamp) as last_transaction,
    SUM(amount) as lifetime_value
FROM `bronze_dataset.transactions`
WHERE customer_id = 'USER_0331'
  AND (is_deleted = false OR is_deleted IS NULL);
```

**If 0 records:** Customer not found, nothing to delete

**If >0 records:** Proceed with deletion

---

### Step 2: Mark Deleted in Bronze (Soft Delete)

**Command:**
```bash
gcloud dataproc jobs submit pyspark \
  gs://bucket/jobs/bronze_mark_deleted_by_customer.py \
  --cluster=dataproc-cluster \
  --region=us-central1 \
  -- \
  --customer_id=USER_0331
```

**What it does:**
- Sets `is_deleted = true` for all customer records
- Sets `deleted_at = current_timestamp()`
- Updates `delta_change_type = 'DELETE'`
- Writes audit log to `bronze.job_control`

**Expected output:**
```json
{
  "customer_id": "USER_0331",
  "total_records": 1240,
  "already_deleted": 0,
  "newly_deleted": 1240,
  "deletion_timestamp": "2026-01-03 14:30:45"
}
```

**Check job status:**
```bash
gcloud dataproc jobs wait <JOB_ID> --region=us-central1
```

---

### Step 3: Verify Bronze Deletion

**Query:**
```sql
-- Should return 1240 records with is_deleted = true
SELECT 
    customer_id,
    COUNT(*) as deleted_records,
    MAX(deleted_at) as deletion_time
FROM `bronze_dataset.transactions`
WHERE customer_id = 'USER_0331'
  AND is_deleted = true
GROUP BY customer_id;
```

**If 0 records:** Job failed, check logs

---

### Step 4: Propagate to Silver (Hard Delete)

**Command:**
```bash
gcloud dataproc jobs submit pyspark \
  gs://bucket/jobs/silver_propagate_deletes.py \
  --cluster=dataproc-cluster \
  --region=us-central1 \
  -- \
  USER_0331
```

**What it does:**
- Finds deleted records in Bronze
- **Permanently deletes** from Silver (MERGE with DELETE)
- Writes audit log to `silver.job_control`

**Expected output:**
```json
{
  "customer_id": "USER_0331",
  "deleted_in_bronze": 1240,
  "silver_before": 1240,
  "silver_after": 0,
  "records_deleted": 1240
}
```

---

### Step 5: Verify Silver Deletion

**Query:**
```sql
-- Should return 0 records
SELECT COUNT(*) 
FROM `silver_dataset.transactions`
WHERE customer_id = 'USER_0331';
```

**If >0 records:** Deletion failed, check logs

---

### Step 6: Generate Compliance Report

**For legal team:**
```sql
-- Audit trail query
SELECT 
    'Bronze Soft Delete' as action,
    job_name,
    status,
    records_written as records_affected,
    started_at,
    completed_at,
    error_message
FROM `bronze_dataset.job_control`
WHERE job_name = 'bronze_compliance_deletion'
  AND error_message LIKE '%USER_0331%'

UNION ALL

SELECT 
    'Silver Hard Delete' as action,
    job_name,
    status,
    records_written as records_affected,
    started_at,
    completed_at,
    error_message
FROM `silver_dataset.job_control`
WHERE job_name = 'silver_propagate_deletes'
  AND started_at > (SELECT MAX(started_at) FROM `bronze_dataset.job_control` 
                    WHERE job_name = 'bronze_compliance_deletion')
ORDER BY started_at;
```

**Expected:** 2 rows (Bronze + Silver), both status = 'SUCCESS'

---

### Step 7: Document Deletion

**Create ticket/log entry:**
```
Customer: USER_0331
Request Date: 2026-01-02
Deletion Date: 2026-01-03
Records Deleted: 1,240 (Bronze soft delete + Silver hard delete)
Completed By: [Your Name]
Verification: BigQuery queries confirm 0 records in Silver
Audit Log: bronze.job_control batch_id = [UUID]
```

---

### GDPR Deletion Checklist

- [ ] Customer ID verified (exists in data)
- [ ] Legal request documented (ticket/email)
- [ ] Bronze soft delete completed (`is_deleted = true`)
- [ ] Bronze deletion verified (query shows deleted records)
- [ ] Silver hard delete completed (MERGE DELETE)
- [ ] Silver deletion verified (query shows 0 records)
- [ ] Audit trail captured (job_control records)
- [ ] Compliance report generated (for legal team)
- [ ] Deletion documented (ticket closed)

**Timeline:** Steps 1-6 take ~10 minutes (automated), Step 7 is documentation

---

## Troubleshooting Guide

### Issue 1: Silver Incremental Processes 0 Records

**Symptom:**
```json
{
  "records_read": 0,
  "records_to_staging": 0,
  "note": "No new Bronze data since last Silver run"
}
```

**Is this an error?** ❌ **No!** This is normal.

**Explanation:**  
Silver reads Bronze where `updated_at > silver_watermark`. If Bronze hasn't loaded new data, Silver correctly processes 0 records.

**When to investigate:**
- Bronze loaded new data (check `bronze.job_control`)
- But Silver still shows 0 records
- **Then:** Watermark issue (see below)

---

### Issue 2: Silver Watermark Not Advancing

**Symptom:** Silver processes same records repeatedly

**Check watermark:**
```sql
SELECT 
    MAX(last_processed_timestamp) as silver_watermark
FROM `silver_dataset.job_control`
WHERE job_name = 'silver_incremental_load'
  AND status = 'SUCCESS';
```

**Compare to Bronze:**
```sql
SELECT 
    MAX(updated_at) as bronze_max_timestamp
FROM `bronze_dataset.transactions`;
```

**If silver_watermark < bronze_max_timestamp:**  
Silver is behind, should process records

**If silver_watermark = bronze_max_timestamp:**  
Silver is caught up, 0 records is correct

**Fix stuck watermark:**
1. Check `silver.job_control` for failed jobs
2. Re-run Silver incremental manually
3. If still stuck, run full refresh (resets watermark)

---

### Issue 3: Deduplication Not Working

**Symptom:** Silver has duplicate transaction_ids

**Check duplicates:**
```sql
SELECT transaction_id, COUNT(*) as versions
FROM `silver_dataset.transactions`
GROUP BY transaction_id
HAVING COUNT(*) > 1
LIMIT 10;
```

**If >0 duplicates:**
1. Check `validate_silver.py` has ROW_NUMBER dedup
2. Check `row_num` column excluded from staging
3. Re-run Silver full refresh

**Prevention:**  
Always use explicit column list (no `SELECT *`)

---

### Issue 4: GDPR Deletion Incomplete

**Symptom:** Customer still exists in Silver after deletion

**Step-by-step debug:**

**1. Check Bronze deletion:**
```sql
SELECT 
    is_deleted,
    deleted_at,
    COUNT(*) 
FROM `bronze_dataset.transactions`
WHERE customer_id = 'USER_0331'
GROUP BY is_deleted, deleted_at;
```

Expected: All records have `is_deleted = true`

**2. Check Silver before propagation:**
```sql
SELECT COUNT(*) 
FROM `silver_dataset.transactions`
WHERE customer_id = 'USER_0331';
```

Expected: >0 before propagation, 0 after

**3. Check propagation job logs:**
```bash
gcloud dataproc jobs describe <JOB_ID> --region=us-central1
```

Look for: `records_deleted` in output

**4. Re-run propagation:**
```bash
# Propagate again (idempotent)
gcloud dataproc jobs submit pyspark \
  gs://bucket/jobs/silver_propagate_deletes.py \
  --cluster=dataproc-cluster \
  --region=us-central1 \
  -- \
  USER_0331
```

---

### Issue 5: Airflow DAG Stuck "Running"

**Symptom:** Silver DAG shows "Running" for >30 minutes

**Check cluster:**
```bash
gcloud dataproc clusters describe dataproc-cluster \
  --region=us-central1 \
  --format="value(status.state)"
```

**If "RUNNING":** Cluster is up, check job

**If "ERROR"/"STOPPED":** Cluster crashed, recreate

**Check Dataproc jobs:**
```bash
gcloud dataproc jobs list \
  --cluster=dataproc-cluster \
  --region=us-central1 \
  --limit=5
```

Look for: Most recent job status

**If job "RUNNING" >10 min:**  
Might be stuck, check Spark UI or kill job

**If job "FAILED":**  
Check error in Airflow logs, fix, re-run

---

## Performance Expectations

### Silver Incremental Load

| Scenario | Records | Duration | Cost |
|----------|---------|----------|------|
| No new data | 0 | 30 sec | $0.003 |
| Typical day | 1,000-5,000 | 45 sec | $0.005 |
| Heavy day | 10,000-20,000 | 90 sec | $0.010 |
| Backlog (week) | 50,000-100,000 | 3-5 min | $0.030 |

**Cluster:** n1-standard-2 (1 master + 2 workers, ephemeral)

---

### Silver Full Refresh

| Bronze Records | Silver Records | Duration | Cost |
|----------------|----------------|----------|------|
| 100K | 95K | 25 sec | $0.003 |
| 500K | 475K | 45 sec | $0.005 |
| 1.4M | 1.38M | 69 sec | $0.008 |
| 5M | 4.9M | 3-4 min | $0.025 |

**Deduplication overhead:** ~5% (ROW_NUMBER window function)

---

### GDPR Deletion

| Records to Delete | Bronze Duration | Silver Duration | Total |
|-------------------|-----------------|-----------------|-------|
| 100 | 20 sec | 15 sec | 35 sec |
| 1,000 | 25 sec | 20 sec | 45 sec |
| 10,000 | 45 sec | 35 sec | 80 sec |
| 100,000 | 2 min | 90 sec | 3.5 min |

**Note:** Bronze is UPDATE (slower), Silver is DELETE (faster)

---

## Recovery Procedures

### Scenario 1: Silver Data Corrupted

**Symptoms:**
- Duplicate transaction_ids
- Missing records (count < Bronze)
- Schema mismatch errors

**Recovery:**
```bash
# 1. Stop all Silver DAGs in Airflow UI

# 2. Run full refresh (rebuilds from Bronze)
gcloud dataproc jobs submit pyspark \
  gs://bucket/jobs/silver_full_refresh.py \
  --cluster=dataproc-cluster \
  --region=us-central1

# 3. Verify record counts
# Should match: Bronze unique IDs = Silver total rows

# 4. Re-enable Silver DAGs
```

**Duration:** 1-5 minutes depending on data size

---

### Scenario 2: Watermark Stuck (Silver Not Processing)

**Symptoms:**
- Silver incremental always processes 0 records
- But Bronze has new data

**Recovery:**
```sql
-- 1. Check current watermark
SELECT MAX(last_processed_timestamp) 
FROM `silver_dataset.job_control`
WHERE job_name = 'silver_incremental_load';

-- 2. Manually reset watermark (if stuck)
INSERT INTO silver.job_control VALUES (
    'silver_watermark_reset',
    'silver',
    'manual-reset-001',
    'manual',
    'SUCCESS',
    CURRENT_DATE(),
    NULL, NULL,
    CAST('2025-12-25 00:00:00' AS TIMESTAMP),  -- Set to date before stuck point
    NULL,
    0, 0, 0, 0,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    0, 0, 3, 
    'Manual watermark reset',
    'manual', 'none', 'none'
);

-- 3. Re-run Silver incremental
-- Should now process records after new watermark
```

---

### Scenario 3: Accidental GDPR Deletion

**Symptoms:**
- Deleted wrong customer
- Deletion request was duplicate/error

**Recovery:**

**For Silver (Hard Deleted):**
❌ **Cannot recover directly** (data permanently deleted)

✅ **Can recover from Bronze:**
```bash
# 1. Check Bronze still has data (soft deleted)
# Bronze keeps is_deleted = true records

# 2. Un-delete in Bronze
gcloud dataproc jobs submit pyspark \
  --cluster=dataproc-cluster \
  --region=us-central1 \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
  <<EOF
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

spark.sql("""
    UPDATE bronze.transactions
    SET is_deleted = false,
        deleted_at = NULL,
        delta_change_type = 'UNDELETE'
    WHERE customer_id = 'USER_0331'
      AND is_deleted = true
""")
EOF

# 3. Re-run Silver incremental (will pick up un-deleted records)
gcloud dataproc jobs submit pyspark \
  gs://bucket/jobs/validate_silver.py ...
gcloud dataproc jobs submit pyspark \
  gs://bucket/jobs/load_silver.py ...
```

**Prevention:**  
Always double-check customer ID before deletion!

---

## Monitoring & Alerts

### Key Metrics to Track

**1. Silver Lag (How far behind Bronze?)**
```sql
SELECT 
    TIMESTAMP_DIFF(
        (SELECT MAX(updated_at) FROM `bronze_dataset.transactions`),
        (SELECT MAX(last_processed_timestamp) FROM `silver_dataset.job_control` 
         WHERE job_name = 'silver_incremental_load' AND status = 'SUCCESS'),
        MINUTE
    ) as lag_minutes;
```

**Alert if:** lag_minutes > 60 (Silver hasn't run in over 1 hour)

---

**2. Deduplication Effectiveness**
```sql
SELECT 
    b.unique_txns as bronze_unique,
    s.total_rows as silver_total,
    ABS(b.unique_txns - s.total_rows) as discrepancy
FROM 
    (SELECT COUNT(DISTINCT transaction_id) as unique_txns 
     FROM `bronze_dataset.transactions`) b,
    (SELECT COUNT(*) as total_rows 
     FROM `silver_dataset.transactions`) s;
```

**Alert if:** discrepancy > 100 (deduplication issue)

---

**3. Job Success Rate (Last 24 Hours)**
```sql
SELECT 
    job_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 1) as success_rate
FROM `silver_dataset.job_control`
WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY job_name;
```

**Alert if:** success_rate < 95%

---

## Quick Reference Commands

**Check Silver status:**
```bash
# Latest job
gcloud dataproc jobs list --cluster=dataproc-cluster --region=us-central1 --limit=1

# Silver watermark
bq query "SELECT MAX(last_processed_timestamp) FROM silver_dataset.job_control 
          WHERE job_name='silver_incremental_load' AND status='SUCCESS'"

# Record counts
bq query "SELECT 'Bronze' as layer, COUNT(*) as records FROM bronze_dataset.transactions
          UNION ALL SELECT 'Silver', COUNT(*) FROM silver_dataset.transactions"
```

---

**Emergency stop:**
```bash
# Pause all Silver DAGs in Airflow UI
# Or kill running job:
gcloud dataproc jobs kill <JOB_ID> --region=us-central1
```

---

**Full pipeline reset:**
```bash
# WARNING: Deletes all Silver data!
bq query "DELETE FROM silver_dataset.transactions WHERE 1=1"
bq query "DELETE FROM silver_dataset.job_control WHERE 1=1"

# Rebuild from Bronze
gcloud dataproc jobs submit pyspark gs://bucket/jobs/silver_full_refresh.py ...
```

---

## Contact & Escalation

**For operational issues:**
- Check this runbook first
- Check `/docs/KNOWN_ISSUES.md` for common problems
- Review Dataproc job logs
- Query `job_control` tables for metadata

**Escalation criteria:**
- Data loss suspected
- Compliance deadline at risk (GDPR request)
- Silver lag > 4 hours
- Multiple job failures (>5 in 24h)

---

**Last Updated:** January 2026 | **On-Call:** Data Engineering Team