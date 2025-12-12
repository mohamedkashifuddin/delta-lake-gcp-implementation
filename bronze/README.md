# Bronze Layer - Payment Gateway Pipeline

**Last Updated:** 2025-12-07  
**Status:** Production-Ready ✅  
**Layer:** Raw Data Ingestion with Audit Trail

---

## Overview

The Bronze layer serves as the **raw data landing zone** with full audit history. It implements 3-tier data quality validation and supports multiple loading patterns for production flexibility.

**Key Characteristics:**
- **Full History:** Stores all transaction versions (status updates tracked)
- **Composite Key:** `(transaction_id, updated_at)` enables multiple versions per transaction
- **Data Quality:** 3-tier validation (Block, Flag, Fix)
- **Audit Trail:** Complete lineage for compliance and time travel queries

---

## Architecture 

```
Raw CSV Files (GCS)
    ↓
Bronze Layer (Full History)
    ├─ bronze.transactions (composite key)
    ├─ bronze.quarantine (Tier 1 failures)
    └─ bronze.job_control (watermark tracking)
    ↓
Silver Layer (Current State - deduplicates Bronze)
```

**Why this Architecture :**
- Bronze = Compliance/audit (all versions)
- Silver/Gold = Analytics (current state)
- Analysts query Bronze directly for point-in-time analysis when needed

---

## Tables

### bronze.transactions (23 columns)

**Primary Key:** `(transaction_id, updated_at)` - Composite key allows multiple versions

**Schema:**
- **Original 17 columns:** transaction_id → updated_at (from raw CSV)
- **CDC columns (2):** delta_change_type, delta_version
- **Tracking columns (4):** is_deleted, deleted_at, is_late_arrival, arrival_delay_hours

**Example Data:**
```
TXN001 | 2025-11-29 10:00:00 | Pending   | INSERT
TXN001 | 2025-11-30 15:30:00 | Completed | MERGE
TXN001 | 2025-12-01 09:00:00 | Refunded  | MERGE
```

**Storage:** `gs://delta-lake-payment-gateway-476820/bronze/transactions`

---

### bronze.quarantine (24 columns)

**Purpose:** Stores Tier 1 validation failures (critical errors)

**Additional Columns:**
- `error_reason`: Why quarantined (NULL_TRANSACTION_ID, NULL_AMOUNT, etc.)
- `error_tier`: Always 'TIER_1'
- `quarantined_at`: Timestamp of quarantine
- `processing_batch_id`: Which job quarantined it

**Action:** Manual review, fix data, resubmit to raw folder

**Storage:** `gs://delta-lake-payment-gateway-476820/quarantine/bronze_transactions`

---

### bronze.job_control (23 columns)

**Purpose:** Tracks job execution metadata and watermarks

**Key Columns:**
- `last_processed_timestamp`: Watermark for incremental loads
- `records_read`, `records_written`, `records_quarantined`: Metrics
- `status`: SUCCESS, FAILED, RUNNING
- `run_mode`: incremental, backfill, full_refresh

**Retention:** 90 days (moved to archive after)

**Storage:** `gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control`

---

## Data Quality - 3-Tier Validation

### Tier 1: Block & Quarantine (Critical)
**Rules:**
- `transaction_id IS NULL`
- `amount IS NULL`
- `transaction_timestamp IS NULL`
- `transaction_id` contains whitespace

**Action:** Write to `bronze.quarantine`, DO NOT load to bronze.transactions

---

### Tier 2: Flag & Load (Business Rules)
**Rules:**
- `amount < 0` (negative amount)
- `transaction_timestamp > CURRENT_TIMESTAMP()` (future timestamp)
- `merchant_id IS NULL` (unknown merchant)
- `transaction_status NOT IN ('Successful', 'Pending', 'Failed')` (invalid status)

**Action:** Load to bronze.transactions with `data_quality_flag = 'FAILED_VALIDATION'`

**Note:** Silver layer filters these out

---

### Tier 3: Fix & Load (Missing Optional Data)
**Rules:**
- `device_type IS NULL` → Default: 'UNKNOWN'
- `location_type IS NULL` → Default: 'NOT_AVAILABLE'
- `product_name IS NULL` → Default: 'NOT_AVAILABLE'
- `merchant_name IS NULL` → Default: 'UNKNOWN_MERCHANT'

**Action:** Apply defaults and load normally

---

## Jobs

### 1. validate_bronze.py

**Purpose:** Applies 3-tier validation, prepares data for loading

**Inputs:**
- `RAW_PATH`: CSV file pattern (e.g., `gs://bucket/raw/20241202/day*.csv`)
- `WATERMARK`: Last processed timestamp (NULL for first run)
- `BATCH_ID`: Unique job execution ID

**Outputs:**
- `bronze.transactions_staging`: Valid records ready for MERGE
- `bronze.quarantine`: Tier 1 failures
- `RESULT_JSON`: Metrics (records_to_merge, records_quarantined, late_arrivals)

**Validation Logic:**
1. Read raw CSV
2. Filter by watermark (if exists)
3. Apply Tier 1 → quarantine failures
4. Apply Tier 2 → flag violations
5. Apply Tier 3 → apply defaults
6. Write to staging table

**Location:** `gs://delta-lake-payment-gateway-476820/airflow/jobs/validate_bronze.py`

---

### 2. load_bronze.py

**Purpose:** MERGE validated data into bronze.transactions using composite key

**Inputs:**
- `BATCH_ID`: Job execution ID
- `JOB_NAME`: 'bronze_incremental_load'
- `RUN_MODE`: 'incremental'
- `RECORDS_READ`: Total records processed
- `RECORDS_QUARANTINED`: Count of quarantined records
- `STARTED_AT`: Job start timestamp

**Logic:**
```sql
MERGE INTO bronze.transactions t
USING bronze.transactions_staging s
ON t.transaction_id = s.transaction_id 
   AND t.updated_at = s.updated_at  -- Composite key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Key Feature:** Composite key allows multiple versions of same transaction_id

**Location:** `gs://delta-lake-payment-gateway-476820/airflow/jobs/load_bronze.py`

---

### 3. bronze_backfill.py

**Purpose:** Reprocess specific date ranges (e.g., Nov 29 - Dec 1)

**Inputs:**
- `RAW_PATH`: CSV file pattern
- `START_DATE`: Inclusive start date (YYYY-MM-DD)
- `END_DATE`: Inclusive end date (YYYY-MM-DD)
- `BATCH_ID`: Job execution ID

**Key Difference from Incremental:**
- Filters by `DATE(transaction_timestamp) BETWEEN start AND end`
- Sets `delta_change_type = 'BACKFILL'`
- No late arrival checks (intentional reprocessing)

**Use Cases:**
- Data correction (source system sent bad data)
- Historical data reload (onboard old transactions)
- Date-specific reprocessing after schema changes

**Location:** `gs://delta-lake-payment-gateway-476820/airflow/jobs/bronze_backfill.py`

---

### 4. bronze_full_refresh.py

**Purpose:** Complete reload of ALL data (destructive operation)

**Inputs:**
- `RAW_PATH`: CSV file pattern
- `BATCH_ID`: Job execution ID

**Key Differences:**
- Uses `INSERT OVERWRITE` (not MERGE)
- Processes all files (no date filter)
- Resets watermark to latest timestamp
- Sets `delta_change_type = 'FULL_REFRESH'`

**Use Cases:**
- Pipeline rebuild after major schema changes
- Disaster recovery
- Testing/validation (reset to known state)

**⚠️ WARNING:** Deletes all existing bronze data

**Location:** `gs://delta-lake-payment-gateway-476820/airflow/jobs/bronze_full_refresh.py`

---

## DAGs

### bronze_incremental_load

**Schedule:** Daily 2 AM  
**Trigger:** Automatic (cron schedule)

**Tasks:**
1. `generate_cluster_name`: Create unique cluster name
2. `read_watermark`: Get last processed timestamp from BigQuery
3. `create_dataproc_cluster`: Spin up ephemeral Dataproc cluster
4. `validate_bronze`: Run validate_bronze.py
5. `load_bronze`: Run load_bronze.py
6. `delete_dataproc_cluster`: Clean up cluster

**Cluster Config:**
- Master: n1-standard-2 (1 instance, 60GB disk)
- Workers: n1-standard-2 (2 instances, 60GB disk each)
- Image: 2.2-debian12
- Components: DELTA, JUPYTER

**Retry Logic:** 3 attempts, 5 min exponential backoff

**Expected Runtime:** ~8-10 minutes (cluster creation + job execution)

---

### bronze_backfill

**Schedule:** Manual trigger only  
**Trigger:** Airflow UI with date parameters

**Parameters:**
```json
{
  "start_date": "2025-11-29",
  "end_date": "2025-12-01"
}
```

**Tasks:**
1. `validate_params`: Check date format, range limits (max 90 days)
2. `create_dataproc_cluster`: Ephemeral cluster
3. `run_backfill`: Execute bronze_backfill.py
4. `delete_dataproc_cluster`: Cleanup

**Validation:**
- ✅ Date format: YYYY-MM-DD
- ✅ start_date <= end_date
- ✅ Range < 90 days (prevents accidents)
- ❌ Missing params → Fails before cluster creation

**Use Case Example:**
```
Scenario: Source system sent corrupted data on Nov 30
Action: 
1. Fix source data
2. Upload corrected CSV to /raw/
3. Trigger backfill DAG: {"start_date": "2025-11-30", "end_date": "2025-11-30"}
4. Bronze reprocesses only Nov 30
```

---

### bronze_full_refresh

**Schedule:** Manual trigger only  
**Trigger:** Airflow UI with confirmation

**Parameters:**
```json
{
  "confirm_full_refresh": "YES"
}
```

**Safety Feature:** Requires explicit "YES" confirmation, fails otherwise

**Tasks:**
1. `validate_confirmation`: Check user typed "YES"
2. `create_dataproc_cluster`: Ephemeral cluster
3. `run_full_refresh`: Execute bronze_full_refresh.py
4. `delete_dataproc_cluster`: Cleanup

**⚠️ DESTRUCTIVE OPERATION:**
- Deletes ALL bronze.transactions data
- Overwrites bronze.quarantine
- Resets watermark
- Use only for complete pipeline rebuild

**Reduced Retries:** 1 attempt (vs 3 for incremental) - full refresh is intentional

---

## Production Notes

### Path Structure (Current vs Production)

**Current (Portfolio Demo):**
```
/raw/20241202/
  ├─ day1_transactions.csv
  ├─ day2_transactions.csv
  └─ day6_transactions.csv
```

**Production Recommendation:**
```
/raw/YYYY/MM/DD/*.csv
  ├─ 2025/11/29/transactions.csv
  ├─ 2025/11/30/transactions.csv
  └─ 2025/12/01/transactions.csv
```

**Benefits:**
- Incremental: Scans only yesterday's folder (~15K rows)
- Backfill: Scans only specified date folders
- Full refresh: Uses wildcard `/raw/*/*/*/*.csv`

**Code Changes Required:**
- Spark jobs: Generate date-based paths dynamically
- DAGs: Remove RAW_PATH arg or adjust logic

---

## Monitoring & Operations

### Key Metrics to Track

**Query job_control table:**
```sql
-- Success rate (last 7 days)
SELECT 
  job_name,
  COUNT(*) as total_runs,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
  ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
FROM bronze.job_control
WHERE started_at >= CURRENT_DATE - INTERVAL 7 DAY
GROUP BY job_name
```

**Quarantine rate:**
```sql
-- Quarantine trend (last 30 days)
SELECT 
  DATE(started_at) as date,
  SUM(records_quarantined) as quarantined,
  SUM(records_written) as loaded,
  ROUND(100.0 * SUM(records_quarantined) / (SUM(records_written) + SUM(records_quarantined)), 2) as quarantine_rate_pct
FROM bronze.job_control
WHERE started_at >= CURRENT_DATE - INTERVAL 30 DAY
GROUP BY DATE(started_at)
ORDER BY date DESC
```

**Job duration:**
```sql
-- Average job duration by mode
SELECT 
  run_mode,
  AVG(duration_seconds) as avg_duration_sec,
  MAX(duration_seconds) as max_duration_sec
FROM bronze.job_control
WHERE status = 'SUCCESS'
  AND started_at >= CURRENT_DATE - INTERVAL 7 DAY
GROUP BY run_mode
```

---

### Alerts to Configure

**Critical:**
- Job failure rate > 10% in 24 hours
- Quarantine rate > 5% (spike indicates data quality issue)
- No successful run in 48 hours (pipeline stalled)

**Warning:**
- Job duration > 20 min (performance degradation)
- Watermark lag > 72 hours (falling behind)

---

## Troubleshooting

### Issue: "DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE"

**Cause:** Multiple rows with same `(transaction_id, updated_at)` in staging

**Solution:** Check validate_bronze.py - ensure no duplicates in staging table

---

### Issue: "No records to merge" but raw data exists

**Cause:** Watermark filter excludes all data

**Solution:**
1. Check watermark: `SELECT * FROM bronze.job_control ORDER BY completed_at DESC LIMIT 1`
2. Check raw data timestamps: Are they older than watermark?
3. If intentional reprocessing needed: Use backfill DAG

---

### Issue: High quarantine rate (> 5%)

**Cause:** Source system sending bad data

**Solution:**
1. Query quarantine table: `SELECT error_reason, COUNT(*) FROM bronze.quarantine GROUP BY error_reason`
2. Identify pattern (e.g., all NULL_AMOUNT from specific merchant)
3. Work with source team to fix upstream issue
4. After fix, use backfill to reload corrected data

---

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
