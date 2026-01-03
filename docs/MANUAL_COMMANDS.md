# Manual Dataproc Job Commands

Quick reference for running all Silver layer jobs manually via gcloud CLI.

---

## Prerequisites

```bash
export PROJECT_ID="grand-jigsaw-476820-t1"
export REGION="us-central1"
export CLUSTER="dataproc-delta-cluster-final"
export BUCKET="gs://delta-lake-payment-gateway-476820"
```

---

## 1. Bronze Incremental Load

```bash
# No parameters needed (uses watermark automatically)
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/validate_bronze.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  -- \
  ${BUCKET}/raw/YYYYMMDD/*.csv \
  NULL \
  $(uuidgen)

# Then load
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/load_bronze.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  -- \
  $(uuidgen) \
  bronze_incremental_load \
  incremental \
  <RECORDS_READ> \
  <RECORDS_QUARANTINED> \
  "$(date '+%Y-%m-%d %H:%M:%S')"
```

---

## 2. Bronze Backfill (Date Range)

```bash
# Parameters: RAW_PATH, START_DATE, END_DATE, BATCH_ID
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/bronze_backfill.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  -- \
  ${BUCKET}/raw/YYYYMMDD/*.csv \
  2025-12-29 \
  2026-01-02 \
  $(uuidgen)

# Example: Backfill Dec 29 - Jan 2
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/bronze_backfill.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  -- \
  gs://delta-lake-payment-gateway-476820/raw/20241202/*.csv \
  2025-12-29 \
  2026-01-02 \
  backfill-$(date +%Y%m%d%H%M%S)
```

---

## 3. Bronze Full Refresh

```bash
# Parameters: RAW_PATH, BATCH_ID
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/bronze_full_refresh.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  -- \
  ${BUCKET}/raw/YYYYMMDD/*.csv \
  $(uuidgen)

# Example: Full reload from all CSV files
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/bronze_full_refresh.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  -- \
  gs://delta-lake-payment-gateway-476820/raw/20241202/*.csv \
  full-refresh-$(date +%Y%m%d%H%M%S)
```

---

## 4. Silver Incremental Load (Validate)

```bash
# No parameters needed (reads Silver watermark automatically)
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/validate_silver.py \
  --cluster=${CLUSTER} \
  --region=${REGION}

# Get job ID to check results
gcloud dataproc jobs wait <JOB_ID> --region=${REGION} 2>&1 | grep "RESULT_JSON"
```

---

## 5. Silver Incremental Load (Load)

```bash
# No parameters needed (reads from staging)
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/load_silver.py \
  --cluster=${CLUSTER} \
  --region=${REGION}

# Get job ID to check results
gcloud dataproc jobs wait <JOB_ID> --region=${REGION} 2>&1 | grep "RESULT_JSON"
```

---

## 6. Silver Full Refresh

```bash
# No parameters needed (reads all Bronze data)
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/silver_full_refresh.py \
  --cluster=${CLUSTER} \
  --region=${REGION}

# Example with job tracking
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/silver_full_refresh.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  && gcloud dataproc jobs list --region=${REGION} --limit=1
```

---

## 7. GDPR Deletion (Bronze)

```bash
# Parameter: --customer_id=USER_XXXX
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/bronze_mark_deleted_by_customer.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  -- \
  --customer_id=USER_0331

# Example: Delete customer USER_0780
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/bronze_mark_deleted_by_customer.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  -- \
  --customer_id=USER_0780
```

---

## 8. GDPR Deletion Propagation (Silver)

```bash
# Parameter: CUSTOMER_ID (optional, if not provided = all deleted customers)
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/silver_propagate_deletes.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  -- \
  USER_0331

# Example: Propagate specific customer deletion
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/silver_propagate_deletes.py \
  --cluster=${CLUSTER} \
  --region=${REGION} \
  -- \
  USER_0780

# Example: Propagate ALL Bronze deletions to Silver
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/silver_propagate_deletes.py \
  --cluster=${CLUSTER} \
  --region=${REGION}
```

---

## 9. Alter Bronze Schema (One-Time Setup)

```bash
# Add data_quality_flag and validation_errors columns
# Only run ONCE during setup
gcloud dataproc jobs submit pyspark \
  ${BUCKET}/airflow/jobs/alter_bronze_add_validation_columns.py \
  --cluster=${CLUSTER} \
  --region=${REGION}
```

---

## Common Patterns

### Check Job Results

```bash
# Get latest job ID
JOB_ID=$(gcloud dataproc jobs list --region=${REGION} --limit=1 --format="value(reference.jobId)")

# Wait and extract JSON result
gcloud dataproc jobs wait ${JOB_ID} --region=${REGION} 2>&1 | grep "RESULT_JSON"
```

### Chain Jobs (Wait for Completion)

```bash
# Bronze → Silver pipeline
gcloud dataproc jobs submit pyspark ${BUCKET}/airflow/jobs/validate_bronze.py ... && \
gcloud dataproc jobs submit pyspark ${BUCKET}/airflow/jobs/load_bronze.py ... && \
gcloud dataproc jobs submit pyspark ${BUCKET}/airflow/jobs/validate_silver.py ... && \
gcloud dataproc jobs submit pyspark ${BUCKET}/airflow/jobs/load_silver.py ...
```

### Generate Unique Batch IDs

```bash
# UUID (preferred)
BATCH_ID=$(uuidgen)

# Timestamp-based
BATCH_ID="batch-$(date +%Y%m%d%H%M%S)"
```

---

## Troubleshooting

### Job Failed - Check Logs

```bash
gcloud dataproc jobs describe <JOB_ID> --region=${REGION}
```

### View Driver Logs

```bash
gcloud logging read \
  "resource.type=cloud_dataproc_job AND resource.labels.job_id=<JOB_ID>" \
  --limit=100 \
  --format=json
```

### List Recent Jobs

```bash
gcloud dataproc jobs list \
  --region=${REGION} \
  --limit=10 \
  --format="table(reference.jobId,status.state,placement.clusterName)"
```

---

## Notes

- **Batch IDs must be unique** - use `$(uuidgen)` or timestamp
- **Date formats:** `YYYY-MM-DD` for backfill dates
- **Cluster must be running** before submitting jobs
- **Check watermarks** in BigQuery before manual runs
- **GDPR deletions require confirmation** - no accidental deletions in production