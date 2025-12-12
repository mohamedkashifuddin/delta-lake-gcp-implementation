# Data Lineage - Source to Gold

**Last Updated:** 2025-12-03

---

## Overview Flow

```
Raw CSV Files (GCS)
    ↓
Bronze Layer (Raw + Validation)
    ↓
Silver Layer (Cleaned + Deduplicated)
    ↓
Gold Layer (Star Schema)
```

---

## Bronze Layer Lineage

### Source → bronze.transactions

**Upstream:**
- **Source:** `gs://delta-lake-payment-gateway-476820/raw/YYYYMMDD/day*.csv`
- **Format:** CSV with header
- **Columns:** 17 (transaction_id → updated_at)

**Transformation:**
1. Read CSV with schema enforcement
2. Apply 3-tier validation (Tier 1 → quarantine, Tier 2 → flag, Tier 3 → defaults)
3. Add CDC columns: _change_type, _delta_version, is_deleted, deleted_at
4. Add late arrival columns: is_late_arrival, arrival_delay_hours
5. MERGE into bronze.transactions on transaction_id and updated_at - composite key 

**Downstream:**
- silver.transactions (reads bronze.transactions WHERE data_quality_flag != 'FAILED_VALIDATION')

**Job Name:** `bronze_incremental_load`

---

### Source → bronze.quarantine

**Upstream:**
- **Source:** Same CSV files as bronze.transactions
- **Filter:** Records that fail Tier 1 validation

**Transformation:**
1. Tier 1 failures (NULL transaction_id, NULL amount, NULL timestamp)
2. Add metadata: error_reason, error_tier, quarantined_at, processing_batch_id
3. INSERT into bronze.quarantine (append-only)

**Downstream:**
- Manual review/correction
- Re-insert into raw CSV after fixing

**Job Name:** `bronze_incremental_load` (same job)

---

### Job Metadata → bronze.job_control

**Upstream:**
- Job execution metadata (batch_id, records_read, records_written, etc.)

**Transformation:**
1. INSERT metadata after each job run
2. Tracks watermark (last_processed_timestamp)

**Downstream:**
- bronze_incremental_load reads watermark for incremental processing
- Monitoring dashboards

**Job Name:** All bronze jobs

---

## Silver Layer Lineage

### bronze.transactions → silver.transactions

**Upstream:**
- **Source:** bronze.transactions
- **Filter:** WHERE data_quality_flag != 'FAILED_VALIDATION' AND is_deleted = false

**Transformation:**
1. Read from bronze.transactions
2. Deduplicate on transaction_id (keep latest updated_at)
3. Remove late arrival columns (is_late_arrival, arrival_delay_hours)
4. MERGE into silver.transactions on transaction_id

**Downstream:**
- gold.fact_transactions

**Job Name:** `silver_incremental_load`

---

### Late Arrivals Handling

**Upstream:**
- **Source:** bronze.transactions WHERE is_late_arrival = true

**Transformation:**
1. Identify late arrivals (transaction_timestamp < watermark BUT updated_at > watermark)
2. UPDATE silver.transactions with latest data
3. Mark as _change_type = 'LATE_ARRIVAL_UPDATE'

**Downstream:**
- gold.fact_transactions (triggers fact table update)

**Job Name:** `silver_late_arrival_handler`

---

### Soft Delete Propagation

**Upstream:**
- **Source:** bronze.transactions WHERE is_deleted = true

**Transformation:**
1. Read deleted records from bronze
2. UPDATE silver.transactions SET is_deleted = true, deleted_at = current_timestamp()

**Downstream:**
- gold.fact_transactions (marks as deleted)

**Job Name:** `silver_soft_delete_propagation`

---

## Gold Layer Lineage

### Static Dimensions (One-Time Load)

#### gold.dim_date

**Upstream:**
- **Source:** Generated in-memory (Python date range)

**Transformation:**
1. Generate dates for 3 years (past 1 year + future 2 years)
2. Calculate day_of_week, quarter, is_weekend, etc.
3. INSERT OVERWRITE gold.dim_date

**Downstream:**
- gold.fact_transactions (date_key join)

**Job Name:** `gold_dim_date_load` (run once)

---

#### gold.dim_payment_method

**Upstream:**
- **Source:** silver.transactions (distinct payment_method values)

**Transformation:**
1. SELECT DISTINCT payment_method FROM silver.transactions
2. Generate surrogate keys (ROW_NUMBER)
3. INSERT into gold.dim_payment_method

**Downstream:**
- gold.fact_transactions (payment_method_key join)

**Job Name:** `gold_dim_payment_method_load` (run once)

---

#### gold.dim_status

**Upstream:**
- **Source:** silver.transactions (distinct transaction_status values)

**Transformation:**
1. SELECT DISTINCT transaction_status FROM silver.transactions
2. Add status_category (Successful → Completed, Pending → In-Progress, Failed → Error)
3. INSERT into gold.dim_status

**Downstream:**
- gold.fact_transactions (status_key join)

**Job Name:** `gold_dim_status_load` (run once)

---

### SCD Type 2 Dimensions

#### gold.dim_customer

**Upstream:**
- **Source:** silver.transactions (customer_id, first_transaction_date, last_transaction_date, lifetime_value)

**Transformation:**
1. Aggregate silver.transactions by customer_id
2. Detect changes in customer_tier (calculated from lifetime_value)
3. For NEW customers → INSERT with is_current = true
4. For CHANGED customers → UPDATE old record (set is_current = false, effective_end_date = today), INSERT new record
5. For UNCHANGED customers → No action

**Downstream:**
- gold.fact_transactions (customer_key join)

**Job Name:** `gold_dim_customer_scd2`

---

#### gold.dim_merchant

**Upstream:**
- **Source:** silver.transactions (merchant_id, merchant_name, location_type)

**Transformation:**
1. SELECT DISTINCT merchant_id, merchant_name FROM silver.transactions
2. Detect merchant_name changes (e.g., "Flipkart" → "Flipkart Private Limited")
3. For NEW merchants → INSERT with is_current = true
4. For CHANGED merchants → SCD Type 2 logic (close old, insert new)

**Downstream:**
- gold.fact_transactions (merchant_key join)

**Job Name:** `gold_dim_merchant_scd2`

---

### Fact Table

#### gold.fact_transactions

**Upstream:**
- **Source:** silver.transactions
- **Lookups:** 
  - gold.dim_customer (customer_id → customer_key)
  - gold.dim_merchant (merchant_id → merchant_key)
  - gold.dim_payment_method (payment_method → payment_method_key)
  - gold.dim_status (transaction_status → status_key)
  - gold.dim_date (DATE(transaction_timestamp) → date_key)

**Transformation:**
1. Read silver.transactions
2. Join to all dimensions to get surrogate keys
3. Calculate measures: net_customer_amount, merchant_net_amount, gateway_revenue
4. MERGE into gold.fact_transactions on transaction_id

**Downstream:**
- BI dashboards (BigQuery external table)

**Job Name:** `gold_fact_transactions_load`

---

## Watermark Flow

```
bronze.job_control (last_processed_timestamp)
    ↓
bronze_incremental_load reads watermark
    ↓
Processes only new data (transaction_timestamp > watermark OR updated_at > watermark)
    ↓
Writes new watermark to bronze.job_control
    ↓
silver_incremental_load reads bronze watermark
    ↓
Processes only new bronze data
    ↓
Writes silver watermark to silver.job_control
    ↓
gold jobs read silver watermark
```

---

## Dependency Graph

```
Raw CSV
    ├─→ bronze.transactions ──→ silver.transactions ──→ gold.fact_transactions
    └─→ bronze.quarantine (dead end, manual review)

silver.transactions
    ├─→ gold.dim_customer (SCD2)
    ├─→ gold.dim_merchant (SCD2)
    ├─→ gold.dim_payment_method (static)
    ├─→ gold.dim_status (static)
    └─→ gold.fact_transactions

Generated Data
    └─→ gold.dim_date (independent)
```

---

## Job Execution Order

**Bronze Layer:**
1. bronze_incremental_load (or bronze_backfill / bronze_full_refresh)

**Silver Layer:**
2. silver_incremental_load
3. silver_late_arrival_handler (parallel with #2)
4. silver_soft_delete_propagation (parallel with #2)

**Gold Layer (First Time Only):**
5. gold_dim_date_load
6. gold_dim_payment_method_load
7. gold_dim_status_load

**Gold Layer (Daily):**
8. gold_dim_customer_scd2
9. gold_dim_merchant_scd2
10. gold_fact_transactions_load (waits for #8 and #9)

---

**End of Lineage**