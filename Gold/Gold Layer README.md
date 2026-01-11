# Gold Layer - Star Schema Implementation

**Layer:** Gold (Analytics-optimized)  
**Pattern:** Star schema with SCD Type 2  
**Status:** Production-ready ✅

---

## Overview

Gold layer implements a 5-dimension star schema optimized for BI queries:
- **5 Dimensions:** customer, merchant, date, payment_method, status
- **1 Fact table:** transactions with pre-calculated measures
- **SCD Type 2:** Tracks historical changes in customer tier and merchant names
- **Surrogate keys:** Integer keys for fast JOINs

**Key metrics:**
- Fact records: 1,379,815 transactions
- Dimensions: 1,000 customers, 500 merchants, 2,192 dates, 5 payment methods, 3 statuses
- Query performance: Sub-second for most BI queries

---

## Architecture

```
        dim_customer (SCD Type 2)
              │
        dim_merchant (SCD Type 2)
              │
        dim_date ──> fact_transactions <── dim_payment_method
              │
        dim_status
```

**Data flow:**
```
Silver (1.4M rows, deduplicated)
    ↓
Gold Dimensions (load with SCD Type 2)
    ↓
Fact Table (JOIN all dimensions → get surrogate keys)
```

---

## Directory Structure

```
gold/
├── README.md                           # This file
├── dim/                                # Dimension tables
│   ├── gold_dim_date.py               # Static (2,192 dates)
│   ├── gold_dim_payment_methods.py    # Static (5 methods)
│   ├── gold_dim_status.py             # Static (3 statuses)
│   ├── gold_dim_customer_scd2.py      # SCD Type 2 (1,000 customers)
│   └── gold_dim_merchant_scd2.py      # SCD Type 2 (500 merchants)
├── fact/                               # Fact table
│   ├── validate_fact_transactions.py  # Step 1: JOIN dimensions
│   ├── load_fact_transactions.py      # Step 2: MERGE to fact
│   └── fact_full_refresh.py           # Rebuild entire fact table
└── dags/                               # Airflow orchestration (TODO)
    ├── gold_static_dims_dag.py        # One-time load
    ├── gold_dim_customer_dag.py       # Daily SCD Type 2
    ├── gold_dim_merchant_dag.py       # Daily SCD Type 2
    └── gold_fact_transactions_dag.py  # Daily fact load
```

---

## Jobs Reference

### Static Dimensions (Run Once)

**Purpose:** Load dimensions that never change

| Job | Records | Runtime | Frequency |
|-----|---------|---------|-----------|
| `gold_dim_date.py` | 2,192 | 60s | Once |
| `gold_dim_payment_methods.py` | 5 | 70s | Once |
| `gold_dim_status.py` | 3 | 65s | Once |

**Run order:** Any (independent)

**Upload:**
```bash
gsutil cp dim/gold_dim_*.py gs://delta-lake-payment-gateway-476820/airflow/jobs/
```

**Execute:**
```bash
# Date dimension
gcloud dataproc jobs submit pyspark \
  gs://bucket/airflow/jobs/gold_dim_date.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1

# Payment methods
gcloud dataproc jobs submit pyspark \
  gs://bucket/airflow/jobs/gold_dim_payment_methods.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1

# Status
gcloud dataproc jobs submit pyspark \
  gs://bucket/airflow/jobs/gold_dim_status.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1
```

---

### SCD Type 2 Dimensions (Run Daily)

**Purpose:** Track historical changes in customer tier and merchant names

| Job | Records | Runtime | Frequency |
|-----|---------|---------|-----------|
| `gold_dim_customer_scd2.py` | 1,000 | 115s | Daily |
| `gold_dim_merchant_scd2.py` | 500 | 100s | Daily |

**Run order:** Any (independent)

**Logic:**
1. Aggregate Silver → calculate current state
2. Compare with existing Gold dimension
3. NEW records → INSERT with `is_current = true`
4. CHANGED records → Close old (set `is_current = false`), INSERT new version

**Example SCD Type 2:**
```
customer_key | customer_id | tier   | effective_start | effective_end | is_current
1            | USER_001    | Bronze | 2023-01-01      | 2024-06-30    | false
2            | USER_001    | Silver | 2024-07-01      | 9999-12-31    | true
```

**Upload:**
```bash
gsutil cp dim/gold_dim_*_scd2.py gs://bucket/airflow/jobs/
```

**Execute:**
```bash
# Customer dimension (SCD Type 2)
gcloud dataproc jobs submit pyspark \
  gs://bucket/airflow/jobs/gold_dim_customer_scd2.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1

# Merchant dimension (SCD Type 2)
gcloud dataproc jobs submit pyspark \
  gs://bucket/airflow/jobs/gold_dim_merchant_scd2.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1
```

---

### Fact Table (Run Daily)

**Purpose:** Load transactions with dimension keys and calculated measures

**Two-job pattern:**

**Job 1: Validate (JOIN dimensions)**
- Runtime: ~135s (1.4M records)
- Reads new Silver records (watermark-based)
- JOINs to all 5 dimensions
- Calculates measures
- Writes to `gold.fact_transactions_staging`

**Job 2: Load (MERGE to fact)**
- Runtime: ~100s
- MERGEs staging → fact table
- Updates watermark
- Writes job_control metadata

**Why two jobs?** If validate fails (bad JOIN), re-run without re-processing. If load fails (MERGE issue), re-run without re-JOINing.

**Upload:**
```bash
gsutil cp fact/*.py gs://bucket/airflow/jobs/
```

**Execute (incremental):**
```bash
# Step 1: Validate (JOIN dimensions)
gcloud dataproc jobs submit pyspark \
  gs://bucket/airflow/jobs/validate_fact_transactions.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1

# Step 2: Load (MERGE to fact)
gcloud dataproc jobs submit pyspark \
  gs://bucket/airflow/jobs/load_fact_transactions.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1
```

**Execute (full refresh):**
```bash
# Rebuild entire fact table from Silver
gcloud dataproc jobs submit pyspark \
  gs://bucket/airflow/jobs/fact_full_refresh.py \
  --cluster=dataproc-delta-cluster-final \
  --region=us-central1
```

---

## Load Order (First Time Setup)

**Run in this order:**

```bash
# 1. Static dimensions (any order)
gold_dim_date.py
gold_dim_payment_methods.py
gold_dim_status.py

# 2. SCD Type 2 dimensions (any order)
gold_dim_customer_scd2.py
gold_dim_merchant_scd2.py

# 3. Fact table (after ALL dimensions loaded)
validate_fact_transactions.py  # Must run AFTER dimensions
load_fact_transactions.py      # Must run AFTER validate
```

**Why this order?** Fact table JOINs to dimensions. Dimensions must exist first.

---

## Daily Incremental Order (Production)

**Airflow dependency chain:**

```
silver_incremental_dag (Bronze → Silver)
    ↓
gold_dim_customer_dag (SCD Type 2)
    ↓
gold_dim_merchant_dag (SCD Type 2)
    ↓
gold_fact_transactions_dag (Validate → Load)
```

**Rationale:**
- Dimensions first (SCD Type 2 updates current versions)
- Fact last (JOINs to current dimension versions)

---

## Schema

### Dimensions

**dim_customer (SCD Type 2):**
```sql
customer_key BIGINT          -- Surrogate key
customer_id STRING           -- Business key
customer_tier STRING         -- Bronze/Silver/Gold/Platinum
is_active BOOLEAN
first_transaction_date DATE
last_transaction_date DATE
lifetime_value DOUBLE
loaded_at TIMESTAMP
source_system STRING
effective_start_date DATE    -- SCD Type 2
effective_end_date DATE      -- SCD Type 2 (9999-12-31 = current)
is_current BOOLEAN           -- SCD Type 2 (true = latest)
```

**dim_merchant (SCD Type 2):**
```sql
merchant_key BIGINT          -- Surrogate key
merchant_id STRING           -- Business key
merchant_name STRING         -- Tracked attribute (changes)
category STRING
location_type STRING
loaded_at TIMESTAMP
source_system STRING
effective_start_date DATE    -- SCD Type 2
effective_end_date DATE      -- SCD Type 2
is_current BOOLEAN           -- SCD Type 2
```

**dim_date (Static):**
```sql
date_key BIGINT              -- YYYYMMDD format
full_date DATE
day_of_month INT
day_name STRING
month_number INT
month_name STRING
year INT
quarter INT
day_of_week INT
day_of_year INT
is_weekend BOOLEAN
```

**dim_payment_method (Static):**
```sql
payment_method_key BIGINT
payment_method STRING
description STRING
loaded_at TIMESTAMP
source_system STRING
```

**dim_status (Static):**
```sql
status_key BIGINT
transaction_status STRING
status_category STRING
is_successful BOOLEAN
loaded_at TIMESTAMP
source_system STRING
```

---

### Fact Table

**fact_transactions:**
```sql
-- Dimension keys (foreign keys)
customer_key BIGINT
merchant_key BIGINT
payment_method_key BIGINT
status_key BIGINT
date_key BIGINT

-- Business key
transaction_id STRING

-- Degenerate dimensions (stay in fact)
product_category STRING
product_name STRING
device_type STRING
currency STRING

-- Measures
amount DOUBLE
fee_amount DOUBLE
cashback_amount DOUBLE
loyalty_points BIGINT

-- Calculated measures
net_customer_amount DOUBLE      -- amount - fee + cashback
merchant_net_amount DOUBLE      -- amount - cashback
gateway_revenue DOUBLE          -- fee_amount

-- Timestamps
transaction_timestamp TIMESTAMP
loaded_at TIMESTAMP

-- Metadata
source_system STRING
created_at TIMESTAMP
updated_at TIMESTAMP
delta_change_type STRING
delta_version BIGINT
is_deleted BOOLEAN
deleted_at TIMESTAMP

-- Refund tracking (placeholder)
is_refunded BOOLEAN
refund_amount DOUBLE
refund_date DATE
attempt_number BIGINT
```

---

## Validation Queries

**Check dimension key population (should be 0 NULLs):**
```sql
SELECT 
    SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) as null_customer,
    SUM(CASE WHEN merchant_key IS NULL THEN 1 ELSE 0 END) as null_merchant,
    SUM(CASE WHEN payment_method_key IS NULL THEN 1 ELSE 0 END) as null_payment,
    SUM(CASE WHEN status_key IS NULL THEN 1 ELSE 0 END) as null_status,
    SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) as null_date
FROM `gold_dataset.fact_transactions`;
```

**Test star schema JOINs:**
```sql
SELECT 
    f.transaction_id,
    dc.customer_tier,
    dm.merchant_name,
    dpm.payment_method,
    ds.transaction_status,
    dd.full_date,
    f.amount,
    f.gateway_revenue
FROM `gold_dataset.fact_transactions` f
JOIN `gold_dataset.dim_customer` dc 
    ON f.customer_key = dc.customer_key 
    AND dc.is_current = true
JOIN `gold_dataset.dim_merchant` dm 
    ON f.merchant_key = dm.merchant_key 
    AND dm.is_current = true
JOIN `gold_dataset.dim_payment_method` dpm 
    ON f.payment_method_key = dpm.payment_method_key
JOIN `gold_dataset.dim_status` ds 
    ON f.status_key = ds.status_key
JOIN `gold_dataset.dim_date` dd 
    ON f.date_key = dd.date_key
LIMIT 10;
```

**Revenue by merchant:**
```sql
SELECT 
    dm.merchant_name,
    COUNT(*) as transaction_count,
    SUM(f.amount) as total_amount,
    SUM(f.gateway_revenue) as total_revenue
FROM `gold_dataset.fact_transactions` f
JOIN `gold_dataset.dim_merchant` dm 
    ON f.merchant_key = dm.merchant_key 
    AND dm.is_current = true
GROUP BY dm.merchant_name
ORDER BY total_revenue DESC
LIMIT 10;
```

**Customer tier analysis:**
```sql
SELECT 
    dc.customer_tier,
    COUNT(DISTINCT f.customer_key) as customers,
    COUNT(*) as transactions,
    SUM(f.amount) as total_amount,
    AVG(f.amount) as avg_amount
FROM `gold_dataset.fact_transactions` f
JOIN `gold_dataset.dim_customer` dc 
    ON f.customer_key = dc.customer_key 
    AND dc.is_current = true
GROUP BY dc.customer_tier
ORDER BY total_amount DESC;
```

---

## Known Limitations

**1. Test Merchant Transactions (100 records)**
- **Issue:** 100 transactions reference test merchants (MERCH_9xxx) filtered from Gold dimensions
- **Impact:** These records have `merchant_key = NULL` in fact table (from previous test run)
- **Status:** Documented limitation, not production issue
- **Resolution:** Would implement unknown member pattern (merchant_key = -1) in production

**2. No Unknown Members Pattern**
- **Current:** NULL dimension keys when JOIN fails
- **Best practice:** Create dimension records with key = -1 for "Unknown"
- **Status:** Deferred to v2.0

---

## Troubleshooting

**Issue: Fact validation finds 0 records**
- **Cause:** Watermark ahead of Silver data
- **Fix:** Reset watermark or run full refresh
- **Check:** `SELECT MAX(updated_at) FROM silver.transactions` vs Gold watermark

**Issue: NULL dimension keys in fact**
- **Cause:** Dimension load failed or data mismatch
- **Fix:** Verify all dimensions loaded, check for test data
- **Query:** `SELECT COUNT(*) FROM fact_transactions WHERE merchant_key IS NULL`

**Issue: SCD Type 2 duplicate current versions**
- **Cause:** Multiple records with `is_current = true` for same business key
- **Fix:** Run emergency cleanup script (see /docs/KNOWN_ISSUES.md)
- **Prevention:** Add deduplication in Step 2 of SCD Type 2 jobs

**Issue: Column order mismatch in staging**
- **Cause:** INSERT without explicit column names
- **Fix:** Use explicit column list matching staging schema exactly
- **Example:** `INSERT INTO staging SELECT col1, col2, col3 FROM source` (order matters!)

---

## Performance

**Dimension loads (one-time):**
- Static dimensions: 3 jobs × 1 min = 3 min total
- SCD Type 2: 2 jobs × 2 min = 4 min total

**Fact incremental (daily):**
- Validate: 135s (1.4M records)
- Load: 100s
- Total: ~4 minutes/day

**Fact full refresh:**
- Duration: 126s (1.4M records)
- Use case: Initial load, data fixes, schema changes

**Cost (with ephemeral clusters):**
- Static dims: $0.02 (one-time)
- SCD Type 2: $0.03/day
- Fact incremental: $0.02/day
- **Total: ~$1.50/month**

---

## Dependencies

**Upstream:**
- Silver layer (source for all Gold jobs)
- All 5 dimensions must load before fact table

**Downstream:**
- BI tools (Looker, Tableau, Power BI)
- Ad-hoc analysis (BigQuery SQL)
- ML pipelines (feature engineering)

---

## Related Documentation

- **Blog Post:** [Blog 3c - Gold Layer Implementation](../docs/BLOG_3C.md)
- **Coding Guide:** [Fact Table Coding Guide](../docs/FACT_CODING_GUIDE.md)
- **Interview Stories:** [Issues & Fixes](../docs/FACT_ISSUES_INTERVIEW.md)
- **Known Issues:** [Troubleshooting Guide](../docs/KNOWN_ISSUES.md)

---

## Next Steps

**After Gold layer is working:**
1. **Blog 3d:** Airflow DAGs (orchestration)
2. **Blog 3e:** Optimization (Z-ordering, compaction, vacuum)
3. **BI Integration:** Connect Looker/Tableau to BigQuery external tables
4. **ML Pipelines:** Use fact table for feature engineering

---

## Contributors

- Mohamed Kashifuddin (Implementation)

---

## License

MIT License - See LICENSE file for details

---

**Status:** Production-ready ✅  
**Last Updated:** January 2026  
**Version:** v1.2-gold