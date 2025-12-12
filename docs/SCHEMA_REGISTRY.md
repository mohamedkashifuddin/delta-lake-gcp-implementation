# Schema Registry - All Delta Tables (FINAL)

**Last Updated:** 2025-12-06  
**Source:** Hive Metastore (validated schemas)  
**Note:** Uses `delta_change_type` (not `_change_type`) for BigQuery compatibility

---

## Bronze Layer (5 Tables)

### bronze.transactions (23 columns)
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| transaction_id | STRING | No | Primary key |
| customer_id | STRING | Yes | Customer identifier |
| transaction_timestamp | TIMESTAMP | Yes | When transaction occurred |
| merchant_id | STRING | Yes | Merchant identifier |
| merchant_name | STRING | Yes | Merchant business name |
| product_category | STRING | Yes | Product category |
| product_name | STRING | Yes | Product name |
| amount | DOUBLE | Yes | Transaction amount |
| fee_amount | DOUBLE | Yes | Processing fee |
| cashback_amount | DOUBLE | Yes | Cashback given |
| loyalty_points | BIGINT | Yes | Points earned |
| payment_method | STRING | Yes | Payment type |
| transaction_status | STRING | Yes | Successful/Pending/Failed |
| device_type | STRING | Yes | Mobile/Desktop/Web |
| location_type | STRING | Yes | Online/Store |
| currency | STRING | Yes | Currency code |
| updated_at | TIMESTAMP | Yes | Last modification time |
| delta_change_type | STRING | Yes | INSERT/UPDATE/MERGE/DELETE |
| delta_version | BIGINT | Yes | Delta Lake version |
| is_deleted | BOOLEAN | Yes | Soft delete flag |
| deleted_at | TIMESTAMP | Yes | Deletion timestamp |
| is_late_arrival | BOOLEAN | Yes | Late arrival flag |
| arrival_delay_hours | INT | Yes | Hours delayed |

**Location:** `gs://delta-lake-payment-gateway-476820/bronze/transactions`  
**Primary Key:** transaction_id  
**Partitioned By:** None

---

### bronze.transactions_staging (23 columns)
**Same schema as bronze.transactions**

**Location:** `gs://delta-lake-payment-gateway-476820/bronze/transactions_staging`  
**Purpose:** Temporary storage between validate_bronze.py and load_bronze.py  
**Lifecycle:** Overwritten on each DAG run (INSERT OVERWRITE)

---

### bronze.job_control (23 columns)
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| job_name | STRING | No | Name of the job |
| layer | STRING | No | bronze/silver/gold |
| batch_id | STRING | No | Unique job execution ID |
| run_mode | STRING | No | incremental/backfill/full_refresh |
| status | STRING | No | SUCCESS/FAILED/RUNNING |
| processing_date | DATE | Yes | Date being processed |
| start_date | DATE | Yes | Backfill start date |
| end_date | DATE | Yes | Backfill end date |
| last_processed_timestamp | TIMESTAMP | Yes | Watermark timestamp |
| last_processed_batch_id | STRING | Yes | Previous batch ID |
| records_read | BIGINT | Yes | Total records read |
| records_written | BIGINT | Yes | Records written to target |
| records_failed | BIGINT | Yes | Failed records |
| records_quarantined | BIGINT | Yes | Records sent to quarantine |
| started_at | TIMESTAMP | Yes | Job start time |
| completed_at | TIMESTAMP | Yes | Job end time |
| duration_seconds | BIGINT | Yes | Execution duration |
| retry_count | BIGINT | Yes | Number of retries |
| max_retries | BIGINT | Yes | Max retry attempts |
| error_message | STRING | Yes | Error details if failed |
| triggered_by | STRING | Yes | Manual/Airflow/API |
| dataproc_cluster | STRING | Yes | Cluster name |
| spark_app_id | STRING | Yes | Spark application ID |

**Location:** `gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control`  
**Primary Key:** batch_id

---

### bronze.quarantine (24 columns)
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| transaction_id | STRING | Yes | Original transaction ID |
| customer_id | STRING | Yes | Customer identifier |
| transaction_timestamp | TIMESTAMP | Yes | Transaction time |
| merchant_id | STRING | Yes | Merchant identifier |
| merchant_name | STRING | Yes | Merchant name |
| product_category | STRING | Yes | Product category |
| product_name | STRING | Yes | Product name |
| amount | DOUBLE | Yes | Transaction amount |
| fee_amount | DOUBLE | Yes | Fee amount |
| cashback_amount | DOUBLE | Yes | Cashback amount |
| loyalty_points | BIGINT | Yes | Loyalty points |
| payment_method | STRING | Yes | Payment method |
| transaction_status | STRING | Yes | Status |
| device_type | STRING | Yes | Device type |
| location_type | STRING | Yes | Location type |
| currency | STRING | Yes | Currency |
| updated_at | TIMESTAMP | Yes | Last update time |
| delta_change_type | STRING | Yes | Always 'QUARANTINE' |
| delta_version | BIGINT | Yes | Delta version |
| error_reason | STRING | No | Why quarantined |
| error_tier | STRING | Yes | TIER_1/TIER_2/TIER_3 |
| quarantined_at | TIMESTAMP | No | Quarantine timestamp |
| source_file | STRING | Yes | Source file path |
| processing_batch_id | STRING | Yes | Batch that quarantined it |

**Location:** `gs://delta-lake-payment-gateway-476820/quarantine/bronze_transactions`  
**Purpose:** Tier 1 validation failures (append-only)

---

### bronze.job_control_archive (23 columns)
**Same schema as bronze.job_control**

**Location:** `gs://delta-lake-payment-gateway-476820/metadata/bronze_job_control_archive`  
**Purpose:** Archive records older than 90 days

---

## Silver Layer (5 Tables)

### silver.transactions (21 columns)
**Columns 1-17:** Same as bronze (transaction_id → updated_at)  
**CDC columns:**
- delta_change_type | STRING | Yes
- delta_version | BIGINT | Yes
- is_deleted | BOOLEAN | Yes
- deleted_at | TIMESTAMP | Yes

**Location:** `gs://delta-lake-payment-gateway-476820/silver/transactions`  
**Differences from Bronze:** Removed is_late_arrival, arrival_delay_hours

---

### silver.transactions_staging (21 columns)
**Same schema as silver.transactions**

**Location:** `gs://delta-lake-payment-gateway-476820/silver/transactions_staging`

---

### silver.job_control, silver.quarantine, silver.job_control_archive
**Same schemas as bronze equivalents**

---

## Gold Layer (11 Tables)

### gold.fact_transactions (30 columns)
| Column | Type | Description |
|--------|------|-------------|
| customer_key | BIGINT | FK to dim_customer |
| merchant_key | BIGINT | FK to dim_merchant |
| payment_method_key | BIGINT | FK to dim_payment_method |
| status_key | BIGINT | FK to dim_status |
| date_key | BIGINT | FK to dim_date |
| transaction_id | STRING | Business key |
| product_category | STRING | Product category |
| product_name | STRING | Product name |
| device_type | STRING | Device type |
| amount | DOUBLE | Transaction amount |
| fee_amount | DOUBLE | Fee amount |
| cashback_amount | DOUBLE | Cashback amount |
| loyalty_points | BIGINT | Loyalty points |
| net_customer_amount | DOUBLE | amount - fee + cashback |
| merchant_net_amount | DOUBLE | amount - cashback |
| gateway_revenue | DOUBLE | fee_amount |
| transaction_timestamp | TIMESTAMP | Transaction time |
| currency | STRING | Currency |
| is_refunded | BOOLEAN | Refund flag |
| refund_amount | DOUBLE | Refund amount |
| refund_date | DATE | Refund date |
| attempt_number | BIGINT | Retry attempt |
| loaded_at | TIMESTAMP | ETL load time |
| source_system | STRING | Source identifier |
| created_at | TIMESTAMP | Record creation |
| updated_at | TIMESTAMP | Last update |
| delta_change_type | STRING | CDC type |
| delta_version | BIGINT | Delta version |
| is_deleted | BOOLEAN | Soft delete |
| deleted_at | TIMESTAMP | Deletion time |

**Location:** `gs://delta-lake-payment-gateway-476820/gold/fact_transactions`

---

### gold.fact_transactions_staging (30 columns)
**Same schema as gold.fact_transactions**

---

### gold.dim_customer (12 columns - SCD Type 2)
| Column | Type | Description |
|--------|------|-------------|
| customer_key | BIGINT | Surrogate key |
| customer_id | STRING | Business key |
| customer_tier | STRING | Platinum/Gold/Silver/Bronze |
| is_active | BOOLEAN | Active status |
| first_transaction_date | DATE | First transaction |
| last_transaction_date | DATE | Last transaction |
| lifetime_value | DOUBLE | Total spend |
| loaded_at | TIMESTAMP | ETL load time |
| source_system | STRING | Source |
| effective_start_date | DATE | SCD Type 2 start |
| effective_end_date | DATE | SCD Type 2 end (9999-12-31 for current) |
| is_current | BOOLEAN | Current version flag |

**Location:** `gs://delta-lake-payment-gateway-476820/gold/dim_customer`

---

### gold.dim_customer_staging (12 columns)
**Same schema as gold.dim_customer**

---

### gold.dim_merchant (10 columns - SCD Type 2)
| Column | Type | Description |
|--------|------|-------------|
| merchant_key | BIGINT | Surrogate key |
| merchant_id | STRING | Business key |
| merchant_name | STRING | Merchant name |
| category | STRING | Business category |
| location_type | STRING | Online/Physical |
| loaded_at | TIMESTAMP | ETL load time |
| source_system | STRING | Source |
| effective_start_date | DATE | SCD Type 2 start |
| effective_end_date | DATE | SCD Type 2 end |
| is_current | BOOLEAN | Current version flag |

**Location:** `gs://delta-lake-payment-gateway-476820/gold/dim_merchant`

---

### gold.dim_merchant_staging (10 columns)
**Same schema as gold.dim_merchant**

---

### gold.dim_date, gold.dim_payment_method, gold.dim_status
**(Schemas unchanged from original - see previous version)**

---

### gold.job_control, gold.job_control_archive
**Same schema as bronze.job_control**

---

## Total Table Count

| Layer | Main Tables | Staging Tables | Job Control | Archives | Total |
|-------|-------------|----------------|-------------|----------|-------|
| Bronze | 2 | 1 | 1 | 1 | 5 |
| Silver | 2 | 1 | 1 | 1 | 5 |
| Gold | 6 | 3 | 1 | 1 | 11 |
| **Total** | **10** | **5** | **3** | **3** | **21** |

**Note:** 21 tables total (16 main + 5 staging)

---

**Schema validated:** 2025-12-06 via DESCRIBE TABLE commands