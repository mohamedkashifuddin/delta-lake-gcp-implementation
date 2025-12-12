# Validation Rules - 3-Tier Framework

**Last Updated:** 2025-12-03

---

## Validation Framework Overview

**Tier 1 - Block (Quarantine):** Critical data quality failures that prevent processing  
**Tier 2 - Flag (Load with warning):** Business rule violations that need attention but don't block processing  
**Tier 3 - Fix (Apply defaults):** Missing optional data that can be filled with defaults  

---

## Bronze Layer Validation Rules

### Tier 1 - Block & Quarantine

| Column | Rule | Error Code | Action |
|--------|------|------------|--------|
| transaction_id | NOT NULL | NULL_TRANSACTION_ID | Quarantine |
| transaction_id | No whitespace | INVALID_TRANSACTION_ID_FORMAT | Quarantine |
| amount | NOT NULL | NULL_AMOUNT | Quarantine |
| transaction_timestamp | NOT NULL | NULL_TIMESTAMP | Quarantine |

**SQL Implementation:**
```sql
WHERE transaction_id IS NULL 
   OR transaction_id LIKE '% %'
   OR amount IS NULL 
   OR transaction_timestamp IS NULL
```

**Destination:** bronze.quarantine  
**Processing:** Manual review and correction required

---

### Tier 2 - Flag & Load

| Column | Rule | Error Code | Action |
|--------|------|------------|--------|
| amount | >= 0 | NEGATIVE_AMOUNT | Flag in validation_errors |
| transaction_timestamp | <= current_timestamp() | FUTURE_TIMESTAMP | Flag in validation_errors |
| merchant_id | NOT NULL | NULL_MERCHANT_ID | Flag in validation_errors |
| transaction_status | IN ('Successful', 'Pending', 'Failed') | INVALID_STATUS | Flag in validation_errors |

**SQL Implementation:**
```sql
CASE 
  WHEN amount < 0 THEN CONCAT(validation_errors, 'NEGATIVE_AMOUNT;')
  WHEN transaction_timestamp > CURRENT_TIMESTAMP() THEN CONCAT(validation_errors, 'FUTURE_TIMESTAMP;')
  WHEN merchant_id IS NULL THEN CONCAT(validation_errors, 'NULL_MERCHANT_ID;')
  WHEN transaction_status NOT IN ('Successful', 'Pending', 'Failed') THEN CONCAT(validation_errors, 'INVALID_STATUS;')
  ELSE NULL
END AS validation_errors
```

**Destination:** bronze.transactions (loaded with data_quality_flag = 'FAILED_VALIDATION')  
**Processing:** Loaded to bronze but flagged for downstream filtering

---

### Tier 3 - Fix with Defaults

| Column | Default Value | Condition |
|--------|---------------|-----------|
| device_type | 'UNKNOWN' | IS NULL |
| location_type | 'NOT_AVAILABLE' | IS NULL |
| product_name | 'NOT_AVAILABLE' | IS NULL |
| merchant_name | 'UNKNOWN_MERCHANT' | IS NULL |

**SQL Implementation:**
```sql
COALESCE(device_type, 'UNKNOWN') AS device_type,
COALESCE(location_type, 'NOT_AVAILABLE') AS location_type,
COALESCE(product_name, 'NOT_AVAILABLE') AS product_name,
COALESCE(merchant_name, 'UNKNOWN_MERCHANT') AS merchant_name
```

**Destination:** bronze.transactions (loaded with defaults applied)  
**Processing:** No special handling required

---

### Late Arrival Detection

**Rule:** `transaction_timestamp < last_watermark AND updated_at > last_watermark`

**Action:** Load to bronze with is_late_arrival = true and calculate arrival_delay_hours

**SQL Implementation:**
```sql
CASE 
  WHEN transaction_timestamp < :last_watermark 
   AND updated_at > :last_watermark 
  THEN TRUE 
  ELSE FALSE 
END AS is_late_arrival,

CASE 
  WHEN transaction_timestamp < :last_watermark 
   AND updated_at > :last_watermark 
  THEN (UNIX_TIMESTAMP(updated_at) - UNIX_TIMESTAMP(transaction_timestamp)) / 3600
  ELSE NULL 
END AS arrival_delay_hours
```

---

## Silver Layer Validation Rules

### Data Quality Filter

**Rule:** Exclude records with data_quality_flag = 'FAILED_VALIDATION'

**SQL Implementation:**
```sql
SELECT * FROM bronze.transactions
WHERE data_quality_flag != 'FAILED_VALIDATION' OR data_quality_flag IS NULL
```

**Reason:** Silver layer should contain only clean data

---

### Soft Delete Filter

**Rule:** Exclude records with is_deleted = true

**SQL Implementation:**
```sql
SELECT * FROM bronze.transactions
WHERE is_deleted = false OR is_deleted IS NULL
```

**Reason:** Deleted records should not propagate to silver

---

### Deduplication

**Rule:** Keep only the latest record per transaction_id based on updated_at

**SQL Implementation:**
```sql
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY updated_at DESC) AS row_num
  FROM bronze.transactions
) WHERE row_num = 1
```

**Reason:** Handle cases where same transaction_id appears multiple times (e.g., status updates)

---

## Gold Layer Validation Rules

### Dimension Lookup Failures

**Rule:** All fact records must have valid dimension keys

**Implementation:**

```sql
SELECT 
  s.*,
  COALESCE(c.customer_key, -1) AS customer_key,
  COALESCE(m.merchant_key, -1) AS merchant_key,
  COALESCE(p.payment_method_key, -1) AS payment_method_key,
  COALESCE(st.status_key, -1) AS status_key,
  COALESCE(d.date_key, -1) AS date_key
FROM silver.transactions s
LEFT JOIN gold.dim_customer c ON s.customer_id = c.customer_id AND c.is_current = true
LEFT JOIN gold.dim_merchant m ON s.merchant_id = m.merchant_id AND m.is_current = true
LEFT JOIN gold.dim_payment_method p ON s.payment_method = p.payment_method
LEFT JOIN gold.dim_status st ON s.transaction_status = st.transaction_status
LEFT JOIN gold.dim_date d ON DATE(s.transaction_timestamp) = d.full_date
```

**Action:** Use -1 as surrogate key for missing dimension records (represents "Unknown")

---

### Calculated Measures Validation

**Rule:** Calculated measures must be internally consistent

| Measure | Formula | Validation |
|---------|---------|------------|
| net_customer_amount | amount - fee_amount + cashback_amount | Must be positive |
| merchant_net_amount | amount - cashback_amount | Must be positive |
| gateway_revenue | fee_amount | Must be positive |

**SQL Implementation:**
```sql
CASE 
  WHEN (amount - fee_amount + cashback_amount) < 0 THEN 0
  ELSE amount - fee_amount + cashback_amount
END AS net_customer_amount
```

---

## Validation Summary by Layer

| Layer | Tier 1 Rules | Tier 2 Rules | Tier 3 Rules | Late Arrival | Deduplication |
|-------|--------------|--------------|--------------|--------------|---------------|
| Bronze | 4 | 4 | 4 | Yes | No |
| Silver | 0 | 0 | 0 | No | Yes |
| Gold | 0 | 0 | 0 | No | No |

**Total Rules:** 16

---

## Error Code Reference

| Code | Description | Tier | Action |
|------|-------------|------|--------|
| NULL_TRANSACTION_ID | Transaction ID is NULL | 1 | Quarantine |
| INVALID_TRANSACTION_ID_FORMAT | Transaction ID contains whitespace | 1 | Quarantine |
| NULL_AMOUNT | Amount is NULL | 1 | Quarantine |
| NULL_TIMESTAMP | Transaction timestamp is NULL | 1 | Quarantine |
| NEGATIVE_AMOUNT | Amount is negative | 2 | Flag |
| FUTURE_TIMESTAMP | Transaction timestamp is in the future | 2 | Flag |
| NULL_MERCHANT_ID | Merchant ID is NULL | 2 | Flag |
| INVALID_STATUS | Status not in allowed values | 2 | Flag |

---

## Validation Metrics

Track these metrics in job_control tables:

- **records_quarantined:** Count of Tier 1 failures
- **records_flagged:** Count of Tier 2 violations (not stored separately, but can be queried)
- **records_fixed:** Count of Tier 3 defaults applied (implicit - all records)
- **late_arrivals_count:** Count of is_late_arrival = true records

**Query Example:**
```sql
SELECT 
  COUNT(*) AS total_records,
  SUM(CASE WHEN data_quality_flag = 'FAILED_VALIDATION' THEN 1 ELSE 0 END) AS flagged_records,
  SUM(CASE WHEN is_late_arrival = true THEN 1 ELSE 0 END) AS late_arrivals
FROM bronze.transactions
WHERE DATE(transaction_timestamp) = '2024-12-03'
```

---