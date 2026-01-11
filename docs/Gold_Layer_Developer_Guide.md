# Gold Layer Developer Guide

**Purpose:** Quick reference for extending Gold layer (star schema)  
**Audience:** Data engineers adding dimensions or modifying fact tables

---

## 📐 Architecture Decisions

### Why Star Schema (Not Snowflake)?

**Star Schema (What We Built):**
```
fact_transactions → dim_customer (flat)
                 → dim_merchant (flat)
                 → dim_date (flat)
```

**Snowflake Schema (What We Avoided):**
```
fact_transactions → dim_customer → dim_customer_tier
                                 → dim_customer_segment
```

**Decision:** Star schema is faster for BI queries. Snowflake saves storage but adds JOINs.

**When to use Snowflake:** Only if dimension normalization saves >30% storage.

---

### Why SCD Type 2 (Not SCD Type 1)?

**SCD Type 1 (Overwrite):**
```
customer_key | customer_id | tier
1            | USER_001    | Gold  ← Latest overwrites history
```

**SCD Type 2 (History Preserved):**
```
customer_key | customer_id | tier   | is_current
1            | USER_001    | Bronze | false
2            | USER_001    | Silver | false
3            | USER_001    | Gold   | true
```

**Use SCD Type 2 when:**
- Business needs historical analysis ("revenue from Platinum customers in Q2 2024")
- Attributes change frequently (customer tier, merchant name)
- Compliance requires tracking changes

**Use SCD Type 1 when:**
- Corrections only (typo fixes)
- History doesn't matter (user preference changes)
- Storage constraints

---

### Why Two-Job Pattern (Validate + Load)?

**Alternative (One Monolithic Job):**
```python
# Read Silver → JOIN 5 dimensions → Calculate measures → MERGE fact
# Problem: If fails, which of 5 JOINs broke?
```

**Our Approach (Two Jobs):**
```python
# Job 1: validate_fact_transactions.py
# - Read Silver
# - JOIN 5 dimensions
# - Write to staging
# If fails: "merchant_key IS NULL" ← Clear problem

# Job 2: load_fact_transactions.py
# - MERGE staging → fact
# If fails: "Duplicate transaction_id" ← Clear problem
```

**Benefits:**
- Easier debugging (know which step failed)
- Cheaper retries (re-run only failed job)
- Can inspect staging before MERGE

---

## 🔧 Adding a New Dimension

### Use Case: Add dim_product

**Step 1: Create Delta Table**

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.dim_product (
        product_key BIGINT NOT NULL,
        product_id STRING NOT NULL,
        product_name STRING,
        category STRING,
        subcategory STRING,
        loaded_at TIMESTAMP,
        source_system STRING
    )
    USING DELTA
    LOCATION 'gs://bucket/gold/dim_product'
""")
```

---

**Step 2: Create Load Job (gold_dim_product.py)**

```python
# Extract distinct products from Silver
products_df = spark.sql("""
    SELECT DISTINCT
        product_category,
        product_name
    FROM silver.transactions
""")

# Assign surrogate keys
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

window_spec = Window.orderBy("product_name")
products_with_keys = products_df.withColumn(
    "product_key",
    row_number().over(window_spec)
)

# MERGE to dimension
products_with_keys.createOrReplaceTempView("new_products")

spark.sql("""
    MERGE INTO gold.dim_product AS target
    USING new_products AS source
    ON target.product_name = source.product_name
       AND target.product_category = source.product_category
    
    WHEN NOT MATCHED THEN
        INSERT (product_key, product_name, category, ...)
        VALUES (source.product_key, source.product_name, ...)
""")
```

---

**Step 3: Update Fact Table Schema**

```sql
ALTER TABLE gold.fact_transactions 
ADD COLUMN product_key BIGINT;

ALTER TABLE gold.fact_transactions_staging 
ADD COLUMN product_key BIGINT;
```

---

**Step 4: Update validate_fact_transactions.py**

```python
# Add to dimension JOINs
LEFT JOIN gold.dim_product dp 
    ON s.product_name = dp.product_name 
    AND s.product_category = dp.category
```

---

**Step 5: Update load_fact_transactions.py**

Add `product_key` to MERGE statement columns.

---

**Step 6: Create Airflow DAG**

Add DAG similar to `gold_dim_customer_scd2_dag.py` but without SCD Type 2 logic (if static).

---

## 🔄 Converting Dimension to SCD Type 2

### Use Case: Track Product Name Changes

**Step 1: Add SCD Type 2 Columns**

```sql
ALTER TABLE gold.dim_product 
ADD COLUMN effective_start_date DATE;

ALTER TABLE gold.dim_product 
ADD COLUMN effective_end_date DATE;

ALTER TABLE gold.dim_product 
ADD COLUMN is_current BOOLEAN;
```

---

**Step 2: Update Load Logic (7 Steps)**

```python
# 1. Aggregate Silver (current state)
current_products = spark.sql("SELECT DISTINCT product_id, product_name FROM silver.transactions")

# 2. Get MAX surrogate key
max_key = spark.sql("SELECT MAX(product_key) FROM gold.dim_product").first()[0] or 0

# 3. Find NEW products
new_products = spark.sql("""
    SELECT * FROM current_products
    WHERE NOT EXISTS (SELECT 1 FROM gold.dim_product WHERE product_id = ...)
""")

# 4. Find CHANGED products
changed_products = spark.sql("""
    SELECT * FROM current_products cp
    INNER JOIN gold.dim_product dp ON cp.product_id = dp.product_id
    WHERE dp.is_current = true AND cp.product_name != dp.product_name
""")

# 5. INSERT new products (with is_current = true)

# 6. Close old versions (MERGE set is_current = false, effective_end_date = today)

# 7. INSERT new versions (new product_key, is_current = true)
```

---

**Step 3: Update Fact JOIN**

```python
# Add is_current filter
LEFT JOIN gold.dim_product dp 
    ON s.product_id = dp.product_id 
    AND dp.is_current = true  ← Critical!
```

---

## 📊 Adding Calculated Measures

### Use Case: Add `profit_margin` to Fact

**Step 1: Update Schema**

```sql
ALTER TABLE gold.fact_transactions 
ADD COLUMN profit_margin DOUBLE;

ALTER TABLE gold.fact_transactions_staging 
ADD COLUMN profit_margin DOUBLE;
```

---

**Step 2: Update validate_fact_transactions.py**

```python
# Add to SELECT in fact_enriched view
(s.gateway_revenue / NULLIF(s.amount, 0)) * 100 as profit_margin
```

---

**Step 3: Update load_fact_transactions.py**

Add to MERGE columns:
```sql
WHEN MATCHED THEN UPDATE SET
    ...,
    target.profit_margin = source.profit_margin

WHEN NOT MATCHED THEN INSERT
    (..., profit_margin)
    VALUES (..., source.profit_margin)
```

---

## 🧪 Testing Checklist

**Before deploying dimension changes:**
- [ ] Test with 100-row Silver sample
- [ ] Verify surrogate keys unique
- [ ] Check NULL dimension keys in fact staging
- [ ] Test SCD Type 2 logic (if applicable)
- [ ] Verify `is_current = true` in fact JOINs
- [ ] Run fact validate + load with new dimension
- [ ] Check fact record counts (before vs after)

**Verification queries:**
```sql
-- Check dimension key uniqueness
SELECT product_key, COUNT(*) 
FROM gold.dim_product 
GROUP BY product_key HAVING COUNT(*) > 1;

-- Check NULL keys in fact
SELECT COUNT(*) FROM gold.fact_transactions WHERE product_key IS NULL;

-- Check SCD Type 2 integrity
SELECT product_id, COUNT(*) 
FROM gold.dim_product 
WHERE is_current = true 
GROUP BY product_id HAVING COUNT(*) > 1;
```

---

## 🚨 Common Issues

### Issue 1: Duplicate Fact Records

**Symptom:** Same transaction_id appears multiple times

**Cause:** Forgot `AND is_current = true` in SCD Type 2 dimension JOIN

**Fix:**
```python
LEFT JOIN gold.dim_customer dc 
    ON s.customer_id = dc.customer_id 
    AND dc.is_current = true  ← Add this!
```

---

### Issue 2: NULL Dimension Keys

**Symptom:** `merchant_key IS NULL` for some transactions

**Cause:** Merchant exists in Silver but not in Gold dimension

**Debug:**
```sql
-- Find orphan merchants
SELECT DISTINCT s.merchant_id 
FROM silver.transactions s
LEFT JOIN gold.dim_merchant dm ON s.merchant_id = dm.merchant_id
WHERE dm.merchant_key IS NULL;
```

**Fix:** Run merchant dimension load job first, then fact load.

---

### Issue 3: Wrong Tier in Historical Analysis

**Symptom:** Q2 2024 revenue shows all customers as current tier

**Cause:** Fact table not using point-in-time dimension version

**Fix:** Either:
1. Use SCD Type 2 with point-in-time JOINs (complex)
2. Store `customer_tier` as degenerate dimension in fact (simpler)

**Decision guide:** If attribute changes frequently AND needed for time-based analysis → SCD Type 2. Otherwise → degenerate dimension.

---

## 📏 Code Conventions

### Dimension Naming

- **Static dimensions:** `dim_[entity]` (e.g., `dim_date`, `dim_status`)
- **SCD Type 2 dimensions:** `dim_[entity]` with `_scd2` in job name (e.g., `gold_dim_customer_scd2.py`)

### Fact Naming

- **Fact tables:** `fact_[process]` (e.g., `fact_transactions`, `fact_payments`)
- **Staging:** `fact_[process]_staging`

### Job Naming

- **Dimensions:** `gold_dim_[entity].py` or `gold_dim_[entity]_scd2.py`
- **Fact validate:** `validate_fact_[process].py`
- **Fact load:** `load_fact_[process].py`
- **Fact full refresh:** `fact_full_refresh.py`

---

## 🎯 Performance Tips

### 1. Broadcast Small Dimensions

```python
from pyspark.sql.functions import broadcast

# If dimension < 10MB
fact_df = silver_df.join(
    broadcast(dim_date_df),  # Broadcast to all workers
    silver_df.date_key == dim_date_df.date_key
)
```

---

### 2. Filter Before JOIN

```python
# Slow (JOIN all Silver, then filter)
fact_df = silver_df.join(dim_customer_df, ...) \
    .filter(col("customer_tier") == "Platinum")

# Fast (filter Silver first, then JOIN)
platinum_silver = silver_df.join(
    dim_customer_df.filter(col("customer_tier") == "Platinum"),
    ...
)
```

---

### 3. Partition Fact Table

```sql
-- Partition by date for date-range queries
CREATE TABLE gold.fact_transactions (...)
USING DELTA
PARTITIONED BY (date_key)
LOCATION 'gs://bucket/gold/fact_transactions';
```

**Benefit:** Queries with `WHERE date_key >= 20250101` scan only relevant partitions.

---

## 📚 Resources

- **Full implementation:** [GitHub - Gold Layer](https://github.com/mohamedkashifuddin/delta-lake-gcp-implementation/tree/v1.2-gold)
- **Blog post:** [Blog 3c - Gold Layer Star Schema](link-to-blog)
- **Schema reference:** `/docs/SCHEMA_REGISTRY.md`
- **Gold README:** `/gold/README.md`
- **Kimball methodology:** [The Data Warehouse Toolkit](https://www.kimballgroup.com/)

---

**Questions?** Add to this guide via PR or contact data engineering team.

---

**Last Updated:** January 2026 | **Maintainer:** Mohamed Kashifuddin