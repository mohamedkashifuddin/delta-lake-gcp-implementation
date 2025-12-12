from pyspark.sql import SparkSession
from datetime import datetime
import sys
import json

spark = SparkSession.builder \
    .appName("BronzeBackfill") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

RAW_PATH = sys.argv[1]
START_DATE = sys.argv[2]  # Format: 2025-11-29
END_DATE = sys.argv[3]    # Format: 2025-12-01
BATCH_ID = sys.argv[4]

print(f"=== BRONZE BACKFILL ===")
print(f"RAW_PATH: {RAW_PATH}")
print(f"Date Range: {START_DATE} to {END_DATE}")
print(f"BATCH_ID: {BATCH_ID}")

# Read raw CSV
raw_df = spark.read.format("csv").option("header", "true").load(RAW_PATH)
raw_df.createOrReplaceTempView("raw_data")

# Apply schema casting and date filter
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW filtered_data AS
    SELECT 
        CAST(transaction_id AS STRING) AS transaction_id,
        CAST(customer_id AS STRING) AS customer_id,
        CAST(transaction_timestamp AS TIMESTAMP) AS transaction_timestamp,
        CAST(merchant_id AS STRING) AS merchant_id,
        CAST(merchant_name AS STRING) AS merchant_name,
        CAST(product_category AS STRING) AS product_category,
        CAST(product_name AS STRING) AS product_name,
        CAST(amount AS DECIMAL(10,2)) AS amount,
        CAST(fee_amount AS DECIMAL(10,2)) AS fee_amount,
        CAST(cashback_amount AS DECIMAL(10,2)) AS cashback_amount,
        CAST(loyalty_points AS INT) AS loyalty_points,
        CAST(payment_method AS STRING) AS payment_method,
        CAST(transaction_status AS STRING) AS transaction_status,
        CAST(device_type AS STRING) AS device_type,
        CAST(location_type AS STRING) AS location_type,
        CAST(currency AS STRING) AS currency,
        CAST(updated_at AS TIMESTAMP) AS updated_at
    FROM raw_data
    WHERE DATE(transaction_timestamp) BETWEEN '{START_DATE}' AND '{END_DATE}'
""")

total_records = spark.sql("SELECT COUNT(*) as cnt FROM filtered_data").first()['cnt']
print(f"Records in date range: {total_records}")

if total_records == 0:
    print("No data in date range. Exiting.")
    result = {"records_processed": 0, "records_quarantined": 0, "date_range": f"{START_DATE} to {END_DATE}"}
    print(f"RESULT_JSON:{json.dumps(result)}")
    spark.stop()
    sys.exit(0)

# Tier 1 validation - Quarantine
spark.sql("""
    CREATE OR REPLACE TEMP VIEW bronze_quarantine_staging AS
    SELECT 
        transaction_id,
        customer_id,
        transaction_timestamp,
        merchant_id,
        merchant_name,
        product_category,
        product_name,
        amount,
        fee_amount,
        cashback_amount,
        loyalty_points,
        payment_method,
        transaction_status,
        device_type,
        location_type,
        currency,
        updated_at,
        'QUARANTINE' AS delta_change_type,
        CAST(NULL AS BIGINT) AS delta_version,
        CASE 
            WHEN transaction_id IS NULL THEN 'NULL_TRANSACTION_ID'
            WHEN transaction_id LIKE '% %' THEN 'INVALID_TRANSACTION_ID_FORMAT'
            WHEN amount IS NULL THEN 'NULL_AMOUNT'
            WHEN transaction_timestamp IS NULL THEN 'NULL_TIMESTAMP'
            ELSE 'UNKNOWN_TIER1_ERROR'
        END AS error_reason,
        'TIER_1' AS error_tier,
        CURRENT_TIMESTAMP() AS quarantined_at,
        CAST(NULL AS STRING) AS source_file,
        '{BATCH_ID}' AS processing_batch_id
    FROM filtered_data
    WHERE transaction_id IS NULL 
       OR transaction_id LIKE '% %'
       OR amount IS NULL 
       OR transaction_timestamp IS NULL
""")

records_quarantined = spark.sql("SELECT COUNT(*) as cnt FROM bronze_quarantine_staging").first()['cnt']
print(f"Tier 1 Quarantined: {records_quarantined}")

if records_quarantined > 0:
    spark.sql("INSERT INTO bronze.quarantine SELECT * FROM bronze_quarantine_staging")

# Tier 2 & 3 validation - Apply defaults and prepare for MERGE
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW bronze_staging AS
    SELECT 
        transaction_id,
        customer_id,
        transaction_timestamp,
        merchant_id,
        COALESCE(merchant_name, 'UNKNOWN_MERCHANT') AS merchant_name,
        product_category,
        COALESCE(product_name, 'NOT_AVAILABLE') AS product_name,
        amount,
        fee_amount,
        cashback_amount,
        loyalty_points,
        payment_method,
        transaction_status,
        COALESCE(device_type, 'UNKNOWN') AS device_type,
        COALESCE(location_type, 'NOT_AVAILABLE') AS location_type,
        currency,
        updated_at,
        'BACKFILL' AS delta_change_type,
        CAST(NULL AS INT) AS delta_version,
        FALSE AS is_deleted,
        CAST(NULL AS TIMESTAMP) AS deleted_at,
        FALSE AS is_late_arrival,
        CAST(NULL AS INT) AS arrival_delay_hours
    FROM filtered_data
    WHERE NOT (transaction_id IS NULL 
            OR transaction_id LIKE '% %'
            OR amount IS NULL 
            OR transaction_timestamp IS NULL)
""")

records_to_merge = spark.sql("SELECT COUNT(*) as cnt FROM bronze_staging").first()['cnt']
print(f"Records to backfill: {records_to_merge}")

# MERGE with replaceWhere (partition overwrite for date range)
spark.sql(f"""
    MERGE INTO bronze.transactions t
    USING bronze_staging s
    ON t.transaction_id = s.transaction_id 
       AND t.updated_at = s.updated_at
    WHEN MATCHED THEN UPDATE SET
        t.customer_id = s.customer_id,
        t.transaction_timestamp = s.transaction_timestamp,
        t.merchant_id = s.merchant_id,
        t.merchant_name = s.merchant_name,
        t.product_category = s.product_category,
        t.product_name = s.product_name,
        t.amount = s.amount,
        t.fee_amount = s.fee_amount,
        t.cashback_amount = s.cashback_amount,
        t.loyalty_points = s.loyalty_points,
        t.payment_method = s.payment_method,
        t.transaction_status = s.transaction_status,
        t.device_type = s.device_type,
        t.location_type = s.location_type,
        t.currency = s.currency,
        t.delta_change_type = 'BACKFILL',
        t.delta_version = s.delta_version,
        t.is_deleted = s.is_deleted,
        t.deleted_at = s.deleted_at,
        t.is_late_arrival = s.is_late_arrival,
        t.arrival_delay_hours = s.arrival_delay_hours
    WHEN NOT MATCHED THEN INSERT (
        transaction_id, customer_id, transaction_timestamp, merchant_id, merchant_name,
        product_category, product_name, amount, fee_amount, cashback_amount,
        loyalty_points, payment_method, transaction_status, device_type, location_type,
        currency, updated_at, delta_change_type, delta_version, is_deleted, deleted_at,
        is_late_arrival, arrival_delay_hours
    ) VALUES (
        s.transaction_id, s.customer_id, s.transaction_timestamp, s.merchant_id, s.merchant_name,
        s.product_category, s.product_name, s.amount, s.fee_amount, s.cashback_amount,
        s.loyalty_points, s.payment_method, s.transaction_status, s.device_type, s.location_type,
        s.currency, s.updated_at, s.delta_change_type, s.delta_version, s.is_deleted, s.deleted_at,
        s.is_late_arrival, s.arrival_delay_hours
    )
""")

print(f"Backfill MERGE completed: {records_to_merge} records")

# Write job control metadata
completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

spark.sql(f"""
    INSERT INTO bronze.job_control VALUES (
        'bronze_backfill',
        'bronze',
        '{BATCH_ID}',
        'backfill',
        'SUCCESS',
        CAST(NULL AS DATE),
        DATE('{START_DATE}'),
        DATE('{END_DATE}'),
        CAST(NULL AS TIMESTAMP),
        '{BATCH_ID}',
        {total_records},
        {records_to_merge},
        0,
        {records_quarantined},
        timestamp('{started_at}'),
        timestamp('{completed_at}'),
        CAST(NULL AS BIGINT),
        0,
        3,
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING)
    )
""")

result = {
    "status": "SUCCESS",
    "records_processed": records_to_merge,
    "records_quarantined": records_quarantined,
    "date_range": f"{START_DATE} to {END_DATE}"
}

print(f"RESULT_JSON:{json.dumps(result)}")
spark.stop()