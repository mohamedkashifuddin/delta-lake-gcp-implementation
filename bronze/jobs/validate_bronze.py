from pyspark.sql import SparkSession
import sys
import json

spark = SparkSession.builder \
    .appName("ValidateBronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

RAW_PATH = sys.argv[1]
WATERMARK = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] != "NULL" else None
BATCH_ID = sys.argv[3]

print(f"Validating: {RAW_PATH}")
print(f"Watermark: {WATERMARK}")
print(f"Batch ID: {BATCH_ID}")

raw_df = spark.read.format("csv").option("header", "true").load(RAW_PATH)
raw_df.createOrReplaceTempView("raw_data")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW filtered_data AS
    SELECT 
        CAST(transaction_id AS STRING) AS transaction_id,
        CAST(customer_id AS STRING) AS customer_id,
        CAST(SUBSTRING(transaction_timestamp, 1, 19) AS TIMESTAMP) AS transaction_timestamp,
        CAST(merchant_id AS STRING) AS merchant_id,
        CAST(merchant_name AS STRING) AS merchant_name,
        CAST(product_category AS STRING) AS product_category,
        CAST(product_name AS STRING) AS product_name,
        CAST(amount AS DOUBLE) AS amount,
        CAST(fee_amount AS DOUBLE) AS fee_amount,
        CAST(cashback_amount AS DOUBLE) AS cashback_amount,
        CAST(loyalty_points AS INT) AS loyalty_points,
        CAST(payment_method AS STRING) AS payment_method,
        CAST(transaction_status AS STRING) AS transaction_status,
        CAST(device_type AS STRING) AS device_type,
        CAST(location_type AS STRING) AS location_type,
        CAST(currency AS STRING) AS currency,
        CAST(updated_at AS TIMESTAMP) AS updated_at
    FROM raw_data
    WHERE {f"transaction_timestamp > timestamp('{WATERMARK}') OR (transaction_timestamp <= timestamp('{WATERMARK}') AND updated_at > timestamp('{WATERMARK}'))" if WATERMARK else "1=1"}
""")

total_records = spark.sql("SELECT COUNT(*) as cnt FROM filtered_data").first()['cnt']
print(f"Records to process: {total_records}")

if total_records == 0:
    print("No new data. Exiting.")
    result = {"records_to_merge": 0, "records_quarantined": 0, "late_arrivals": 0}
    print(f"RESULT_JSON:{json.dumps(result)}")
    spark.stop()
    sys.exit(0)

if WATERMARK:
    wm_ts_condition = f"timestamp('{WATERMARK}')"
    is_late_condition = f"transaction_timestamp < {wm_ts_condition}"
    updated_after_wm_condition = f"updated_at > {wm_ts_condition}"
    late_arrival_filter = f"{is_late_condition} AND {updated_after_wm_condition}"
else:
    late_arrival_filter = "FALSE"

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
            WHEN transaction_timestamp > CURRENT_TIMESTAMP() THEN 'FUTURE_TIMESTAMP'
            ELSE 'UNKNOWN_TIER1_ERROR'
        END AS error_reason,
        'TIER_1' AS error_tier,
        CURRENT_TIMESTAMP() AS quarantined_at,
        CAST(NULL AS STRING) AS source_file,
        CAST(NULL AS STRING) AS processing_batch_id
    FROM filtered_data
    WHERE transaction_id IS NULL 
       OR transaction_id LIKE '% %'
       OR amount IS NULL 
       OR transaction_timestamp IS NULL
       OR transaction_timestamp > CURRENT_TIMESTAMP()
""")

records_quarantined = spark.sql("SELECT COUNT(*) as cnt FROM bronze_quarantine_staging").first()['cnt']
print(f"Tier 1 Quarantined: {records_quarantined}")

if records_quarantined > 0:
    spark.sql("INSERT INTO bronze.quarantine SELECT * FROM bronze_quarantine_staging")

# Create temp view with deduplication
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW staging_with_dedup AS
    SELECT * FROM (
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
            'INSERT' AS delta_change_type,
            CAST(NULL AS INT) AS delta_version,
            FALSE AS is_deleted,
            CAST(NULL AS TIMESTAMP) AS deleted_at,
            CASE 
                WHEN {late_arrival_filter}
                THEN TRUE 
                ELSE FALSE 
            END AS is_late_arrival,
            CASE 
                WHEN {late_arrival_filter}
                THEN CAST((UNIX_TIMESTAMP(updated_at) - UNIX_TIMESTAMP(transaction_timestamp)) / 3600 AS INT)
                ELSE CAST(NULL AS INT)
            END AS arrival_delay_hours,
            CASE
                WHEN amount < 0 
                    OR merchant_id IS NULL 
                    OR transaction_status NOT IN ('Successful', 'Pending', 'Failed')
                THEN 'FAILED_VALIDATION'
                ELSE 'PASSED'
            END AS data_quality_flag,
            CONCAT_WS(';',
                CASE WHEN amount < 0 THEN 'NEGATIVE_AMOUNT' END,
                CASE WHEN merchant_id IS NULL THEN 'NULL_MERCHANT_ID' END,
                CASE WHEN transaction_status NOT IN ('Successful', 'Pending', 'Failed') THEN 'INVALID_STATUS' END
            ) AS validation_errors,
            ROW_NUMBER() OVER (PARTITION BY transaction_id, updated_at ORDER BY transaction_id) AS row_num
        FROM filtered_data
        WHERE NOT (transaction_id IS NULL 
                OR transaction_id LIKE '% %'
                OR amount IS NULL 
                OR transaction_timestamp IS NULL
                OR transaction_timestamp > CURRENT_TIMESTAMP())
    ) WHERE row_num = 1
""")

# Insert from deduplicated view
spark.sql("""
    INSERT OVERWRITE bronze.transactions_staging
    SELECT 
        transaction_id, customer_id, transaction_timestamp, merchant_id, merchant_name,
        product_category, product_name, amount, fee_amount, cashback_amount,
        loyalty_points, payment_method, transaction_status, device_type, location_type,
        currency, updated_at, delta_change_type, delta_version, is_deleted, deleted_at,
        is_late_arrival, arrival_delay_hours, data_quality_flag, validation_errors
    FROM staging_with_dedup
""")

records_to_merge = spark.sql("SELECT COUNT(*) as cnt FROM bronze.transactions_staging").first()['cnt']
print(f"Records to merge: {records_to_merge}")

late_arrivals = spark.sql("SELECT COUNT(*) as cnt FROM bronze.transactions_staging WHERE is_late_arrival = TRUE").first()['cnt']
print(f"Late arrivals: {late_arrivals}")

tier2_violations = spark.sql("SELECT COUNT(*) as cnt FROM bronze.transactions_staging WHERE data_quality_flag = 'FAILED_VALIDATION'").first()['cnt']
print(f"Tier 2 violations (flagged): {tier2_violations}")

result = {
    "records_to_merge": records_to_merge,
    "records_quarantined": records_quarantined,
    "late_arrivals": late_arrivals,
    "tier2_violations": tier2_violations
}

print(f"RESULT_JSON:{json.dumps(result)}")
spark.stop()