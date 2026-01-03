from pyspark.sql import SparkSession
from datetime import datetime
import sys
import json

spark = SparkSession.builder \
    .appName("BronzeFullRefresh") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

RAW_PATH = sys.argv[1]
BATCH_ID = sys.argv[2]

print(f"=== BRONZE FULL REFRESH ===")
print(f"RAW_PATH: {RAW_PATH}")
print(f"BATCH_ID: {BATCH_ID}")
print(f"WARNING: This will OVERWRITE all bronze data")

raw_df = spark.read.format("csv").option("header", "true").load(RAW_PATH)
raw_df.createOrReplaceTempView("raw_data")

spark.sql("""
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
""")

total_records = spark.sql("SELECT COUNT(*) as cnt FROM filtered_data").first()['cnt']
print(f"Total records to process: {total_records}")

if total_records == 0:
    print("ERROR: No data found in raw path")
    result = {"status": "FAILED", "error": "NO_RAW_DATA"}
    print(f"RESULT_JSON:{json.dumps(result)}")
    spark.stop()
    sys.exit(1)

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
        '{BATCH_ID}' AS processing_batch_id
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
    spark.sql("INSERT OVERWRITE bronze.quarantine SELECT * FROM bronze_quarantine_staging")

# Create temp view with deduplication
spark.sql("""
    CREATE OR REPLACE TEMP VIEW bronze_staging AS
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
            'FULL_REFRESH' AS delta_change_type,
            CAST(NULL AS INT) AS delta_version,
            FALSE AS is_deleted,
            CAST(NULL AS TIMESTAMP) AS deleted_at,
            FALSE AS is_late_arrival,
            CAST(NULL AS INT) AS arrival_delay_hours,
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

records_to_load = spark.sql("SELECT COUNT(*) as cnt FROM bronze_staging").first()['cnt']
print(f"Records to load: {records_to_load}")

print("Executing INSERT OVERWRITE bronze.transactions...")
spark.sql("""
    INSERT OVERWRITE bronze.transactions
    SELECT * FROM bronze_staging
""")

print(f"Full refresh completed: {records_to_load} records")

new_watermark = spark.sql("""
    SELECT GREATEST(MAX(transaction_timestamp), MAX(updated_at)) as wm
    FROM bronze.transactions
""").first()['wm']

print(f"New watermark: {new_watermark}")

completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
watermark_str = new_watermark.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if new_watermark else "NULL"

spark.sql(f"""
    INSERT INTO bronze.job_control VALUES (
        'bronze_full_refresh',
        'bronze',
        '{BATCH_ID}',
        'full_refresh',
        'SUCCESS',
        CAST(NULL AS DATE),
        CAST(NULL AS DATE),
        CAST(NULL AS DATE),
        timestamp('{watermark_str}'),
        '{BATCH_ID}',
        {total_records},
        {records_to_load},
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
    "records_loaded": records_to_load,
    "records_quarantined": records_quarantined,
    "new_watermark": watermark_str,
    "mode": "FULL_REFRESH"
}

print(f"RESULT_JSON:{json.dumps(result)}")
spark.stop()