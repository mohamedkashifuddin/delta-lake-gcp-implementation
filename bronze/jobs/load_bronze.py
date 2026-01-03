from pyspark.sql import SparkSession
from datetime import datetime
import sys
import json

spark = SparkSession.builder \
    .appName("LoadBronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

BATCH_ID = sys.argv[1]
JOB_NAME = sys.argv[2]
RUN_MODE = sys.argv[3]
RECORDS_READ = int(sys.argv[4])
RECORDS_QUARANTINED = int(sys.argv[5])
STARTED_AT = sys.argv[6]

print(f"Loading batch: {BATCH_ID}")
print(f"Job: {JOB_NAME}, Mode: {RUN_MODE}")

try:
    records_to_merge = spark.sql("SELECT COUNT(*) as cnt FROM bronze.transactions_staging").first()['cnt']
    
    if records_to_merge == 0:
        print("No records to merge. Writing metadata only.")
        
        completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        spark.sql(f"""
            INSERT INTO bronze.job_control VALUES (
                '{JOB_NAME}',
                'bronze',
                '{BATCH_ID}',
                '{RUN_MODE}',
                'SUCCESS',
                CAST(NULL AS DATE),
                CAST(NULL AS DATE),
                CAST(NULL AS DATE),
                CAST(NULL AS TIMESTAMP),
                CAST(NULL AS STRING),
                {RECORDS_READ},
                0,
                0,
                {RECORDS_QUARANTINED},
                timestamp('{STARTED_AT}'),
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
        
        result = {"status": "SUCCESS", "records_written": 0}
        print(f"RESULT_JSON:{json.dumps(result)}")
        spark.stop()
        sys.exit(0)
    
    print(f"Merging {records_to_merge} records...")
    
    spark.sql("""
        MERGE INTO bronze.transactions t
        USING bronze.transactions_staging s
        ON t.transaction_id = s.transaction_id
            and t.updated_at = s.updated_at
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
            t.updated_at = s.updated_at,
            t.delta_change_type = 'MERGE',
            t.delta_version = s.delta_version,
            t.is_deleted = s.is_deleted,
            t.deleted_at = s.deleted_at,
            t.is_late_arrival = s.is_late_arrival,
            t.arrival_delay_hours = s.arrival_delay_hours,
            t.data_quality_flag = s.data_quality_flag,
            t.validation_errors = s.validation_errors
        WHEN NOT MATCHED THEN INSERT (
            transaction_id, customer_id, transaction_timestamp, merchant_id, merchant_name,
            product_category, product_name, amount, fee_amount, cashback_amount,
            loyalty_points, payment_method, transaction_status, device_type, location_type,
            currency, updated_at, delta_change_type, delta_version, is_deleted, deleted_at,
            is_late_arrival, arrival_delay_hours, data_quality_flag, validation_errors
        ) VALUES (
            s.transaction_id, s.customer_id, s.transaction_timestamp, s.merchant_id, s.merchant_name,
            s.product_category, s.product_name, s.amount, s.fee_amount, s.cashback_amount,
            s.loyalty_points, s.payment_method, s.transaction_status, s.device_type, s.location_type,
            s.currency, s.updated_at, s.delta_change_type, s.delta_version, s.is_deleted, s.deleted_at,
            s.is_late_arrival, s.arrival_delay_hours, s.data_quality_flag, s.validation_errors
        )
    """)
    
    print(f"MERGE completed: {records_to_merge} records")
    
    new_watermark = spark.sql("""
        SELECT GREATEST(MAX(transaction_timestamp), MAX(updated_at)) as wm
        FROM bronze.transactions_staging
    """).first()['wm']
    
    print(f"New watermark: {new_watermark}")
    
    completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    watermark_str = new_watermark.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if new_watermark else "NULL"
    
    spark.sql(f"""
        INSERT INTO bronze.job_control VALUES (
            '{JOB_NAME}',
            'bronze',
            '{BATCH_ID}',
            '{RUN_MODE}',
            'SUCCESS',
            CAST(NULL AS DATE),
            CAST(NULL AS DATE),
            CAST(NULL AS DATE),
            timestamp('{watermark_str}'),
            '{BATCH_ID}',
            {RECORDS_READ},
            {records_to_merge},
            0,
            {RECORDS_QUARANTINED},
            timestamp('{STARTED_AT}'),
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
        "records_written": records_to_merge,
        "new_watermark": watermark_str
    }
    
    print(f"RESULT_JSON:{json.dumps(result)}")
    
except Exception as e:
    error_msg = str(e)[:500]
    print(f"ERROR: {error_msg}")
    
    completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    try:
        spark.sql(f"""
            INSERT INTO bronze.job_control VALUES (
                '{JOB_NAME}',
                'bronze',
                '{BATCH_ID}',
                '{RUN_MODE}',
                'FAILED',
                CAST(NULL AS DATE),
                CAST(NULL AS DATE),
                CAST(NULL AS DATE),
                CAST(NULL AS TIMESTAMP),
                CAST(NULL AS STRING),
                {RECORDS_READ},
                0,
                0,
                {RECORDS_QUARANTINED},
                timestamp('{STARTED_AT}'),
                timestamp('{completed_at}'),
                CAST(NULL AS BIGINT),
                0,
                3,
                '{error_msg.replace("'", "''")}',
                CAST(NULL AS STRING),
                CAST(NULL AS STRING),
                CAST(NULL AS STRING)
            )
        """)
    except:
        pass
    
    result = {"status": "FAILED", "error": error_msg}
    print(f"RESULT_JSON:{json.dumps(result)}")
    spark.stop()
    sys.exit(1)

spark.stop()