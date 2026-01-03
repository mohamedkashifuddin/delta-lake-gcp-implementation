#!/usr/bin/env python3

from pyspark.sql import SparkSession
from datetime import datetime
import uuid
import json
import sys

spark = SparkSession.builder \
    .appName("Load_Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

batch_id = str(uuid.uuid4())
job_name = "load_silver"
start_time = datetime.now()

print(f"\n{'='*80}")
print(f"Job: {job_name}")
print(f"Batch ID: {batch_id}")
print(f"Started: {start_time}")
print(f"{'='*80}\n")

try:
    print("STEP 1: Checking silver.transactions_staging...")
    
    staging_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.transactions_staging").collect()[0]['cnt']
    
    print(f"✓ Records in staging: {staging_count}")
    
    if staging_count == 0:
        print("\n⚠️  No records in staging to load.")
        print("📋 Context: validate_silver found no new data from Bronze.")
        
        # ====================================================================
        # FIXED: Still write job_control with NULL watermark for 0 records
        # This indicates Silver ran but found nothing new
        # ====================================================================
        spark.sql(f"""
            INSERT INTO silver.job_control VALUES (
                'silver_incremental_load',
                'silver',
                '{batch_id}',
                'incremental',
                'SUCCESS',
                CURRENT_DATE(),
                NULL, NULL, 
                NULL,  -- No watermark update (no new data)
                NULL,
                0, 0, 0, 0,
                CAST('{start_time}' AS TIMESTAMP),
                CURRENT_TIMESTAMP(),
                CAST((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(CAST('{start_time}' AS TIMESTAMP))) AS BIGINT),
                0, 3, NULL,
                'Airflow',
                'ephemeral-cluster',
                '{spark.sparkContext.applicationId}'
            )
        """)
        
        result = {
            "records_merged": 0,
            "inserts": 0,
            "updates": 0,
            "silver_total": spark.sql("SELECT COUNT(*) as cnt FROM silver.transactions").collect()[0]['cnt'],
            "status": "SUCCESS_NO_DATA",
            "note": "No new data from Bronze since last Silver run"
        }
        
        print(f"\nRESULT_JSON:{json.dumps(result)}")
        sys.exit(0)
    
    print("\nSTEP 2: Getting pre-merge metrics...")
    
    silver_count_before = spark.sql("SELECT COUNT(*) as cnt FROM silver.transactions").collect()[0]['cnt']
    print(f"✓ Silver records before MERGE: {silver_count_before}")
    
    print("\nSTEP 3: Merging staging -> silver.transactions...")
    
    spark.sql("""
        MERGE INTO silver.transactions AS target
        USING silver.transactions_staging AS source
        ON target.transaction_id = source.transaction_id
        
        WHEN MATCHED THEN
            UPDATE SET
                target.customer_id = source.customer_id,
                target.transaction_timestamp = source.transaction_timestamp,
                target.merchant_id = source.merchant_id,
                target.merchant_name = source.merchant_name,
                target.product_category = source.product_category,
                target.product_name = source.product_name,
                target.amount = source.amount,
                target.fee_amount = source.fee_amount,
                target.cashback_amount = source.cashback_amount,
                target.loyalty_points = source.loyalty_points,
                target.payment_method = source.payment_method,
                target.transaction_status = source.transaction_status,
                target.device_type = source.device_type,
                target.location_type = source.location_type,
                target.currency = source.currency,
                target.updated_at = source.updated_at,
                target.delta_change_type = 'UPDATE',
                target.delta_version = source.delta_version,
                target.is_deleted = source.is_deleted,
                target.deleted_at = source.deleted_at
        
        WHEN NOT MATCHED THEN
            INSERT (
                transaction_id, customer_id, transaction_timestamp,
                merchant_id, merchant_name, product_category, product_name,
                amount, fee_amount, cashback_amount, loyalty_points,
                payment_method, transaction_status, device_type,
                location_type, currency, updated_at,
                delta_change_type, delta_version, is_deleted, deleted_at
            )
            VALUES (
                source.transaction_id, source.customer_id, source.transaction_timestamp,
                source.merchant_id, source.merchant_name, source.product_category, source.product_name,
                source.amount, source.fee_amount, source.cashback_amount, source.loyalty_points,
                source.payment_method, source.transaction_status, source.device_type,
                source.location_type, source.currency, source.updated_at,
                'INSERT', source.delta_version, source.is_deleted, source.deleted_at
            )
    """)
    
    print("✓ MERGE completed")
    
    print("\nSTEP 4: Getting post-merge metrics...")
    
    silver_count_after = spark.sql("SELECT COUNT(*) as cnt FROM silver.transactions").collect()[0]['cnt']
    
    inserts = silver_count_after - silver_count_before
    updates = staging_count - inserts
    
    print(f"✓ Silver records after MERGE: {silver_count_after}")
    print(f"✓ Inserts: {inserts}")
    print(f"✓ Updates: {updates}")
    
    # ====================================================================
    # FIXED: Calculate watermark from STAGING (what we just processed)
    # This becomes Silver's watermark for next incremental run
    # ====================================================================
    print("\nSTEP 5: Calculating new Silver watermark...")
    
    max_updated_at = spark.sql("""
        SELECT MAX(updated_at) as max_ts
        FROM silver.transactions_staging
    """).collect()[0]['max_ts']
    
    print(f"✓ New Silver watermark: {max_updated_at}")
    
    print("\nSTEP 6: Writing job control metadata...")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    spark.sql(f"""
        INSERT INTO silver.job_control VALUES (
            'silver_incremental_load',
            'silver',
            '{batch_id}',
            'incremental',
            'SUCCESS',
            CURRENT_DATE(),
            NULL, NULL,
            CAST('{max_updated_at}' AS TIMESTAMP),  -- Silver's watermark
            NULL,
            {staging_count},
            {staging_count},
            0, 0,
            CAST('{start_time}' AS TIMESTAMP),
            CAST('{end_time}' AS TIMESTAMP),
            {int(duration)},
            0, 3, NULL,
            'Airflow',
            'ephemeral-cluster',
            '{spark.sparkContext.applicationId}'
        )
    """)
    
    print("✓ Job control metadata written")
    
    result = {
        "records_merged": staging_count,
        "inserts": inserts,
        "updates": updates,
        "silver_count_before": silver_count_before,
        "silver_count_after": silver_count_after,
        "new_watermark": str(max_updated_at),
        "duration_seconds": round(duration, 2)
    }
    
    print(f"\n{'='*80}")
    print("✓ Silver Load SUCCEEDED")
    print(f"  Records merged: {staging_count}")
    print(f"  Inserts: {inserts}")
    print(f"  Updates: {updates}")
    print(f"  Silver total: {silver_count_after}")
    print(f"  New Silver watermark: {max_updated_at}")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"{'='*80}\n")
    
    print(f"RESULT_JSON:{json.dumps(result)}")

except Exception as e:
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    error_msg = str(e).replace("'", "''")
    
    print(f"\n{'='*80}")
    print(f"✗ Silver Load FAILED")
    print(f"  Error: {e}")
    print(f"{'='*80}\n")
    
    try:
        spark.sql(f"""
            INSERT INTO silver.job_control VALUES (
                'silver_incremental_load',
                'silver',
                '{batch_id}',
                'incremental',
                'FAILED',
                CURRENT_DATE(),
                NULL, NULL, NULL, NULL,
                0, 0, 0, 0,
                CAST('{start_time}' AS TIMESTAMP),
                CAST('{end_time}' AS TIMESTAMP),
                {int(duration)},
                0, 3,
                '{error_msg}',
                'Airflow',
                'ephemeral-cluster',
                '{spark.sparkContext.applicationId}'
            )
        """)
    except Exception as meta_error:
        print(f"⚠️  Could not write failure metadata: {meta_error}")
    
    result = {
        "error": str(e),
        "status": "FAILED"
    }
    
    print(f"RESULT_JSON:{json.dumps(result)}")
    raise

finally:
    spark.stop()