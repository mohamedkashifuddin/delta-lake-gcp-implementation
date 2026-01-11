#!/usr/bin/env python3

from pyspark.sql import SparkSession
from datetime import datetime
import uuid
import json
import sys

spark = SparkSession.builder \
    .appName("Load_Fact_Transactions") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

batch_id = str(uuid.uuid4())
job_name = "load_fact_transactions"
start_time = datetime.now()

print(f"\n{'='*80}")
print(f"Job: {job_name}")
print(f"Batch ID: {batch_id}")
print(f"Started: {start_time}")
print(f"{'='*80}\n")

try:
    # ========================================================================
    # STEP 1: Check staging table
    # ========================================================================
    print("STEP 1: Checking gold.fact_transactions_staging...")
    
    staging_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.fact_transactions_staging").collect()[0]['cnt']
    
    print(f"✓ Records in staging: {staging_count}")
    
    if staging_count == 0:
        print("\n⚠️  No records in staging to load.")
        print("📋 Context: validate_fact_transactions found no new data from Silver.")
        
        # Write job_control with NULL watermark
        spark.sql(f"""
            INSERT INTO gold.job_control VALUES (
                'gold_fact_transactions', 'gold', '{batch_id}',
                'incremental', 'SUCCESS', CURRENT_DATE(),
                NULL, NULL, NULL, NULL,
                0, 0, 0, 0,
                CAST('{start_time}' AS TIMESTAMP), CURRENT_TIMESTAMP(),
                CAST((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(CAST('{start_time}' AS TIMESTAMP))) AS BIGINT),
                0, 3, NULL, 'Airflow', 'ephemeral-cluster',
                '{spark.sparkContext.applicationId}'
            )
        """)
        
        result = {
            "records_merged": 0,
            "inserts": 0,
            "updates": 0,
            "fact_total": spark.sql("SELECT COUNT(*) as cnt FROM gold.fact_transactions").collect()[0]['cnt'],
            "status": "SUCCESS_NO_DATA",
            "note": "No new data from Silver since last Gold run"
        }
        
        print(f"\nRESULT_JSON:{json.dumps(result)}")
        sys.exit(0)
    
    # ========================================================================
    # STEP 2: Get pre-merge metrics
    # ========================================================================
    print("\nSTEP 2: Getting pre-merge metrics...")
    
    fact_count_before = spark.sql("SELECT COUNT(*) as cnt FROM gold.fact_transactions").collect()[0]['cnt']
    print(f"✓ Fact records before MERGE: {fact_count_before}")
    
    # ========================================================================
    # STEP 3: MERGE staging -> fact_transactions
    # ========================================================================
    print("\nSTEP 3: Merging staging -> gold.fact_transactions...")
    
    spark.sql("""
        MERGE INTO gold.fact_transactions AS target
        USING gold.fact_transactions_staging AS source
        ON target.transaction_id = source.transaction_id
        
        WHEN MATCHED THEN
            UPDATE SET
                target.customer_key = source.customer_key,
                target.merchant_key = source.merchant_key,
                target.payment_method_key = source.payment_method_key,
                target.status_key = source.status_key,
                target.date_key = source.date_key,
                target.product_category = source.product_category,
                target.product_name = source.product_name,
                target.device_type = source.device_type,
                target.currency = source.currency,
                target.amount = source.amount,
                target.fee_amount = source.fee_amount,
                target.cashback_amount = source.cashback_amount,
                target.loyalty_points = source.loyalty_points,
                target.net_customer_amount = source.net_customer_amount,
                target.merchant_net_amount = source.merchant_net_amount,
                target.gateway_revenue = source.gateway_revenue,
                target.transaction_timestamp = source.transaction_timestamp,
                target.loaded_at = source.loaded_at,
                target.source_system = source.source_system,
                target.created_at = source.created_at,
                target.updated_at = source.updated_at,
                target.delta_change_type = 'UPDATE',
                target.delta_version = source.delta_version,
                target.is_deleted = source.is_deleted,
                target.deleted_at = source.deleted_at,
                target.is_refunded = source.is_refunded,
                target.refund_amount = source.refund_amount,
                target.refund_date = source.refund_date,
                target.attempt_number = source.attempt_number
        
        WHEN NOT MATCHED THEN
            INSERT (
                customer_key, merchant_key, payment_method_key, status_key, date_key,
                transaction_id, product_category, product_name, device_type, currency,
                amount, fee_amount, cashback_amount, loyalty_points,
                net_customer_amount, merchant_net_amount, gateway_revenue,
                transaction_timestamp, loaded_at,
                source_system, created_at, updated_at,
                delta_change_type, delta_version, is_deleted, deleted_at,
                is_refunded, refund_amount, refund_date, attempt_number
            )
            VALUES (
                source.customer_key, source.merchant_key, source.payment_method_key, 
                source.status_key, source.date_key,
                source.transaction_id, source.product_category, source.product_name, 
                source.device_type, source.currency,
                source.amount, source.fee_amount, source.cashback_amount, source.loyalty_points,
                source.net_customer_amount, source.merchant_net_amount, source.gateway_revenue,
                source.transaction_timestamp, source.loaded_at,
                source.source_system, source.created_at, source.updated_at,
                'INSERT', source.delta_version, source.is_deleted, source.deleted_at,
                source.is_refunded, source.refund_amount, source.refund_date, source.attempt_number
            )
    """)
    
    print("✓ MERGE completed")
    
    # ========================================================================
    # STEP 4: Get post-merge metrics
    # ========================================================================
    print("\nSTEP 4: Getting post-merge metrics...")
    
    fact_count_after = spark.sql("SELECT COUNT(*) as cnt FROM gold.fact_transactions").collect()[0]['cnt']
    
    inserts = fact_count_after - fact_count_before
    updates = staging_count - inserts
    
    print(f"✓ Fact records after MERGE: {fact_count_after}")
    print(f"✓ Inserts: {inserts}")
    print(f"✓ Updates: {updates}")
    
    # ========================================================================
    # STEP 5: Calculate new Gold watermark from staging
    # ========================================================================
    print("\nSTEP 5: Calculating new Gold watermark...")
    
    max_updated_at = spark.sql("""
        SELECT MAX(updated_at) as max_ts
        FROM gold.fact_transactions_staging
    """).collect()[0]['max_ts']
    
    print(f"✓ New Gold watermark: {max_updated_at}")
    
    # ========================================================================
    # STEP 6: Write job_control metadata
    # ========================================================================
    print("\nSTEP 6: Writing job control metadata...")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    spark.sql(f"""
        INSERT INTO gold.job_control VALUES (
            'gold_fact_transactions', 'gold', '{batch_id}',
            'incremental', 'SUCCESS', CURRENT_DATE(),
            NULL, NULL,
            CAST('{max_updated_at}' AS TIMESTAMP),
            NULL,
            {staging_count}, {staging_count}, 0, 0,
            CAST('{start_time}' AS TIMESTAMP), CAST('{end_time}' AS TIMESTAMP),
            {int(duration)},
            0, 3, NULL, 'Airflow', 'ephemeral-cluster',
            '{spark.sparkContext.applicationId}'
        )
    """)
    
    print("✓ Job control metadata written")
    
    # ========================================================================
    # STEP 7: Calculate final statistics
    # ========================================================================
    print("\nSTEP 7: Calculating final statistics...")
    
    stats = spark.sql("""
        SELECT 
            COUNT(*) as total_records,
            SUM(amount) as total_amount,
            SUM(gateway_revenue) as total_revenue,
            COUNT(DISTINCT customer_key) as unique_customers,
            COUNT(DISTINCT merchant_key) as unique_merchants,
            COUNT(DISTINCT date_key) as unique_dates
        FROM gold.fact_transactions
    """).collect()[0]
    
    print(f"  Total fact records: {stats['total_records']}")
    print(f"  Total amount: ₹{stats['total_amount']:,.2f}")
    print(f"  Total revenue: ₹{stats['total_revenue']:,.2f}")
    print(f"  Unique customers: {stats['unique_customers']}")
    print(f"  Unique merchants: {stats['unique_merchants']}")
    print(f"  Unique dates: {stats['unique_dates']}")
    
    result = {
        "records_merged": staging_count,
        "inserts": inserts,
        "updates": updates,
        "fact_count_before": fact_count_before,
        "fact_count_after": fact_count_after,
        "total_amount": float(stats['total_amount']),
        "total_revenue": float(stats['total_revenue']),
        "unique_customers": stats['unique_customers'],
        "unique_merchants": stats['unique_merchants'],
        "new_watermark": str(max_updated_at),
        "duration_seconds": round(duration, 2)
    }
    
    print(f"\n{'='*80}")
    print("✓ Fact Load SUCCEEDED")
    print(f"  Records merged: {staging_count}")
    print(f"  Inserts: {inserts}")
    print(f"  Updates: {updates}")
    print(f"  Fact total: {fact_count_after}")
    print(f"  New Gold watermark: {max_updated_at}")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"{'='*80}\n")
    
    print(f"RESULT_JSON:{json.dumps(result)}")

except Exception as e:
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    error_msg = str(e).replace("'", "''")
    
    print(f"\n{'='*80}")
    print(f"✗ Fact Load FAILED")
    print(f"  Error: {e}")
    print(f"{'='*80}\n")
    
    try:
        spark.sql(f"""
            INSERT INTO gold.job_control VALUES (
                'gold_fact_transactions', 'gold', '{batch_id}',
                'incremental', 'FAILED', CURRENT_DATE(),
                NULL, NULL, NULL, NULL,
                0, 0, 0, 0,
                CAST('{start_time}' AS TIMESTAMP), CAST('{end_time}' AS TIMESTAMP),
                {int(duration)},
                0, 3, '{error_msg}', 'Airflow', 'ephemeral-cluster',
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