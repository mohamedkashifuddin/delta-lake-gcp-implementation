#!/usr/bin/env python3

from pyspark.sql import SparkSession
from datetime import datetime
import uuid
import json
import sys

spark = SparkSession.builder \
    .appName("Validate_Fact_Transactions") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

batch_id = str(uuid.uuid4())
job_name = "validate_fact_transactions"
start_time = datetime.now()

print(f"\n{'='*80}")
print(f"Job: {job_name}")
print(f"Batch ID: {batch_id}")
print(f"Started: {start_time}")
print(f"{'='*80}\n")

try:
    # ========================================================================
    # STEP 1: Read Gold watermark
    # ========================================================================
    print("STEP 1: Reading Gold fact watermark...")
    
    gold_watermark_df = spark.sql("""
        SELECT last_processed_timestamp
        FROM gold.job_control
        WHERE job_name = 'gold_fact_transactions'
          AND status = 'SUCCESS'
          AND last_processed_timestamp IS NOT NULL
        ORDER BY completed_at DESC
        LIMIT 1
    """)
    
    if gold_watermark_df.count() == 0:
        gold_watermark = "1970-01-01 00:00:00"
        print(f"⚠️  No fact watermark found. Using epoch: {gold_watermark}")
    else:
        gold_watermark = gold_watermark_df.collect()[0][0]
        print(f"✓ Gold fact watermark: {gold_watermark}")
    
    # ========================================================================
    # STEP 2: Read new Silver transactions (exclude test merchants)
    # ========================================================================
    print("\nSTEP 2: Reading new Silver transactions...")
    
    silver_new_df = spark.sql(f"""
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
            delta_change_type,
            delta_version,
            is_deleted,
            deleted_at
        FROM silver.transactions
        WHERE updated_at > CAST('{gold_watermark}' AS TIMESTAMP)
          AND (is_deleted = false OR is_deleted IS NULL)
          AND merchant_id NOT LIKE 'MERCH_9%'
    """)
    
    records_read = silver_new_df.count()
    print(f"✓ Silver records read: {records_read}")
    print(f"  (Filtered out test merchants: MERCH_9xxx)")
    
    if records_read == 0:
        print("\n⚠️  No new records to process.")
        
        result = {
            "records_read": 0,
            "records_to_staging": 0,
            "gold_watermark": str(gold_watermark),
            "note": "No new Silver data since last Gold run"
        }
        
        print(f"\nRESULT_JSON:{json.dumps(result)}")
        
        # Write empty staging with correct fact schema (not silver schema!)
        spark.sql("""
            INSERT OVERWRITE gold.fact_transactions_staging
            SELECT 
                CAST(NULL AS BIGINT) as customer_key,
                CAST(NULL AS BIGINT) as merchant_key,
                CAST(NULL AS BIGINT) as payment_method_key,
                CAST(NULL AS BIGINT) as status_key,
                CAST(NULL AS BIGINT) as date_key,
                CAST(NULL AS STRING) as transaction_id,
                CAST(NULL AS STRING) as product_category,
                CAST(NULL AS STRING) as product_name,
                CAST(NULL AS STRING) as device_type,
                CAST(NULL AS DOUBLE) as amount,
                CAST(NULL AS DOUBLE) as fee_amount,
                CAST(NULL AS DOUBLE) as cashback_amount,
                CAST(NULL AS BIGINT) as loyalty_points,
                CAST(NULL AS DOUBLE) as net_customer_amount,
                CAST(NULL AS DOUBLE) as merchant_net_amount,
                CAST(NULL AS DOUBLE) as gateway_revenue,
                CAST(NULL AS TIMESTAMP) as transaction_timestamp,
                CAST(NULL AS STRING) as currency,
                CAST(NULL AS BOOLEAN) as is_refunded,
                CAST(NULL AS DOUBLE) as refund_amount,
                CAST(NULL AS DATE) as refund_date,
                CAST(NULL AS BIGINT) as attempt_number,
                CAST(NULL AS TIMESTAMP) as loaded_at,
                CAST(NULL AS STRING) as source_system,
                CAST(NULL AS TIMESTAMP) as created_at,
                CAST(NULL AS TIMESTAMP) as updated_at,
                CAST(NULL AS STRING) as delta_change_type,
                CAST(NULL AS BIGINT) as delta_version,
                CAST(NULL AS BOOLEAN) as is_deleted,
                CAST(NULL AS TIMESTAMP) as deleted_at
            WHERE 1=0
        """)
        print("✓ Empty staging table written (correct fact schema)")
        
        sys.exit(0)
    
    # ========================================================================
    # STEP 3: Create temp view from Silver
    # ========================================================================
    print("\nSTEP 3: Creating Silver temp view...")
    
    silver_new_df.createOrReplaceTempView("silver_new")
    print("✓ Temp view created: silver_new")
    
    # ========================================================================
    # STEP 4: JOIN to all 5 dimensions + calculate measures
    # ========================================================================
    print("\nSTEP 4: Joining to dimensions and calculating measures...")
    
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW fact_enriched AS
        SELECT 
            -- Dimension keys
            dc.customer_key,
            dm.merchant_key,
            dpm.payment_method_key,
            ds.status_key,
            dd.date_key,
            
            -- Business key
            s.transaction_id,
            
            -- Degenerate dimensions (stay in fact)
            s.product_category,
            s.product_name,
            s.device_type,
            s.currency,
            
            -- Measures
            s.amount,
            s.fee_amount,
            s.cashback_amount,
            s.loyalty_points,
            
            -- Calculated measures
            (s.amount - s.fee_amount + s.cashback_amount) as net_customer_amount,
            (s.amount - s.cashback_amount) as merchant_net_amount,
            s.fee_amount as gateway_revenue,
            
            -- Timestamps
            s.transaction_timestamp,
            CURRENT_TIMESTAMP() as loaded_at,
            
            -- Metadata
            'payment_gateway' as source_system,
            s.transaction_timestamp as created_at,
            s.updated_at,
            s.delta_change_type,
            s.delta_version,
            s.is_deleted,
            s.deleted_at,
            
            -- Refund tracking (placeholder - no refund data in source)
            false as is_refunded,
            CAST(NULL AS DOUBLE) as refund_amount,
            CAST(NULL AS DATE) as refund_date,
            1 as attempt_number
            
        FROM silver_new s
        
        -- JOIN dim_customer (current version only)
        LEFT JOIN gold.dim_customer dc
            ON s.customer_id = dc.customer_id
            AND dc.is_current = true
        
        -- JOIN dim_merchant (current version only)
        LEFT JOIN gold.dim_merchant dm
            ON s.merchant_id = dm.merchant_id
            AND dm.is_current = true
        
        -- JOIN dim_payment_method
        LEFT JOIN gold.dim_payment_method dpm
            ON s.payment_method = dpm.payment_method
        
        -- JOIN dim_status
        LEFT JOIN gold.dim_status ds
            ON s.transaction_status = ds.transaction_status
        
        -- JOIN dim_date (extract date from transaction_timestamp)
        LEFT JOIN gold.dim_date dd
            ON CAST(s.transaction_timestamp AS DATE) = dd.full_date
    """)
    
    print("✓ Dimension joins completed")
    
    # ========================================================================
    # STEP 5: Check for NULL dimension keys (data quality)
    # ========================================================================
    print("\nSTEP 5: Checking for NULL dimension keys...")
    
    null_checks = spark.sql("""
        SELECT 
            SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) as null_customer_key,
            SUM(CASE WHEN merchant_key IS NULL THEN 1 ELSE 0 END) as null_merchant_key,
            SUM(CASE WHEN payment_method_key IS NULL THEN 1 ELSE 0 END) as null_payment_key,
            SUM(CASE WHEN status_key IS NULL THEN 1 ELSE 0 END) as null_status_key,
            SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) as null_date_key
        FROM fact_enriched
    """).collect()[0]
    
    print(f"  NULL customer_key: {null_checks['null_customer_key']}")
    print(f"  NULL merchant_key: {null_checks['null_merchant_key']}")
    print(f"  NULL payment_method_key: {null_checks['null_payment_key']}")
    print(f"  NULL status_key: {null_checks['null_status_key']}")
    print(f"  NULL date_key: {null_checks['null_date_key']}")
    
    total_nulls = sum([
        null_checks['null_customer_key'],
        null_checks['null_merchant_key'],
        null_checks['null_payment_key'],
        null_checks['null_status_key'],
        null_checks['null_date_key']
    ])
    
    if total_nulls > 0:
        print(f"\n⚠️  WARNING: {total_nulls} records have NULL dimension keys")
        print("   This indicates missing dimension data or JOIN mismatches")
    else:
        print("\n✓ All dimension keys populated (no NULLs)")
    
    # ========================================================================
    # STEP 6: Write to staging table (explicit column order)
    # ========================================================================
    print("\nSTEP 6: Writing to gold.fact_transactions_staging...")
    
    spark.sql("""
        INSERT OVERWRITE gold.fact_transactions_staging
        SELECT 
            customer_key,
            merchant_key,
            payment_method_key,
            status_key,
            date_key,
            transaction_id,
            product_category,
            product_name,
            device_type,
            amount,
            fee_amount,
            cashback_amount,
            loyalty_points,
            net_customer_amount,
            merchant_net_amount,
            gateway_revenue,
            transaction_timestamp,
            currency,
            is_refunded,
            refund_amount,
            refund_date,
            attempt_number,
            loaded_at,
            source_system,
            created_at,
            updated_at,
            delta_change_type,
            delta_version,
            is_deleted,
            deleted_at
        FROM fact_enriched
    """)
    
    staging_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.fact_transactions_staging").collect()[0]['cnt']
    print(f"✓ Staging records written: {staging_count}")
    
    # ========================================================================
    # STEP 7: Verify staging matches enriched
    # ========================================================================
    print("\nSTEP 7: Verifying staging table...")
    
    enriched_count = spark.sql("SELECT COUNT(*) as cnt FROM fact_enriched").collect()[0]['cnt']
    
    if staging_count != enriched_count:
        raise Exception(f"Staging count mismatch! Enriched: {enriched_count}, Staging: {staging_count}")
    
    print(f"✓ Staging verified: {staging_count} records")
    
    # ========================================================================
    # STEP 8: Calculate summary statistics
    # ========================================================================
    print("\nSTEP 8: Calculating summary statistics...")
    
    stats = spark.sql("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT transaction_id) as unique_transactions,
            SUM(amount) as total_amount,
            SUM(gateway_revenue) as total_revenue,
            COUNT(DISTINCT customer_key) as unique_customers,
            COUNT(DISTINCT merchant_key) as unique_merchants
        FROM gold.fact_transactions_staging
    """).collect()[0]
    
    print(f"  Total records: {stats['total_records']}")
    print(f"  Unique transactions: {stats['unique_transactions']}")
    print(f"  Total amount: ₹{stats['total_amount']:,.2f}")
    print(f"  Total revenue: ₹{stats['total_revenue']:,.2f}")
    print(f"  Unique customers: {stats['unique_customers']}")
    print(f"  Unique merchants: {stats['unique_merchants']}")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    result = {
        "records_read": records_read,
        "records_to_staging": staging_count,
        "null_dimension_keys": total_nulls,
        "unique_transactions": stats['unique_transactions'],
        "total_amount": float(stats['total_amount']),
        "total_revenue": float(stats['total_revenue']),
        "unique_customers": stats['unique_customers'],
        "unique_merchants": stats['unique_merchants'],
        "gold_watermark": str(gold_watermark),
        "duration_seconds": round(duration, 2)
    }
    
    print(f"\n{'='*80}")
    print("✓ Fact Validation SUCCEEDED")
    print(f"  Records read: {records_read}")
    print(f"  Records to staging: {staging_count}")
    print(f"  NULL dimension keys: {total_nulls}")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"{'='*80}\n")
    
    print(f"RESULT_JSON:{json.dumps(result)}")

except Exception as e:
    print(f"\n{'='*80}")
    print(f"✗ Fact Validation FAILED")
    print(f"  Error: {e}")
    print(f"{'='*80}\n")
    
    result = {
        "error": str(e),
        "status": "FAILED"
    }
    
    print(f"RESULT_JSON:{json.dumps(result)}")
    raise

finally:
    spark.stop()