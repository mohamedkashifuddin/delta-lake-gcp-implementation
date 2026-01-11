#!/usr/bin/env python3

from pyspark.sql import SparkSession
from datetime import datetime
import uuid
import json

spark = SparkSession.builder \
    .appName("Fact_Full_Refresh") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

batch_id = str(uuid.uuid4())
job_name = "fact_full_refresh"
start_time = datetime.now()

print(f"\n{'='*80}")
print(f"Job: {job_name}")
print(f"Batch ID: {batch_id}")
print(f"Started: {start_time}")
print(f"{'='*80}\n")

try:
    # ========================================================================
    # STEP 1: Read ALL Silver transactions (no watermark filter)
    # ========================================================================
    print("STEP 1: Reading ALL Silver transactions (full refresh)...")
    
    silver_all_df = spark.sql("""
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
        WHERE (is_deleted = false OR is_deleted IS NULL)
          AND merchant_id NOT LIKE 'MERCH_9%'
    """)
    
    records_read = silver_all_df.count()
    print(f"✓ Silver records read: {records_read}")
    
    if records_read == 0:
        print("\n⚠️  No records in Silver. Cannot perform full refresh.")
        raise Exception("Silver table is empty")
    
    # ========================================================================
    # STEP 2: Create temp view from Silver
    # ========================================================================
    print("\nSTEP 2: Creating Silver temp view...")
    
    silver_all_df.createOrReplaceTempView("silver_all")
    print("✓ Temp view created: silver_all")
    
    # ========================================================================
    # STEP 3: JOIN to all 5 dimensions + calculate measures
    # ========================================================================
    print("\nSTEP 3: Joining to dimensions and calculating measures...")
    
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
            
            -- Degenerate dimensions
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
            
            -- Refund tracking
            false as is_refunded,
            CAST(NULL AS DOUBLE) as refund_amount,
            CAST(NULL AS DATE) as refund_date,
            1 as attempt_number
            
        FROM silver_all s
        
        LEFT JOIN gold.dim_customer dc
            ON s.customer_id = dc.customer_id
            AND dc.is_current = true
        
        LEFT JOIN gold.dim_merchant dm
            ON s.merchant_id = dm.merchant_id
            AND dm.is_current = true
        
        LEFT JOIN gold.dim_payment_method dpm
            ON s.payment_method = dpm.payment_method
        
        LEFT JOIN gold.dim_status ds
            ON s.transaction_status = ds.transaction_status
        
        LEFT JOIN gold.dim_date dd
            ON CAST(s.transaction_timestamp AS DATE) = dd.full_date
    """)
    
    print("✓ Dimension joins completed")
    
    # ========================================================================
    # STEP 4: Check for NULL dimension keys
    # ========================================================================
    print("\nSTEP 4: Checking for NULL dimension keys...")
    
    null_checks = spark.sql("""
        SELECT 
            SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) as null_customer_key,
            SUM(CASE WHEN merchant_key IS NULL THEN 1 ELSE 0 END) as null_merchant_key,
            SUM(CASE WHEN payment_method_key IS NULL THEN 1 ELSE 0 END) as null_payment_key,
            SUM(CASE WHEN status_key IS NULL THEN 1 ELSE 0 END) as null_status_key,
            SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) as null_date_key
        FROM fact_enriched
    """).collect()[0]
    
    total_nulls = sum([
        null_checks['null_customer_key'],
        null_checks['null_merchant_key'],
        null_checks['null_payment_key'],
        null_checks['null_status_key'],
        null_checks['null_date_key']
    ])
    
    print(f"  NULL customer_key: {null_checks['null_customer_key']}")
    print(f"  NULL merchant_key: {null_checks['null_merchant_key']}")
    print(f"  NULL payment_method_key: {null_checks['null_payment_key']}")
    print(f"  NULL status_key: {null_checks['null_status_key']}")
    print(f"  NULL date_key: {null_checks['null_date_key']}")
    
    if total_nulls > 0:
        print(f"\n⚠️  WARNING: {total_nulls} records have NULL dimension keys")
    else:
        print("\n✓ All dimension keys populated")
    
    # ========================================================================
    # STEP 5: INSERT OVERWRITE fact table (destructive!)
    # ========================================================================
    print("\nSTEP 5: INSERT OVERWRITE gold.fact_transactions (full refresh)...")
    print("⚠️  This will DELETE all existing fact records!")
    
    spark.sql("""
        INSERT OVERWRITE gold.fact_transactions
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
    
    fact_count_after = spark.sql("SELECT COUNT(*) as cnt FROM gold.fact_transactions").collect()[0]['cnt']
    print(f"✓ Fact table reloaded: {fact_count_after} records")
    
    # ========================================================================
    # STEP 6: Calculate new watermark
    # ========================================================================
    print("\nSTEP 6: Calculating new Gold watermark...")
    
    max_updated_at = spark.sql("""
        SELECT MAX(updated_at) as max_ts
        FROM gold.fact_transactions
    """).collect()[0]['max_ts']
    
    print(f"✓ New Gold watermark: {max_updated_at}")
    
    # ========================================================================
    # STEP 7: Write job_control metadata
    # ========================================================================
    print("\nSTEP 7: Writing job control metadata...")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    spark.sql(f"""
        INSERT INTO gold.job_control VALUES (
            'gold_fact_transactions', 'gold', '{batch_id}',
            'full_refresh', 'SUCCESS', CURRENT_DATE(),
            NULL, NULL,
            CAST('{max_updated_at}' AS TIMESTAMP),
            NULL,
            {records_read}, {fact_count_after}, 0, 0,
            CAST('{start_time}' AS TIMESTAMP), CAST('{end_time}' AS TIMESTAMP),
            {int(duration)},
            0, 3, NULL, 'Manual', 'ephemeral-cluster',
            '{spark.sparkContext.applicationId}'
        )
    """)
    
    print("✓ Job control metadata written")
    
    # ========================================================================
    # STEP 8: Calculate summary statistics
    # ========================================================================
    print("\nSTEP 8: Calculating summary statistics...")
    
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
    
    print(f"  Total records: {stats['total_records']}")
    print(f"  Total amount: ₹{stats['total_amount']:,.2f}")
    print(f"  Total revenue: ₹{stats['total_revenue']:,.2f}")
    print(f"  Unique customers: {stats['unique_customers']}")
    print(f"  Unique merchants: {stats['unique_merchants']}")
    print(f"  Unique dates: {stats['unique_dates']}")
    
    result = {
        "records_read": records_read,
        "records_written": fact_count_after,
        "null_dimension_keys": total_nulls,
        "total_amount": float(stats['total_amount']),
        "total_revenue": float(stats['total_revenue']),
        "unique_customers": stats['unique_customers'],
        "unique_merchants": stats['unique_merchants'],
        "new_watermark": str(max_updated_at),
        "duration_seconds": round(duration, 2),
        "run_mode": "full_refresh"
    }
    
    print(f"\n{'='*80}")
    print("✓ Fact Full Refresh SUCCEEDED")
    print(f"  Records read: {records_read}")
    print(f"  Records written: {fact_count_after}")
    print(f"  NULL dimension keys: {total_nulls}")
    print(f"  New Gold watermark: {max_updated_at}")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"{'='*80}\n")
    
    print(f"RESULT_JSON:{json.dumps(result)}")

except Exception as e:
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    error_msg = str(e).replace("'", "''")
    
    print(f"\n{'='*80}")
    print(f"✗ Fact Full Refresh FAILED")
    print(f"  Error: {e}")
    print(f"{'='*80}\n")
    
    try:
        spark.sql(f"""
            INSERT INTO gold.job_control VALUES (
                'gold_fact_transactions', 'gold', '{batch_id}',
                'full_refresh', 'FAILED', CURRENT_DATE(),
                NULL, NULL, NULL, NULL,
                0, 0, 0, 0,
                CAST('{start_time}' AS TIMESTAMP), CAST('{end_time}' AS TIMESTAMP),
                {int(duration)},
                0, 3, '{error_msg}', 'Manual', 'ephemeral-cluster',
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