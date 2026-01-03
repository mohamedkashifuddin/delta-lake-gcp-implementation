#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from datetime import datetime
import json
import sys

spark = SparkSession.builder \
    .appName("Validate_Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

job_name = "validate_silver"
start_time = datetime.now()

print(f"\n{'='*80}")
print(f"Job: {job_name}")
print(f"Started: {start_time}")
print(f"{'='*80}\n")

try:
    # ============================================================================
    # FIXED: Read SILVER's watermark (not Bronze's)
    # ============================================================================
    print("STEP 1: Reading SILVER watermark...")
    
    silver_watermark_df = spark.sql("""
        SELECT last_processed_timestamp
        FROM silver.job_control
        WHERE job_name = 'silver_incremental_load'
          AND status = 'SUCCESS'
          AND last_processed_timestamp IS NOT NULL
        ORDER BY completed_at DESC
        LIMIT 1
    """)
    
    if silver_watermark_df.count() == 0:
        # First run after setup - use epoch or read from full_refresh
        full_refresh_watermark = spark.sql("""
            SELECT last_processed_timestamp
            FROM silver.job_control
            WHERE job_name = 'silver_full_refresh'
              AND status = 'SUCCESS'
            ORDER BY completed_at DESC
            LIMIT 1
        """)
        
        if full_refresh_watermark.count() > 0:
            silver_watermark = full_refresh_watermark.collect()[0][0]
            print(f"⚠️  No incremental watermark. Using full_refresh: {silver_watermark}")
        else:
            silver_watermark = "1970-01-01 00:00:00"
            print(f"⚠️  No Silver watermark found. Using epoch: {silver_watermark}")
    else:
        silver_watermark = silver_watermark_df.collect()[0][0]
        print(f"✓ Silver watermark: {silver_watermark}")
    
    # ============================================================================
    # Read Bronze records AFTER Silver's watermark
    # ============================================================================
    print("\nSTEP 2: Reading new Bronze records...")
    
    bronze_new_df = spark.sql(f"""
        SELECT 
            transaction_id, customer_id, transaction_timestamp,
            merchant_id, merchant_name, product_category, product_name,
            amount, fee_amount, cashback_amount, loyalty_points,
            payment_method, transaction_status, device_type,
            location_type, currency, updated_at,
            delta_change_type, delta_version, is_deleted, deleted_at
        FROM bronze.transactions
        WHERE updated_at > CAST('{silver_watermark}' AS TIMESTAMP)
          AND (is_deleted = false OR is_deleted IS NULL)
    """)
    
    records_read = bronze_new_df.count()
    print(f"✓ Records read from Bronze: {records_read}")
    
    if records_read == 0:
        print("\n⚠️  No new records to process.")
        print("This means Bronze hasn't loaded new data since Silver's last run.")
        
        result = {
            "records_read": 0,
            "records_filtered": 0,
            "records_deduped": 0,
            "duplicates_removed": 0,
            "records_to_staging": 0,
            "silver_watermark": str(silver_watermark),
            "note": "No new Bronze data since last Silver run"
        }
        
        print(f"\nRESULT_JSON:{json.dumps(result)}")
        
        # Write empty staging to prevent stale data
        empty_schema_df = spark.createDataFrame([], bronze_new_df.schema)
        empty_schema_df.createOrReplaceTempView("empty_staging")
        spark.sql("INSERT OVERWRITE silver.transactions_staging SELECT * FROM empty_staging")
        print("✓ Empty staging table written (prevents stale data)")
        
        sys.exit(0)
    
    print("\nSTEP 3: Counting filtered records...")
    
    total_bronze_new = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM bronze.transactions
        WHERE updated_at > CAST('{silver_watermark}' AS TIMESTAMP)
    """).collect()[0]['cnt']
    
    records_filtered = total_bronze_new - records_read
    print(f"✓ Total new Bronze records: {total_bronze_new}")
    print(f"✓ Records filtered out (bad data/deleted): {records_filtered}")
    
    print("\nSTEP 4: Deduplicating to latest record per transaction_id...")
    
    window_spec = Window.partitionBy("transaction_id").orderBy(
        col("updated_at").desc(),
        col("transaction_id").asc()
    )
    
    deduped_df = bronze_new_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    records_deduped = deduped_df.count()
    duplicates_removed = records_read - records_deduped
    
    print(f"✓ Records after deduplication: {records_deduped}")
    print(f"✓ Duplicates removed: {duplicates_removed}")
    
    print("\nSTEP 5: Loading to silver.transactions_staging...")
    
    deduped_df.createOrReplaceTempView("silver_staging_temp")
    
    spark.sql("""
        INSERT OVERWRITE silver.transactions_staging
        SELECT * FROM silver_staging_temp
    """)
    
    print(f"✓ Staging table loaded: {records_deduped} records")
    
    print("\nSTEP 6: Verifying staging table...")
    
    staging_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.transactions_staging").collect()[0]['cnt']
    
    if staging_count != records_deduped:
        raise Exception(f"Staging count mismatch! Expected {records_deduped}, got {staging_count}")
    
    print(f"✓ Staging verified: {staging_count} records")
    
    print("\nSTEP 7: Checking for late arrivals...")
    
    late_arrivals = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM silver.transactions_staging
        WHERE transaction_timestamp < CAST('{silver_watermark}' AS TIMESTAMP)
    """).collect()[0]['cnt']
    
    print(f"✓ Late arrivals detected: {late_arrivals}")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    result = {
        "records_read": records_read,
        "records_filtered": records_filtered,
        "records_deduped": records_deduped,
        "duplicates_removed": duplicates_removed,
        "records_to_staging": staging_count,
        "late_arrivals": late_arrivals,
        "silver_watermark": str(silver_watermark),
        "duration_seconds": round(duration, 2)
    }
    
    print(f"\n{'='*80}")
    print("✓ Silver Validation SUCCEEDED")
    print(f"  Records read: {records_read}")
    print(f"  Records filtered: {records_filtered}")
    print(f"  Duplicates removed: {duplicates_removed}")
    print(f"  Records to staging: {staging_count}")
    print(f"  Late arrivals: {late_arrivals}")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"{'='*80}\n")
    
    print(f"RESULT_JSON:{json.dumps(result)}")

except Exception as e:
    print(f"\n{'='*80}")
    print(f"✗ Silver Validation FAILED")
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