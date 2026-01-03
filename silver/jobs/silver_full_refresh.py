#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, max as spark_max
from pyspark.sql.window import Window
from datetime import datetime
import uuid
import json

spark = SparkSession.builder \
    .appName("Silver_Full_Refresh") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

batch_id = str(uuid.uuid4())
job_name = "silver_full_refresh"
start_time = datetime.now()

print(f"\n{'='*80}")
print(f"Job: {job_name}")
print(f"Batch ID: {batch_id}")
print(f"Started: {start_time}")
print(f"⚠️  FULL REFRESH - Will replace ALL Silver data")
print(f"{'='*80}\n")

try:
    print("STEP 1: Reading ALL Bronze records...")
    
    bronze_all_df = spark.sql("""
        SELECT 
            transaction_id, customer_id, transaction_timestamp,
            merchant_id, merchant_name, product_category, product_name,
            amount, fee_amount, cashback_amount, loyalty_points,
            payment_method, transaction_status, device_type,
            location_type, currency, updated_at,
            delta_change_type, delta_version, is_deleted, deleted_at
        FROM bronze.transactions
        WHERE (is_deleted = false OR is_deleted IS NULL)
    """)
    
    records_read = bronze_all_df.count()
    print(f"✓ Records read from Bronze: {records_read}")
    
    if records_read == 0:
        print("\n⚠️  No records in Bronze. Cannot perform full refresh.")
        print("   Load data to Bronze first.")
        
        result = {
            "error": "No Bronze data found",
            "status": "FAILED"
        }
        
        print(f"\nRESULT_JSON:{json.dumps(result)}")
        raise Exception("Bronze table is empty")
    
    print("\nSTEP 2: Deduplicating to latest record per transaction_id...")
    
    window_spec = Window.partitionBy("transaction_id").orderBy(
        col("updated_at").desc(),
        col("transaction_id").asc()
    )
    
    deduped_df = bronze_all_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    records_deduped = deduped_df.count()
    duplicates_removed = records_read - records_deduped
    
    print(f"✓ Records after deduplication: {records_deduped}")
    print(f"✓ Duplicates removed: {duplicates_removed}")
    
    print("\nSTEP 3: INSERT OVERWRITE to silver.transactions...")
    print("⚠️  Existing Silver data will be DELETED")
    
    deduped_df.createOrReplaceTempView("full_refresh_temp")
    
    spark.sql("""
        INSERT OVERWRITE silver.transactions
        SELECT * FROM full_refresh_temp
    """)
    
    print(f"✓ Silver table refreshed: {records_deduped} records")
    
    #  Verify and Get Watermark
    print("\nSTEP 5: Verifying and calculating new watermark...") 
       
    print("\nSTEP 4: Verifying Silver table...")
    silver_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.transactions").collect()[0]['cnt']
    
    if silver_count != records_deduped:
        raise Exception(f"Silver count mismatch! Expected {records_deduped}, got {silver_count}")
    
    print(f"✓ Silver verified: {silver_count} records")
    
    print("\nSTEP 5: Calculating new watermark...")
    
    # Calculate High Watermark (Max Updated At) from the fresh data
    max_updated_at = spark.sql("""
        SELECT MAX(updated_at) as max_ts
        FROM silver.transactions
    """).collect()[0]['max_ts']
    
    print(f"✓ New watermark: {max_updated_at}")
    
    print("\nSTEP 6: Writing job control metadata...")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    spark.sql(f"""
        INSERT INTO silver.job_control VALUES (
            'silver_full_refresh',
            'silver',
            '{batch_id}',
            'full_refresh',
            'SUCCESS',
            CURRENT_DATE(),
            NULL, NULL,
            CAST('{max_updated_at}' AS TIMESTAMP),
            NULL,
            {records_read},
            {records_deduped},
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
        "records_read": records_read,
        "records_written": records_deduped,
        "duplicates_removed": duplicates_removed,
        "silver_total": silver_count,
        "new_watermark": str(max_updated_at),
        "duration_seconds": round(duration, 2),
        "run_mode": "full_refresh"
    }
    
    print(f"\n{'='*80}")
    print("✓ Silver Full Refresh SUCCEEDED")
    print(f"  Records read: {records_read}")
    print(f"  Duplicates removed: {duplicates_removed}")
    print(f"  Records written: {records_deduped}")
    print(f"  New watermark: {max_updated_at}")  
    print(f"  Duration: {duration:.2f} seconds")
    print(f"{'='*80}\n")
    
    print(f"RESULT_JSON:{json.dumps(result)}")

except Exception as e:
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    error_msg = str(e).replace("'", "''")
    
    print(f"\n{'='*80}")
    print(f"✗ Silver Full Refresh FAILED")
    print(f"  Error: {e}")
    print(f"{'='*80}\n")
    
    try:
        spark.sql(f"""
            INSERT INTO silver.job_control VALUES (
                'silver_full_refresh',
                'silver',
                '{batch_id}',
                'full_refresh',
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