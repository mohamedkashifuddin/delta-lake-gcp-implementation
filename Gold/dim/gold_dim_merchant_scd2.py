#!/usr/bin/env python3

from pyspark.sql import SparkSession
from datetime import datetime
import uuid
import json
import sys

spark = SparkSession.builder \
    .appName("Gold_Dim_Merchant_SCD2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

batch_id = str(uuid.uuid4())
job_name = "gold_dim_merchant_scd2"
start_time = datetime.now()

print(f"\n{'='*80}")
print(f"Job: {job_name}")
print(f"Batch ID: {batch_id}")
print(f"Started: {start_time}")
print(f"{'='*80}\n")

try:
    # ========================================================================
    # STEP 1: Get distinct merchants from Silver (with transaction counts)
    # Filter out test merchants (MERCH_9xxx) - these are Bronze Tier 2 test data
    # ========================================================================
    print("STEP 1: Extracting distinct merchants from Silver...")
    
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW merchant_aggregates AS
        SELECT 
            merchant_id,
            merchant_name,
            location_type,
            product_category,
            COUNT(DISTINCT transaction_id) as transaction_count,
            MAX(transaction_timestamp) as last_transaction_date
        FROM silver.transactions
        WHERE merchant_id IS NOT NULL
          AND (is_deleted = false OR is_deleted IS NULL)
          AND merchant_id NOT LIKE 'MERCH_9%'
        GROUP BY merchant_id, merchant_name, location_type, product_category
    """)
    
    raw_agg_count = spark.sql("SELECT COUNT(*) as cnt FROM merchant_aggregates").collect()[0]['cnt']
    print(f"✓ Raw aggregates: {raw_agg_count} rows (before deduplication)")
    print(f"  (Filtered out test merchants: MERCH_9xxx)")
    
    # ========================================================================
    # STEP 2: Deduplicate to ONE row per merchant_id
    # Pick: Latest name + Most common category
    # ========================================================================
    print("\nSTEP 2: Deduplicating to one row per merchant_id...")
    
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW merchant_current AS
        SELECT 
            merchant_id,
            merchant_name,
            category,
            location_type
        FROM (
            SELECT 
                merchant_id,
                merchant_name,
                location_type,
                product_category as category,
                transaction_count,
                last_transaction_date,
                ROW_NUMBER() OVER (
                    PARTITION BY merchant_id 
                    ORDER BY last_transaction_date DESC,  -- Latest name
                             transaction_count DESC        -- Most common category
                ) as rn
            FROM merchant_aggregates
        )
        WHERE rn = 1
    """)
    
    merchant_count = spark.sql("SELECT COUNT(*) as cnt FROM merchant_current").collect()[0]['cnt']
    print(f"✓ Unique merchants after deduplication: {merchant_count}")
    print(f"  (Removed {raw_agg_count - merchant_count} duplicate combinations)")
    
    # ========================================================================
    # STEP 3: Get max merchant_key for surrogate key generation
    # ========================================================================
    print("\nSTEP 3: Getting max merchant_key...")
    
    max_key_result = spark.sql("""
        SELECT COALESCE(MAX(merchant_key), 0) as max_key
        FROM gold.dim_merchant
    """).collect()[0]['max_key']
    
    print(f"✓ Max merchant_key: {max_key_result}")
    
    # ========================================================================
    # STEP 4: Identify NEW merchants (not in gold.dim_merchant)
    # ========================================================================
    print("\nSTEP 4: Identifying new merchants...")
    
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW new_merchants AS
        SELECT 
            (ROW_NUMBER() OVER (ORDER BY merchant_id) + {max_key_result}) as merchant_key,
            merchant_id,
            merchant_name,
            category,
            location_type,
            CURRENT_TIMESTAMP() as loaded_at,
            'payment_gateway' as source_system,
            CURRENT_DATE() as effective_start_date,
            CAST('9999-12-31' AS DATE) as effective_end_date,
            true as is_current
        FROM merchant_current mc
        WHERE NOT EXISTS (
            SELECT 1 FROM gold.dim_merchant dm
            WHERE dm.merchant_id = mc.merchant_id
        )
    """)
    
    new_count = spark.sql("SELECT COUNT(*) as cnt FROM new_merchants").collect()[0]['cnt']
    print(f"✓ New merchants identified: {new_count}")
    
    # ========================================================================
    # STEP 5: Identify CHANGED merchants (merchant_name changed)
    # ========================================================================
    print("\nSTEP 5: Identifying merchants with name changes...")
    
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW changed_merchants AS
        SELECT 
            mc.merchant_id,
            mc.merchant_name as new_name,
            dm.merchant_name as old_name,
            mc.category,
            mc.location_type
        FROM merchant_current mc
        INNER JOIN gold.dim_merchant dm
            ON mc.merchant_id = dm.merchant_id
        WHERE dm.is_current = true
          AND mc.merchant_name != dm.merchant_name
    """)
    
    changed_count = spark.sql("SELECT COUNT(*) as cnt FROM changed_merchants").collect()[0]['cnt']
    print(f"✓ Changed merchants identified: {changed_count}")
    
    if new_count == 0 and changed_count == 0:
        print("\n⚠️  No new or changed merchants. Nothing to update.")
        
        # Write job_control with NULL watermark
        spark.sql(f"""
            INSERT INTO gold.job_control VALUES (
                'gold_dim_merchant_scd2', 'gold', '{batch_id}',
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
            "new_merchants": 0,
            "changed_merchants": 0,
            "total_merchants": merchant_count,
            "status": "SUCCESS_NO_CHANGES"
        }
        
        print(f"\nRESULT_JSON:{json.dumps(result)}")
        sys.exit(0)
    
    # ========================================================================
    # STEP 6: INSERT new merchants
    # ========================================================================
    if new_count > 0:
        print(f"\nSTEP 6: Inserting {new_count} new merchants...")
        
        spark.sql("""
            INSERT INTO gold.dim_merchant
            SELECT 
                merchant_key, merchant_id, merchant_name, category, location_type,
                loaded_at, source_system,
                effective_start_date, effective_end_date, is_current
            FROM new_merchants
        """)
        
        print(f"✓ Inserted {new_count} new merchants")
    else:
        print("\nSTEP 6: No new merchants to insert")
    
    # ========================================================================
    # STEP 7: Handle CHANGED merchants (SCD Type 2)
    # ========================================================================
    if changed_count > 0:
        print(f"\nSTEP 7: Handling {changed_count} changed merchants (SCD Type 2)...")
        
        # Step 7a: Close old records (only existing versions, not today's inserts)
        print("  7a: Closing old merchant records...")
        
        spark.sql("""
            MERGE INTO gold.dim_merchant AS target
            USING changed_merchants AS source
            ON target.merchant_id = source.merchant_id
               AND target.is_current = true
               AND target.effective_start_date < CURRENT_DATE()
            WHEN MATCHED THEN
                UPDATE SET
                    target.is_current = false,
                    target.effective_end_date = CURRENT_DATE()
        """)
        
        print(f"  ✓ Closed {changed_count} old records")
        
        # Step 7b: Get new surrogate keys for changed merchants
        print("  7b: Getting new surrogate keys for changed merchants...")
        
        new_max_key = spark.sql("""
            SELECT COALESCE(MAX(merchant_key), 0) as max_key
            FROM gold.dim_merchant
        """).collect()[0]['max_key']
        
        print(f"  ✓ New max key: {new_max_key}")
        
        # Step 7c: Create temp view with new versions
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW changed_merchants_new_version AS
            SELECT 
                (ROW_NUMBER() OVER (ORDER BY merchant_id) + {new_max_key}) as merchant_key,
                merchant_id,
                new_name as merchant_name,
                category,
                location_type,
                CURRENT_TIMESTAMP() as loaded_at,
                'payment_gateway' as source_system,
                CURRENT_DATE() as effective_start_date,
                CAST('9999-12-31' AS DATE) as effective_end_date,
                true as is_current
            FROM changed_merchants
        """)
        
        # Step 7d: Insert new versions
        print("  7c: Inserting new versions of changed merchants...")
        
        spark.sql("""
            INSERT INTO gold.dim_merchant
            SELECT 
                merchant_key, merchant_id, merchant_name, category, location_type,
                loaded_at, source_system,
                effective_start_date, effective_end_date, is_current
            FROM changed_merchants_new_version
        """)
        
        print(f"  ✓ Inserted {changed_count} new versions")
    else:
        print("\nSTEP 7: No changed merchants to update")
    
    # ========================================================================
    # STEP 8: Verify results
    # ========================================================================
    print("\nSTEP 8: Verifying results...")
    
    total_merchants = spark.sql("SELECT COUNT(*) as cnt FROM gold.dim_merchant").collect()[0]['cnt']
    current_merchants = spark.sql("SELECT COUNT(*) as cnt FROM gold.dim_merchant WHERE is_current = true").collect()[0]['cnt']
    
    print(f"✓ Total dim_merchant records: {total_merchants}")
    print(f"✓ Current merchants (is_current=true): {current_merchants}")
    
    # ========================================================================
    # STEP 9: Write job_control metadata
    # ========================================================================
    print("\nSTEP 9: Writing job control metadata...")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    records_written = new_count + (changed_count * 2)
    
    spark.sql(f"""
        INSERT INTO gold.job_control VALUES (
            'gold_dim_merchant_scd2', 'gold', '{batch_id}',
            'incremental', 'SUCCESS', CURRENT_DATE(),
            NULL, NULL, NULL, NULL,
            {merchant_count}, {records_written}, 0, 0,
            CAST('{start_time}' AS TIMESTAMP), CAST('{end_time}' AS TIMESTAMP),
            {int(duration)},
            0, 3, NULL, 'Airflow', 'ephemeral-cluster',
            '{spark.sparkContext.applicationId}'
        )
    """)
    
    print("✓ Job control metadata written")
    
    result = {
        "merchants_processed": merchant_count,
        "new_merchants": new_count,
        "changed_merchants": changed_count,
        "records_written": records_written,
        "total_dim_merchant_records": total_merchants,
        "current_merchants": current_merchants,
        "duration_seconds": round(duration, 2)
    }
    
    print(f"\n{'='*80}")
    print("✓ Gold Dim Merchant SCD2 SUCCEEDED")
    print(f"  Merchants processed: {merchant_count}")
    print(f"  New merchants: {new_count}")
    print(f"  Changed merchants: {changed_count}")
    print(f"  Records written: {records_written}")
    print(f"  Total dim_merchant records: {total_merchants}")
    print(f"  Current merchants: {current_merchants}")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"{'='*80}\n")
    
    print(f"RESULT_JSON:{json.dumps(result)}")

except Exception as e:
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    error_msg = str(e).replace("'", "''")
    
    print(f"\n{'='*80}")
    print(f"✗ Gold Dim Merchant SCD2 FAILED")
    print(f"  Error: {e}")
    print(f"{'='*80}\n")
    
    try:
        spark.sql(f"""
            INSERT INTO gold.job_control VALUES (
                'gold_dim_merchant_scd2', 'gold', '{batch_id}',
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