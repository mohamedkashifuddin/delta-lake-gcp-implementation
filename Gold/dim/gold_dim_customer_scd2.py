#!/usr/bin/env python3

from pyspark.sql import SparkSession
from datetime import datetime
import uuid
import json
import sys

spark = SparkSession.builder \
    .appName("Gold_Dim_Customer_SCD2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

batch_id = str(uuid.uuid4())
job_name = "gold_dim_customer_scd2"
start_time = datetime.now()

print(f"\n{'='*80}")
print(f"Job: {job_name}")
print(f"Batch ID: {batch_id}")
print(f"Started: {start_time}")
print(f"{'='*80}\n")

try:
    # ========================================================================
    # STEP 1: Aggregate Silver to calculate customer metrics
    # ========================================================================
    print("STEP 1: Aggregating Silver transactions by customer...")
    
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW customer_aggregates AS
        SELECT 
            customer_id,
            COUNT(*) as transaction_count,
            MIN(transaction_timestamp) as first_transaction_date,
            MAX(transaction_timestamp) as last_transaction_date,
            SUM(amount) as lifetime_value,
            MAX(CASE 
                WHEN transaction_status = 'Successful' THEN true 
                ELSE false 
            END) as is_active,
            CASE 
                WHEN COUNT(*) >= 100 THEN 'Platinum'
                WHEN COUNT(*) >= 50 THEN 'Gold'
                WHEN COUNT(*) >= 20 THEN 'Silver'
                ELSE 'Bronze'
            END as customer_tier
        FROM silver.transactions
        WHERE customer_id IS NOT NULL
          AND (is_deleted = false OR is_deleted IS NULL)
        GROUP BY customer_id
    """)
    
    customer_agg_count = spark.sql("SELECT COUNT(*) as cnt FROM customer_aggregates").collect()[0]['cnt']
    print(f"✓ Customer aggregates calculated: {customer_agg_count} customers")
    
    # ========================================================================
    # STEP 2: Get max customer_key for new surrogate key generation
    # ========================================================================
    print("\nSTEP 2: Getting max customer_key...")
    
    max_key_result = spark.sql("""
        SELECT COALESCE(MAX(customer_key), 0) as max_key
        FROM gold.dim_customer
    """).collect()[0]['max_key']
    
    print(f"✓ Max customer_key: {max_key_result}")
    
    # ========================================================================
    # STEP 3: Identify NEW customers (not in gold.dim_customer)
    # ========================================================================
    print("\nSTEP 3: Identifying new customers...")
    
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW new_customers AS
        SELECT 
            (ROW_NUMBER() OVER (ORDER BY customer_id) + {max_key_result}) as customer_key,
            customer_id,
            customer_tier,
            is_active,
            CAST(first_transaction_date AS DATE) as first_transaction_date,
            CAST(last_transaction_date AS DATE) as last_transaction_date,
            lifetime_value,
            CURRENT_TIMESTAMP() as loaded_at,
            'payment_gateway' as source_system,
            CURRENT_DATE() as effective_start_date,
            CAST('9999-12-31' AS DATE) as effective_end_date,
            true as is_current
        FROM customer_aggregates ca
        WHERE NOT EXISTS (
            SELECT 1 FROM gold.dim_customer dc
            WHERE dc.customer_id = ca.customer_id
        )
    """)
    
    new_count = spark.sql("SELECT COUNT(*) as cnt FROM new_customers").collect()[0]['cnt']
    print(f"✓ New customers identified: {new_count}")
    
    # ========================================================================
    # STEP 4: Identify CHANGED customers (tier changed)
    # ========================================================================
    print("\nSTEP 4: Identifying customers with tier changes...")
    
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW changed_customers AS
        SELECT 
            ca.customer_id,
            ca.customer_tier as new_tier,
            dc.customer_tier as old_tier,
            ca.is_active,
            CAST(ca.first_transaction_date AS DATE) as first_transaction_date,
            CAST(ca.last_transaction_date AS DATE) as last_transaction_date,
            ca.lifetime_value
        FROM customer_aggregates ca
        INNER JOIN gold.dim_customer dc
            ON ca.customer_id = dc.customer_id
        WHERE dc.is_current = true
          AND ca.customer_tier != dc.customer_tier
    """)
    
    changed_count = spark.sql("SELECT COUNT(*) as cnt FROM changed_customers").collect()[0]['cnt']
    print(f"✓ Changed customers identified: {changed_count}")
    
    if new_count == 0 and changed_count == 0:
        print("\n⚠️  No new or changed customers. Nothing to update.")
        
        # Write job_control with NULL watermark
        spark.sql(f"""
            INSERT INTO gold.job_control VALUES (
                'gold_dim_customer_scd2', 'gold', '{batch_id}',
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
            "new_customers": 0,
            "changed_customers": 0,
            "total_customers": customer_agg_count,
            "status": "SUCCESS_NO_CHANGES"
        }
        
        print(f"\nRESULT_JSON:{json.dumps(result)}")
        sys.exit(0)
    
    # ========================================================================
    # STEP 5: INSERT new customers
    # ========================================================================
    if new_count > 0:
        print(f"\nSTEP 5: Inserting {new_count} new customers...")
        
        spark.sql("""
            INSERT INTO gold.dim_customer
            SELECT 
                customer_key, customer_id, customer_tier, is_active,
                first_transaction_date, last_transaction_date, lifetime_value,
                loaded_at, source_system,
                effective_start_date, effective_end_date, is_current
            FROM new_customers
        """)
        
        print(f"✓ Inserted {new_count} new customers")
    else:
        print("\nSTEP 5: No new customers to insert")
    
    # ========================================================================
    # STEP 6: Handle CHANGED customers (SCD Type 2)
    # ========================================================================
    if changed_count > 0:
        print(f"\nSTEP 6: Handling {changed_count} changed customers (SCD Type 2)...")
        
        # Step 6a: Close old records (set is_current = false, effective_end_date = today)
        print("  6a: Closing old customer records...")
        
        spark.sql("""
            MERGE INTO gold.dim_customer AS target
            USING changed_customers AS source
            ON target.customer_id = source.customer_id
               AND target.is_current = true
            WHEN MATCHED THEN
                UPDATE SET
                    target.is_current = false,
                    target.effective_end_date = CURRENT_DATE()
        """)
        
        print(f"  ✓ Closed {changed_count} old records")
        
        # Step 6b: Get new surrogate keys for changed customers
        print("  6b: Getting new surrogate keys for changed customers...")
        
        new_max_key = spark.sql("""
            SELECT COALESCE(MAX(customer_key), 0) as max_key
            FROM gold.dim_customer
        """).collect()[0]['max_key']
        
        print(f"  ✓ New max key: {new_max_key}")
        
        # Step 6c: Create temp view with new versions
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW changed_customers_new_version AS
            SELECT 
                (ROW_NUMBER() OVER (ORDER BY customer_id) + {new_max_key}) as customer_key,
                customer_id,
                new_tier as customer_tier,
                is_active,
                first_transaction_date,
                last_transaction_date,
                lifetime_value,
                CURRENT_TIMESTAMP() as loaded_at,
                'payment_gateway' as source_system,
                CURRENT_DATE() as effective_start_date,
                CAST('9999-12-31' AS DATE) as effective_end_date,
                true as is_current
            FROM changed_customers
        """)
        
        # Step 6d: Insert new versions
        print("  6c: Inserting new versions of changed customers...")
        
        spark.sql("""
            INSERT INTO gold.dim_customer
            SELECT 
                customer_key, customer_id, customer_tier, is_active,
                first_transaction_date, last_transaction_date, lifetime_value,
                loaded_at, source_system,
                effective_start_date, effective_end_date, is_current
            FROM changed_customers_new_version
        """)
        
        print(f"  ✓ Inserted {changed_count} new versions")
    else:
        print("\nSTEP 6: No changed customers to update")
    
    # ========================================================================
    # STEP 7: Verify results
    # ========================================================================
    print("\nSTEP 7: Verifying results...")
    
    total_customers = spark.sql("SELECT COUNT(*) as cnt FROM gold.dim_customer").collect()[0]['cnt']
    current_customers = spark.sql("SELECT COUNT(*) as cnt FROM gold.dim_customer WHERE is_current = true").collect()[0]['cnt']
    
    print(f"✓ Total dim_customer records: {total_customers}")
    print(f"✓ Current customers (is_current=true): {current_customers}")
    
    # ========================================================================
    # STEP 8: Write job_control metadata
    # ========================================================================
    print("\nSTEP 8: Writing job control metadata...")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    records_written = new_count + (changed_count * 2)  # changed = close old + insert new
    
    spark.sql(f"""
        INSERT INTO gold.job_control VALUES (
            'gold_dim_customer_scd2', 'gold', '{batch_id}',
            'incremental', 'SUCCESS', CURRENT_DATE(),
            NULL, NULL, NULL, NULL,
            {customer_agg_count}, {records_written}, 0, 0,
            CAST('{start_time}' AS TIMESTAMP), CAST('{end_time}' AS TIMESTAMP),
            {int(duration)},
            0, 3, NULL, 'Airflow', 'ephemeral-cluster',
            '{spark.sparkContext.applicationId}'
        )
    """)
    
    print("✓ Job control metadata written")
    
    result = {
        "customers_processed": customer_agg_count,
        "new_customers": new_count,
        "changed_customers": changed_count,
        "records_written": records_written,
        "total_dim_customer_records": total_customers,
        "current_customers": current_customers,
        "duration_seconds": round(duration, 2)
    }
    
    print(f"\n{'='*80}")
    print("✓ Gold Dim Customer SCD2 SUCCEEDED")
    print(f"  Customers processed: {customer_agg_count}")
    print(f"  New customers: {new_count}")
    print(f"  Changed customers: {changed_count}")
    print(f"  Records written: {records_written}")
    print(f"  Total dim_customer records: {total_customers}")
    print(f"  Current customers: {current_customers}")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"{'='*80}\n")
    
    print(f"RESULT_JSON:{json.dumps(result)}")

except Exception as e:
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    error_msg = str(e).replace("'", "''")
    
    print(f"\n{'='*80}")
    print(f"✗ Gold Dim Customer SCD2 FAILED")
    print(f"  Error: {e}")
    print(f"{'='*80}\n")
    
    try:
        spark.sql(f"""
            INSERT INTO gold.job_control VALUES (
                'gold_dim_customer_scd2', 'gold', '{batch_id}',
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