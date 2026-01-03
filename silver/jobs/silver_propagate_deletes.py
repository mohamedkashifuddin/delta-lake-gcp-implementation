#!/usr/bin/env python3
"""
Propagate Bronze deletions to Silver
Run after Bronze compliance deletion to sync soft deletes
"""

from pyspark.sql import SparkSession
from datetime import datetime
import uuid
import json
import sys

spark = SparkSession.builder \
    .appName("Silver_Propagate_Deletes") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

# Parse command line arguments (match Bronze's pattern)
CUSTOMER_ID = None
for arg in sys.argv:
    if arg.startswith("--customer_id="):
        CUSTOMER_ID = arg.split("=")[1]

batch_id = str(uuid.uuid4())
start_time = datetime.now()

print(f"\n{'='*80}")
print(f"Job: Silver Delete Propagation")
print(f"Batch ID: {batch_id}")
if CUSTOMER_ID:
    print(f"Customer ID: {CUSTOMER_ID} (targeted deletion)")
else:
    print("Mode: Full propagation (all Bronze deletions)")
print(f"Started: {start_time}")
print(f"{'='*80}\n")

try:
    # STEP 1: Find deleted records in Bronze
    print("STEP 1: Finding deleted records in Bronze...")
    
    if CUSTOMER_ID:
        # Targeted: Only this customer
        deleted_query = f"""
            SELECT DISTINCT transaction_id, deleted_at
            FROM bronze.transactions
            WHERE customer_id = '{CUSTOMER_ID}'
              AND is_deleted = true
        """
    else:
        # Full: All deleted records not yet in Silver
        deleted_query = """
            SELECT DISTINCT b.transaction_id, b.deleted_at
            FROM bronze.transactions b
            WHERE b.is_deleted = true
              AND EXISTS (
                SELECT 1 FROM silver.transactions s 
                WHERE s.transaction_id = b.transaction_id
                  AND (s.is_deleted = false OR s.is_deleted IS NULL)
              )
        """
    
    deleted_bronze_df = spark.sql(deleted_query)
    deleted_count = deleted_bronze_df.count()
    
    print(f"✓ Found {deleted_count} deleted records in Bronze")
    
    if deleted_count == 0:
        print("\n⚠️  No deleted records to propagate.")
        
        result = {
            "deleted_in_bronze": 0,
            "deleted_in_silver": 0,
            "status": "SUCCESS_NO_DATA"
        }
        
        print(f"\nRESULT_JSON:{json.dumps(result)}")
        sys.exit(0)
    
    # STEP 2: Check Silver BEFORE deletion
    print("\nSTEP 2: Checking Silver records BEFORE deletion...")
    
    if CUSTOMER_ID:
        silver_before_query = f"""
            SELECT COUNT(*) as cnt
            FROM silver.transactions
            WHERE customer_id = '{CUSTOMER_ID}'
        """
    else:
        silver_before_query = """
            SELECT COUNT(*) as cnt
            FROM silver.transactions s
            WHERE EXISTS (
                SELECT 1 FROM bronze.transactions b
                WHERE b.transaction_id = s.transaction_id
                  AND b.is_deleted = true
            )
            AND (s.is_deleted = false OR s.is_deleted IS NULL)
        """
    
    silver_before_count = spark.sql(silver_before_query).collect()[0]['cnt']
    print(f"✓ Silver records before deletion: {silver_before_count}")
    
    if silver_before_count == 0:
        print("\n⚠️  No Silver records to delete (already deleted or not present).")
        
        result = {
            "deleted_in_bronze": deleted_count,
            "silver_before": 0,
            "silver_after": 0,
            "records_deleted": 0,
            "status": "SUCCESS_NO_DATA"
        }
        
        print(f"\nRESULT_JSON:{json.dumps(result)}")
        sys.exit(0)
    
    # STEP 3: Find matching records in Silver
    print("\nSTEP 3: Finding matching records in Silver...")
    
    deleted_bronze_df.createOrReplaceTempView("deleted_ids")
    
    silver_match_count = spark.sql("""
        SELECT COUNT(*) as cnt
        FROM silver.transactions s
        INNER JOIN deleted_ids d ON s.transaction_id = d.transaction_id
        WHERE s.is_deleted = false OR s.is_deleted IS NULL
    """).collect()[0]['cnt']
    
    print(f"✓ Found {silver_match_count} matching records in Silver to delete")
    
    if silver_match_count == 0:
        print("\n⚠️  No Silver records to update (already deleted or not present).")
        
        result = {
            "deleted_in_bronze": deleted_count,
            "silver_before": silver_before_count,
            "silver_after": silver_before_count,
            "records_deleted": 0,
            "status": "SUCCESS_NO_DATA"
        }
        
        print(f"\nRESULT_JSON:{json.dumps(result)}")
        sys.exit(0)
    
    # STEP 4: Delete from Silver using MERGE (better performance)
    print(f"\nSTEP 4: DELETING {silver_match_count} records from Silver...")
    
    spark.sql("""
        MERGE INTO silver.transactions AS target
        USING deleted_ids AS source
        ON target.transaction_id = source.transaction_id
        WHEN MATCHED THEN DELETE
    """)
    
    print(f"✓ Deleted {silver_match_count} Silver records")
    
    # STEP 5: Verify deletion (should be 0)
    print("\nSTEP 5: Verifying deletion...")
    
    if CUSTOMER_ID:
        verify_query = f"""
            SELECT COUNT(*) as cnt
            FROM silver.transactions
            WHERE customer_id = '{CUSTOMER_ID}'
        """
    else:
        verify_query = """
            SELECT COUNT(*) as cnt
            FROM silver.transactions s
            WHERE EXISTS (
                SELECT 1 FROM deleted_ids d
                WHERE d.transaction_id = s.transaction_id
            )
        """
    
    silver_after_count = spark.sql(verify_query).collect()[0]['cnt']
    
    print(f"✓ Silver records after deletion: {silver_after_count}")
    
    if silver_after_count > 0:
        print(f"⚠️  Warning: {silver_after_count} records still remain in Silver!")
    else:
        print("✓ All records successfully deleted from Silver")
    
    # STEP 6: Write job control
    print("\nSTEP 6: Writing job control metadata...")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    spark.sql(f"""
        INSERT INTO silver.job_control VALUES (
            'silver_propagate_deletes',
            'silver',
            '{batch_id}',
            'delete_propagation',
            'SUCCESS',
            CURRENT_DATE(),
            NULL, NULL, NULL, NULL,
            {deleted_count},
            {silver_match_count},
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
        "customer_id": CUSTOMER_ID if CUSTOMER_ID else "ALL",
        "deleted_in_bronze": deleted_count,
        "silver_before": silver_before_count,
        "silver_after": silver_after_count,
        "records_deleted": silver_match_count,
        "duration_seconds": round(duration, 2),
        "status": "SUCCESS" if silver_after_count == 0 else "PARTIAL"
    }
    
    print(f"\n{'='*80}")
    print("✓ Silver Delete Propagation COMPLETED")
    print(f"  Customer: {CUSTOMER_ID if CUSTOMER_ID else 'ALL'}")
    print(f"  Bronze deleted records: {deleted_count}")
    print(f"  Silver before: {silver_before_count}")
    print(f"  Silver after: {silver_after_count}")
    print(f"  Records deleted: {silver_match_count}")
    print(f"  Status: {'SUCCESS' if silver_after_count == 0 else 'PARTIAL - Some records remain'}")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"{'='*80}\n")
    
    print(f"RESULT_JSON:{json.dumps(result)}")

except Exception as e:
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    error_msg = str(e).replace("'", "''")
    
    print(f"\n{'='*80}")
    print(f"✗ Silver Delete Propagation FAILED")
    print(f"  Error: {e}")
    print(f"{'='*80}\n")
    
    try:
        spark.sql(f"""
            INSERT INTO silver.job_control VALUES (
                'silver_propagate_deletes',
                'silver',
                '{batch_id}',
                'delete_propagation',
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