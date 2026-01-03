#!/usr/bin/env python3
"""
GDPR Compliance: Mark customer's transactions as deleted in Bronze
Usage: Submit with --customer_id parameter
Example: gcloud dataproc jobs submit pyspark bronze_mark_deleted_by_customer.py \
         --cluster=dataproc-delta-cluster-final --region=us-central1 \
         -- --customer_id=USER_0123
"""

from pyspark.sql import SparkSession
from datetime import datetime
import uuid
import json
import sys

spark = SparkSession.builder \
    .appName("Bronze_Mark_Deleted_By_Customer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

# Parse command line arguments
if len(sys.argv) < 2:
    print("ERROR: Missing required parameter --customer_id")
    print("Usage: -- --customer_id=USER_XXXX")
    sys.exit(1)

customer_id = None
for arg in sys.argv:
    if arg.startswith("--customer_id="):
        customer_id = arg.split("=")[1]

if not customer_id:
    print("ERROR: --customer_id parameter not found")
    sys.exit(1)

batch_id = str(uuid.uuid4())
start_time = datetime.now()

print(f"\n{'='*80}")
print(f"Job: Bronze GDPR Compliance Deletion")
print(f"Batch ID: {batch_id}")
print(f"Customer ID: {customer_id}")
print(f"Started: {start_time}")
print(f"⚠️  COMPLIANCE ACTION: Marking customer's data as deleted")
print(f"{'='*80}\n")

try:
    # STEP 1: Check if customer exists
    print("STEP 1: Checking if customer exists in Bronze...")
    
    customer_check = spark.sql(f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN is_deleted = true THEN 1 END) as already_deleted,
            COUNT(CASE WHEN is_deleted = false OR is_deleted IS NULL THEN 1 END) as active_records
        FROM bronze.transactions
        WHERE customer_id = '{customer_id}'
    """).collect()[0]
    
    total_records = customer_check['total_records']
    already_deleted = customer_check['already_deleted']
    active_records = customer_check['active_records']
    
    print(f"✓ Customer found in Bronze:")
    print(f"  Total records: {total_records}")
    print(f"  Already deleted: {already_deleted}")
    print(f"  Active records: {active_records}")
    
    if total_records == 0:
        print(f"\n⚠️  Customer {customer_id} not found in Bronze.")
        print("   No action taken.")
        
        result = {
            "customer_id": customer_id,
            "total_records": 0,
            "already_deleted": 0,
            "newly_deleted": 0,
            "status": "NOT_FOUND"
        }
        
        print(f"\nRESULT_JSON:{json.dumps(result)}")
        sys.exit(0)
    
    if active_records == 0:
        print(f"\n⚠️  All records for customer {customer_id} already marked as deleted.")
        print("   No action taken.")
        
        result = {
            "customer_id": customer_id,
            "total_records": total_records,
            "already_deleted": already_deleted,
            "newly_deleted": 0,
            "status": "ALREADY_DELETED"
        }
        
        print(f"\nRESULT_JSON:{json.dumps(result)}")
        sys.exit(0)
    
    # STEP 2: Get sample transactions before deletion
    print("\nSTEP 2: Sample transactions to be deleted:")
    
    sample_txns = spark.sql(f"""
        SELECT 
            transaction_id,
            transaction_timestamp,
            merchant_name,
            amount,
            transaction_status
        FROM bronze.transactions
        WHERE customer_id = '{customer_id}'
          AND (is_deleted = false OR is_deleted IS NULL)
        ORDER BY transaction_timestamp DESC
        LIMIT 5
    """)
    
    sample_txns.show(truncate=False)
    
    # STEP 3: Mark records as deleted
    print(f"\nSTEP 3: Marking {active_records} records as deleted...")
    print("⚠️  This action cannot be easily undone!")
    
    deletion_timestamp = datetime.now()
    
    spark.sql(f"""
        UPDATE bronze.transactions
        SET 
            is_deleted = true,
            deleted_at = CAST('{deletion_timestamp}' AS TIMESTAMP),
            delta_change_type = 'DELETE'
        WHERE customer_id = '{customer_id}'
          AND (is_deleted = false OR is_deleted IS NULL)
    """)
    
    print(f"✓ Marked {active_records} records as deleted")
    
    # STEP 4: Verify deletion
    print("\nSTEP 4: Verifying deletion...")
    
    verify = spark.sql(f"""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN is_deleted = true THEN 1 END) as deleted_count,
            COUNT(CASE WHEN is_deleted = false OR is_deleted IS NULL THEN 1 END) as active_count
        FROM bronze.transactions
        WHERE customer_id = '{customer_id}'
    """).collect()[0]
    
    print(f"✓ Verification results:")
    print(f"  Total records: {verify['total']}")
    print(f"  Deleted: {verify['deleted_count']}")
    print(f"  Active: {verify['active_count']}")
    
    if verify['active_count'] != 0:
        raise Exception(f"Deletion incomplete! {verify['active_count']} records still active")
    
    # STEP 5: Write compliance log to job_control
    print("\nSTEP 5: Writing compliance audit log...")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    spark.sql(f"""
        INSERT INTO bronze.job_control VALUES (
            'bronze_compliance_deletion',
            'bronze',
            '{batch_id}',
            'compliance_deletion',
            'SUCCESS',
            CURRENT_DATE(),
            NULL, NULL, NULL, NULL,
            0,  -- records_read (not applicable)
            {active_records},  -- records_written (records marked deleted)
            0, 0,
            CAST('{start_time}' AS TIMESTAMP),
            CAST('{end_time}' AS TIMESTAMP),
            {int(duration)},
            0, 3,
            'GDPR deletion for customer {customer_id}',
            'Manual',
            'dataproc-cluster',
            '{spark.sparkContext.applicationId}'
        )
    """)
    
    print("✓ Compliance audit log written")
    
    result = {
        "customer_id": customer_id,
        "total_records": total_records,
        "already_deleted": already_deleted,
        "newly_deleted": active_records,
        "deletion_timestamp": str(deletion_timestamp),
        "duration_seconds": round(duration, 2),
        "status": "SUCCESS",
        "next_steps": "Run silver_incremental_load to propagate deletion"
    }
    
    print(f"\n{'='*80}")
    print("✓ GDPR Compliance Deletion SUCCEEDED")
    print(f"  Customer ID: {customer_id}")
    print(f"  Records marked deleted: {active_records}")
    print(f"  Deletion timestamp: {deletion_timestamp}")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"\n  NEXT STEP: Run silver_incremental_load DAG")
    print(f"  Silver will exclude these {active_records} records")
    print(f"{'='*80}\n")
    
    print(f"RESULT_JSON:{json.dumps(result)}")

except Exception as e:
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    error_msg = str(e).replace("'", "''")
    
    print(f"\n{'='*80}")
    print(f"✗ GDPR Compliance Deletion FAILED")
    print(f"  Customer ID: {customer_id}")
    print(f"  Error: {e}")
    print(f"{'='*80}\n")
    
    try:
        spark.sql(f"""
            INSERT INTO bronze.job_control VALUES (
                'bronze_compliance_deletion',
                'bronze',
                '{batch_id}',
                'compliance_deletion',
                'FAILED',
                CURRENT_DATE(),
                NULL, NULL, NULL, NULL,
                0, 0, 0, 0,
                CAST('{start_time}' AS TIMESTAMP),
                CAST('{end_time}' AS TIMESTAMP),
                {int(duration)},
                0, 3,
                'GDPR deletion FAILED for customer {customer_id}: {error_msg}',
                'Manual',
                'dataproc-cluster',
                '{spark.sparkContext.applicationId}'
            )
        """)
    except Exception as meta_error:
        print(f"⚠️  Could not write failure audit log: {meta_error}")
    
    result = {
        "customer_id": customer_id,
        "error": str(e),
        "status": "FAILED"
    }
    
    print(f"RESULT_JSON:{json.dumps(result)}")
    raise

finally:
    spark.stop()