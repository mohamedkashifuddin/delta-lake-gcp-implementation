"""
=================================================================
FILE 2: shared/write_watermark.py
=================================================================
Write job execution metadata to job_control Delta table
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from datetime import datetime
import sys

def write_watermark(spark, layer, job_metadata):
    """
    Write job execution metadata to job_control
    
    Args:
        spark: SparkSession
        layer: 'bronze', 'silver', or 'gold'
        job_metadata: dict with job execution details
    
    Required keys in job_metadata:
        - batch_id (str)
        - job_name (str)
        - run_mode (str): 'incremental', 'backfill', 'full_refresh'
        - status (str): 'SUCCESS', 'FAILED', 'RUNNING'
        - started_at (timestamp)
        - completed_at (timestamp)
        - records_read (int)
        - records_written (int)
        - records_failed (int)
        - records_quarantined (int)
        - last_processed_timestamp (timestamp)
        - error_message (str, optional)
    """
    
    table_name = f"{layer}_job_control"
    
    # Build metadata row
    metadata = {
        'batch_id': job_metadata['batch_id'],
        'job_name': job_metadata['job_name'],
        'run_mode': job_metadata['run_mode'],
        'status': job_metadata['status'],
        'started_at': job_metadata['started_at'],
        'completed_at': job_metadata['completed_at'],
        'records_read': job_metadata.get('records_read', 0),
        'records_written': job_metadata.get('records_written', 0),
        'records_failed': job_metadata.get('records_failed', 0),
        'records_quarantined': job_metadata.get('records_quarantined', 0),
        'retry_count': job_metadata.get('retry_count', 0),
        'max_retries': job_metadata.get('max_retries', 3),
        'error_message': job_metadata.get('error_message', None),
        'last_processed_timestamp': job_metadata.get('last_processed_timestamp'),
        'last_processed_batch_id': job_metadata.get('last_processed_batch_id')
    }
    
    # Create DataFrame
    df = spark.createDataFrame([metadata])
    
    # Write to Delta (append mode)
    df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(table_name)
    
    print(f"Watermark written to {table_name}: batch_id={metadata['batch_id']}, status={metadata['status']}")


if __name__ == "__main__":
    # For testing
    spark = SparkSession.builder \
        .appName("WriteWatermark") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Test metadata
    test_metadata = {
        'batch_id': 'test-batch-123',
        'job_name': 'test_job',
        'run_mode': 'incremental',
        'status': 'SUCCESS',
        'started_at': datetime.now(),
        'completed_at': datetime.now(),
        'records_read': 100,
        'records_written': 95,
        'records_failed': 5,
        'records_quarantined': 5,
        'last_processed_timestamp': datetime.now()
    }
    
    layer = sys.argv[1] if len(sys.argv) > 1 else "bronze"
    write_watermark(spark, layer, test_metadata)
    
    spark.stop()


